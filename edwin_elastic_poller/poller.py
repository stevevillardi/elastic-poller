"""Poll cycle orchestration."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Tuple

from edwin_elastic_poller import bookmark, config, dedupe, delivery, elasticsearch
from edwin_elastic_poller import storage_paths
from edwin_elastic_poller.observability import lm_logs


def process_hits(
    hits: List[Dict[str, Any]],
    query_bookmark: int,
    watermark: int,
    bookmark_loaded: bool,
) -> Tuple[List[Dict[str, Any]], int]:
    """Map ES hits to CEF events.

    Returns the event list and the @timestamp (ms) of the last hit processed.
    """
    event_list = []
    last_timestamp_ms = query_bookmark

    for hit in hits:
        cef = delivery.map_hit_to_cef(
            hit, query_bookmark, watermark, bookmark_loaded
        )
        event_list.append(cef)
        last_timestamp_ms = elasticsearch.hit_timestamp_ms(hit)
        config.logger.debug(
            "Processed hit timestamp %s",
            elasticsearch.epoch_ms_to_zulu(last_timestamp_ms),
        )

    return event_list, last_timestamp_ms


def _log_poll_summary(
    *,
    status: str,
    initial_bookmark_ms: int,
    final_bookmark_ms: int,
    pages_fetched: int,
    events_delivered: int,
    issues: List[str],
    duration_ms: int,
    dedupe_evicted: int = 0,
) -> None:
    """Emit one operational summary per poll cycle for stderr and LM Logs."""
    bookmark_advanced = final_bookmark_ms != initial_bookmark_ms
    errors_encountered = bool(issues)
    issue_text = ",".join(issues) if issues else "none"

    message = (
        f"Poll cycle finished: status={status}, events_delivered={events_delivered}, "
        f"pages_fetched={pages_fetched}, bookmark_advanced={str(bookmark_advanced).lower()}, "
        f"errors={str(errors_encountered).lower()}, issues={issue_text}, "
        f"bookmark={elasticsearch.epoch_ms_to_zulu(final_bookmark_ms)}, "
        f"duration_ms={duration_ms}, dedupe_evicted={dedupe_evicted}"
    )

    lm_logs.log_with_context(
        config.logger,
        lm_logs.operational_log_level(),
        message,
        event_type="poll_summary",
        status=status,
        bookmark_advanced=bookmark_advanced,
        initial_bookmark_ms=initial_bookmark_ms,
        initial_bookmark_zulu=elasticsearch.epoch_ms_to_zulu(initial_bookmark_ms),
        final_bookmark_ms=final_bookmark_ms,
        final_bookmark_zulu=elasticsearch.epoch_ms_to_zulu(final_bookmark_ms),
        pages_fetched=pages_fetched,
        events_delivered=events_delivered,
        errors_encountered=errors_encountered,
        issues=",".join(issues) if issues else "",
        duration_ms=duration_ms,
        dedupe_evicted=dedupe_evicted,
    )


def poll_cycle(bookmark_ms: int, watermark: int, bookmark_loaded: bool) -> int:
    """Run one poll cycle inside a point-in-time, paginating until drained or delivery fails."""
    cycle_started = time.monotonic()
    initial_bookmark_ms = bookmark_ms
    cycle_bookmark = max(0, bookmark_ms - config.ELASTIC_OVERLAP_MS)
    search_after = None
    updated_bookmark = bookmark_ms
    pages_fetched = 0
    events_delivered = 0
    status = "complete"
    issues: List[str] = []
    pit_id = None

    lm_logs.log_with_context(
        config.logger,
        logging.DEBUG,
        "Poll cycle started",
        event_type="poll_started",
        bookmark_ms=cycle_bookmark,
        bookmark_zulu=elasticsearch.epoch_ms_to_zulu(cycle_bookmark),
    )

    try:
        pit_id = elasticsearch.open_point_in_time(
            config.ELASTIC_INDEX,
            keep_alive=config.ELASTIC_PIT_KEEP_ALIVE,
            **elasticsearch._es_conn_kwargs(),
        )
    except (elasticsearch.ElasticsearchQueryError, ValueError) as exc:
        status = "pit_open_failed"
        issues.append(f"pit_open_failed: {exc}")
        config.logger.error(
            "Could not open point-in-time on %s: %s", config.ELASTIC_INDEX, exc
        )
    else:
        try:
            while True:
                lm_logs.log_with_context(
                    config.logger,
                    logging.DEBUG,
                    "Querying Elasticsearch",
                    bookmark_ms=cycle_bookmark,
                    bookmark_zulu=elasticsearch.epoch_ms_to_zulu(cycle_bookmark),
                )
                hits, _took, pit_id = elasticsearch.fetch_elasticsearch_hits(
                    cycle_bookmark, search_after=search_after, pit_id=pit_id
                )
                pages_fetched += 1

                if not hits:
                    break

                new_hits = [hit for hit in hits if not dedupe.is_delivered(hit)]
                event_list, _last_timestamp_ms = process_hits(
                    new_hits, bookmark_ms, watermark, bookmark_loaded
                )
                page_last_timestamp_ms = max(
                    elasticsearch.hit_timestamp_ms(hit) for hit in hits
                )

                lm_logs.log_with_context(
                    config.logger,
                    logging.DEBUG,
                    "Events mapped for delivery",
                    cycle_bookmark_ms=cycle_bookmark,
                    last_timestamp_ms=page_last_timestamp_ms,
                    last_timestamp_zulu=elasticsearch.epoch_ms_to_zulu(
                        page_last_timestamp_ms
                    ),
                    event_count=len(event_list),
                    duplicate_count=len(hits) - len(new_hits),
                )

                if event_list and not delivery.send_event(event_list):
                    status = "delivery_failed"
                    issues.append("edwin_delivery_failed")
                    config.logger.warning(
                        "Edwin delivery failed; bookmark not advanced "
                        "(last successful bookmark %s)",
                        updated_bookmark,
                    )
                    break

                if event_list:
                    dedupe.mark_delivered(
                        new_hits, elasticsearch.hit_timestamp_ms
                    )
                previous_bookmark = updated_bookmark
                updated_bookmark = max(updated_bookmark, page_last_timestamp_ms)
                bookmark.set_bookmark(updated_bookmark)
                bookmark_loaded = True
                events_delivered += len(event_list)

                lm_logs.log_with_context(
                    config.logger,
                    logging.DEBUG,
                    "Bookmark advanced",
                    previous_bookmark_ms=previous_bookmark,
                    new_bookmark_ms=updated_bookmark,
                    new_bookmark_zulu=elasticsearch.epoch_ms_to_zulu(updated_bookmark),
                )

                if len(hits) < config.ELASTIC_BATCH_SIZE:
                    break

                search_after = hits[-1].get("sort")
                if not search_after:
                    status = "incomplete"
                    issues.append("hits_missing_sort_values")
                    config.logger.error(
                        "Elasticsearch returned hits without sort values; ending cycle"
                    )
                    break

        except (delivery.EventMappingError, delivery.DeliveryError) as exc:
            status = "delivery_or_mapping_error"
            issues.append(f"delivery_or_mapping_failed: {exc}")
            config.logger.error("Event processing failed: %s", exc)
        except elasticsearch.ElasticsearchQueryError as exc:
            if exc.is_missing_context:
                status = "pit_expired"
                issues.append("pit_expired_mid_cycle")
                config.logger.warning(
                    "Point-in-time expired mid-cycle; retrying next interval"
                )
                pit_id = None
            else:
                status = "es_error"
                issues.append(f"elasticsearch_query_failed: {exc}")
                config.logger.error("Elasticsearch query failed mid-cycle: %s", exc)
        except Exception as exc:
            status = "unexpected_error"
            issues.append(f"unexpected_error: {exc}")
            config.logger.exception("Unexpected poll-cycle failure")
        finally:
            elasticsearch.close_point_in_time(pit_id, **elasticsearch._es_conn_kwargs())

    dedupe_evicted = 0
    try:
        dedupe_evicted = dedupe.maintain(
            max(0, updated_bookmark - config.ELASTIC_OVERLAP_MS)
        )
    except Exception as exc:
        status = "dedupe_maintenance_failed"
        issues.append(f"dedupe_maintenance_failed: {exc}")
        config.logger.exception("Deduplication maintenance failed")

    duration_ms = int((time.monotonic() - cycle_started) * 1000)
    _log_poll_summary(
        status=status,
        initial_bookmark_ms=initial_bookmark_ms,
        final_bookmark_ms=updated_bookmark,
        pages_fetched=pages_fetched,
        events_delivered=events_delivered,
        issues=issues,
        duration_ms=duration_ms,
        dedupe_evicted=dedupe_evicted,
    )
    return updated_bookmark


def log_startup() -> None:
    """Emit a sanitized configuration snapshot at startup."""
    context = lm_logs.build_startup_context(
        edwin_org=config.EDWIN_ORG,
        elastic_url=config.ELASTIC_URL,
        elastic_index=config.ELASTIC_INDEX,
        elastic_query=config.ELASTIC_QUERY,
        elastic_batch_size=config.ELASTIC_BATCH_SIZE,
        verify_ssl=config.ELASTIC_VERIFY_SSL,
        elastic_pit_keep_alive=config.ELASTIC_PIT_KEEP_ALIVE,
        poller_interval=str(config.PAUSE_INTERVAL),
        bookmark_path=storage_paths.bookmark_file(),
        lm_logs_enabled=config.LM_LOGS_ENABLED,
        elastic_overlap_ms=config.ELASTIC_OVERLAP_MS,
    )
    metadata = {key: value for key, value in context.items() if key != "msg"}
    lm_logs.log_with_context(
        config.logger, lm_logs.operational_log_level(), context["msg"], **metadata
    )
