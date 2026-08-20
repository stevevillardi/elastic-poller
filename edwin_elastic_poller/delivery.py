"""Edwin event mapping and delivery."""

from __future__ import annotations

import time
from typing import Any, Dict, List

from edwin_elastic_poller import config
from edwin_elastic_poller import mappings
from edwin_elastic_poller.sdk import common_event, edwin_request


class EventMappingError(Exception):
    """Raised when an Elasticsearch hit cannot be mapped to CEF."""


class DeliveryError(Exception):
    """Raised when Edwin delivery cannot be completed."""

_client = None


def _get_client():
    global _client
    if _client is not None:
        token = _client.access_token
        expires_at = token.get("expires_at")
        if expires_at is None or float(expires_at) > time.time() + 60:
            return _client

    auth_dict = {
        "edwin_org": config.EDWIN_ORG,
        "client_id": config.EDWIN_ID,
        "client_secret": config.EDWIN_TOKEN,
    }
    _client = edwin_request.EdwinRequest.new_from_param(auth_dict=auth_dict)
    return _client


def create_event(payload: Dict[str, Any]):
    """Map a raw Elasticsearch hit to a CommonEvent using the active mapping file."""
    mapping_file_name, mapping_file_path = mappings.mapping_sources(
        config.EVENT_MAPPING_FILE
    )
    try:
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name=mapping_file_name,
            mapping_file_path=mapping_file_path,
            original_record=payload,
        )
        config.logger.debug("Successfully mapped event payload to CEF")
    except Exception as error:
        config.logger.exception("Exception mapping Elasticsearch event")
        raise EventMappingError(str(error)) from error
    return event


def map_hit_to_cef(
    hit: Dict[str, Any],
    query_bookmark: int,
    watermark: int,
    bookmark_loaded: bool,
) -> Dict[str, Any]:
    """Map one Elasticsearch hit to a deliverable CEF payload."""
    event = create_event(hit)
    event.set_enrichment_value("lm_bookmark", query_bookmark)
    event.set_enrichment_value("lm_watermark", watermark)
    event.set_enrichment_value("lm_loaded", bookmark_loaded)
    event.set_enrichment_value("lm_elastic_index", config.ELASTIC_INDEX)

    try:
        space_ids = hit["_source"].get("kibana", {}).get("space_ids", [])
        if space_ids:
            event.set_enrichment_value("lm_service_id", ",".join(space_ids))
    except (TypeError, AttributeError):
        pass

    cef = event.get_cef()
    cef["cef"]["event_source_id"] = cef["cef"]["source_record"]["_id"]

    if "," in cef["cef"]["event_ci"]:
        ci = cef["cef"]["event_ci"]
        cef["cef"]["event_ci"] = ci.split(",")[0]
        try:
            if cef["cef"]["source_record"]["_source"]["event"].get("end"):
                cef["cef"]["event_severity"] = 0
        except (TypeError, NameError) as exc:
            config.logger.debug("Could not evaluate event.end for severity: %s", exc)

    return cef


def send_event(event_list: List[Dict[str, Any]]) -> bool:
    """Deliver a batch of CEF events to Edwin. Returns False if any batch fails."""
    global _client
    try:
        client = _get_client()
        access_token = client.access_token
        success = client.send(access_token=access_token, data=event_list)
        if not success:
            config.logger.error("send_event: one or more batches failed to deliver")
            _client = None
            return False
        config.logger.debug("Successfully sent Edwin events")
    except Exception as error:
        config.logger.exception("Exception delivering events to Edwin")
        raise DeliveryError(str(error)) from error
    return True
