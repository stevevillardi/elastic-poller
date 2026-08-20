"""Shared Elasticsearch helpers for integration tests and local E2E runs."""

from __future__ import annotations

import json
import os
import time
import unittest
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import requests

DocPair = Tuple[str, dict]

DEFAULT_ES_URL = (os.getenv("ES_TEST_URL") or "").rstrip("/")


def es_is_reachable(es_url: str, *, timeout: float = 2.0) -> bool:
    """Return True when Elasticsearch responds at *es_url*."""
    if not es_url:
        return False
    try:
        response = requests.get(f"{es_url.rstrip('/')}/", timeout=timeout)
        return response.status_code < 500
    except requests.RequestException:
        return False


def integration_skip_reason(es_url: str | None = None) -> str | None:
    """Explain why integration tests should be skipped, or None to run them."""
    url = (es_url or DEFAULT_ES_URL or "").rstrip("/")
    if not url:
        return "ES_TEST_URL not set; skipping Elasticsearch integration tests"
    if os.getenv("ES_REQUIRE_INTEGRATION"):
        return None
    if not es_is_reachable(url):
        return (
            f"Elasticsearch not reachable at {url}; skipping integration tests "
            "(start Elasticsearch or unset ES_TEST_URL)"
        )
    return None


def skip_unless_integration(es_url: str | None = None):
    """Return a unittest skip decorator for integration/live ES tests."""
    reason = integration_skip_reason(es_url)
    return unittest.skipIf(reason is not None, reason or "")


def integration_doc(
    timestamp: str,
    index: int,
    action: str,
    *,
    message_prefix: str = "integration",
) -> dict:
    """Build a document shaped to map through elastic_event_mappings.yaml."""
    return {
        "@timestamp": timestamp,
        "message": f"{message_prefix} event {index}",
        "event": {
            "provider": "alerting",
            "action": action,
            "kind": "alert",
            "category": ["logs"],
        },
        "rule": {
            "id": f"rule-{index}",
            "name": f"integration-rule-{index}",
            "category": "logs.alert.document.count",
            "license": "basic",
        },
        "kibana": {
            "alert": {"rule": {"rule_type_id": "logs.alert.document.count"}},
            "space_ids": ["itspace"],
        },
    }


def format_timestamp(dt: datetime) -> str:
    """Format a datetime as an Elasticsearch date string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def current_base_time() -> datetime:
    """UTC now — base for seeding integration and live-test documents."""
    return datetime.now(timezone.utc)


def integration_fixture_times(
    *, tail_count: int = 5
) -> Tuple[str, int, datetime, int]:
    """Timestamps for pagination tests: identical-ms batch plus a later tail.

    Uses a time slightly in the past so Elasticsearch PIT pagination is stable
    (seeding at ``now`` can truncate pages in single-node test clusters).

    Returns ``identical_ts``, ``identical_ts_ms``, ``tail_start``, ``last_tail_ms``.
    """
    identical_dt = (current_base_time() - timedelta(days=7)).replace(microsecond=365000)
    identical_ts = format_timestamp(identical_dt)
    identical_ms = int(identical_dt.timestamp() * 1000)
    tail_start = identical_dt.replace(microsecond=0) + timedelta(seconds=2)
    last_tail_ms = int(
        (tail_start + timedelta(seconds=tail_count - 1)).timestamp() * 1000
    )
    return identical_ts, identical_ms, tail_start, last_tail_ms


def wait_for_cluster(es_url: Optional[str] = None, timeout: int = 120) -> None:
    """Block until Elasticsearch reports yellow or green."""
    base = (es_url or DEFAULT_ES_URL).rstrip("/")
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(
                f"{base}/_cluster/health",
                params={"wait_for_status": "yellow", "timeout": "5s"},
                timeout=10,
            )
            if response.ok and response.json().get("status") in ("green", "yellow"):
                return
            last = response.text
        except requests.RequestException as exc:
            last = str(exc)
        time.sleep(2)
    raise RuntimeError(f"Elasticsearch not ready at {base}: {last}")


def create_test_index(
    index: str,
    *,
    shards: int = 3,
    es_url: Optional[str] = None,
) -> None:
    """Create an index with mappings compatible with elastic_event_mappings.yaml."""
    base = (es_url or DEFAULT_ES_URL).rstrip("/")
    body = {
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "message": {"type": "text"},
                "event": {
                    "properties": {
                        "provider": {"type": "keyword"},
                        "action": {"type": "keyword"},
                        "kind": {"type": "keyword"},
                        "category": {"type": "keyword"},
                    }
                },
                "rule": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "license": {"type": "keyword"},
                    }
                },
                "kibana": {
                    "properties": {
                        "space_ids": {"type": "keyword"},
                        "alert": {
                            "properties": {
                                "rule": {
                                    "properties": {
                                        "rule_type_id": {"type": "keyword"}
                                    }
                                }
                            }
                        },
                    }
                },
            }
        },
    }
    response = requests.put(f"{base}/{index}", json=body, timeout=30)
    response.raise_for_status()


def delete_index(index: str, *, es_url: Optional[str] = None) -> None:
    """Delete an index, ignoring transport errors."""
    base = (es_url or DEFAULT_ES_URL).rstrip("/")
    try:
        requests.delete(f"{base}/{index}", timeout=30)
    except requests.RequestException:
        pass


def bulk_seed(
    index: str,
    docs: Sequence[DocPair],
    *,
    es_url: Optional[str] = None,
) -> set[str]:
    """Bulk-index documents and return the set of document ids."""
    base = (es_url or DEFAULT_ES_URL).rstrip("/")
    lines: List[str] = []
    for doc_id, source in docs:
        lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}))
        lines.append(json.dumps(source))
    payload = "\n".join(lines) + "\n"

    response = requests.post(
        f"{base}/_bulk",
        params={"refresh": "wait_for"},
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=60,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("errors"):
        raise RuntimeError(f"bulk seed failed: {json.dumps(body)[:2000]}")
    return {doc_id for doc_id, _ in docs}


def build_batch_docs(
    batch_number: int,
    docs_per_batch: int,
    *,
    base_time: datetime,
    ms_step: int = 10,
    action_prefix: str = "batch",
) -> List[DocPair]:
    """Build a batch of documents with strictly increasing timestamps."""
    docs: List[DocPair] = []
    for position in range(docs_per_batch):
        offset = batch_number * docs_per_batch + position
        stamp = base_time + timedelta(milliseconds=offset * ms_step)
        timestamp = format_timestamp(stamp)
        doc_id = f"batch{batch_number}-{position:03d}"
        docs.append(
            (
                doc_id,
                integration_doc(
                    timestamp,
                    offset,
                    f"{action_prefix}-{batch_number}",
                ),
            )
        )
    return docs


def seed_batches_over_time(
    index: str,
    batch_count: int,
    docs_per_batch: int,
    *,
    base_time: Optional[datetime] = None,
    es_url: Optional[str] = None,
) -> set[str]:
    """Seed multiple batches of documents with monotonically increasing timestamps."""
    start = base_time or current_base_time()
    all_ids: set[str] = set()
    for batch in range(batch_count):
        docs = build_batch_docs(batch, docs_per_batch, base_time=start)
        all_ids |= bulk_seed(index, docs, es_url=es_url)
    return all_ids
