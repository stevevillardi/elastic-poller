"""Verify that a real Kibana alert event can be mapped by the poller."""

from __future__ import annotations

import os
import time
from typing import Any

import requests

from edwin_elastic_poller import config, delivery


ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200").rstrip("/")
RULE_UUID = os.getenv("KIBANA_TEST_RULE_UUID")
TIMEOUT_SECONDS = 180


def search_events(action: str | None = None) -> list[dict[str, Any]]:
    query: dict[str, Any] = {"match_all": {}}
    if action:
        query = {"term": {"event.action": action}}
    response = requests.post(
        f"{ELASTICSEARCH_URL}/.kibana-event-log-ds/_search",
        json={
            "size": 100,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": query,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("hits", {}).get("hits", [])


def wait_for_events(action: str) -> list[dict[str, Any]]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        hits = search_events(action)
        if hits:
            return hits
        time.sleep(2)
    raise AssertionError(f"No Kibana {action!r} events appeared within {TIMEOUT_SECONDS}s")


def map_event(hit: dict[str, Any]) -> dict[str, Any]:
    config.bootstrap()
    cef = delivery.map_hit_to_cef(
        hit,
        query_bookmark=0,
        watermark=0,
        bookmark_loaded=False,
    )
    return cef["cef"]


def main() -> None:
    active_hits = wait_for_events("active-instance")
    execute_hits = wait_for_events("execute")
    active_hit = active_hits[0]
    cef = map_event(active_hit)

    assert cef["event_name"] == ".es-query"
    assert cef["event_severity"] == 5
    assert cef["event_id"] == active_hit["_source"]["kibana"]["alert"]["uuid"]
    assert cef["event_source_id"] == active_hit["_id"]
    assert cef["event_time"].endswith("Z")
    if RULE_UUID:
        assert (
            active_hit["_source"]["kibana"]["saved_objects"][0]["id"] == RULE_UUID
        )

    execute_cef = map_event(execute_hits[0])
    assert execute_cef["event_name"] == ".es-query"
    assert execute_cef["event_time"].endswith("Z")
    print(
        f"Mapped Kibana event-log documents successfully: "
        f"active={len(active_hits)} execute={len(execute_hits)}"
    )


if __name__ == "__main__":
    main()
