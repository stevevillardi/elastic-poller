"""Elasticsearch query, transport, and point-in-time helpers."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from edwin_elastic_poller.observability import lm_logs
from edwin_elastic_poller import config


class ElasticsearchQueryError(Exception):
    """Raised when an Elasticsearch request fails.

    Carries the HTTP status and response body so callers can tell an expired
    point-in-time apart from a genuine cluster failure.
    """

    def __init__(self, message, status_code=None, body=""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body or ""

    @property
    def is_missing_context(self) -> bool:
        """True when Elasticsearch says the point-in-time no longer exists."""
        if self.status_code == 404:
            return True
        return "search_context_missing" in self.body or "No search context" in self.body


def build_logs_query(
    text: str,
    bookmark_ms: int,
    end: str = "now",
    size: int = 10000,
    timestamp_field: str = "@timestamp",
    search_after: Optional[List[Any]] = None,
    pit_id: Optional[str] = None,
    keep_alive: Optional[str] = None,
) -> Dict[str, Any]:
    """Build an Elasticsearch _search body with an exclusive millisecond lower bound."""
    query: Dict[str, Any] = {
        "size": size,
        "query": {
            "bool": {
                "must": [{"query_string": {"query": text}}],
                "filter": [
                    {
                        "range": {
                            timestamp_field: {
                                "gt": bookmark_ms,
                                "lte": end,
                                "format": "epoch_millis",
                            }
                        }
                    }
                ],
            }
        },
        "sort": [{timestamp_field: {"order": "asc"}}],
    }
    if pit_id:
        query["pit"] = {
            "id": pit_id,
            "keep_alive": keep_alive or config.ELASTIC_PIT_KEEP_ALIVE,
        }
        query["sort"].append({"_shard_doc": "asc"})
    if search_after is not None:
        query["search_after"] = search_after
    return query


def _es_request(
    method: str,
    path: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    base_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
    verify_ssl: bool = True,
    timeout: int = 30,
) -> Dict[str, Any]:
    """Single entry point for every Elasticsearch HTTP call."""
    base = (base_url if base_url is not None else config.ELASTIC_URL) or ""

    if api_key and (username or password):
        raise ValueError("Use either api_key or username/password, not both.")

    if len(base) < 5:
        raise ValueError("You have not supplied a correct url for ElasticSearch.")

    url = f"{base.rstrip('/')}/{path.lstrip('/')}"

    headers = {"Accept": "application/json"}
    auth = None
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    elif username and password:
        auth = (username, password)
    if body is not None:
        headers["Content-Type"] = "application/json"

    try:
        response = requests.request(
            method,
            url,
            json=body,
            params=params,
            headers=headers,
            auth=auth,
            verify=verify_ssl,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    except requests.RequestException as exc:
        status = None
        detail = ""
        response = getattr(exc, "response", None)
        if response is not None:
            status = getattr(response, "status_code", None)
            try:
                detail = response.text
            except Exception:
                detail = ""
        message = f"Elasticsearch {method} /{path.lstrip('/')} failed: {exc}."
        if detail:
            message += f" Response body: {detail}"
        raise ElasticsearchQueryError(
            message, status_code=status, body=detail
        ) from exc


def _es_conn_kwargs() -> Dict[str, Any]:
    """Connection kwargs read from config at call time (test-friendly)."""
    return {
        "base_url": config.ELASTIC_URL,
        "username": config.ELASTIC_USER,
        "password": config.ELASTIC_PASS,
        "api_key": config.ELASTIC_TOKEN,
        "verify_ssl": config.ELASTIC_VERIFY_SSL,
    }


def query_elasticsearch(
    index: Optional[str],
    query: Dict[str, Any],
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
    verify_ssl: bool = True,
    timeout: int = 30,
) -> Dict[str, Any]:
    """POST a query DSL body to Elasticsearch and return the JSON response."""
    path = f"{index}/_search" if index else "_search"
    return _es_request(
        "POST",
        path,
        body=query,
        username=username,
        password=password,
        api_key=api_key,
        verify_ssl=verify_ssl,
        timeout=timeout,
    )


def open_point_in_time(
    index: str,
    keep_alive: Optional[str] = None,
    timeout: int = 30,
    **conn: Any,
) -> str:
    """Open a point-in-time over `index` and return its id."""
    result = _es_request(
        "POST",
        f"{index}/_pit",
        params={"keep_alive": keep_alive or config.ELASTIC_PIT_KEEP_ALIVE},
        timeout=timeout,
        **conn,
    )
    pit_id = result.get("id")
    if not pit_id:
        raise ElasticsearchQueryError(
            f"Elasticsearch did not return a point-in-time id: {result}"
        )
    return pit_id


def close_point_in_time(
    pit_id: Optional[str],
    timeout: int = 30,
    **conn: Any,
) -> bool:
    """Release a point-in-time. Never raises."""
    if not pit_id:
        return False

    try:
        result = _es_request(
            "DELETE", "_pit", body={"id": pit_id}, timeout=timeout, **conn
        )
    except ElasticsearchQueryError as exc:
        if exc.status_code == 404:
            return False
        config.logger.warning("Failed to close point-in-time: %s", exc)
        return False
    except Exception as exc:  # pragma: no cover - defensive
        config.logger.warning("Unexpected error closing point-in-time: %s", exc)
        return False

    return bool(result.get("succeeded"))


def fetch_elasticsearch_hits(
    bookmark_ms: int,
    search_after: Optional[List[Any]] = None,
    pit_id: Optional[str] = None,
    keep_alive: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
    """Fetch one page of hits from Elasticsearch."""
    query_body = build_logs_query(
        text=config.ELASTIC_QUERY,
        bookmark_ms=bookmark_ms,
        size=config.ELASTIC_BATCH_SIZE,
        search_after=search_after,
        pit_id=pit_id,
        keep_alive=keep_alive or config.ELASTIC_PIT_KEEP_ALIVE,
    )
    result = query_elasticsearch(
        index=None if pit_id else config.ELASTIC_INDEX,
        query=query_body,
        username=config.ELASTIC_USER,
        password=config.ELASTIC_PASS,
        api_key=config.ELASTIC_TOKEN,
        verify_ssl=config.ELASTIC_VERIFY_SSL,
        timeout=60,
    )
    took = result.get("took", 0)
    hits = result.get("hits", {}).get("hits", [])
    lm_logs.log_with_context(
        config.logger,
        logging.DEBUG,
        "Elasticsearch page fetched",
        bookmark_ms=bookmark_ms,
        hit_count=len(hits),
        took_ms=took,
    )
    return hits, took, result.get("pit_id", pit_id)


def hit_timestamp_ms(hit: Dict[str, Any]) -> int:
    """Return the @timestamp of an ES hit as epoch milliseconds."""
    timestamp_str = hit["_source"]["@timestamp"]
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def epoch_ms_to_zulu(ts_ms: int) -> str:
    """Format epoch milliseconds as an ISO-8601 Zulu string for logging."""
    return (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z"
    )
