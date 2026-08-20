"""Durable Elasticsearch document deduplication state."""

from __future__ import annotations

import os
import sqlite3
from typing import Iterable, Optional

from edwin_elastic_poller import config
from edwin_elastic_poller import storage_paths


def document_key(hit: dict) -> Optional[str]:
    """Return a stable identity for an Elasticsearch hit."""
    index = hit.get("_index")
    document_id = hit.get("_id")
    if index is None or document_id is None:
        return None
    return f"{index}:{document_id}"


def _connect() -> sqlite3.Connection:
    path = storage_paths.dedupe_db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS delivered_documents (
            document_key TEXT PRIMARY KEY,
            timestamp_ms INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_delivered_documents_timestamp
        ON delivered_documents(timestamp_ms)
        """
    )
    return connection


def is_delivered(hit: dict) -> bool:
    """Return whether this hit was previously delivered successfully."""
    key = document_key(hit)
    if key is None:
        return False
    with _connect() as connection:
        row = connection.execute(
            "SELECT 1 FROM delivered_documents WHERE document_key = ?",
            (key,),
        ).fetchone()
    return row is not None


def mark_delivered(hits: Iterable[dict], timestamp_for_hit) -> None:
    """Record hits after Edwin has accepted their batch."""
    rows = []
    for hit in hits:
        key = document_key(hit)
        if key is None:
            continue
        rows.append((key, int(timestamp_for_hit(hit))))
    if not rows:
        return
    with _connect() as connection:
        connection.executemany(
            """
            INSERT INTO delivered_documents(document_key, timestamp_ms)
            VALUES (?, ?)
            ON CONFLICT(document_key) DO UPDATE SET timestamp_ms = excluded.timestamp_ms
            """,
            rows,
        )


def maintain(before_timestamp_ms: int) -> int:
    """Prune old records and enforce configured row and file-size limits.

    If a hard limit requires eviction inside the overlap window, the oldest
    records are removed. This can permit a duplicate delivery, but avoids
    unbounded disk growth and preserves forward progress.
    """
    database_path = storage_paths.dedupe_db_path()
    evicted = 0
    connection = _connect()
    try:
        connection.execute(
            "DELETE FROM delivered_documents WHERE timestamp_ms < ?",
            (int(before_timestamp_ms),),
        )
        connection.commit()

        record_count = connection.execute(
            "SELECT COUNT(*) FROM delivered_documents"
        ).fetchone()[0]
        if record_count > config.DEDUPE_MAX_RECORDS:
            number_to_remove = record_count - config.DEDUPE_MAX_RECORDS
            connection.execute(
                """
                DELETE FROM delivered_documents
                WHERE rowid IN (
                    SELECT rowid FROM delivered_documents
                    ORDER BY timestamp_ms ASC, rowid ASC
                    LIMIT ?
                )
                """,
                (number_to_remove,),
            )
            evicted += number_to_remove
            connection.commit()

        max_bytes = config.DEDUPE_MAX_SIZE_MB * 1024 * 1024
        while os.path.getsize(database_path) > max_bytes:
            record_count = connection.execute(
                "SELECT COUNT(*) FROM delivered_documents"
            ).fetchone()[0]
            if not record_count:
                break
            number_to_remove = max(1, min(record_count, record_count // 10))
            connection.execute(
                """
                DELETE FROM delivered_documents
                WHERE rowid IN (
                    SELECT rowid FROM delivered_documents
                    ORDER BY timestamp_ms ASC, rowid ASC
                    LIMIT ?
                )
                """,
                (number_to_remove,),
            )
            evicted += number_to_remove
            connection.commit()
            connection.execute("VACUUM")
    finally:
        connection.close()

    if evicted:
        config.logger.warning(
            "Deduplication retention limit evicted records=%s "
            "max_records=%s max_size_mb=%s; duplicate redelivery is possible",
            evicted,
            config.DEDUPE_MAX_RECORDS,
            config.DEDUPE_MAX_SIZE_MB,
        )
    return evicted


def prune(before_timestamp_ms: int) -> None:
    """Backward-compatible alias for deduplication maintenance."""
    maintain(before_timestamp_ms)
