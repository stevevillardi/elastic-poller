"""Resolve bookmark and deduplication storage locations."""

from __future__ import annotations

import os

from edwin_elastic_poller import config


def data_dir() -> str:
    """Return the directory used for bookmark and dedupe state."""
    if config.BOOKMARK_PATH:
        return config.BOOKMARK_PATH.rstrip("/")
    return "."


def bookmark_file() -> str:
    """Return the bookmark file path for the configured Edwin org."""
    return os.path.join(data_dir(), f"{config.EDWIN_ORG}.elastic.bookmark")


def dedupe_db_path() -> str:
    """Return the SQLite deduplication database path."""
    return os.path.join(data_dir(), f"{config.EDWIN_ORG}.elastic.dedupe.sqlite")
