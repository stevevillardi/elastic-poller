"""Bookmark file persistence."""

from __future__ import annotations

import os
import tempfile

from edwin_elastic_poller import config
from edwin_elastic_poller import storage_paths


class BookmarkError(ValueError):
    """Raised when bookmark state cannot be read or written safely."""


def get_bookmark() -> int:
    """Read the bookmark file. Creates the file with 0 if it does not exist."""
    bookmark_dir = storage_paths.data_dir()
    bookmark_file = storage_paths.bookmark_file()
    os.makedirs(bookmark_dir, exist_ok=True)
    if not os.path.exists(bookmark_file):
        config.logger.info("Bookmark file not found, creating it")
        with open(bookmark_file, "w", encoding="utf-8") as fh:
            fh.write("0")

    try:
        with open(bookmark_file, "r", encoding="utf-8") as fh:
            value = int(float(fh.read().strip()))
    except (OSError, ValueError) as error:
        config.logger.error("Invalid bookmark file %s: %s", bookmark_file, error)
        raise BookmarkError(
            f"Bookmark file {bookmark_file} is unreadable; preserve it and "
            "repair or reset it before restarting"
        ) from error
    if value < 0:
        raise BookmarkError(f"Bookmark file {bookmark_file} contains a negative value")
    return value


def set_bookmark(bookmark: int) -> None:
    """Persist the bookmark as epoch milliseconds (last successfully sent event)."""
    bookmark_dir = storage_paths.data_dir()
    bookmark_file = storage_paths.bookmark_file()
    os.makedirs(bookmark_dir, exist_ok=True)
    if bookmark < 0:
        raise BookmarkError("Bookmark must be non-negative")
    fd, temporary_path = tempfile.mkstemp(
        prefix=".bookmark-", dir=bookmark_dir, text=True
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(str(int(bookmark)))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary_path, bookmark_file)
    except OSError as error:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise BookmarkError(
            f"Could not atomically write bookmark file {bookmark_file}"
        ) from error
