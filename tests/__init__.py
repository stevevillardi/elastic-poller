"""Test package — ensures the repository root is importable."""

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from edwin_elastic_poller import config

config.bootstrap()

CONFIG_PATCH_ATTRS = {
    "ELASTIC_URL",
    "ELASTIC_INDEX",
    "ELASTIC_BATCH_SIZE",
    "ELASTIC_QUERY",
    "ELASTIC_USER",
    "ELASTIC_PASS",
    "ELASTIC_TOKEN",
    "ELASTIC_VERIFY_SSL",
    "EDWIN_VERIFY_SSL",
    "VERIFY_SSL",
    "ELASTIC_PIT_KEEP_ALIVE",
    "ELASTIC_OVERLAP_MS",
    "BOOKMARK_PATH",
    "DEDUPE_MAX_RECORDS",
    "DEDUPE_MAX_SIZE_MB",
    "EDWIN_ORG",
    "EDWIN_ID",
    "EDWIN_TOKEN",
}
STORAGE_PATCH_ATTRS = {"data_dir", "bookmark_file", "dedupe_db_path"}


def storage_patches(temp_dir: str, bookmark_filename: str) -> dict[str, object]:
    """Return config and storage path overrides for an isolated bookmark directory."""
    bookmark_path = os.path.join(temp_dir, bookmark_filename)
    dedupe_path = os.path.join(
        temp_dir,
        bookmark_filename.replace(".bookmark", ".dedupe.sqlite"),
    )
    return {
        "BOOKMARK_PATH": temp_dir,
        "data_dir": lambda: temp_dir,
        "bookmark_file": lambda: bookmark_path,
        "dedupe_db_path": lambda: dedupe_path,
    }


def patch_target(name: str):
    """Return the module that owns a patchable edwin_elastic_poller setting."""
    if name == "send_event":
        from edwin_elastic_poller import delivery

        return delivery
    if name in STORAGE_PATCH_ATTRS:
        from edwin_elastic_poller import storage_paths

        return storage_paths
    if name in CONFIG_PATCH_ATTRS:
        from edwin_elastic_poller import config

        return config
    import edwin_elastic_poller

    return edwin_elastic_poller
