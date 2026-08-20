"""Bundled CEF mapping configuration."""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path

MAPPING_FILE_NAME = "elastic_event_mappings.yaml"


def mapping_directory() -> Path:
    """Return the directory containing the bundled mapping file."""
    return Path(files("edwin_elastic_poller.mappings"))


def mapping_file_path() -> Path:
    """Return the bundled elastic_event_mappings.yaml path."""
    return mapping_directory() / MAPPING_FILE_NAME


def mapping_sources(custom_file: str | None = None) -> tuple[str, str]:
    """Return ``(file_name, parent_directory)`` for ``CommonEvent.new_from_file``."""
    if custom_file:
        path = Path(custom_file).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"Event mapping file not found: {path}")
        return path.name, str(path.parent)
    return MAPPING_FILE_NAME, str(mapping_directory())


def active_mapping_file_path(custom_file: str | None = None) -> Path:
    """Return the mapping YAML path in use (bundled or overridden)."""
    file_name, directory = mapping_sources(custom_file)
    return Path(directory) / file_name
