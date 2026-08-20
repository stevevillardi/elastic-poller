"""Command-line interface for edwin-elastic-poller."""

from __future__ import annotations

import argparse
from typing import Optional, Sequence


def build_parser() -> argparse.ArgumentParser:
    """Return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="edwin-elastic-poller",
        description=(
            "Poll Elasticsearch Kibana event logs and deliver them to Edwin."
        ),
    )
    parser.add_argument(
        "--env-file",
        metavar="PATH",
        help="Load environment variables from PATH instead of searching for .env",
    )
    parser.add_argument(
        "--mapping-file",
        metavar="PATH",
        help="Path to a custom CEF mapping YAML (overrides EVENT_MAPPING_FILE)",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    return build_parser().parse_args(argv)
