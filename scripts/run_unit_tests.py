#!/usr/bin/env python3
"""Run unit tests only (no Elasticsearch or Edwin required)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for module_name in (
        "tests.test_edwin_elastic_poller",
        "tests.test_env_config",
        "tests.test_lm_logs",
        "tests.test_cli_config",
    ):
        suite.addTests(loader.loadTestsFromName(module_name))

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
