#!/usr/bin/env python3
"""Local end-to-end runner: seed Elasticsearch in waves and poll between them.

Use this to exercise bookmark advancement and pagination across multiple poll
cycles without running the full unittest suite. By default delivery is mocked;
pass --live to send events to Edwin using credentials from the environment or
a .env file.

Examples:

    # Mock delivery against a local Elasticsearch container
    python scripts/local_e2e.py --es-url http://localhost:9200

    # Same, with verbose per-cycle logging
    DEBUG=true python scripts/local_e2e.py --es-url http://localhost:9200 --batches 6

    # Live delivery to Edwin (requires EDWIN_* or DEXDA_* in .env or environment)
    ES_LIVE_DELIVERY=1 python scripts/local_e2e.py --es-url http://localhost:9200 --live
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from edwin_elastic_poller import bookmark, config, delivery, elasticsearch, poller

load_dotenv(REPO_ROOT / ".env")


class _MockCollector:
    def __init__(self) -> None:
        self.batches: List[list] = []
        self.ids: List[str] = []

    def __call__(self, event_list):
        self.batches.append(event_list)
        self.ids.extend(event["cef"]["event_source_id"] for event in event_list)
        print(
            f"  delivered {len(event_list)} events "
            f"(total unique ids: {len(set(self.ids))})"
        )
        return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--es-url",
        default=os.getenv("ES_TEST_URL") or os.getenv("ELASTIC_URL"),
        help="Elasticsearch base URL (default: ES_TEST_URL or ELASTIC_URL)",
    )
    parser.add_argument(
        "--batches", type=int, default=4, help="Number of seed/poll waves"
    )
    parser.add_argument(
        "--docs-per-batch",
        type=int,
        default=20,
        help="Documents indexed before each poll cycle",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=5,
        help="ELASTIC_BATCH_SIZE for each poll cycle",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Deliver to Edwin instead of using a mock collector",
    )
    parser.add_argument(
        "--keep-index",
        action="store_true",
        help="Do not delete the test index on exit",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.es_url:
        print(
            "ERROR: --es-url or ES_TEST_URL/ELASTIC_URL is required",
            file=sys.stderr,
        )
        return 1

    if args.live and not os.getenv("ES_LIVE_DELIVERY"):
        print(
            "ERROR: --live requires ES_LIVE_DELIVERY=1 to avoid accidental "
            "production delivery",
            file=sys.stderr,
        )
        return 1

    if args.live:
        missing = config.missing_edwin_credential_names()
        if missing:
            print(
                f"ERROR: missing Edwin credentials: {', '.join(missing)}",
                file=sys.stderr,
            )
            return 1

    from tests import es_test_support

    config.bootstrap()
    es_url = args.es_url.rstrip("/")
    index = f"poller-e2e-{uuid.uuid4().hex[:8]}"
    base_time = es_test_support.current_base_time()

    print(f"Elasticsearch: {es_url}")
    print(f"Index: {index}")
    print(
        f"Waves: {args.batches} x {args.docs_per_batch} docs "
        f"(page size {args.page_size})"
    )
    print(f"Delivery: {'live Edwin' if args.live else 'mock collector'}")

    es_test_support.wait_for_cluster(es_url)
    es_test_support.create_test_index(index, shards=3, es_url=es_url)

    temp_dir = tempfile.TemporaryDirectory()

    config.ELASTIC_URL = es_url
    config.ELASTIC_INDEX = index
    config.ELASTIC_VERIFY_SSL = False
    config.ELASTIC_BATCH_SIZE = args.page_size
    config.ELASTIC_QUERY = "*"
    config.ELASTIC_USER = None
    config.ELASTIC_PASS = None
    config.ELASTIC_TOKEN = None
    config.ELASTIC_PIT_KEEP_ALIVE = "2m"
    config.BOOKMARK_PATH = temp_dir.name
    config.EDWIN_ORG = "e2e"
    bookmark.set_bookmark(0)

    collector = None
    if not args.live:
        collector = _MockCollector()
        delivery.send_event = collector

    expected_ids: set[str] = set()
    bookmark_ms = 0
    bookmark_loaded = False
    watermark = 0

    try:
        for batch in range(args.batches):
            print(f"\n--- wave {batch + 1}/{args.batches} ---")
            docs = es_test_support.build_batch_docs(
                batch,
                args.docs_per_batch,
                base_time=base_time,
            )
            wave_ids = es_test_support.bulk_seed(index, docs, es_url=es_url)
            expected_ids |= wave_ids
            print(f"  indexed {len(wave_ids)} documents")

            bookmark_ms = poller.poll_cycle(bookmark_ms, watermark, bookmark_loaded)
            bookmark_loaded = True
            print(
                f"  bookmark now {bookmark_ms} "
                f"({elasticsearch.epoch_ms_to_zulu(bookmark_ms)})"
            )

            if collector is not None:
                if set(collector.ids) != expected_ids:
                    print(
                        "ERROR: collector mismatch after wave "
                        f"{batch + 1}: expected {len(expected_ids)}, "
                        f"got {len(set(collector.ids))}",
                        file=sys.stderr,
                    )
                    return 1

        print("\nE2E run complete.")
        if collector is not None:
            print(
                f"Delivered {len(collector.ids)} events in "
                f"{len(collector.batches)} batches "
                f"({len(set(collector.ids))} unique ids)"
            )
        return 0
    finally:
        if not args.keep_index:
            es_test_support.delete_index(index, es_url=es_url)
            print(f"Deleted index {index}")
        temp_dir.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
