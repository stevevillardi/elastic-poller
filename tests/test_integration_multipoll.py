"""Multi-poll integration tests against a real Elasticsearch.

Exercises the full poll loop across several cycles: documents are seeded in
tranches between poll cycles, so bookmark advancement and pagination must work
together end to end. Delivery is mocked via a collector, so no Edwin credentials
are required.

Skipped unless ES_TEST_URL is set.

    ES_TEST_URL=http://localhost:9200 ES_REQUIRE_INTEGRATION=1 \
      python -m unittest test_integration_multipoll.py -v
"""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from unittest.mock import patch

from edwin_elastic_poller import bookmark, mappings, poller
from tests import es_test_support, patch_target, storage_patches

ES_TEST_URL = es_test_support.DEFAULT_ES_URL

requires_es = unittest.skipUnless(
    ES_TEST_URL, "ES_TEST_URL not set; skipping Elasticsearch integration tests"
)

PAGE_SIZE = 5
DOCS_PER_BATCH = 20
BATCH_COUNT = 4


def setUpModule():
    if os.getenv("ES_REQUIRE_INTEGRATION") and not ES_TEST_URL:
        raise RuntimeError(
            "ES_REQUIRE_INTEGRATION is set but ES_TEST_URL is empty; "
            "the integration job would have passed without running anything"
        )
    if ES_TEST_URL and not mappings.mapping_file_path().exists():
        raise RuntimeError(
            "Bundled mapping file is missing; install the package or run from "
            "a checkout that includes edwin_elastic_poller/mappings/"
        )


class _Collector:
    """Stands in for send_event; records every batch it is handed."""

    def __init__(self) -> None:
        self.batches: list = []
        self.ids: list[str] = []

    def __call__(self, event_list):
        self.batches.append(event_list)
        self.ids.extend(event["cef"]["event_source_id"] for event in event_list)
        return True


@requires_es
class MultiPollIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        es_test_support.wait_for_cluster(ES_TEST_URL)

    def setUp(self):
        self.index = f"poller-mp-{uuid.uuid4().hex[:8]}"
        es_test_support.create_test_index(self.index, shards=3, es_url=ES_TEST_URL)
        self.addCleanup(es_test_support.delete_index, self.index, es_url=ES_TEST_URL)

        self.collector = _Collector()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        patches = {
            "ELASTIC_URL": ES_TEST_URL,
            "ELASTIC_INDEX": self.index,
            "ELASTIC_VERIFY_SSL": False,
            "ELASTIC_BATCH_SIZE": PAGE_SIZE,
            "ELASTIC_QUERY": "*",
            "ELASTIC_USER": None,
            "ELASTIC_PASS": None,
            "ELASTIC_TOKEN": None,
            "ELASTIC_PIT_KEEP_ALIVE": "1m",
            "ELASTIC_OVERLAP_MS": 300000,
            "send_event": self.collector,
            **storage_patches(self.temp_dir.name, "multipoll.bookmark"),
        }
        for name, value in patches.items():
            patcher = patch.object(patch_target(name), name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        bookmark.set_bookmark(0)

    def test_events_collected_across_multiple_poll_cycles(self):
        """Seed a tranche, poll, repeat — bookmark must advance between cycles."""
        base_time = es_test_support.current_base_time()
        expected_ids: set[str] = set()
        bookmark_ms = 0
        bookmark_loaded = False
        watermark = 0
        batches_before = 0

        for batch in range(BATCH_COUNT):
            docs = es_test_support.build_batch_docs(
                batch,
                DOCS_PER_BATCH,
                base_time=base_time,
            )
            expected_ids |= es_test_support.bulk_seed(
                self.index, docs, es_url=ES_TEST_URL
            )

            bookmark_ms = poller.poll_cycle(bookmark_ms, watermark, bookmark_loaded)
            bookmark_loaded = True

            self.assertEqual(
                set(self.collector.ids),
                expected_ids,
                f"after batch {batch}",
            )
            self.assertGreater(
                len(self.collector.batches) - batches_before,
                1,
                f"batch {batch} should paginate across multiple pages",
            )
            batches_before = len(self.collector.batches)

        self.assertEqual(len(self.collector.ids), len(set(self.collector.ids)))
        self.assertGreater(bookmark.get_bookmark(), 0)

    def test_late_arriving_events_picked_up_on_subsequent_cycles(self):
        """Documents indexed after an empty poll are delivered on the next cycle."""
        base_time = es_test_support.current_base_time()
        empty_bookmark = poller.poll_cycle(0, 0, False)
        self.assertEqual(self.collector.ids, [])
        self.assertEqual(empty_bookmark, 0)

        docs = es_test_support.build_batch_docs(0, 8, base_time=base_time)
        expected = es_test_support.bulk_seed(self.index, docs, es_url=ES_TEST_URL)

        bookmark_ms = poller.poll_cycle(0, 0, False)
        self.assertEqual(set(self.collector.ids), expected)
        self.assertGreater(bookmark_ms, 0)


if __name__ == "__main__":
    unittest.main()
