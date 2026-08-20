"""Live delivery integration tests against Edwin and optional LM Logs.

These tests use real credentials and are skipped unless explicitly enabled.
They seed a small number of documents into Elasticsearch and run poll_cycle
without mocking send_event.

Required for Edwin delivery:

    EDWIN_ORG, EDWIN_ID, EDWIN_TOKEN, ES_TEST_URL, ES_LIVE_DELIVERY=1

Optional for LM Logs shipping during the same run:

    LM_LOGS_ENABLED=true, LM_LOGS_BEARER_TOKEN
    LM_LOGS_ACCOUNT (defaults to EDWIN_ORG)

    ES_TEST_URL=http://localhost:9200 ES_LIVE_DELIVERY=1 \
      python -m unittest test_live_delivery.py -v
"""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from unittest.mock import patch

from edwin_elastic_poller import bookmark, config, mappings, poller
from edwin_elastic_poller.observability import lm_logs
from tests import es_test_support, patch_target, storage_patches

ES_TEST_URL = es_test_support.DEFAULT_ES_URL
LIVE_CREDENTIALS = config.has_edwin_credentials()

requires_es = unittest.skipUnless(
    ES_TEST_URL, "ES_TEST_URL not set; skipping live delivery tests"
)
requires_live = unittest.skipUnless(
    LIVE_CREDENTIALS and lm_logs.env_bool("ES_LIVE_DELIVERY"),
    "Set EDWIN_* (or DEXDA_*), and ES_LIVE_DELIVERY=1 for live tests",
)

LIVE_DOC_COUNT = 5
PAGE_SIZE = 3


def setUpModule():
    if os.getenv("ES_REQUIRE_INTEGRATION") and not ES_TEST_URL:
        raise RuntimeError(
            "ES_REQUIRE_INTEGRATION is set but ES_TEST_URL is empty"
        )
    if ES_TEST_URL and not mappings.mapping_file_path().exists():
        raise RuntimeError(
            "Bundled mapping file is missing; install the package or run from "
            "a checkout that includes edwin_elastic_poller/mappings/"
        )


@requires_es
@requires_live
class LiveDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.index = f"poller-live-{uuid.uuid4().hex[:8]}"
        es_test_support.wait_for_cluster(ES_TEST_URL)
        es_test_support.create_test_index(cls.index, shards=3, es_url=ES_TEST_URL)
        cls.addClassCleanup(
            es_test_support.delete_index, cls.index, es_url=ES_TEST_URL
        )

    def setUp(self):
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
            "ELASTIC_PIT_KEEP_ALIVE": "2m",
            "ELASTIC_OVERLAP_MS": 300000,
            **storage_patches(self.temp_dir.name, "live.bookmark"),
        }
        for name, value in patches.items():
            patcher = patch.object(patch_target(name), name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        bookmark.set_bookmark(0)

        if lm_logs.env_bool("LM_LOGS_ENABLED") and os.getenv("LM_LOGS_BEARER_TOKEN"):
            config.logger = lm_logs.configure_logging(
                debug=lm_logs.env_bool("DEBUG"),
                log_enabled=True,
                lm_logs_enabled=True,
                lm_logs_account=os.getenv("LM_LOGS_ACCOUNT") or config.edwin_org(),
                lm_logs_bearer_token=os.getenv("LM_LOGS_BEARER_TOKEN"),
                lm_logs_resource_id=os.getenv("LM_LOGS_RESOURCE_ID"),
            )
        elif os.getenv("LM_LOGS_BEARER_TOKEN"):
            # Bearer token without LM_LOGS_ENABLED still needs logger setup for tests.
            config.logger = lm_logs.configure_logging(
                debug=lm_logs.env_bool("DEBUG"),
                log_enabled=True,
                lm_logs_enabled=True,
                lm_logs_account=os.getenv("LM_LOGS_ACCOUNT") or config.edwin_org(),
                lm_logs_bearer_token=os.getenv("LM_LOGS_BEARER_TOKEN"),
                lm_logs_resource_id=os.getenv("LM_LOGS_RESOURCE_ID"),
            )

    def test_live_delivery_across_two_poll_cycles(self):
        """Deliver a small batch to Edwin, seed more docs, deliver again."""
        base_time = es_test_support.current_base_time()
        first_docs = es_test_support.build_batch_docs(
            0,
            LIVE_DOC_COUNT,
            base_time=base_time,
            action_prefix="live-delivery",
        )
        es_test_support.bulk_seed(self.index, first_docs, es_url=ES_TEST_URL)

        bookmark_ms = poller.poll_cycle(0, 0, False)
        self.assertGreater(bookmark_ms, 0)
        self.assertEqual(bookmark.get_bookmark(), bookmark_ms)

        second_docs = es_test_support.build_batch_docs(
            1,
            LIVE_DOC_COUNT,
            base_time=base_time,
            action_prefix="live-delivery",
        )
        es_test_support.bulk_seed(self.index, second_docs, es_url=ES_TEST_URL)

        next_bookmark = poller.poll_cycle(bookmark_ms, 0, True)
        self.assertGreater(next_bookmark, bookmark_ms)


if __name__ == "__main__":
    unittest.main()
