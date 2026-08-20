"""Integration tests for edwin_elastic_poller against a real Elasticsearch.

These exist because the unit suite structurally cannot catch a query body that
Elasticsearch rejects: every ES-touching unit test mocks the transport, so the
query is only ever asserted against itself. That is how the `_shard_doc` sort
shipped without a point-in-time and failed with HTTP 400 on every poll.

Skipped unless ES_TEST_URL is set, so `python -m unittest` still passes with no
container. Set ES_REQUIRE_INTEGRATION=1 in CI so a broken URL fails the job
instead of silently skipping every test.

    docker run -d --name es-test -p 9200:9200 \
      -e discovery.type=single-node -e xpack.security.enabled=false \
      -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
      docker.elastic.co/elasticsearch/elasticsearch:8.19.20

    ES_TEST_URL=http://localhost:9200 ES_REQUIRE_INTEGRATION=1 \
      python -m unittest test_integration_elasticsearch.py -v

Must be run from the repository root or with the package installed.
"""

import json
import os
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import requests

from edwin_elastic_poller import bookmark, elasticsearch, mappings, poller
from tests import es_test_support, patch_target, storage_patches


ES_TEST_URL = (os.getenv("ES_TEST_URL") or "").rstrip("/")

requires_es = unittest.skipUnless(
    ES_TEST_URL, "ES_TEST_URL not set; skipping Elasticsearch integration tests"
)

INDEX = f"poller-it-{uuid.uuid4().hex[:8]}"

# 25 documents sharing one identical millisecond. This is the shape that breaks
# a timestamp-only sort: without a stable tie-breaker, paginating through them
# skips or repeats rows.
IDENTICAL_COUNT = 25
TAIL_COUNT = 5
(
    IDENTICAL_TS,
    IDENTICAL_TS_MS,
    TAIL_START,
    LAST_TAIL_MS,
) = es_test_support.integration_fixture_times(tail_count=TAIL_COUNT)

PAGE_SIZE = 3  # 30 docs over ~11 pages, so pagination is genuinely exercised


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


def _doc(timestamp, index, action):
    """A document shaped to map cleanly through elastic_event_mappings.yaml.

    rule.name must contain no comma: process_hits treats a comma in event_ci as
    a multi-CI marker and truncates it.
    """
    return {
        "@timestamp": timestamp,
        "message": f"integration test event {index}",
        "event": {
            "provider": "alerting",
            "action": action,
            "kind": "alert",
            "category": ["logs"],
        },
        "rule": {
            "id": f"rule-{index}",
            "name": f"integration-rule-{index}",
            "category": "logs.alert.document.count",
            "license": "basic",
        },
        "kibana": {
            "alert": {"rule": {"rule_type_id": "logs.alert.document.count"}},
            "space_ids": ["itspace"],
        },
    }


class _Collector:
    """Stands in for send_event; records every batch it is handed."""

    def __init__(self, fail_on_batch=None):
        self.batches = []
        self.ids = []
        self.fail_on_batch = fail_on_batch

    def __call__(self, event_list):
        self.batches.append(event_list)
        if self.fail_on_batch is not None and len(self.batches) > self.fail_on_batch:
            return False
        # process_hits forces event_source_id to the Elasticsearch _id.
        self.ids.extend(event["cef"]["event_source_id"] for event in event_list)
        return True


@requires_es
class ElasticsearchIntegrationTests(unittest.TestCase):
    seeded_ids = set()

    @classmethod
    def setUpClass(cls):
        cls._wait_for_cluster()
        cls.es_version = cls._server_version()
        cls.addClassCleanup(cls._delete_index)
        cls._create_index()
        cls._seed()

    @classmethod
    def _server_version(cls):
        response = requests.get(ES_TEST_URL, timeout=15)
        response.raise_for_status()
        raw = response.json()["version"]["number"]
        major, minor = raw.split(".")[:2]
        return (int(major), int(minor), raw)

    @classmethod
    def _wait_for_cluster(cls, timeout=120):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                response = requests.get(
                    f"{ES_TEST_URL}/_cluster/health",
                    params={"wait_for_status": "yellow", "timeout": "5s"},
                    timeout=10,
                )
                if response.ok and response.json().get("status") in ("green", "yellow"):
                    return
                last = response.text
            except requests.RequestException as exc:
                last = str(exc)
            time.sleep(2)
        raise AssertionError(f"Elasticsearch not ready at {ES_TEST_URL}: {last}")

    @classmethod
    def _create_index(cls):
        body = {
            "settings": {
                # Three shards is the point of this suite. _shard_doc tie-breaking
                # only matters when documents sharing a timestamp land on
                # different shards; a single-shard index would pass even with the
                # bug present.
                "number_of_shards": 3,
                "number_of_replicas": 0,
            },
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "message": {"type": "text"},
                    "event": {
                        "properties": {
                            "provider": {"type": "keyword"},
                            "action": {"type": "keyword"},
                            "kind": {"type": "keyword"},
                            "category": {"type": "keyword"},
                        }
                    },
                    "rule": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "name": {"type": "keyword"},
                            "category": {"type": "keyword"},
                            "license": {"type": "keyword"},
                        }
                    },
                    "kibana": {
                        "properties": {
                            "space_ids": {"type": "keyword"},
                            "alert": {
                                "properties": {
                                    "rule": {
                                        "properties": {
                                            "rule_type_id": {"type": "keyword"}
                                        }
                                    }
                                }
                            },
                        }
                    },
                }
            },
        }
        response = requests.put(f"{ES_TEST_URL}/{INDEX}", json=body, timeout=30)
        response.raise_for_status()

    @classmethod
    def _seed(cls):
        docs = [
            (f"same-{i:03d}", _doc(IDENTICAL_TS, i, "same-batch"))
            for i in range(IDENTICAL_COUNT)
        ]
        for i in range(TAIL_COUNT):
            stamp = TAIL_START + timedelta(seconds=i)
            timestamp = stamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            docs.append((f"tail-{i:03d}", _doc(timestamp, i, "tail-only")))

        lines = []
        for doc_id, source in docs:
            lines.append(json.dumps({"index": {"_index": INDEX, "_id": doc_id}}))
            lines.append(json.dumps(source))
        payload = "\n".join(lines) + "\n"

        response = requests.post(
            f"{ES_TEST_URL}/_bulk",
            params={"refresh": "wait_for"},
            data=payload.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=60,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise AssertionError(f"bulk seed failed: {json.dumps(body)[:2000]}")
        cls.seeded_ids = {doc_id for doc_id, _ in docs}

    @classmethod
    def _delete_index(cls):
        try:
            requests.delete(f"{ES_TEST_URL}/{INDEX}", timeout=30)
        except requests.RequestException:
            pass

    def setUp(self):
        self._patch("ELASTIC_URL", ES_TEST_URL)
        self._patch("ELASTIC_INDEX", INDEX)
        self._patch("ELASTIC_VERIFY_SSL", False)
        self._patch("ELASTIC_BATCH_SIZE", PAGE_SIZE)
        self._patch("ELASTIC_QUERY", "*")
        self._patch("ELASTIC_USER", None)
        self._patch("ELASTIC_PASS", None)
        self._patch("ELASTIC_TOKEN", None)
        self._patch("ELASTIC_PIT_KEEP_ALIVE", "1m")

        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        for name, value in storage_patches(temp_dir.name, "it.elastic.bookmark").items():
            self._patch(name, value)

        self.collector = _Collector()
        self._patch("send_event", self.collector)

    def _patch(self, name, value):
        patcher = patch.object(patch_target(name), name, value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _open_contexts(self):
        response = requests.get(
            f"{ES_TEST_URL}/_nodes/stats/indices/search", timeout=15
        )
        response.raise_for_status()
        return sum(
            node["indices"]["search"]["open_contexts"]
            for node in response.json()["nodes"].values()
        )

    def _assert_contexts_return_to(self, baseline, timeout=10):
        """ES frees contexts on DELETE /_pit, but allow a moment for stats to settle."""
        deadline = time.monotonic() + timeout
        current = None
        while time.monotonic() < deadline:
            current = self._open_contexts()
            if current <= baseline:
                return
            time.sleep(0.5)
        self.fail(f"point-in-time leaked: {current} open contexts, expected <= {baseline}")

    # --- preconditions -----------------------------------------------------

    def test_shards_actually_hold_documents(self):
        """Guard: if documents land on one shard, every assertion below is vacuous."""
        response = requests.get(
            f"{ES_TEST_URL}/{INDEX}/_stats", params={"level": "shards"}, timeout=30
        )
        response.raise_for_status()
        shards = response.json()["indices"][INDEX]["shards"]
        populated = [
            shard_id
            for shard_id, copies in shards.items()
            if copies[0]["docs"]["count"] > 0
        ]
        self.assertGreaterEqual(
            len(populated),
            2,
            f"documents must span shards for _shard_doc to matter; got {shards}",
        )

    # --- the bug itself ----------------------------------------------------

    def test_shard_doc_without_pit_behaviour_is_version_dependent(self):
        """Negative control: the exact body that shipped, pinned per version.

        Elastic relaxed this validation during the 8.x line:

          * 8.11.4  -> HTTP 400 "[_shard_doc] sort field cannot be used without
            [point in time]" -- the reported failure.
          * 8.19.20, 9.5.1 -> HTTP 200. The query is accepted, but _shard_doc is
            only meaningful within one search context, so using it as a
            search_after tie-breaker across separate requests is unstable. On
            these versions the same bug was silent rather than loud.

        Either way the fix is the same and this test records which mode the
        server under test is in, so a future relaxation (or re-tightening) shows
        up as a deliberate update here rather than a mystery.
        """
        body = {
            "size": 3,
            "query": {"match_all": {}},
            "sort": [{"@timestamp": {"order": "asc"}}, {"_shard_doc": "asc"}],
        }
        response = requests.post(f"{ES_TEST_URL}/{INDEX}/_search", json=body, timeout=30)

        major, minor, raw = self.es_version
        if (major, minor) <= (8, 11):
            self.assertEqual(response.status_code, 400, response.text)
            self.assertIn("point in time", response.text)
        else:
            self.assertEqual(
                response.status_code,
                200,
                f"ES {raw} unexpectedly rejected _shard_doc without a PIT; "
                f"the version boundary in this test needs revisiting: {response.text}",
            )

    def test_real_query_is_accepted_by_elasticsearch(self):
        """The regression the unit suite structurally cannot catch."""
        pit_id = elasticsearch.open_point_in_time(
            INDEX, **elasticsearch._es_conn_kwargs()
        )
        self.addCleanup(
            elasticsearch.close_point_in_time, pit_id, **elasticsearch._es_conn_kwargs()
        )

        hits, _took, returned_pit = elasticsearch.fetch_elasticsearch_hits(
            0, pit_id=pit_id
        )

        self.assertEqual(len(hits), PAGE_SIZE)
        self.assertTrue(returned_pit)
        for hit in hits:
            self.assertEqual(
                len(hit["sort"]), 2, "expected timestamp + _shard_doc sort values"
            )

    # --- pagination correctness -------------------------------------------

    def test_poll_cycle_collects_every_doc_exactly_once(self):
        result = poller.poll_cycle(0, 0, False)

        collected = self.collector.ids
        self.assertEqual(
            len(collected), len(set(collected)), "duplicate documents delivered"
        )
        self.assertEqual(
            set(collected), self.seeded_ids, "documents dropped or unexpected extras"
        )
        self.assertGreater(
            len(self.collector.batches), 5, "pagination did not actually engage"
        )
        self.assertEqual(result, LAST_TAIL_MS)

    def test_bookmark_advances_to_last_timestamp(self):
        result = poller.poll_cycle(0, 0, False)
        self.assertEqual(result, LAST_TAIL_MS)
        self.assertEqual(bookmark.get_bookmark(), LAST_TAIL_MS)

    def test_bookmark_not_advanced_on_delivery_failure(self):
        collector = _Collector(fail_on_batch=1)
        self._patch("send_event", collector)
        bookmark.set_bookmark(0)

        result = poller.poll_cycle(0, 0, False)

        # Only the first batch was accepted, so the bookmark stops there.
        self.assertEqual(result, IDENTICAL_TS_MS)
        self.assertEqual(bookmark.get_bookmark(), IDENTICAL_TS_MS)
        self.assertLess(len(collector.ids), len(self.seeded_ids))

    def test_query_string_filter_is_applied(self):
        self._patch("ELASTIC_QUERY", "event.action:tail-only")
        poller.poll_cycle(0, 0, False)
        self.assertEqual(
            set(self.collector.ids),
            {f"tail-{i:03d}" for i in range(TAIL_COUNT)},
        )

    # --- point-in-time lifecycle ------------------------------------------

    def test_pit_is_released_after_successful_cycle(self):
        baseline = self._open_contexts()
        poller.poll_cycle(0, 0, False)
        self._assert_contexts_return_to(baseline)

    def test_pit_is_released_when_delivery_fails(self):
        """The leak the try/finally exists to prevent."""
        self._patch("send_event", _Collector(fail_on_batch=0))
        baseline = self._open_contexts()
        poller.poll_cycle(0, 0, False)
        self._assert_contexts_return_to(baseline)

    def test_close_point_in_time_reports_succeeded(self):
        conn = elasticsearch._es_conn_kwargs()
        pit_id = elasticsearch.open_point_in_time(INDEX, **conn)
        self.assertTrue(elasticsearch.close_point_in_time(pit_id, **conn))
        # Closing an already-released PIT must not raise.
        self.assertFalse(elasticsearch.close_point_in_time(pit_id, **conn))


@requires_es
class MultiIndexIntegrationTests(unittest.TestCase):
    """ELASTIC_INDEXS is interpolated straight into the Elasticsearch path.

    The plural name is not a misnomer: ES path syntax accepts a comma-separated
    list and wildcards, so one setting has always been able to span several
    indices. The original upstream readme documented it as `.ds-file*`.

    Moving to a point-in-time changes which endpoint receives that value, so
    these tests pin that _pit accepts the same syntax _search does. Without
    them, a future change could quietly reduce the poller to a single index.
    """

    PREFIX = f"poller-multi-{uuid.uuid4().hex[:8]}"

    @classmethod
    def setUpClass(cls):
        cls.indices = [f"{cls.PREFIX}-{suffix}" for suffix in ("alpha", "beta")]
        cls.addClassCleanup(cls._cleanup)
        lines = []
        for position, index in enumerate(cls.indices):
            response = requests.put(
                f"{ES_TEST_URL}/{index}",
                json={"settings": {"number_of_shards": 2, "number_of_replicas": 0}},
                timeout=30,
            )
            response.raise_for_status()
            for i in range(4):
                doc_id = f"{index}-{i}"
                stamp = TAIL_START + timedelta(seconds=position * 10 + i)
                timestamp = stamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}))
                lines.append(json.dumps(_doc(timestamp, i, "multi")))

        response = requests.post(
            f"{ES_TEST_URL}/_bulk",
            params={"refresh": "wait_for"},
            data=("\n".join(lines) + "\n").encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=60,
        )
        response.raise_for_status()
        if response.json().get("errors"):
            raise AssertionError("bulk seed failed for multi-index fixtures")
        cls.expected_ids = {
            f"{index}-{i}" for index in cls.indices for i in range(4)
        }

    @classmethod
    def _cleanup(cls):
        try:
            requests.delete(f"{ES_TEST_URL}/{cls.PREFIX}-*", timeout=30)
        except requests.RequestException:
            pass

    def _run_cycle_over(self, index_expression):
        collector = _Collector()
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        patches = {
            "ELASTIC_URL": ES_TEST_URL,
            "ELASTIC_INDEX": index_expression,
            "ELASTIC_VERIFY_SSL": False,
            "ELASTIC_BATCH_SIZE": 3,
            "ELASTIC_QUERY": "*",
            "ELASTIC_USER": None,
            "ELASTIC_PASS": None,
            "ELASTIC_TOKEN": None,
            "ELASTIC_OVERLAP_MS": 300000,
            "send_event": collector,
            **storage_patches(temp_dir.name, "multi.bookmark"),
        }
        for name, value in patches.items():
            patcher = patch.object(patch_target(name), name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        poller.poll_cycle(0, 0, False)
        return collector

    def test_comma_separated_indices_are_all_polled(self):
        collector = self._run_cycle_over(",".join(self.indices))
        self.assertEqual(set(collector.ids), self.expected_ids)

    def test_wildcard_index_pattern_is_expanded(self):
        collector = self._run_cycle_over(f"{self.PREFIX}-*")
        self.assertEqual(set(collector.ids), self.expected_ids)


if __name__ == "__main__":
    unittest.main()
