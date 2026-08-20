"""Unit tests for edwin_elastic_poller bookmark, query, pagination, and event mapping."""

import json
import os
import shutil
import tempfile
import unittest
import unittest.mock
from unittest.mock import patch

import requests

from edwin_elastic_poller import bookmark, config, dedupe, delivery, elasticsearch, poller, storage_paths
from edwin_elastic_poller import mappings
from edwin_elastic_poller.observability import lm_logs
from edwin_elastic_poller.sdk import common_event
from tests import storage_patches


SAMPLE_HIT = {
    "_index": ".ds-.kibana-event-log-ds-2026.08.08-000027",
    "_id": "43365083-0423-40ea-b045-7362858aad31",
    "_score": None,
    "_source": {
        "@timestamp": "2026-08-13T17:14:18.365Z",
        "event": {
            "provider": "alerting",
            "action": "execute-start",
            "kind": "alert",
            "category": ["logs"],
            "start": "2026-08-13T17:14:18.365Z",
        },
        "kibana": {
            "alert": {
                "rule": {
                    "rule_type_id": "logs.alert.document.count",
                    "consumer": "alerts",
                    "execution": {
                        "uuid": "f025a6bf-38fb-45e3-b477-9555fc64b642",
                    },
                },
            },
            "saved_objects": [
                {
                    "rel": "primary",
                    "type": "alert",
                    "id": "c7707340-424b-11ee-ae1d-b3a96bbcb023",
                    "type_id": "logs.alert.document.count",
                    "namespace": "cnrwm",
                }
            ],
            "space_ids": ["cnrwm"],
        },
        "rule": {
            "id": "c7707340-424b-11ee-ae1d-b3a96bbcb023",
            "license": "basic",
            "category": "logs.alert.document.count",
            "ruleset": "logs",
        },
        "message": 'rule execution start: "c7707340-424b-11ee-ae1d-b3a96bbcb023"',
    },
    "sort": [1786641258365, 0],
}


class BuildLogsQueryTests(unittest.TestCase):
    def test_uses_exclusive_gt_with_epoch_millis(self):
        bookmark_ms = 1786641258556
        query = elasticsearch.build_logs_query(text="*", bookmark_ms=bookmark_ms, size=500)

        range_filter = query["query"]["bool"]["filter"][0]["range"]["@timestamp"]
        self.assertEqual(range_filter["gt"], bookmark_ms)
        self.assertEqual(range_filter["format"], "epoch_millis")
        self.assertNotIn("gte", range_filter)

    def test_sort_omits_shard_doc_without_pit(self):
        """_shard_doc outside a PIT is what ES rejects with a 400."""
        query = elasticsearch.build_logs_query(text="*", bookmark_ms=0, size=500)
        self.assertEqual(query["sort"], [{"@timestamp": {"order": "asc"}}])
        self.assertNotIn("pit", query)

    def test_sort_includes_shard_doc_when_pit_supplied(self):
        query = elasticsearch.build_logs_query(
            text="*", bookmark_ms=0, size=500, pit_id="pit-abc"
        )
        self.assertEqual(
            query["sort"],
            [{"@timestamp": {"order": "asc"}}, {"_shard_doc": "asc"}],
        )

    def test_search_after_included_when_provided(self):
        search_after = [1786641258365, "doc-id"]
        query = elasticsearch.build_logs_query(
            text="*",
            bookmark_ms=1786641258000,
            size=500,
            search_after=search_after,
        )
        self.assertEqual(query["search_after"], search_after)

    def test_bookmark_ms_not_truncated_to_seconds(self):
        """Bookmark 8556ms must not collapse to same query as 8000ms."""
        query_a = elasticsearch.build_logs_query(text="*", bookmark_ms=1786641258556, size=500)
        query_b = elasticsearch.build_logs_query(text="*", bookmark_ms=1786641258000, size=500)
        self.assertNotEqual(
            query_a["query"]["bool"]["filter"][0]["range"]["@timestamp"]["gt"],
            query_b["query"]["bool"]["filter"][0]["range"]["@timestamp"]["gt"],
        )

    def test_custom_query_string_is_passed_through(self):
        query = elasticsearch.build_logs_query(
            text="NOT event.action:execute-start",
            bookmark_ms=0,
            size=500,
        )
        self.assertEqual(
            query["query"]["bool"]["must"][0]["query_string"]["query"],
            "NOT event.action:execute-start",
        )


class HitTimestampTests(unittest.TestCase):
    def test_hit_timestamp_ms_from_iso_string(self):
        self.assertEqual(
            elasticsearch.hit_timestamp_ms(SAMPLE_HIT),
            1786641258365,
        )


class GlobalPatchMixin:
    """Rebind edwin_elastic_poller settings for the duration of one test."""

    CONFIG_ATTRS = {
        "ELASTIC_URL",
        "ELASTIC_INDEX",
        "ELASTIC_BATCH_SIZE",
        "ELASTIC_QUERY",
        "ELASTIC_USER",
        "ELASTIC_PASS",
        "ELASTIC_TOKEN",
        "ELASTIC_VERIFY_SSL",
        "ELASTIC_PIT_KEEP_ALIVE",
        "ELASTIC_OVERLAP_MS",
        "BOOKMARK_PATH",
        "DEDUPE_MAX_RECORDS",
        "DEDUPE_MAX_SIZE_MB",
        "EDWIN_ORG",
        "EDWIN_ID",
        "EDWIN_TOKEN",
    }
    STORAGE_ATTRS = {"data_dir", "bookmark_file", "dedupe_db_path"}

    def _patch(self, name, value, target=None):
        if target is None:
            if name in self.CONFIG_ATTRS:
                target = config
            elif name in self.STORAGE_ATTRS:
                target = storage_paths
            else:
                import edwin_elastic_poller

                target = edwin_elastic_poller
        patcher = patch.object(target, name, value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _use_temp_bookmark(self, filename="testorg.elastic.bookmark"):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        for name, value in storage_patches(temp_dir.name, filename).items():
            self._patch(name, value)
        return temp_dir


class BookmarkFileTests(GlobalPatchMixin, unittest.TestCase):
    def setUp(self):
        self._use_temp_bookmark()

    def test_set_and_get_bookmark_roundtrip(self):
        bookmark.set_bookmark(1786641258556)
        self.assertEqual(bookmark.get_bookmark(), 1786641258556)

    def test_get_bookmark_creates_file_with_zero(self):
        self.assertEqual(bookmark.get_bookmark(), 0)

    def test_corrupt_bookmark_raises_actionable_error(self):
        with open(storage_paths.bookmark_file(), "w", encoding="utf-8") as fh:
            fh.write("not-a-timestamp")
        with self.assertRaises(bookmark.BookmarkError):
            bookmark.get_bookmark()

    def test_dedupe_maintains_maximum_record_count(self):
        self._patch("DEDUPE_MAX_RECORDS", 2)
        self._patch("DEDUPE_MAX_SIZE_MB", 256)
        hits = [
            dict(SAMPLE_HIT, _id=f"dedupe-{index}")
            for index in range(3)
        ]
        dedupe.mark_delivered(
            hits, lambda _hit: 1786641258365
        )

        self.assertEqual(
            dedupe.maintain(0),
            1,
        )
        self.assertFalse(dedupe.is_delivered(hits[0]))
        self.assertTrue(dedupe.is_delivered(hits[2]))


class PollCycleTests(GlobalPatchMixin, unittest.TestCase):
    def setUp(self):
        self._use_temp_bookmark()
        self._patch("ELASTIC_BATCH_SIZE", 500)
        self._patch("ELASTIC_INDEX", "test-index")
        self._patch("ELASTIC_URL", "http://es.invalid:9200")
        # poll_cycle now opens a real PIT; stub the lifecycle for these tests.
        self.mock_open_pit = self._patch_call("open_point_in_time", return_value="pit-1")
        self.mock_close_pit = self._patch_call("close_point_in_time", return_value=True)

    def _patch_call(self, name, **kwargs):
        patcher = patch.object(elasticsearch, name, **kwargs)
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    @patch.object(delivery, "send_event", return_value=True)
    @patch.object(elasticsearch, "fetch_elasticsearch_hits")
    def test_poll_cycle_advances_bookmark_on_success(self, mock_fetch, mock_send):
        mock_fetch.return_value = ([SAMPLE_HIT], 5, "pit-1")
        result = poller.poll_cycle(1786641258000, 1786641258000, True)
        self.assertEqual(result, 1786641258365)
        self.assertEqual(bookmark.get_bookmark(), 1786641258365)
        mock_send.assert_called_once()

    @patch.object(lm_logs, "log_with_context")
    @patch.object(delivery, "send_event", return_value=True)
    @patch.object(elasticsearch, "fetch_elasticsearch_hits")
    def test_poll_cycle_emits_operational_summary(
        self, mock_fetch, mock_send, mock_log
    ):
        mock_fetch.return_value = ([SAMPLE_HIT], 5, "pit-1")
        poller.poll_cycle(1786641258000, 1786641258000, True)

        finished = [
            call
            for call in mock_log.call_args_list
            if len(call.args) > 2
            and str(call.args[2]).startswith("Poll cycle finished")
        ]
        self.assertEqual(len(finished), 1)
        summary = finished[0].kwargs
        self.assertEqual(summary["status"], "complete")
        self.assertTrue(summary["bookmark_advanced"])
        self.assertEqual(summary["events_delivered"], 1)
        self.assertEqual(summary["pages_fetched"], 1)
        self.assertFalse(summary["errors_encountered"])

    @patch.object(lm_logs, "log_with_context")
    @patch.object(delivery, "send_event", return_value=False)
    @patch.object(elasticsearch, "fetch_elasticsearch_hits")
    def test_poll_cycle_summary_records_delivery_failure(
        self, mock_fetch, mock_send, mock_log
    ):
        mock_fetch.return_value = ([SAMPLE_HIT], 5, "pit-1")
        poller.poll_cycle(1786641258000, 1786641258000, True)

        finished = [
            call
            for call in mock_log.call_args_list
            if len(call.args) > 2
            and str(call.args[2]).startswith("Poll cycle finished")
        ]
        self.assertEqual(len(finished), 1)
        summary = finished[0].kwargs
        self.assertEqual(summary["status"], "delivery_failed")
        self.assertTrue(summary["errors_encountered"])
        self.assertIn("edwin_delivery_failed", summary["issues"])
        self.assertFalse(summary["bookmark_advanced"])

    @patch.object(delivery, "send_event", return_value=False)
    @patch.object(elasticsearch, "fetch_elasticsearch_hits")
    def test_poll_cycle_does_not_advance_bookmark_on_delivery_failure(self, mock_fetch, mock_send):
        bookmark.set_bookmark(1786641258000)
        mock_fetch.return_value = ([SAMPLE_HIT], 5, "pit-1")
        result = poller.poll_cycle(1786641258000, 1786641258000, True)
        self.assertEqual(result, 1786641258000)
        self.assertEqual(bookmark.get_bookmark(), 1786641258000)

    @patch.object(delivery, "send_event", return_value=True)
    @patch.object(elasticsearch, "fetch_elasticsearch_hits")
    def test_poll_cycle_paginates_with_search_after(self, mock_fetch, mock_send):
        self._patch("ELASTIC_BATCH_SIZE", 2)
        hit_a = dict(SAMPLE_HIT)
        hit_b = dict(SAMPLE_HIT, _id="bbbb", sort=[1786641258365, 1])
        hit_c = dict(SAMPLE_HIT, _id="cccc", sort=[1786641259000, 2])
        hit_c["_source"] = dict(SAMPLE_HIT["_source"])
        hit_c["_source"]["@timestamp"] = "2026-08-13T17:14:19.000Z"

        mock_fetch.side_effect = [
            ([hit_a, hit_b], 5, "pit-2"),
            ([hit_c], 3, "pit-2"),
        ]

        result = poller.poll_cycle(1786641258000, 1786641258000, True)
        self.assertEqual(mock_fetch.call_count, 2)
        self.assertEqual(mock_fetch.call_args_list[1].kwargs["search_after"], [1786641258365, 1])
        self.assertEqual(mock_fetch.call_args_list[1].kwargs["pit_id"], "pit-2")
        self.assertEqual(result, 1786641259000)

    @patch.object(delivery, "send_event", return_value=True)
    @patch.object(elasticsearch, "fetch_elasticsearch_hits")
    def test_overlap_delivers_late_document_without_redelivering_old_document(
        self, mock_fetch, mock_send
    ):
        late_hit = dict(SAMPLE_HIT, _id="late-document")
        mock_fetch.return_value = ([SAMPLE_HIT], 1, "pit-1")
        first_bookmark = poller.poll_cycle(
            1786641258000, 1786641258000, True
        )

        mock_fetch.reset_mock()
        mock_fetch.return_value = ([SAMPLE_HIT, late_hit], 1, "pit-1")
        second_bookmark = poller.poll_cycle(
            first_bookmark, first_bookmark, True
        )

        self.assertEqual(mock_send.call_count, 2)
        self.assertEqual(len(mock_send.call_args_list[1].args[0]), 1)
        self.assertEqual(second_bookmark, 1786641258365)


class BuildLogsQueryPitTests(unittest.TestCase):
    def test_pit_block_carries_id_and_keep_alive(self):
        query = elasticsearch.build_logs_query(
            text="*", bookmark_ms=0, size=500, pit_id="pit-abc", keep_alive="2m"
        )
        self.assertEqual(query["pit"], {"id": "pit-abc", "keep_alive": "2m"})

    def test_keep_alive_defaults_to_module_constant(self):
        query = elasticsearch.build_logs_query(
            text="*", bookmark_ms=0, size=500, pit_id="pit-abc"
        )
        self.assertEqual(
            query["pit"]["keep_alive"], config.ELASTIC_PIT_KEEP_ALIVE
        )

    def test_shard_doc_present_if_and_only_if_pit_present(self):
        """The invariant the reported bug violated, asserted directly.

        ES only materializes _shard_doc inside a point-in-time, so the two must
        appear together or not at all.
        """
        for pit_id in (None, "", "pit-abc"):
            with self.subTest(pit_id=pit_id):
                query = elasticsearch.build_logs_query(
                    text="*", bookmark_ms=0, size=500, pit_id=pit_id
                )
                has_shard_doc = "_shard_doc" in json.dumps(query["sort"])
                self.assertEqual(has_shard_doc, "pit" in query)
                self.assertEqual(has_shard_doc, bool(pit_id))

    def test_search_after_and_pit_coexist(self):
        query = elasticsearch.build_logs_query(
            text="*",
            bookmark_ms=0,
            size=500,
            pit_id="pit-abc",
            search_after=[1786641258365, 7],
        )
        self.assertEqual(query["search_after"], [1786641258365, 7])
        self.assertIn("pit", query)


def _fake_response(status_code=200, payload=None, text=""):
    """Minimal stand-in for requests.Response."""
    response = unittest.mock.Mock()
    response.status_code = status_code
    response.text = text or json.dumps(payload or {})
    response.content = b"x"
    response.json.return_value = payload if payload is not None else {}
    response.raise_for_status.return_value = None
    return response


def _error_response(status_code, text):
    """A response whose raise_for_status raises, as requests does on 4xx/5xx."""
    response = _fake_response(status_code=status_code, payload={}, text=text)
    error = requests.HTTPError(f"{status_code} error", response=response)
    response.raise_for_status.side_effect = error
    return response


class EsRequestTests(unittest.TestCase):
    """Covers the wire-level behaviour no test previously exercised."""

    def setUp(self):
        patcher = patch.object(elasticsearch.requests, "request")
        self.mock_request = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_request.return_value = _fake_response(payload={"ok": True})

    def _called_url(self):
        return self.mock_request.call_args.args[1]

    def test_search_url_includes_index_when_no_pit(self):
        with patch.object(config, "ELASTIC_URL", "http://es.invalid:9200"):
            elasticsearch.query_elasticsearch("my-index", {"size": 1}, verify_ssl=False)
        self.assertTrue(self._called_url().endswith("/my-index/_search"))

    def test_search_url_omits_index_when_pit_active(self):
        """A PIT pins the index set; naming it in the path too is an error."""
        with patch.object(config, "ELASTIC_URL", "http://es.invalid:9200"):
            elasticsearch.query_elasticsearch(None, {"size": 1}, verify_ssl=False)
        self.assertTrue(self._called_url().endswith("/_search"))
        self.assertNotIn("my-index", self._called_url())

    def test_api_key_sets_authorization_header(self):
        elasticsearch._es_request(
            "GET", "_cluster/health", base_url="http://es.invalid:9200", api_key="k1"
        )
        headers = self.mock_request.call_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "ApiKey k1")
        self.assertIsNone(self.mock_request.call_args.kwargs["auth"])

    def test_basic_auth_sets_auth_tuple(self):
        elasticsearch._es_request(
            "GET",
            "_cluster/health",
            base_url="http://es.invalid:9200",
            username="u",
            password="p",
        )
        self.assertEqual(self.mock_request.call_args.kwargs["auth"], ("u", "p"))

    def test_api_key_and_password_together_raises_value_error(self):
        with self.assertRaises(ValueError):
            elasticsearch._es_request(
                "GET",
                "_cluster/health",
                base_url="http://es.invalid:9200",
                api_key="k1",
                username="u",
                password="p",
            )

    def test_short_base_url_raises_value_error(self):
        with self.assertRaises(ValueError):
            elasticsearch._es_request("GET", "_cluster/health", base_url="")

    def test_http_error_wrapped_with_status_and_body(self):
        body = (
            '{"error":{"root_cause":[{"type":"illegal_argument_exception",'
            '"reason":"[_shard_doc] sort field cannot be used without [point in time]"}]}}'
        )
        self.mock_request.return_value = _error_response(400, body)
        with self.assertRaises(elasticsearch.ElasticsearchQueryError) as ctx:
            elasticsearch._es_request(
                "POST", "idx/_search", body={}, base_url="http://es.invalid:9200"
            )
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("point in time", str(ctx.exception))
        self.assertFalse(ctx.exception.is_missing_context)

    def test_404_body_sets_is_missing_context(self):
        self.mock_request.return_value = _error_response(
            404, '{"error":{"type":"search_context_missing_exception"}}'
        )
        with self.assertRaises(elasticsearch.ElasticsearchQueryError) as ctx:
            elasticsearch._es_request(
                "POST", "_search", body={}, base_url="http://es.invalid:9200"
            )
        self.assertTrue(ctx.exception.is_missing_context)


class PointInTimeTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(elasticsearch.requests, "request")
        self.mock_request = patcher.start()
        self.addCleanup(patcher.stop)
        self.conn = {"base_url": "http://es.invalid:9200", "verify_ssl": False}

    def test_open_posts_to_index_pit_with_keep_alive_param(self):
        self.mock_request.return_value = _fake_response(payload={"id": "pit-abc"})
        pit_id = elasticsearch.open_point_in_time(
            "my-index", keep_alive="5m", **self.conn
        )
        self.assertEqual(pit_id, "pit-abc")
        self.assertEqual(self.mock_request.call_args.args[0], "POST")
        self.assertTrue(self.mock_request.call_args.args[1].endswith("/my-index/_pit"))
        self.assertEqual(
            self.mock_request.call_args.kwargs["params"], {"keep_alive": "5m"}
        )
        # Opening a PIT is a bodyless POST.
        self.assertIsNone(self.mock_request.call_args.kwargs["json"])

    def test_open_raises_when_no_id_returned(self):
        self.mock_request.return_value = _fake_response(payload={})
        with self.assertRaises(elasticsearch.ElasticsearchQueryError):
            elasticsearch.open_point_in_time("my-index", **self.conn)

    def test_close_deletes_pit_with_id_body(self):
        self.mock_request.return_value = _fake_response(payload={"succeeded": True})
        self.assertTrue(elasticsearch.close_point_in_time("pit-abc", **self.conn))
        self.assertEqual(self.mock_request.call_args.args[0], "DELETE")
        self.assertTrue(self.mock_request.call_args.args[1].endswith("/_pit"))
        self.assertEqual(self.mock_request.call_args.kwargs["json"], {"id": "pit-abc"})

    def test_close_returns_false_on_404_without_raising(self):
        self.mock_request.return_value = _error_response(404, "gone")
        self.assertFalse(elasticsearch.close_point_in_time("pit-abc", **self.conn))

    def test_close_swallows_other_errors(self):
        """close runs in a finally and must never mask the original exception."""
        self.mock_request.return_value = _error_response(500, "boom")
        self.assertFalse(elasticsearch.close_point_in_time("pit-abc", **self.conn))

    def test_close_noop_on_none_id(self):
        self.assertFalse(elasticsearch.close_point_in_time(None, **self.conn))
        self.mock_request.assert_not_called()


class FetchHitsPitTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(elasticsearch, "query_elasticsearch")
        self.mock_query = patcher.start()
        self.addCleanup(patcher.stop)

    def test_returns_rotated_pit_id_from_response(self):
        self.mock_query.return_value = {
            "took": 3,
            "pit_id": "pit-2",
            "hits": {"hits": [SAMPLE_HIT]},
        }
        hits, took, pit_id = elasticsearch.fetch_elasticsearch_hits(0, pit_id="pit-1")
        self.assertEqual(pit_id, "pit-2")
        self.assertEqual(len(hits), 1)
        self.assertEqual(took, 3)

    def test_returns_input_pit_id_when_response_omits_it(self):
        self.mock_query.return_value = {"took": 1, "hits": {"hits": []}}
        _hits, _took, pit_id = elasticsearch.fetch_elasticsearch_hits(0, pit_id="pit-1")
        self.assertEqual(pit_id, "pit-1")

    def test_index_omitted_from_search_when_pit_supplied(self):
        self.mock_query.return_value = {"took": 1, "hits": {"hits": []}}
        elasticsearch.fetch_elasticsearch_hits(0, pit_id="pit-1")
        self.assertIsNone(self.mock_query.call_args.kwargs["index"])

    def test_index_used_when_no_pit(self):
        self.mock_query.return_value = {"took": 1, "hits": {"hits": []}}
        with patch.object(config, "ELASTIC_INDEX", "my-index"):
            elasticsearch.fetch_elasticsearch_hits(0)
        self.assertEqual(self.mock_query.call_args.kwargs["index"], "my-index")


class PollCyclePitLifecycleTests(GlobalPatchMixin, unittest.TestCase):
    def setUp(self):
        self._use_temp_bookmark()
        self._patch("ELASTIC_BATCH_SIZE", 2)
        self._patch("ELASTIC_INDEX", "test-index")
        self._patch("ELASTIC_URL", "http://es.invalid:9200")

        self.mock_open = self._start(
            patch.object(elasticsearch, "open_point_in_time")
        )
        self.mock_open.return_value = "pit-1"
        self.mock_close = self._start(
            patch.object(elasticsearch, "close_point_in_time", return_value=True)
        )
        self.mock_fetch = self._start(
            patch.object(elasticsearch, "fetch_elasticsearch_hits")
        )
        self.mock_send = self._start(
            patch.object(delivery, "send_event", return_value=True)
        )

    def _start(self, patcher):
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    def test_pit_opened_once_and_closed_once_on_success(self):
        self.mock_fetch.return_value = ([SAMPLE_HIT], 5, "pit-1")
        poller.poll_cycle(0, 0, True)
        self.mock_open.assert_called_once()
        self.mock_close.assert_called_once()
        self.assertEqual(self.mock_close.call_args.args[0], "pit-1")

    def test_pit_closed_when_delivery_fails(self):
        self.mock_fetch.return_value = ([SAMPLE_HIT], 5, "pit-1")
        self.mock_send.return_value = False
        result = poller.poll_cycle(1786641258000, 0, True)
        self.assertEqual(result, 1786641258000)
        self.mock_close.assert_called_once_with("pit-1", **elasticsearch._es_conn_kwargs())

    def test_pit_closed_when_search_raises(self):
        self.mock_fetch.side_effect = elasticsearch.ElasticsearchQueryError(
            "boom", status_code=500, body="cluster_block_exception"
        )
        result = poller.poll_cycle(1786641258000, 0, True)
        self.assertEqual(result, 1786641258000)
        self.assertEqual(self.mock_close.call_args.args[0], "pit-1")

    def test_expired_pit_is_not_closed_and_cycle_aborts(self):
        self.mock_fetch.side_effect = elasticsearch.ElasticsearchQueryError(
            "gone", status_code=404, body="search_context_missing_exception"
        )
        result = poller.poll_cycle(1786641258000, 0, True)
        self.assertEqual(result, 1786641258000)
        # Already released server-side; closing it again would be a pointless 404.
        self.assertIsNone(self.mock_close.call_args.args[0])

    def test_pit_open_failure_returns_bookmark_unchanged_and_does_not_fetch(self):
        self.mock_open.side_effect = elasticsearch.ElasticsearchQueryError(
            "no such index", status_code=404, body="index_not_found_exception"
        )
        result = poller.poll_cycle(1786641258000, 0, True)
        self.assertEqual(result, 1786641258000)
        self.mock_fetch.assert_not_called()
        self.mock_close.assert_not_called()

    def test_rotated_pit_id_is_threaded_into_next_page_and_closed(self):
        hit_a = dict(SAMPLE_HIT)
        hit_b = dict(SAMPLE_HIT, _id="bbbb", sort=[1786641258365, 1])
        self.mock_fetch.side_effect = [
            ([hit_a, hit_b], 5, "pit-2"),
            ([], 1, "pit-3"),
        ]
        poller.poll_cycle(0, 0, True)
        self.assertEqual(self.mock_fetch.call_args_list[1].kwargs["pit_id"], "pit-2")
        # The latest id must be the one released, not the one we opened with.
        self.assertEqual(self.mock_close.call_args.args[0], "pit-3")

    def test_missing_sort_on_last_hit_ends_cycle_without_keyerror(self):
        no_sort = {k: v for k, v in SAMPLE_HIT.items() if k != "sort"}
        self.mock_fetch.return_value = ([dict(no_sort), dict(no_sort)], 5, "pit-1")
        result = poller.poll_cycle(0, 0, True)
        self.assertEqual(result, 1786641258365)
        self.assertEqual(self.mock_fetch.call_count, 1)
        self.mock_close.assert_called_once()


class EventMappingTests(unittest.TestCase):
    def test_custom_mapping_file_overrides_bundled_default(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
            shutil.copy(mappings.mapping_file_path(), fh.name)
            custom_path = fh.name
        self.addCleanup(os.unlink, custom_path)

        with patch.object(config, "EVENT_MAPPING_FILE", custom_path):
            event = delivery.create_event(SAMPLE_HIT)
        self.assertTrue(event.get_cef()["cef"]["event_time"].endswith("Z"))

    def test_event_time_matches_source_timestamp(self):
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name=mappings.MAPPING_FILE_NAME,
            mapping_file_path=str(mappings.mapping_directory()),
            original_record=SAMPLE_HIT,
        )
        cef = event.get_cef()["cef"]
        self.assertTrue(cef["event_time"].startswith("2026-08-13T17:14:18.365"))
        self.assertTrue(cef["event_time"].endswith("Z"))

    def test_event_id_uses_execution_uuid(self):
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name=mappings.MAPPING_FILE_NAME,
            mapping_file_path=str(mappings.mapping_directory()),
            original_record=SAMPLE_HIT,
        )
        cef = event.get_cef()["cef"]
        self.assertEqual(cef["event_id"], "f025a6bf-38fb-45e3-b477-9555fc64b642")

    def test_event_id_falls_back_to_document_id(self):
        hit = json.loads(json.dumps(SAMPLE_HIT))
        del hit["_source"]["kibana"]["alert"]["rule"]["execution"]
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name=mappings.MAPPING_FILE_NAME,
            mapping_file_path=str(mappings.mapping_directory()),
            original_record=hit,
        )
        cef = event.get_cef()["cef"]
        self.assertEqual(cef["event_id"], SAMPLE_HIT["_id"])

    def test_event_id_is_stable_across_mapping_calls(self):
        ids = []
        for _ in range(3):
            event = common_event.CommonEvent.new_from_file(
                mapping_file_name=mappings.MAPPING_FILE_NAME,
                mapping_file_path=str(mappings.mapping_directory()),
                original_record=SAMPLE_HIT,
            )
            ids.append(event.get_cef()["cef"]["event_id"])
        self.assertEqual(len(set(ids)), 1)


class ProcessHitsTests(unittest.TestCase):
    def test_lm_service_id_enrichment_from_space_ids(self):
        event_list, _ = poller.process_hits(
            [SAMPLE_HIT], query_bookmark=0, watermark=0, bookmark_loaded=False
        )
        self.assertEqual(event_list[0]["enrichments"]["lm_service_id"], "cnrwm")


if __name__ == "__main__":
    unittest.main()
