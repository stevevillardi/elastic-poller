"""Unit tests for LM Logs handler and configuration helpers."""

import logging
import time
import unittest
from unittest.mock import MagicMock, patch

from edwin_elastic_poller import config, delivery
from edwin_elastic_poller.observability import lm_logs


class EnvBoolTests(unittest.TestCase):
    def test_truthy_values(self):
        with patch.dict("os.environ", {"TEST_FLAG": "true"}):
            self.assertTrue(lm_logs.env_bool("TEST_FLAG"))
        with patch.dict("os.environ", {"TEST_FLAG": "1"}):
            self.assertTrue(lm_logs.env_bool("TEST_FLAG"))

    def test_falsy_values(self):
        with patch.dict("os.environ", {"TEST_FLAG": "false"}):
            self.assertFalse(lm_logs.env_bool("TEST_FLAG"))
        with patch.dict("os.environ", {}, clear=True):
            self.assertFalse(lm_logs.env_bool("TEST_FLAG", default=False))


class SanitizeElasticUrlTests(unittest.TestCase):
    def test_strips_credentials(self):
        url = "https://user:secret@es.example.com:9200"
        self.assertEqual(lm_logs.sanitize_elastic_url(url), "es.example.com:9200")

    def test_host_only(self):
        self.assertEqual(
            lm_logs.sanitize_elastic_url("https://es.example.com:9200"),
            "es.example.com:9200",
        )

    def test_empty_url(self):
        self.assertEqual(lm_logs.sanitize_elastic_url(None), "")
        self.assertEqual(lm_logs.sanitize_elastic_url(""), "")


class BuildStartupContextTests(unittest.TestCase):
    def test_omits_secrets(self):
        context = lm_logs.build_startup_context(
            edwin_org="acme",
            elastic_url="https://user:pass@es.example.com:9200",
            elastic_index=".kibana-event-log-ds",
            elastic_query="*",
            elastic_batch_size=500,
            verify_ssl=True,
            elastic_pit_keep_alive="5m",
            poller_interval="240",
            bookmark_path="/data/acme.elastic.bookmark",
            lm_logs_enabled=True,
        )
        self.assertEqual(context["edwin_org"], "acme")
        self.assertEqual(context["elastic_host"], "es.example.com:9200")
        self.assertNotIn("pass", context["elastic_host"])
        self.assertNotIn("client_secret", str(context))
        self.assertNotIn("token", str(context).lower())


class OperationalLogLevelTests(unittest.TestCase):
    def test_info_for_operational_summaries(self):
        self.assertEqual(lm_logs.operational_log_level(), logging.INFO)


class LmLogsHandlerTests(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.NOTSET)
        self.handler = lm_logs.LmLogsHandler(
            account="acme",
            bearer_token="test-token",
            min_level=logging.INFO,
            resource_id="device-123",
        )

    def _wait_for_delivery(self):
        self.handler._queue.join()
        time.sleep(0.01)

    @patch("edwin_elastic_poller.observability.lm_logs.requests.post")
    def test_emits_info_and_above_at_operational_level(self, mock_post):
        mock_post.return_value = MagicMock(status_code=202)
        test_logger = logging.getLogger("test.lm_logs.info_filter")
        test_logger.handlers.clear()
        test_logger.propagate = False
        test_logger.addHandler(self.handler)
        test_logger.setLevel(logging.DEBUG)

        test_logger.debug("debug detail")
        mock_post.assert_not_called()

        test_logger.info("poll cycle finished")
        self._wait_for_delivery()
        mock_post.assert_called_once()

    @patch("edwin_elastic_poller.observability.lm_logs.requests.post")
    def test_emits_debug_when_min_level_debug(self, mock_post):
        handler = lm_logs.LmLogsHandler(
            account="acme",
            bearer_token="test-token",
            min_level=logging.DEBUG,
        )
        test_logger = logging.getLogger("test.lm_logs.debug_filter")
        test_logger.handlers.clear()
        test_logger.propagate = False
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.DEBUG)

        mock_post.return_value = MagicMock(status_code=202)
        test_logger.debug("page fetched")
        handler._queue.join()
        mock_post.assert_called_once()

    @patch("edwin_elastic_poller.observability.lm_logs.requests.post")
    def test_payload_includes_msg_and_bearer_auth(self, mock_post):
        mock_post.return_value = MagicMock(status_code=202)
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="startup",
            args=(),
            exc_info=None,
        )
        record.lm_context = {"hit_count": 3, "bookmark_ms": 1000}
        self.handler.emit(record)
        self._wait_for_delivery()

        mock_post.assert_called_once()
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(
            kwargs["headers"]["Authorization"],
            "Bearer test-token",
        )
        payload = kwargs["json"][0]
        self.assertEqual(payload["msg"], "startup")
        self.assertEqual(payload["log_level"], "WARNING")
        self.assertNotIn("level", payload)
        self.assertEqual(payload["hit_count"], 3)
        self.assertEqual(payload["bookmark_ms"], 1000)
        self.assertEqual(
            payload["_lm.resourceId"],
            {"system.deviceId": "device-123"},
        )

    @patch("edwin_elastic_poller.observability.lm_logs.requests.post")
    def test_does_not_raise_on_http_error(self, mock_post):
        mock_post.return_value = MagicMock(status_code=500, text="error")
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="failure",
            args=(),
            exc_info=None,
        )
        self.handler.emit(record)  # must not raise
        self._wait_for_delivery()

    @patch("edwin_elastic_poller.observability.lm_logs.requests.post", side_effect=ConnectionError("network down"))
    def test_does_not_raise_on_network_error(self, mock_post):
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="failure",
            args=(),
            exc_info=None,
        )
        self.handler.emit(record)  # must not raise
        self._wait_for_delivery()


class ConfigureLoggingTests(unittest.TestCase):
    def test_log_disabled_sets_high_level(self):
        logger = lm_logs.configure_logging(log_enabled=False)
        self.assertEqual(logger.level, logging.CRITICAL + 1)
        self.assertEqual(logger.handlers, [])

    def test_lm_handler_added_when_enabled(self):
        logger = lm_logs.configure_logging(
            log_enabled=True,
            lm_logs_enabled=True,
            lm_logs_account="acme",
            lm_logs_bearer_token="token",
        )
        handler_types = [type(h).__name__ for h in logger.handlers]
        self.assertIn("StreamHandler", handler_types)
        self.assertIn("LmLogsHandler", handler_types)

    def test_configure_logging_resets_global_disable(self):
        logging.disable(logging.CRITICAL)
        logger = lm_logs.configure_logging(log_enabled=True)
        self.assertTrue(logger.isEnabledFor(logging.INFO))

    def test_lm_handler_ships_info_when_debug_false(self):
        logger = lm_logs.configure_logging(
            log_enabled=True,
            debug=False,
            lm_logs_enabled=True,
            lm_logs_account="acme",
            lm_logs_bearer_token="token",
        )
        lm_handlers = [
            handler
            for handler in logger.handlers
            if isinstance(handler, lm_logs.LmLogsHandler)
        ]
        self.assertEqual(len(lm_handlers), 1)
        self.assertEqual(lm_handlers[0].level, logging.INFO)

    def test_third_party_loggers_quiet_by_default(self):
        lm_logs.configure_third_party_loggers(debug=False)
        self.assertEqual(
            logging.getLogger("edwin_elastic_poller.sdk.common_event").level, logging.ERROR
        )
        self.assertEqual(
            logging.getLogger("edwin_elastic_poller.sdk.edwin_request").level, logging.WARNING
        )

    def test_third_party_loggers_verbose_when_debug(self):
        lm_logs.configure_third_party_loggers(debug=True)
        self.assertEqual(
            logging.getLogger("edwin_elastic_poller.sdk.common_event").level, logging.DEBUG
        )
        self.assertEqual(
            logging.getLogger("edwin_elastic_poller.sdk.edwin_request").level, logging.DEBUG
        )


class CommonEventLoggingRegressionTests(unittest.TestCase):
    def test_event_mapping_does_not_disable_application_logging(self):
        from tests.test_edwin_elastic_poller import SAMPLE_HIT

        config.logger = lm_logs.configure_logging(
            log_enabled=True,
            debug=True,
            lm_logs_enabled=False,
        )
        delivery.create_event(SAMPLE_HIT)
        self.assertEqual(logging.root.manager.disable, logging.NOTSET)
        self.assertTrue(config.logger.isEnabledFor(logging.INFO))


if __name__ == "__main__":
    unittest.main()
