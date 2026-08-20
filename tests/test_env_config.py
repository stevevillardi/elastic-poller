"""Unit tests for Edwin credential environment variable aliases."""

import os
import unittest
from unittest.mock import patch

from edwin_elastic_poller import config


class EnvConfigAliasTests(unittest.TestCase):
    def test_prefers_edwin_names_over_legacy_aliases(self):
        with patch.dict(
            os.environ,
            {
                "EDWIN_ORG": "edwin-org",
                "DEXDA_ORG": "dexda-org",
                "EDWIN_ID": "edwin-id",
                "DEXDA_ID": "dexda-id",
                "EDWIN_TOKEN": "edwin-token",
                "DEXDA_TOKEN": "dexda-token",
            },
            clear=True,
        ):
            self.assertEqual(config.edwin_org(), "edwin-org")
            self.assertEqual(config.edwin_client_id(), "edwin-id")
            self.assertEqual(config.edwin_client_token(), "edwin-token")

    def test_falls_back_to_legacy_dexda_env_names(self):
        with patch.dict(
            os.environ,
            {
                "DEXDA_ORG": "dexda-org",
                "DEXDA_ID": "dexda-id",
                "DEXDA_TOKEN": "dexda-token",
            },
            clear=True,
        ):
            self.assertEqual(config.edwin_org(), "dexda-org")
            self.assertTrue(config.has_edwin_credentials())

    def test_missing_edwin_credentials(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(config.has_edwin_credentials())
            self.assertEqual(len(config.missing_edwin_credential_names()), 3)


class VerifySslConfigTests(unittest.TestCase):
    def test_verify_ssl_applies_to_all_clients(self):
        with patch.dict(os.environ, {"VERIFY_SSL": "false"}, clear=True):
            self.assertFalse(config.resolve_verify_ssl("ELASTIC_VERIFY_SSL"))
            self.assertFalse(config.resolve_verify_ssl("EDWIN_VERIFY_SSL"))

    def test_verify_ssl_overrides_legacy_aliases(self):
        with patch.dict(
            os.environ,
            {
                "VERIFY_SSL": "true",
                "ELASTIC_VERIFY_SSL": "false",
                "EDWIN_VERIFY_SSL": "false",
            },
            clear=True,
        ):
            self.assertTrue(config.resolve_verify_ssl("ELASTIC_VERIFY_SSL"))
            self.assertTrue(config.resolve_verify_ssl("EDWIN_VERIFY_SSL"))

    def test_legacy_aliases_used_when_verify_ssl_unset(self):
        with patch.dict(
            os.environ,
            {"ELASTIC_VERIFY_SSL": "false", "EDWIN_VERIFY_SSL": "true"},
            clear=True,
        ):
            self.assertFalse(config.resolve_verify_ssl("ELASTIC_VERIFY_SSL"))
            self.assertTrue(config.resolve_verify_ssl("EDWIN_VERIFY_SSL"))


class RuntimeValidationTests(unittest.TestCase):
    def test_rejects_missing_required_settings(self):
        with patch.dict(os.environ, {}, clear=True), patch.multiple(
            config,
            ELASTIC_URL=None,
            ELASTIC_INDEX=None,
            ELASTIC_BATCH_SIZE=500,
            ELASTIC_OVERLAP_MS=0,
            DEDUPE_MAX_RECORDS=250000,
            DEDUPE_MAX_SIZE_MB=256,
            ELASTIC_TOKEN=None,
            ELASTIC_USER=None,
            ELASTIC_PASS=None,
            PAUSE_INTERVAL=240,
            LM_LOGS_ENABLED=False,
            EVENT_MAPPING_FILE=None,
        ):
            with self.assertRaises(config.ConfigurationError) as context:
                config.validate_config()
        self.assertIn("ELASTIC_URL is required", str(context.exception))

    def test_accepts_complete_configuration(self):
        values = {
            "EDWIN_ORG": "org",
            "EDWIN_ID": "id",
            "EDWIN_TOKEN": "token",
        }
        with patch.dict(os.environ, values, clear=True), patch.multiple(
            config,
            ELASTIC_URL="https://es.example.com",
            ELASTIC_INDEX="events-*",
            ELASTIC_BATCH_SIZE=500,
            ELASTIC_OVERLAP_MS=300000,
            DEDUPE_MAX_RECORDS=250000,
            DEDUPE_MAX_SIZE_MB=256,
            ELASTIC_TOKEN="api-key",
            ELASTIC_USER=None,
            ELASTIC_PASS=None,
            PAUSE_INTERVAL=240,
            LM_LOGS_ENABLED=False,
            EVENT_MAPPING_FILE=None,
        ):
            config.validate_config()

    def test_rejects_missing_custom_mapping_file(self):
        with patch.dict(os.environ, {}, clear=True), patch.multiple(
            config,
            ELASTIC_URL="https://es.example.com",
            ELASTIC_INDEX="events-*",
            ELASTIC_BATCH_SIZE=500,
            ELASTIC_OVERLAP_MS=300000,
            DEDUPE_MAX_RECORDS=250000,
            DEDUPE_MAX_SIZE_MB=256,
            ELASTIC_TOKEN="api-key",
            ELASTIC_USER=None,
            ELASTIC_PASS=None,
            PAUSE_INTERVAL=240,
            LM_LOGS_ENABLED=False,
            EVENT_MAPPING_FILE="/path/does/not/exist.yaml",
        ):
            with self.assertRaises(config.ConfigurationError) as context:
                config.validate_config()
        self.assertIn("EVENT_MAPPING_FILE", str(context.exception))


if __name__ == "__main__":
    unittest.main()
