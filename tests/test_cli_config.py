"""Tests for CLI config file loading."""

from __future__ import annotations

import os
import tempfile
import unittest

from edwin_elastic_poller import cli, config


class CliConfigTests(unittest.TestCase):
    def test_env_file_loads_settings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = os.path.join(temp_dir, "poller.env")
            with open(env_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "\n".join(
                        [
                            "EDWIN_ORG=test-org",
                            "EDWIN_ID=test-id",
                            "EDWIN_TOKEN=test-token",
                            "ELASTIC_URL=http://localhost:9200",
                            "ELASTIC_INDEXS=.kibana-event-log-ds",
                        ]
                    )
                )

            config.load_environment(env_file=env_path)

            self.assertEqual(config.EDWIN_ORG, "test-org")
            self.assertEqual(config.ELASTIC_URL, "http://localhost:9200")
            self.assertEqual(config.ELASTIC_INDEX, ".kibana-event-log-ds")

    def test_mapping_file_flag_sets_event_mapping_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            mapping_path = os.path.join(temp_dir, "custom.yaml")
            mapping_path_obj = mapping_path
            with open(mapping_path_obj, "w", encoding="utf-8") as handle:
                handle.write("mappings: []\n")

            config.load_environment(mapping_file=mapping_path)

            self.assertEqual(config.EVENT_MAPPING_FILE, mapping_path)

    def test_missing_env_file_raises_configuration_error(self):
        with self.assertRaises(config.ConfigurationError) as context:
            config.load_environment(env_file="/path/does/not/exist.env")
        self.assertIn("Env file does not exist", str(context.exception))

    def test_missing_mapping_file_raises_configuration_error(self):
        with self.assertRaises(config.ConfigurationError) as context:
            config.load_environment(mapping_file="/path/does/not/exist.yaml")
        self.assertIn("Mapping file does not exist", str(context.exception))

    def test_mapping_file_overrides_env_file_value(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = os.path.join(temp_dir, "poller.env")
            bundled_mapping = os.path.join(temp_dir, "bundled.yaml")
            override_mapping = os.path.join(temp_dir, "override.yaml")
            for path in (bundled_mapping, override_mapping):
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("mappings: []\n")
            with open(env_path, "w", encoding="utf-8") as handle:
                handle.write(f"EVENT_MAPPING_FILE={bundled_mapping}\n")

            config.load_environment(
                env_file=env_path,
                mapping_file=override_mapping,
            )

            self.assertEqual(config.EVENT_MAPPING_FILE, override_mapping)

    def test_parse_args_accepts_env_and_mapping_flags(self):
        args = cli.parse_args(
            [
                "--env-file",
                "/etc/poller.env",
                "--mapping-file",
                "/etc/mapping.yaml",
            ]
        )
        self.assertEqual(args.env_file, "/etc/poller.env")
        self.assertEqual(args.mapping_file, "/etc/mapping.yaml")


if __name__ == "__main__":
    unittest.main()
