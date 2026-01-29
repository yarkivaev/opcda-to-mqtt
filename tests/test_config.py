# -*- coding: utf-8 -*-
"""
Tests for JsonConfig and MergedConfig.
"""
from __future__ import print_function

import argparse
import json
import logging
import os
import random
import string
import tempfile
import unittest

from opcda_to_mqtt.app.config import JsonConfig, MergedConfig

logging.disable(logging.CRITICAL)


class TestJsonConfig(unittest.TestCase):
    """Tests for JsonConfig."""

    def test_json_config_load_returns_right_for_valid_file(self):
        content = {"da-progid": "OPC.Server.1"}
        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(content, f)
            result = JsonConfig(path).load()
            self.assertTrue(
                result.is_right(),
                "JsonConfig.load should return Right for valid file"
            )
        finally:
            os.unlink(path)

    def test_json_config_load_returns_content(self):
        progid = "".join(random.choice(string.ascii_letters) for _ in range(10))
        content = {"da-progid": progid}
        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(content, f)
            result = JsonConfig(path).load()
            data = result.fold(lambda e: {}, lambda c: c)
            self.assertEqual(
                data["da-progid"],
                progid,
                "JsonConfig.load should return file content"
            )
        finally:
            os.unlink(path)

    def test_json_config_load_returns_left_for_missing_file(self):
        path = "/nonexistent/%s.json" % "".join(
            random.choice(string.ascii_letters) for _ in range(10)
        )
        result = JsonConfig(path).load()
        self.assertFalse(
            result.is_right(),
            "JsonConfig.load should return Left for missing file"
        )

    def test_json_config_load_returns_left_for_invalid_json(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                f.write("{invalid json}")
            result = JsonConfig(path).load()
            self.assertFalse(
                result.is_right(),
                "JsonConfig.load should return Left for invalid JSON"
            )
        finally:
            os.unlink(path)

    def test_json_config_repr_shows_path(self):
        path = "/some/config.json"
        self.assertIn(
            path,
            repr(JsonConfig(path)),
            "JsonConfig repr should show path"
        )


class TestMergedConfig(unittest.TestCase):
    """Tests for MergedConfig."""

    def test_merged_config_returns_cli_value_over_file(self):
        file = {"da-progid": "file-value"}
        cli = argparse.Namespace(da_progid="cli-value")
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.da_progid(),
            "cli-value",
            "MergedConfig should prefer CLI over file"
        )

    def test_merged_config_returns_file_value_when_cli_is_none(self):
        progid = "".join(random.choice(string.ascii_letters) for _ in range(10))
        file = {"da-progid": progid}
        cli = argparse.Namespace(da_progid=None)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.da_progid(),
            progid,
            "MergedConfig should use file when CLI is None"
        )

    def test_merged_config_returns_default_when_both_missing(self):
        file = {}
        cli = argparse.Namespace(da_host=None)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.da_host(),
            "localhost",
            "MergedConfig should return default when both missing"
        )

    def test_merged_config_handles_underscore_and_dash_keys(self):
        progid = "".join(random.choice(string.ascii_letters) for _ in range(8))
        file = {"da-progid": progid}
        cli = argparse.Namespace(da_progid=None)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.da_progid(),
            progid,
            "MergedConfig should handle dash keys in file"
        )

    def test_merged_config_da_host_default(self):
        cfg = MergedConfig({}, argparse.Namespace(da_host=None))
        self.assertEqual(
            cfg.da_host(),
            "localhost",
            "da_host default should be localhost"
        )

    def test_merged_config_mqtt_port_default(self):
        cfg = MergedConfig({}, argparse.Namespace(mqtt_port=None))
        self.assertEqual(
            cfg.mqtt_port(),
            1883,
            "mqtt_port default should be 1883"
        )

    def test_merged_config_prefix_default(self):
        cfg = MergedConfig({}, argparse.Namespace(prefix=None))
        self.assertEqual(
            cfg.prefix(),
            "",
            "prefix default should be empty string"
        )

    def test_merged_config_tags_default(self):
        cfg = MergedConfig({}, argparse.Namespace(tags=None))
        self.assertEqual(
            cfg.tags(),
            [],
            "tags default should be empty list"
        )

    def test_merged_config_interval_default(self):
        cfg = MergedConfig({}, argparse.Namespace(interval=None))
        self.assertEqual(
            cfg.interval(),
            500,
            "interval default should be 500"
        )

    def test_merged_config_workers_default(self):
        cfg = MergedConfig({}, argparse.Namespace(workers=None))
        self.assertEqual(
            cfg.workers(),
            50,
            "workers default should be 50"
        )

    def test_merged_config_mqtt_host_from_file(self):
        host = "".join(random.choice(string.ascii_letters) for _ in range(8))
        file = {"mqtt-host": host}
        cli = argparse.Namespace(mqtt_host=None)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.mqtt_host(),
            host,
            "mqtt_host should come from file"
        )

    def test_merged_config_mqtt_topic_from_cli(self):
        topic = "factory/" + "".join(
            random.choice(string.ascii_letters) for _ in range(5)
        )
        file = {"mqtt-topic": "file-topic"}
        cli = argparse.Namespace(mqtt_topic=topic)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.mqtt_topic(),
            topic,
            "mqtt_topic should prefer CLI"
        )

    def test_merged_config_tags_from_file(self):
        tags = ["Tag1", "Tag2"]
        file = {"tags": tags}
        cli = argparse.Namespace(tags=None)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.tags(),
            tags,
            "tags should come from file"
        )

    def test_merged_config_exclude_default(self):
        cfg = MergedConfig({}, argparse.Namespace(exclude=None))
        self.assertEqual(
            cfg.exclude(),
            [],
            "exclude default should be empty list"
        )

    def test_merged_config_exclude_from_file(self):
        patterns = ["*.Device exchange", "*.Status"]
        file = {"exclude": patterns}
        cli = argparse.Namespace(exclude=None)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.exclude(),
            patterns,
            "exclude should come from file"
        )

    def test_merged_config_exclude_from_cli(self):
        patterns = ["*.Test", "*.Debug"]
        file = {"exclude": ["*.Other"]}
        cli = argparse.Namespace(exclude=patterns)
        cfg = MergedConfig(file, cli)
        self.assertEqual(
            cfg.exclude(),
            patterns,
            "exclude should prefer CLI over file"
        )

    def test_merged_config_dry_run_default(self):
        cfg = MergedConfig({}, argparse.Namespace(dry_run=None))
        self.assertFalse(
            cfg.dry_run(),
            "dry_run default should be False"
        )

    def test_merged_config_dry_run_from_file(self):
        file = {"dry-run": True}
        cli = argparse.Namespace(dry_run=None)
        cfg = MergedConfig(file, cli)
        self.assertTrue(
            cfg.dry_run(),
            "dry_run should come from file"
        )

    def test_merged_config_dry_run_from_cli(self):
        file = {"dry-run": False}
        cli = argparse.Namespace(dry_run=True)
        cfg = MergedConfig(file, cli)
        self.assertTrue(
            cfg.dry_run(),
            "dry_run should prefer CLI over file"
        )


if __name__ == "__main__":
    unittest.main()
