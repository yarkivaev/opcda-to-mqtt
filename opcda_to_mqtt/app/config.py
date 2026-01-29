# -*- coding: utf-8 -*-
"""
Configuration loader from JSON file.

Example:
    >>> config = JsonConfig("config.json")
    >>> config.load().fold(lambda e: {}, lambda c: c)
    {'da_progid': 'OPC.Server.1', ...}
"""
from __future__ import print_function

import json
import os

from opcda_to_mqtt.result.either import Right, Left, Problem


class JsonConfig:
    """
    Loads configuration from a JSON file.

    Reads connection details from config.json.

    Example:
        >>> config = JsonConfig("config.json")
        >>> result = config.load()
        >>> result.is_right()
        True
    """

    def __init__(self, path):
        """
        Create a JsonConfig.

        Args:
            path: Path to JSON configuration file
        """
        self._path = path

    def load(self):
        """
        Load configuration from file.

        Returns:
            Either[Problem, dict] with configuration values
        """
        if not os.path.exists(self._path):
            return Left(Problem(
                "Configuration file not found",
                {"path": self._path}
            ))
        try:
            with open(self._path, "r") as f:
                data = json.load(f)
            return Right(data)
        except ValueError as e:
            return Left(Problem(
                "Invalid JSON in configuration file",
                {"path": self._path, "error": str(e)}
            ))
        except IOError as e:
            return Left(Problem(
                "Cannot read configuration file",
                {"path": self._path, "error": str(e)}
            ))

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing JsonConfig path
        """
        return "JsonConfig(%r)" % self._path


class MergedConfig:
    """
    Merges configuration from file and CLI arguments.

    CLI arguments override file configuration.

    Example:
        >>> merged = MergedConfig(file_config, cli_args)
        >>> merged.get("da_progid")
        'OPC.Server.1'
    """

    def __init__(self, file, cli):
        """
        Create a MergedConfig.

        Args:
            file: Dict from JSON file (or empty)
            cli: Namespace from argparse
        """
        self._file = file
        self._cli = cli

    def get(self, key, default):
        """
        Get configuration value.

        CLI takes precedence, then file, then default.

        Args:
            key: Configuration key (underscore format)
            default: Default value if not found

        Returns:
            Configuration value
        """
        cli_val = getattr(self._cli, key, None)
        if cli_val is not None:
            return cli_val
        file_key = key.replace("_", "-")
        if file_key in self._file:
            return self._file[file_key]
        if key in self._file:
            return self._file[key]
        return default

    def da_progid(self):
        """
        Get OPC-DA server ProgID.

        Returns:
            ProgID string or None
        """
        return self.get("da_progid", None)

    def da_host(self):
        """
        Get OPC-DA server host.

        Returns:
            Host string
        """
        return self.get("da_host", "localhost")

    def mqtt_host(self):
        """
        Get MQTT broker host.

        Returns:
            Host string or None
        """
        return self.get("mqtt_host", None)

    def mqtt_port(self):
        """
        Get MQTT broker port.

        Returns:
            Port integer
        """
        return self.get("mqtt_port", 1883)

    def mqtt_topic(self):
        """
        Get base MQTT topic.

        Returns:
            Topic string or None
        """
        return self.get("mqtt_topic", None)

    def prefix(self):
        """
        Get OPC tag prefix.

        Returns:
            Prefix string
        """
        return self.get("prefix", "")

    def tags(self):
        """
        Get explicit tag list.

        Returns:
            List of tag strings
        """
        return self.get("tags", [])

    def interval(self):
        """
        Get polling interval in milliseconds.

        Returns:
            Interval integer
        """
        return self.get("interval", 500)

    def workers(self):
        """
        Get number of worker threads.

        Returns:
            Worker count integer
        """
        return self.get("workers", 50)

    def exclude(self):
        """
        Get tag exclusion patterns.

        Returns:
            List of glob patterns to exclude
        """
        return self.get("exclude", [])

    def dry_run(self):
        """
        Check if dry-run mode is enabled.

        Returns:
            True if dry-run mode is enabled
        """
        return self.get("dry_run", False)
