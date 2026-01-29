# -*- coding: utf-8 -*-
"""
CLI argument parser for OPC-DA to MQTT bridge.

Example:
    >>> parser = ArgumentParser()
    >>> config = parser.parse(["--config", "config.json"])
"""
from __future__ import print_function

import argparse


class ArgumentParser:
    """
    Parses command line arguments.

    Provides configuration for OPC-DA and MQTT connections.
    Supports --config option to load from JSON file.

    Example:
        >>> parser = ArgumentParser()
        >>> args = parser.parse(["--config", "config.json"])
    """

    def __init__(self):
        """
        Create an ArgumentParser.

        Sets up argument definitions.
        """
        self._parser = argparse.ArgumentParser(
            description="OPC-DA to MQTT Publisher"
        )
        self._add()

    def _add(self):
        """
        Add argument definitions.
        """
        self._parser.add_argument(
            "--config",
            default="config.json",
            help="Path to JSON configuration file"
        )
        self._parser.add_argument(
            "--da-progid",
            default=None,
            help="OPC-DA server ProgID"
        )
        self._parser.add_argument(
            "--da-host",
            default=None,
            help="OPC-DA server host"
        )
        self._parser.add_argument(
            "--mqtt-host",
            default=None,
            help="MQTT broker host"
        )
        self._parser.add_argument(
            "--mqtt-port",
            type=int,
            default=None,
            help="MQTT broker port"
        )
        self._parser.add_argument(
            "--mqtt-topic",
            default=None,
            help="Base MQTT topic"
        )
        self._parser.add_argument(
            "--prefix",
            default=None,
            help="OPC tag prefix for discovery"
        )
        self._parser.add_argument(
            "--tags",
            nargs="*",
            default=None,
            help="Explicit tag list"
        )
        self._parser.add_argument(
            "--interval",
            type=int,
            default=None,
            help="Polling interval in milliseconds"
        )
        self._parser.add_argument(
            "--workers",
            type=int,
            default=None,
            help="Number of worker threads"
        )
        self._parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print to stdout instead of MQTT"
        )

    def parse(self, argv):
        """
        Parse command line arguments.

        Args:
            argv: List of argument strings

        Returns:
            Configuration namespace
        """
        return self._parser.parse_args(argv)
