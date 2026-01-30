# -*- coding: utf-8 -*-
"""
Logging configuration for OPC-DA to MQTT bridge.

Example:
    >>> logger = LogConfig().setup()
    >>> logger.info("Starting bridge")
"""
from __future__ import print_function

import logging
import sys


class LogConfig:
    """
    Configures logging for the application.

    Sets up console output with appropriate formatting.

    Example:
        >>> config = LogConfig()
        >>> logger = config.setup()
        >>> logger.info("Ready")
    """

    def __init__(self):
        """
        Create a LogConfig.

        Sets default format and level.
        """
        self._format = "%(asctime)s %(levelname)s %(message)s"
        self._level = logging.INFO

    def setup(self):
        """
        Configure and return the root logger.

        Returns:
            Configured Logger instance
        """
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(self._format))
        logger = logging.getLogger("opcda_mqtt")
        logger.setLevel(self._level)
        logger.addHandler(handler)
        return logger
