# -*- coding: utf-8 -*-
"""
Tests for ConsoleBroker.
"""
from __future__ import print_function

import logging
import random
import string
import sys
import unittest

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from opcda_to_mqtt.mqtt.console import ConsoleBroker
from opcda_to_mqtt.mqtt.broker import Connected, Published, Disconnected

logging.disable(logging.CRITICAL)


class TestConsoleBroker(unittest.TestCase):
    """Tests for ConsoleBroker."""

    def test_console_broker_connect_returns_right(self):
        broker = ConsoleBroker()
        result = broker.connect()
        self.assertTrue(
            result.is_right(),
            "ConsoleBroker.connect should return Right"
        )

    def test_console_broker_connect_returns_connected(self):
        broker = ConsoleBroker()
        result = broker.connect()
        marker = result.fold(lambda e: "error", lambda c: c)
        self.assertEqual(
            marker,
            Connected(),
            "ConsoleBroker.connect should return Connected"
        )

    def test_console_broker_publish_returns_right(self):
        broker = ConsoleBroker()
        old = sys.stdout
        sys.stdout = StringIO()
        try:
            result = broker.publish("topic", "message")
        finally:
            sys.stdout = old
        self.assertTrue(
            result.is_right(),
            "ConsoleBroker.publish should return Right"
        )

    def test_console_broker_publish_returns_published(self):
        broker = ConsoleBroker()
        old = sys.stdout
        sys.stdout = StringIO()
        try:
            result = broker.publish("t", "m")
        finally:
            sys.stdout = old
        marker = result.fold(lambda e: "error", lambda p: p)
        self.assertEqual(
            marker,
            Published(),
            "ConsoleBroker.publish should return Published"
        )

    def test_console_broker_publish_prints_topic_and_message(self):
        broker = ConsoleBroker()
        topic = "".join(random.choice(string.ascii_letters) for _ in range(8))
        message = "".join(random.choice(string.ascii_letters) for _ in range(12))
        old = sys.stdout
        sys.stdout = StringIO()
        try:
            broker.publish(topic, message)
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old
        self.assertIn(
            "%s: %s" % (topic, message),
            output,
            "ConsoleBroker.publish should print topic and message"
        )

    def test_console_broker_disconnect_returns_right(self):
        broker = ConsoleBroker()
        result = broker.disconnect()
        self.assertTrue(
            result.is_right(),
            "ConsoleBroker.disconnect should return Right"
        )

    def test_console_broker_disconnect_returns_disconnected(self):
        broker = ConsoleBroker()
        result = broker.disconnect()
        marker = result.fold(lambda e: "error", lambda d: d)
        self.assertEqual(
            marker,
            Disconnected(),
            "ConsoleBroker.disconnect should return Disconnected"
        )

    def test_console_broker_handles_cyrillic_message(self):
        broker = ConsoleBroker()
        message = u"\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435"
        old = sys.stdout
        sys.stdout = StringIO()
        try:
            broker.publish("topic", message)
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old
        self.assertIn(
            message,
            output,
            "ConsoleBroker should handle Cyrillic message"
        )

    def test_console_broker_repr(self):
        broker = ConsoleBroker()
        self.assertEqual(
            repr(broker),
            "ConsoleBroker()",
            "ConsoleBroker repr should be ConsoleBroker()"
        )


if __name__ == "__main__":
    unittest.main()
