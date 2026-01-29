# -*- coding: utf-8 -*-
"""
ConsoleBroker for dry-run mode.

Prints messages to stdout instead of publishing to MQTT.

Example:
    >>> broker = ConsoleBroker()
    >>> broker.connect()
    >>> broker.publish("topic", "message")
    topic: message
    >>> broker.disconnect()
"""
from __future__ import print_function

from opcda_to_mqtt.mqtt.broker import (
    MqttBroker, Connected, Published, Disconnected
)
from opcda_to_mqtt.result.either import Right


class ConsoleBroker(MqttBroker):
    """
    Console broker that prints messages to stdout.

    Useful for dry-run testing without MQTT infrastructure.

    Example:
        >>> broker = ConsoleBroker()
        >>> broker.connect().is_right()
        True
    """

    def connect(self):
        """
        Simulate connection.

        Returns:
            Either[Problem, Connected] always Right
        """
        return Right(Connected())

    def publish(self, topic, message):
        """
        Print message to stdout.

        Args:
            topic: MQTT topic string
            message: Message content string

        Returns:
            Either[Problem, Published] always Right
        """
        print("%s: %s" % (topic, message))
        return Right(Published())

    def disconnect(self):
        """
        Simulate disconnection.

        Returns:
            Either[Problem, Disconnected] always Right
        """
        return Right(Disconnected())

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing ConsoleBroker
        """
        return "ConsoleBroker()"
