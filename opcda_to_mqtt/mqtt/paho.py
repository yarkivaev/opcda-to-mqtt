# -*- coding: utf-8 -*-
"""
PahoBroker for real MQTT publishing.

Example:
    >>> broker = PahoBroker("192.168.1.100", 1883)
    >>> broker.connect()
    >>> broker.publish("topic", "message")
    >>> broker.disconnect()
"""
from __future__ import print_function

import logging

from opcda_to_mqtt.mqtt.broker import (
    MqttBroker, Connected, Published, Disconnected
)
from opcda_to_mqtt.result.either import Right, Left, Problem

_log = logging.getLogger("opcda_mqtt")


class PahoBroker(MqttBroker):
    """
    Real MQTT broker using paho-mqtt.

    Connects to MQTT broker and publishes messages.

    Example:
        >>> broker = PahoBroker("localhost", 1883)
        >>> broker.connect().is_right()
        True
    """

    def __init__(self, host, port):
        """
        Create a PahoBroker.

        Args:
            host: MQTT broker hostname
            port: MQTT broker port
        """
        self._host = host
        self._port = port
        self._client = None

    def connect(self):
        """
        Connect to the MQTT broker.

        Returns:
            Either[Problem, Connected]
        """
        try:
            import paho.mqtt.client as mqtt
            _log.info("MQTT: connecting to %s:%d", self._host, self._port)
            self._client = mqtt.Client(client_id="opcda-mqtt-bridge")
            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.connect(self._host, self._port)
            self._client.loop_start()
            _log.debug("MQTT: loop started")
            return Right(Connected())
        except Exception as e:
            _log.error("MQTT: connection failed: %s", e)
            return Left(Problem(
                "MQTT connection failed",
                {"host": self._host, "port": str(self._port), "error": str(e)}
            ))

    def _on_connect(self, client, userdata, flags, rc):
        """
        Callback when connected to broker.
        """
        if rc == 0:
            _log.info("MQTT: connected successfully")
        else:
            _log.error("MQTT: connection failed with code %d", rc)

    def _on_disconnect(self, client, userdata, rc):
        """
        Callback when disconnected from broker.
        """
        if rc == 0:
            _log.info("MQTT: disconnected cleanly")
        else:
            _log.warning("MQTT: unexpected disconnect, code %d", rc)

    def publish(self, topic, message):
        """
        Publish a message to a topic.

        Args:
            topic: MQTT topic string
            message: Message content string

        Returns:
            Either[Problem, Published]
        """
        try:
            if self._client is None:
                _log.error("MQTT: publish failed - not connected")
                return Left(Problem("Not connected", {}))
            _log.debug("MQTT: publishing to %s", topic)
            result = self._client.publish(topic, message)
            _log.debug("MQTT: publish result rc=%d, mid=%d", result.rc, result.mid)
            return Right(Published())
        except Exception as e:
            _log.error("MQTT: publish failed: %s", e)
            return Left(Problem(
                "MQTT publish failed",
                {"topic": topic, "error": str(e)}
            ))

    def disconnect(self):
        """
        Disconnect from the MQTT broker.

        Returns:
            Either[Problem, Disconnected]
        """
        try:
            if self._client is not None:
                _log.info("MQTT: disconnecting")
                self._client.loop_stop()
                self._client.disconnect()
                self._client = None
            return Right(Disconnected())
        except Exception as e:
            _log.error("MQTT: disconnect failed: %s", e)
            return Left(Problem(
                "MQTT disconnect failed",
                {"error": str(e)}
            ))

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing PahoBroker configuration
        """
        return "PahoBroker(%r, %d)" % (self._host, self._port)
