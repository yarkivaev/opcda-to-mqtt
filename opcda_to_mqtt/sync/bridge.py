# -*- coding: utf-8 -*-
"""
Bridge orchestrator for OPC-DA to MQTT publishing.

Example:
    >>> bridge = Bridge(queue, workers, timer, broker, interval, topic)
    >>> bridge.run(tags)  # Blocks until stopped
"""
from __future__ import print_function

import json

from opcda_to_mqtt.sync.task import ReadTask
from opcda_to_mqtt.domain.value import TagValue
from opcda_to_mqtt.domain.quality import OpcQuality


class Bridge:
    """
    Orchestrates OPC-DA reading and MQTT publishing.

    Manages workers, timer, and task flow.

    Example:
        >>> bridge = Bridge(queue, workers, timer, broker, interval, topic)
        >>> bridge.start([TagPath("Tag1"), TagPath("Tag2")])
        >>> # Tags are continuously read and published
        >>> bridge.stop()
    """

    def __init__(self, queue, workers, timer, broker):
        """
        Create a Bridge.

        Args:
            queue: TaskQueue for task distribution
            workers: List of Worker instances
            timer: TimerThread for delayed scheduling
            broker: MqttBroker for publishing
        """
        self._queue = queue
        self._workers = workers
        self._timer = timer
        self._broker = broker

    def start(self, tags, interval, topic):
        """
        Start the bridge with given tags.

        Args:
            tags: List of TagPath to monitor
            interval: Milliseconds between reads
            topic: Base MQTT topic prefix
        """
        self._interval = interval
        self._topic = topic
        self._broker.connect()
        self._timer.start()
        for worker in self._workers:
            worker.start()
        for tag in tags:
            self._enqueue(tag)

    def _enqueue(self, tag):
        """
        Create and enqueue a read task for tag.

        Args:
            tag: TagPath to read
        """
        callback = self._callback(tag)
        task = ReadTask(tag, callback)
        self._queue.put(task)

    def _callback(self, tag):
        """
        Create callback for tag read completion.

        Args:
            tag: TagPath being read

        Returns:
            Function to handle read result
        """
        def handle(result):
            value, quality, _ = result
            message = json.dumps({
                "value": TagValue(value).json(),
                "quality": OpcQuality(quality).text()
            })
            mqtt = tag.topic(self._topic)
            self._broker.publish(mqtt, message)
            delay = self._interval.seconds()
            self._timer.schedule(delay, lambda: self._enqueue(tag))
        return handle

    def stop(self):
        """
        Stop the bridge.

        Stops timer, sends sentinels, waits for workers.
        """
        self._timer.stop()
        for _ in self._workers:
            self._queue.put(None)
        for worker in self._workers:
            worker.join()
        self._broker.disconnect()

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing Bridge configuration
        """
        return "Bridge(workers=%d)" % len(self._workers)
