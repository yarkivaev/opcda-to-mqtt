# -*- coding: utf-8 -*-
"""
Bridge orchestrator for OPC-DA to MQTT publishing.

Example:
    >>> bridge = Bridge(queue, workers, timer, broker, interval, topic)
    >>> bridge.run(tags)  # Blocks until stopped
"""
from __future__ import print_function

import json
import logging

from opcda_to_mqtt.sync.task import ReadTask
from opcda_to_mqtt.domain.value import TagValue
from opcda_to_mqtt.domain.quality import OpcQuality

_log = logging.getLogger("opcda_mqtt")


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
        _log.debug("Bridge.start: connecting broker")
        self._interval = interval
        self._topic = topic
        self._broker.connect()
        _log.debug("Bridge.start: starting timer")
        self._timer.start()
        _log.debug("Bridge.start: starting %d workers", len(self._workers))
        for i, worker in enumerate(self._workers):
            _log.debug("Bridge.start: starting worker %d", i)
            worker.start()
        _log.debug("Bridge.start: enqueueing %d tags", len(tags))
        for tag in tags:
            self._enqueue(tag)
        _log.debug("Bridge.start: done")

    def _enqueue(self, tag):
        """
        Create and enqueue a read task for tag.

        Args:
            tag: TagPath to read
        """
        _log.debug("Bridge._enqueue: %s", tag.text())
        callback = self._callback(tag)
        task = ReadTask(tag, callback)
        self._queue.put(task)
        _log.debug("Bridge._enqueue: task queued for %s", tag.text())

    def _callback(self, tag):
        """
        Create callback for tag read completion.

        Args:
            tag: TagPath being read

        Returns:
            Function to handle read result
        """
        def handle(result):
            _log.debug("Bridge.callback: received result for %s", tag.text())
            value, quality, _ = result
            message = json.dumps({
                "value": TagValue(value).json(),
                "quality": OpcQuality(quality).text()
            })
            mqtt = tag.topic(self._topic)
            _log.debug("Bridge.callback: publishing to %s", mqtt)
            self._broker.publish(mqtt, message)
            delay = self._interval.seconds()
            _log.debug("Bridge.callback: scheduling next read in %s sec", delay)
            self._timer.schedule(delay, lambda: self._enqueue(tag))
        return handle

    def stop(self):
        """
        Stop the bridge.

        Stops timer, sends sentinels, waits for workers.
        """
        _log.debug("Bridge.stop: stopping timer")
        self._timer.stop()
        _log.debug("Bridge.stop: sending sentinels to %d workers", len(self._workers))
        for _ in self._workers:
            self._queue.put(None)
        _log.debug("Bridge.stop: waiting for workers to finish")
        for i, worker in enumerate(self._workers):
            _log.debug("Bridge.stop: joining worker %d", i)
            worker.join()
        _log.debug("Bridge.stop: disconnecting broker")
        self._broker.disconnect()
        _log.debug("Bridge.stop: done")

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing Bridge configuration
        """
        return "Bridge(workers=%d)" % len(self._workers)
