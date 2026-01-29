# -*- coding: utf-8 -*-
"""
Entry point for OPC-DA to MQTT bridge.

Example:
    python -m opcda_to_mqtt.app.main --config config.json

    python -m opcda_to_mqtt.app.main \
        --da-progid "OPCDataStore.TOPCElemerServer.2" \
        --mqtt-host "192.168.1.100" \
        --mqtt-topic "factory/line1" \
        --prefix "COM1"
"""
from __future__ import print_function

import fnmatch
import signal
import sys

from opcda_to_mqtt.app.args import ArgumentParser
from opcda_to_mqtt.app.config import JsonConfig, MergedConfig
from opcda_to_mqtt.app.log import LogConfig
from opcda_to_mqtt.domain.path import TagPath
from opcda_to_mqtt.domain.interval import Milliseconds
from opcda_to_mqtt.sync.queue import TaskQueue
from opcda_to_mqtt.sync.timer import TimerThread
from opcda_to_mqtt.sync.bridge import Bridge


def _matches(text, patterns):
    """
    Check if text matches any pattern.

    Args:
        text: String to check
        patterns: List of glob patterns

    Returns:
        True if text matches any pattern
    """
    for pattern in patterns:
        if fnmatch.fnmatch(text, pattern):
            return True
    return False


def main():
    """
    Main entry point.

    Parses arguments, loads config, creates components, and runs bridge.
    """
    args = ArgumentParser().parse(sys.argv[1:])
    logger = LogConfig().setup()
    logger.info("Starting OPC-DA to MQTT bridge")
    file = JsonConfig(args.config).load().fold(lambda e: {}, lambda c: c)
    cfg = MergedConfig(file, args)
    if not cfg.da_progid():
        logger.error("Missing required: da-progid")
        sys.exit(1)
    if not cfg.dry_run() and not cfg.mqtt_host():
        logger.error("Missing required: mqtt-host")
        sys.exit(1)
    if not cfg.mqtt_topic():
        logger.error("Missing required: mqtt-topic")
        sys.exit(1)
    try:
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        from opcda_to_mqtt.sync.openopc_worker import OpenOpcWorker
        if cfg.dry_run():
            from opcda_to_mqtt.mqtt.console import ConsoleBroker
        else:
            from opcda_to_mqtt.mqtt.paho_broker import PahoBroker
    except ImportError as e:
        logger.error("Missing dependency: %s" % e)
        sys.exit(1)
    source = OpenOpcSource(cfg.da_progid(), cfg.da_host())
    if cfg.dry_run():
        broker = ConsoleBroker()
        logger.info("Dry-run mode: printing to stdout")
    else:
        broker = PahoBroker(cfg.mqtt_host(), cfg.mqtt_port())
    queue = TaskQueue()
    timer = TimerThread()
    workers = [
        OpenOpcWorker(queue, cfg.da_progid(), cfg.da_host())
        for _ in range(cfg.workers())
    ]
    bridge = Bridge(queue, workers, timer, broker)
    if cfg.tags():
        tags = [TagPath(t) for t in cfg.tags()]
    else:
        result = source.discover(cfg.prefix())
        if not result.is_right():
            logger.error("Discovery failed: %s" % result.fold(
                lambda e: e.text(), lambda _: ""
            ))
            sys.exit(1)
        tags = result.fold(lambda _: [], lambda t: t)
    excludes = cfg.exclude()
    if excludes:
        before = len(tags)
        tags = [t for t in tags if not _matches(t.text(), excludes)]
        logger.info("Excluded %d tags by pattern" % (before - len(tags)))
    if not tags:
        logger.error("No tags to monitor")
        sys.exit(1)
    logger.info("Monitoring %d tags:" % len(tags))
    print("text?")
    print(len(tags))
    for tag in tags:
        logger.info("  - %s" % tag.text())
    print("interval?")
    interval = Milliseconds(cfg.interval())
    topic = cfg.mqtt_topic()
    running = [True]

    def handler(sig, frame):
        logger.info("Shutting down")
        running[0] = False

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    bridge.start(tags, interval, topic)
    try:
        while running[0]:
            signal.pause()
    except AttributeError:
        import time
        while running[0]:
            time.sleep(1)
    bridge.stop()
    logger.info("Bridge stopped")


if __name__ == "__main__":
    main()
