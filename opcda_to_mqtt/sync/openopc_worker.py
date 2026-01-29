# -*- coding: utf-8 -*-
"""
OpenOpcWorker for real OPC-DA tag reading.

Example:
    >>> worker = OpenOpcWorker(queue, "OPC.Server.1", "localhost")
    >>> worker.start()
    >>> queue.put(task)
    >>> queue.put(None)
    >>> worker.join()
"""
from __future__ import print_function

import logging
import threading

from opcda_to_mqtt.sync.worker import Worker

_log = logging.getLogger("opcda_mqtt")
_worker_id = [0]


class OpenOpcWorker(Worker):
    """
    Real worker using OpenOPC for tag reads.

    Each worker has its own OPC connection for COM thread safety.

    Example:
        >>> worker = OpenOpcWorker(queue, "OPC.Server", "localhost")
        >>> worker.start()
        >>> worker.stop()
        >>> worker.join()
    """

    def __init__(self, queue, progid, host):
        """
        Create an OpenOpcWorker.

        Args:
            queue: TaskQueue to pull tasks from
            progid: OPC-DA server ProgID
            host: Server hostname
        """
        self._queue = queue
        self._progid = progid
        self._host = host
        self._id = _worker_id[0]
        _worker_id[0] += 1
        self._thread = threading.Thread(target=self._run)
        _log.debug("Worker[%d]: created", self._id)

    def start(self):
        """
        Start the worker thread.
        """
        _log.debug("Worker[%d]: starting thread", self._id)
        self._thread.start()

    def stop(self):
        """
        Signal stop (Bridge sends sentinel to queue).
        """
        _log.debug("Worker[%d]: stop called", self._id)

    def join(self):
        """
        Wait for worker thread to finish.
        """
        _log.debug("Worker[%d]: joining thread", self._id)
        self._thread.join()
        _log.debug("Worker[%d]: thread joined", self._id)

    def _run(self):
        """
        Main worker loop.

        Connects to OPC, executes tasks until sentinel.
        """
        _log.debug("Worker[%d]: _run started", self._id)
        import OpenOPC
        _log.debug("Worker[%d]: creating OPC client", self._id)
        client = OpenOPC.client()
        _log.debug("Worker[%d]: connecting to %s@%s", self._id, self._progid, self._host)
        client.connect(self._progid, self._host)
        _log.debug("Worker[%d]: connected, entering loop", self._id)
        try:
            while True:
                _log.debug("Worker[%d]: waiting for task", self._id)
                task = self._queue.get()
                if task is None:
                    _log.debug("Worker[%d]: received sentinel, exiting", self._id)
                    break
                _log.debug("Worker[%d]: executing task %s", self._id, task)
                task.execute(client)
                _log.debug("Worker[%d]: task done", self._id)
        finally:
            _log.debug("Worker[%d]: closing OPC client", self._id)
            client.close()
        _log.debug("Worker[%d]: _run finished", self._id)

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing OpenOpcWorker configuration
        """
        return "OpenOpcWorker(%r, %r)" % (self._progid, self._host)
