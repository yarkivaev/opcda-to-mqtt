# -*- coding: utf-8 -*-
"""
OpenOpcWorker for real OPC-DA tag reading.

Example:
    >>> worker = OpenOpcWorker(queue, "OPC.Server.1", "localhost")
    >>> worker.start()
    >>> queue.put(tag)
    >>> queue.put(None)
    >>> worker.join()
"""
from __future__ import print_function

import threading

from opcda_to_mqtt.sync.worker import Worker


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
        self._thread = threading.Thread(target=self._run)

    def start(self):
        """
        Start the worker thread.
        """
        self._thread.start()

    def stop(self):
        """
        Signal stop (Bridge sends sentinel to queue).
        """
        pass

    def join(self):
        """
        Wait for worker thread to finish.
        """
        self._thread.join()

    def _run(self):
        """
        Main worker loop.

        Connects to OPC, executes tags until sentinel.
        """
        import OpenOPC
        client = OpenOPC.client()
        client.connect(self._progid, self._host)
        while True:
            tag = self._queue.get()
            if tag is None:
                break
            try:
                tag.reading(client).publish()
            except Exception:
                tag._later_bound()
                client.close()
                client = OpenOPC.client()
                client.connect(self._progid, self._host)
        client.close()

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing OpenOpcWorker configuration
        """
        return "OpenOpcWorker(%r, %r)" % (self._progid, self._host)
