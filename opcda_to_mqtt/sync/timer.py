# -*- coding: utf-8 -*-
"""
TimerThread for delayed task scheduling.

Example:
    >>> timer = TimerThread()
    >>> timer.start()
    >>> timer.schedule(0.1, lambda: print("fired"))
    >>> time.sleep(0.2)
    >>> timer.stop()
"""
from __future__ import print_function

import heapq
import threading
import time


class TimerThread:
    """
    Single thread for all delayed callbacks.

    Schedules callbacks to fire after a delay.
    Uses a heap for efficient ordering.

    Example:
        >>> timer = TimerThread()
        >>> timer.start()
        >>> fired = []
        >>> timer.schedule(0.01, lambda: fired.append(1))
        >>> time.sleep(0.05)
        >>> timer.stop()
        >>> len(fired)
        1
    """

    def __init__(self):
        """
        Create a TimerThread.

        Initializes heap, condition, and running flag.
        """
        self._heap = []
        self._condition = threading.Condition()
        self._running = False
        self._thread = threading.Thread(target=self._run)

    def start(self):
        """
        Start the timer thread.
        """
        self._running = True
        self._thread.start()

    def stop(self):
        """
        Stop the timer thread.

        Signals thread to stop and waits for it.
        """
        with self._condition:
            self._running = False
            self._condition.notify()
        self._thread.join()

    def schedule(self, delay, callback):
        """
        Schedule a callback after delay seconds.

        Args:
            delay: Seconds to wait before firing
            callback: Function to call (no arguments)
        """
        fire = time.time() + delay
        with self._condition:
            heapq.heappush(self._heap, (fire, callback))
            self._condition.notify()

    def _run(self):
        """
        Main loop of the timer thread.

        Waits for callbacks and fires them at their scheduled time.
        """
        with self._condition:
            while self._running:
                if not self._heap:
                    self._condition.wait()
                    continue
                fire = self._heap[0][0]
                now = time.time()
                if now >= fire:
                    _, callback = heapq.heappop(self._heap)
                    self._condition.release()
                    try:
                        callback()
                    finally:
                        self._condition.acquire()
                else:
                    self._condition.wait(fire - now)

    def pending(self):
        """
        Get number of pending callbacks.

        Returns:
            Number of scheduled callbacks
        """
        with self._condition:
            return len(self._heap)

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing TimerThread state
        """
        return "TimerThread(pending=%d)" % self.pending()
