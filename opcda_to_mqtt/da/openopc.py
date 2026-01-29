# -*- coding: utf-8 -*-
"""
OpenOpcSource for real OPC-DA tag discovery.

Example:
    >>> source = OpenOpcSource("OPCDataStore.TOPCElemerServer.2", "localhost")
    >>> result = source.discover("COM1")
"""
from __future__ import print_function

from opcda_to_mqtt.da.source import DaSource
from opcda_to_mqtt.domain.path import TagPath
from opcda_to_mqtt.result.either import Right, Left, Problem


class OpenOpcSource(DaSource):
    """
    Real OPC-DA tag discovery using OpenOPC.

    Connects to OPC-DA server and lists available tags.

    Example:
        >>> source = OpenOpcSource("OPC.Server.1", "localhost")
        >>> result = source.discover("COM1")
        >>> result.is_right()
        True
    """

    def __init__(self, progid, host):
        """
        Create an OpenOpcSource.

        Args:
            progid: OPC-DA server ProgID
            host: Server hostname
        """
        self._progid = progid
        self._host = host

    def discover(self, prefix):
        """
        Discover tags under the given prefix.

        Args:
            prefix: Tag path prefix to search

        Returns:
            Either[Problem, list of TagPath]
        """
        try:
            import OpenOPC
            client = OpenOPC.client()
            client.connect(self._progid, self._host)
            try:
                items = self._flatten(client, prefix)
                tags = [TagPath(item) for item in items if item]
                return Right(tags)
            finally:
                client.close()
        except Exception as e:
            return Left(Problem(
                "Discovery failed",
                {"progid": self._progid, "host": self._host, "error": str(e)}
            ))

    def _flatten(self, client, prefix):
        """
        Recursively flatten tag hierarchy.

        Args:
            client: OpenOPC client
            prefix: Current path prefix

        Returns:
            List of tag path strings
        """
        result = []
        if prefix:
            children = client.list(prefix)
        else:
            children = client.list()
        for child in children:
            if prefix and child.startswith(prefix + "."):
                full = child
            elif prefix:
                full = "%s.%s" % (prefix, child)
            else:
                full = child
            subchildren = client.list(full)
            if subchildren and subchildren != [full]:
                result.extend(self._flatten(client, full))
            else:
                result.append(full)
        return result

    def __repr__(self):
        """
        Return string representation.

        Returns:
            String showing OpenOpcSource configuration
        """
        return "OpenOpcSource(%r, %r)" % (self._progid, self._host)
