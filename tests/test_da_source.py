# -*- coding: utf-8 -*-
"""
Tests for DaSource and FakeDaSource.
"""
from __future__ import print_function

import logging
import random
import string
import unittest

from opcda_to_mqtt.da.fake import FakeDaSource
from opcda_to_mqtt.domain.path import TagPath

logging.disable(logging.CRITICAL)


class TestFakeDaSource(unittest.TestCase):
    """Tests for FakeDaSource."""

    def test_fake_source_discover_returns_right(self):
        tags = [TagPath("Tag1"), TagPath("Tag2")]
        source = FakeDaSource(tags)
        result = source.discover("prefix")
        self.assertTrue(
            result.is_right(),
            "FakeDaSource.discover should return Right"
        )

    def test_fake_source_discover_returns_configured_tags(self):
        path = "".join(random.choice(string.ascii_letters) for _ in range(10))
        tags = [TagPath(path)]
        source = FakeDaSource(tags)
        result = source.discover("")
        discovered = result.fold(lambda e: [], lambda t: t)
        self.assertEqual(
            len(discovered),
            1,
            "FakeDaSource should return configured tags"
        )

    def test_fake_source_discover_ignores_prefix(self):
        tags = [TagPath("A"), TagPath("B")]
        source = FakeDaSource(tags)
        prefix = "".join(random.choice(string.ascii_letters) for _ in range(5))
        result = source.discover(prefix)
        discovered = result.fold(lambda e: [], lambda t: t)
        self.assertEqual(
            len(discovered),
            2,
            "FakeDaSource should ignore prefix"
        )

    def test_fake_source_tags_returns_copy(self):
        tags = [TagPath("Original")]
        source = FakeDaSource(tags)
        returned = source.tags()
        returned.append(TagPath("Added"))
        self.assertEqual(
            len(source.tags()),
            1,
            "FakeDaSource.tags should return a copy"
        )

    def test_fake_source_discover_returns_independent_list(self):
        tags = [TagPath("Tag")]
        source = FakeDaSource(tags)
        result1 = source.discover("")
        list1 = result1.fold(lambda e: [], lambda t: t)
        list1.append(TagPath("New"))
        result2 = source.discover("")
        list2 = result2.fold(lambda e: [], lambda t: t)
        self.assertEqual(
            len(list2),
            1,
            "Each discover call should return independent list"
        )

    def test_fake_source_handles_empty_tags(self):
        source = FakeDaSource([])
        result = source.discover("any")
        discovered = result.fold(lambda e: ["error"], lambda t: t)
        self.assertEqual(
            len(discovered),
            0,
            "FakeDaSource should handle empty tags"
        )

    def test_fake_source_preserves_cyrillic_tags(self):
        path = u"COM1.\u0422\u041c_5104"
        tags = [TagPath(path)]
        source = FakeDaSource(tags)
        result = source.discover("")
        discovered = result.fold(lambda e: [], lambda t: t)
        self.assertEqual(
            discovered[0].text(),
            path,
            "FakeDaSource should preserve Cyrillic tags"
        )


class StubOpcClient:
    """Stub OPC client for testing flatten logic."""

    def __init__(self, hierarchy):
        """
        Create stub with hierarchy dict.

        Args:
            hierarchy: Dict mapping prefix to list of relative children
        """
        self._hierarchy = hierarchy

    def list(self, prefix=None):
        """
        Return relative children for prefix.

        Args:
            prefix: Path prefix

        Returns:
            List of relative child names
        """
        if prefix is None:
            return self._hierarchy.get("", [])
        return self._hierarchy.get(prefix, [])


class TestOpenOpcSourceFlatten(unittest.TestCase):
    """Tests for OpenOpcSource._flatten method."""

    def test_flatten_returns_leaves_from_flat_structure(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "COM1": ["Tag1", "Tag2"],
            "COM1.Tag1": [],
            "COM1.Tag2": []
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            sorted(result),
            ["COM1.Tag1", "COM1.Tag2"],
            "Should return full path leaf tags"
        )

    def test_flatten_recurses_into_branches(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "COM1": ["Device1"],
            "COM1.Device1": ["Temp", "Pressure"],
            "COM1.Device1.Temp": [],
            "COM1.Device1.Pressure": []
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            sorted(result),
            ["COM1.Device1.Pressure", "COM1.Device1.Temp"],
            "Should recurse into branches"
        )

    def test_flatten_handles_multiple_levels(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "A": ["B"],
            "A.B": ["C"],
            "A.B.C": ["Tag"],
            "A.B.C.Tag": []
        })
        result = source._flatten(client, "A")
        self.assertEqual(
            result,
            ["A.B.C.Tag"],
            "Should handle multiple nesting levels"
        )

    def test_flatten_handles_empty_prefix(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "": ["Root1", "Root2"],
            "Root1": [],
            "Root2": []
        })
        result = source._flatten(client, "")
        self.assertEqual(
            sorted(result),
            ["Root1", "Root2"],
            "Should handle empty prefix"
        )

    def test_flatten_returns_empty_for_no_children(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "COM1": []
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            result,
            [],
            "Should return empty for no children"
        )

    def test_flatten_handles_mixed_branches_and_leaves(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "COM1": ["Tag1", "Device"],
            "COM1.Tag1": [],
            "COM1.Device": ["Tag2"],
            "COM1.Device.Tag2": []
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            sorted(result),
            ["COM1.Device.Tag2", "COM1.Tag1"],
            "Should handle mixed branches and leaves"
        )

    def test_flatten_preserves_cyrillic_paths(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        child = u"\u0422\u041c_5104"
        client = StubOpcClient({
            "COM1": [child],
            "COM1." + child: []
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            result,
            ["COM1." + child],
            "Should preserve Cyrillic paths"
        )

    def test_flatten_handles_full_paths_from_server(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "COM1": ["COM1.Tag1", "COM1.Tag2"],
            "COM1.Tag1": [],
            "COM1.Tag2": []
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            sorted(result),
            ["COM1.Tag1", "COM1.Tag2"],
            "Should handle full paths returned by server"
        )

    def test_flatten_handles_full_paths_with_branches(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "COM1": ["COM1.Device"],
            "COM1.Device": ["COM1.Device.Tag"],
            "COM1.Device.Tag": []
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            result,
            ["COM1.Device.Tag"],
            "Should handle full paths with branches"
        )

    def test_flatten_handles_leaf_returning_itself(self):
        from opcda_to_mqtt.da.openopc import OpenOpcSource
        source = OpenOpcSource("progid", "host")
        client = StubOpcClient({
            "COM1": ["Device"],
            "COM1.Device": ["COM1.Device.Channel"],
            "COM1.Device.Channel": ["COM1.Device.Channel"]
        })
        result = source._flatten(client, "COM1")
        self.assertEqual(
            result,
            ["COM1.Device.Channel"],
            "Should treat leaf returning itself as leaf"
        )


if __name__ == "__main__":
    unittest.main()
