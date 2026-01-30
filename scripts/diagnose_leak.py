# -*- coding: utf-8 -*-
"""
Memory leak diagnostic script.

Run this in production to identify which objects are accumulating.
Usage: python scripts/diagnose_leak.py
"""
from __future__ import print_function

import gc
import sys
import time


def count_objects():
    """Count objects by type name."""
    gc.collect()
    counts = {}
    for obj in gc.get_objects():
        name = type(obj).__name__
        counts[name] = counts.get(name, 0) + 1
    return counts


def diff_counts(before, after):
    """Return types that grew by more than threshold."""
    growth = {}
    for name in after:
        diff = after.get(name, 0) - before.get(name, 0)
        if diff > 0:
            growth[name] = diff
    return growth


def main():
    print("Memory Leak Diagnostic")
    print("=" * 50)
    print("Taking initial snapshot...")
    gc.collect()
    initial = count_objects()
    print("Initial object count: %d" % sum(initial.values()))
    print("")
    print("Waiting 30 seconds...")
    time.sleep(30)
    print("")
    print("Taking second snapshot...")
    gc.collect()
    middle = count_objects()
    print("Middle object count: %d" % sum(middle.values()))
    print("")
    print("Waiting another 30 seconds...")
    time.sleep(30)
    print("")
    print("Taking final snapshot...")
    gc.collect()
    final = count_objects()
    print("Final object count: %d" % sum(final.values()))
    print("")
    print("=" * 50)
    print("GROWTH FROM INITIAL TO MIDDLE (30s):")
    print("=" * 50)
    growth1 = diff_counts(initial, middle)
    for name, count in sorted(growth1.items(), key=lambda x: -x[1])[:20]:
        print("  %s: +%d" % (name, count))
    print("")
    print("=" * 50)
    print("GROWTH FROM MIDDLE TO FINAL (30s):")
    print("=" * 50)
    growth2 = diff_counts(middle, final)
    for name, count in sorted(growth2.items(), key=lambda x: -x[1])[:20]:
        print("  %s: +%d" % (name, count))
    print("")
    print("=" * 50)
    print("CONSISTENT GROWTH (appearing in both periods):")
    print("=" * 50)
    consistent = set(growth1.keys()) & set(growth2.keys())
    for name in sorted(consistent, key=lambda x: -(growth1.get(x, 0) + growth2.get(x, 0))):
        print("  %s: +%d then +%d" % (name, growth1[name], growth2[name]))


if __name__ == "__main__":
    main()
