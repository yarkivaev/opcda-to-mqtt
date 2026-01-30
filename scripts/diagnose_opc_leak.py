# -*- coding: utf-8 -*-
"""
OpenOPC memory leak diagnostic.

Measures actual process memory during OPC reads to identify COM leaks.
Run on Windows with OpenOPC installed.

Usage: python scripts/diagnose_opc_leak.py <progid> <host> <tag>
Example: python scripts/diagnose_opc_leak.py "Matrikon.OPC.Simulation" "localhost" "Random.Int1"
"""
from __future__ import print_function

import gc
import sys
import time

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

try:
    import win32api
    import win32process
    HAS_WIN32 = True
except ImportError:
    HAS_WIN32 = False


def get_memory_mb():
    """Get current process memory in MB."""
    if HAS_PSUTIL:
        import os
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    elif HAS_WIN32:
        handle = win32api.GetCurrentProcess()
        info = win32process.GetProcessMemoryInfo(handle)
        return info["WorkingSetSize"] / (1024 * 1024)
    else:
        return 0


def count_objects():
    """Count objects by type and module."""
    gc.collect()
    counts = {}
    for obj in gc.get_objects():
        typ = type(obj)
        module = getattr(typ, "__module__", "")
        name = typ.__name__
        key = "%s.%s" % (module, name) if module else name
        counts[key] = counts.get(key, 0) + 1
    return counts


def diff_counts(before, after, threshold=5):
    """Return types that grew by more than threshold."""
    growth = {}
    for name in after:
        diff = after.get(name, 0) - before.get(name, 0)
        if diff >= threshold:
            growth[name] = diff
    return growth


def print_object_breakdown(label):
    """Print top object types by count."""
    gc.collect()
    counts = {}
    sizes = {}
    for obj in gc.get_objects():
        typ = type(obj)
        module = getattr(typ, "__module__", "")
        name = typ.__name__
        key = "%s.%s" % (module, name) if module else name
        counts[key] = counts.get(key, 0) + 1
        try:
            sizes[key] = sizes.get(key, 0) + sys.getsizeof(obj)
        except TypeError:
            pass
    print("  %s - Top 15 object types:" % label)
    sorted_counts = sorted(counts.items(), key=lambda x: -x[1])[:15]
    for name, count in sorted_counts:
        size_kb = sizes.get(name, 0) / 1024.0
        print("    %-40s count=%-6d size=%.1f KB" % (name, count, size_kb))


def main():
    if len(sys.argv) < 4:
        print("Usage: python diagnose_opc_leak.py <progid> <host> <tag>")
        print("Example: python diagnose_opc_leak.py Matrikon.OPC.Simulation localhost Random.Int1")
        sys.exit(1)
    progid = sys.argv[1]
    host = sys.argv[2]
    tag = sys.argv[3]
    if not HAS_PSUTIL and not HAS_WIN32:
        print("WARNING: Cannot measure memory - install psutil or pywin32")
        print("")
    print("OpenOPC Memory Leak Diagnostic")
    print("=" * 70)
    print("Server: %s @ %s" % (progid, host))
    print("Tag: %s" % tag)
    print("=" * 70)
    print("")
    import pythoncom
    import OpenOPC
    pythoncom.CoInitialize()
    print("TEST: 5000 reads with detailed memory breakdown")
    print("-" * 70)
    client = OpenOPC.client()
    client.connect(progid, host)
    gc.collect()
    mem_before = get_memory_mb()
    objects_before = count_objects()
    gc_count_before = sum(objects_before.values())
    print("BEFORE:")
    print("  Process memory: %.2f MB" % mem_before)
    print("  GC object count: %d" % gc_count_before)
    print_object_breakdown("BEFORE")
    print("")
    for checkpoint in [1000, 2000, 3000, 4000, 5000]:
        start = checkpoint - 1000
        for i in range(start, checkpoint):
            client.read(tag, sync=True)
        gc.collect()
        mem_now = get_memory_mb()
        objects_now = count_objects()
        gc_count_now = sum(objects_now.values())
        print("AFTER %d READS:" % checkpoint)
        print("  Process memory: %.2f MB (+%.2f MB)" % (mem_now, mem_now - mem_before))
        print("  GC object count: %d (+%d)" % (gc_count_now, gc_count_now - gc_count_before))
        growth = diff_counts(objects_before, objects_now, threshold=10)
        if growth:
            print("  Growing object types (diff > 10):")
            for name, diff in sorted(growth.items(), key=lambda x: -x[1])[:10]:
                print("    %-40s +%d" % (name, diff))
        else:
            print("  No significant growth in GC-tracked objects")
        native_leak = (mem_now - mem_before) * 1024 - (gc_count_now - gc_count_before) * 0.1
        if native_leak > 100:
            print("  ** NATIVE/COM MEMORY LEAK DETECTED **")
            print("  Process memory grew but GC objects stable = COM leak")
        print("")
    client.close()
    print("=" * 70)
    print("FINAL BREAKDOWN:")
    print("=" * 70)
    gc.collect()
    print_object_breakdown("FINAL")
    print("")
    print("=" * 70)
    print("ANALYSIS:")
    print("=" * 70)
    mem_final = get_memory_mb()
    gc_final = sum(count_objects().values())
    mem_growth = mem_final - mem_before
    gc_growth = gc_final - gc_count_before
    print("Total process memory growth: %.2f MB" % mem_growth)
    print("Total GC object growth: %d objects" % gc_growth)
    print("")
    if mem_growth > 1 and gc_growth < 100:
        print("CONCLUSION: Native/COM memory leak")
        print("  Process memory grew significantly but Python objects are stable.")
        print("  The leak is in OpenOPC COM objects or Windows COM subsystem.")
        print("  This is NOT fixable from Python code.")
        print("")
        print("WORKAROUND: Restart the process periodically")
        leak_per_read = mem_growth / 5000.0
        print("  Leak rate: %.4f MB per read (%.2f MB per 1000 reads)" % (leak_per_read, leak_per_read * 1000))
    elif gc_growth > 100:
        print("CONCLUSION: Python object leak")
        print("  GC-tracked objects are growing. Check the growing types above.")
    else:
        print("CONCLUSION: No significant leak detected")
    print("=" * 70)
    pythoncom.CoUninitialize()


if __name__ == "__main__":
    main()
