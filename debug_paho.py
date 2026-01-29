# -*- coding: utf-8 -*-
from __future__ import print_function
import sys

print("Python executable:", sys.executable)
print("Python version:", sys.version)
print("Path:")
for p in sys.path:
    print("  ", p)

print()
try:
    import paho
    print("paho location:", paho.__file__)
except ImportError as e:
    print("paho import failed:", e)

try:
    import paho.mqtt
    print("paho.mqtt location:", paho.mqtt.__file__)
except ImportError as e:
    print("paho.mqtt import failed:", e)

try:
    import paho.mqtt.client as mqtt
    print("paho.mqtt.client OK, version:", mqtt.__version__)
except ImportError as e:
    print("paho.mqtt.client failed:", e)
