import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for p in (os.path.join(ROOT, "common"), os.path.join(ROOT, "vm4_search"), os.path.join(ROOT, "tools")):
    if p not in sys.path:
        sys.path.insert(0, p)
