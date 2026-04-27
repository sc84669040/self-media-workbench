from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
WEB_DIR = ROOT / "web"

for path in (WEB_DIR, SCRIPTS_DIR):
    text = str(path)
    if text in sys.path:
        sys.path.remove(text)
    sys.path.insert(0, text)
