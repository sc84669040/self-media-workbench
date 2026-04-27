from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"

scripts_dir = str(SCRIPTS_DIR)
if scripts_dir in sys.path:
    sys.path.remove(scripts_dir)
sys.path.insert(0, scripts_dir)
