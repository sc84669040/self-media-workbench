from __future__ import annotations

import sys
import importlib.util
from pathlib import Path

CREATIVE_CONFIG = Path(__file__).resolve().parents[3] / "apps" / "creative-studio" / "scripts"
creative_config_text = str(CREATIVE_CONFIG)
if creative_config_text in sys.path:
    sys.path.remove(creative_config_text)
sys.path.insert(0, creative_config_text)

_SPEC = importlib.util.spec_from_file_location("_creative_studio_config", CREATIVE_CONFIG / "create_studio_config.py")
if _SPEC is None or _SPEC.loader is None:
    raise ImportError("Cannot load creative-studio config adapter")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

load_create_studio_config = _MODULE.load_create_studio_config
get_create_studio_config_summary = _MODULE.get_create_studio_config_summary
