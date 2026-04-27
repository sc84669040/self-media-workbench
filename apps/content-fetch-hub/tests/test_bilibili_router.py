from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from fetch_router import build_default_registry  # noqa: E402


def test_registry_resolves_bilibili_before_web_adapter():
    reg = build_default_registry()

    route = reg.resolve("https://www.bilibili.com/video/BV1ZuQvB9EVr/")

    assert route.channel == "bilibili"
    assert route.adapter.name == "bilibili"
