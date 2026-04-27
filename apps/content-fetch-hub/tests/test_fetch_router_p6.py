from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from fetch_router import build_default_registry  # noqa: E402


def test_registry_avoids_false_positive_x_match_from_query_string():
    reg = build_default_registry()

    route = reg.resolve("https://example.com/redirect?next=https://x.com/openai/status/123456")

    assert route.channel == "web"
    assert route.adapter.name == "web"


def test_registry_resolves_real_x_url_to_x_adapter():
    reg = build_default_registry()

    route = reg.resolve("https://x.com/openai/status/123456")

    assert route.channel == "x"
    assert route.adapter.name == "x"
