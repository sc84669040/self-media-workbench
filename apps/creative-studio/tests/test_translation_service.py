from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from translation_service import enrich_raw_item_translations, get_or_translate_field, should_translate_text


@pytest.fixture()
def translation_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_PROVIDER", "mock")
    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_CACHE_DB_PATH", str(tmp_path / "translation_cache.db"))


def test_should_translate_text_only_for_english_like_content():
    assert should_translate_text("Claude Code usage guide for beginners") is True
    assert should_translate_text("这是中文标题") is False
    assert should_translate_text("AI") is False


def test_get_or_translate_field_uses_cache_on_second_call(translation_env):
    first = get_or_translate_field(101, "title", "Claude Code usage guide")
    second = get_or_translate_field(101, "title", "Claude Code usage guide")

    assert first["translated"] is True
    assert first["display_text"].startswith("【中文】")
    assert second["translated"] is True
    assert second["cached"] is True


def test_enrich_raw_item_translations_populates_display_fields(translation_env):
    item = enrich_raw_item_translations(
        {
            "raw_item_id": 301,
            "title": "Claude Code release notes",
            "summary": "A practical walkthrough for the newest Claude Code release.",
            "content_preview": "A practical walkthrough for the newest Claude Code release.",
            "body_text": "This update adds better editing, planning, and review support.",
        },
        include_body=True,
    )

    assert item["display_title"].startswith("【中文】")
    assert item["display_summary"].startswith("【中文】")
    assert item["display_body_text"].startswith("【中文】")
    assert item["translation_meta"]["title_translated"] is True
