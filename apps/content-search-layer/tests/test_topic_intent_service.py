from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from topic_intent_service import build_topic_intent  # noqa: E402


def test_topic_intent_expands_value_question_into_facets_and_queries():
    intent = build_topic_intent("Hermes 的价值")

    assert intent.topic == "Hermes 的价值"
    assert intent.normalized_topic == "Hermes 在现有内容生产链路中的价值"
    assert "Hermes" in intent.entities
    assert "重要性" in intent.topic_facets
    assert "创新点" in intent.topic_facets
    assert "成绩" in intent.topic_facets
    assert "对比" in intent.topic_facets
    assert "Hermes 重要性" in intent.expanded_queries
    assert "Hermes 创新点" in intent.expanded_queries


def test_topic_intent_can_extract_multiple_entities_for_compare_topic():
    intent = build_topic_intent("Hermes 和 OpenClaw 的区别")

    assert "Hermes" in intent.entities
    assert "OpenClaw" in intent.entities
    assert "差异化" in intent.topic_facets
    assert any("对比" in item for item in intent.expanded_queries)
