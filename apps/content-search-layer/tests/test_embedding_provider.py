from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from embedding_provider import DisabledEmbeddingProvider, MockEmbeddingProvider, load_embedding_provider  # noqa: E402


def test_load_embedding_provider_defaults_to_disabled():
    provider = load_embedding_provider({"semantic_search": {"embedding_provider": "disabled", "enable_embedding": False}})
    assert isinstance(provider, DisabledEmbeddingProvider)
    assert provider.enabled is False


def test_mock_embedding_provider_ranks_more_relevant_object_first():
    provider = MockEmbeddingProvider(model_name="mock-semantic-v1")
    result = provider.search(
        query="Hermes 的价值",
        query_terms=["Hermes 价值", "内容生产链路"],
        topic_intent={"entities": ["Hermes"], "topic_facets": ["重要性"]},
        objects=[
            {
                "object_uid": "OBJ-001",
                "title": "Hermes 的价值判断",
                "summary": "解释 Hermes 在内容生产链路中的价值。",
                "body_text": "Hermes 把检索和写作编排成稳定链路。",
                "related_topics": ["Hermes", "内容生产链路"],
                "tags": ["创作台"],
            },
            {
                "object_uid": "OBJ-002",
                "title": "无关样本",
                "summary": "背景噪音。",
                "body_text": "这条内容基本不相关。",
                "related_topics": ["杂项"],
                "tags": ["背景"],
            },
        ],
        top_k=5,
    )

    assert result["ok"] is True
    assert result["enabled"] is True
    assert result["items"][0]["object_uid"] == "OBJ-001"
    assert result["items"][0]["embedding_score"] > 0
    assert result["items"][0]["explain"]["matched_phrases"]
