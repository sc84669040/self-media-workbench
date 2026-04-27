from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from create_studio_store import CreateStudioStore  # noqa: E402
from semantic_search_service import hybrid_search_content_objects  # noqa: E402


def _build_store(tmp_path: Path) -> tuple[CreateStudioStore, Path]:
    db_path = tmp_path / "create_studio.db"
    store = CreateStudioStore(db_path)
    store.initialize()
    return store, db_path


def test_hybrid_search_returns_scores_and_explain(tmp_path):
    store, db_path = _build_store(tmp_path)
    store.upsert_content_object(
        {
            "object_uid": "OBJ-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://hermes",
            "title": "Hermes 的价值判断",
            "summary": "解释 Hermes 在内容生产链路中的价值。",
            "body_text": "Hermes 把检索、证据和写作编排成稳定链路。",
            "body_ready": True,
            "published_at": "2026-04-20T09:00:00+08:00",
            "source_name": "Hermes",
            "related_topics": ["Hermes", "内容生产链路"],
            "tags": ["创作台", "编排层"],
        }
    )
    store.upsert_content_object(
        {
            "object_uid": "OBJ-002",
            "source_kind": "raw_article",
            "platform": "wechat",
            "source_ref": "raw://workflow",
            "title": "工作流案例",
            "summary": "提供一条补充案例。",
            "body_text": "更多是案例补充，不是主题主判断。",
            "body_ready": True,
            "published_at": "2026-04-19T09:00:00+08:00",
            "source_name": "公众号",
        }
    )

    result = hybrid_search_content_objects(
        create_studio_db_path=db_path,
        query="Hermes 的价值",
        query_terms=["Hermes 价值", "内容生产链路"],
        topic_intent={"entities": ["Hermes"], "topic_facets": ["重要性"]},
        config={
            "semantic_search": {
                "enable_fts": True,
                "enable_embedding": True,
                "embedding_provider": "mock",
                "rerank_top_k": 20,
                "final_top_k": 10,
            }
        },
    )

    assert result["ok"] is True
    assert result["items"]
    top = result["items"][0]
    assert top["object_uid"] == "OBJ-001"
    assert top["search_scores"]["final_score"] > 0
    assert top["search_scores"]["embedding_score"] > 0
    assert top["search_explain"]["structured"]["matched_entities"] == ["hermes"]
    assert result["query_explain"]["embedding_provider"] == "mock"


def test_hybrid_search_fetches_fts_hits_outside_recent_window(tmp_path):
    store, db_path = _build_store(tmp_path)

    for index in range(200):
        store.upsert_content_object(
            {
                "object_uid": f"OBJ-{index:03d}",
                "source_kind": "wechat_raw_item",
                "platform": "wechat",
                "source_ref": f"raw://{index:03d}",
                "canonical_url": f"https://example.com/{index:03d}",
                "title": f"普通文章 {index}",
                "summary": "没有目标关键词。",
                "body_text": "这是一篇普通正文。",
                "body_ready": True,
                "published_at": f"2026-04-21T00:{index % 60:02d}:00+08:00",
                "source_name": "测试源",
                "tags": [],
                "related_topics": [],
                "metadata": {},
            }
        )

    store.upsert_content_object(
        {
            "object_uid": "OBJ-LEGACY-AI",
            "source_kind": "wechat_raw_item",
            "platform": "wechat",
            "source_ref": "raw://legacy-ai",
            "canonical_url": "https://example.com/legacy-ai",
            "title": "AI 深度文章",
            "summary": "这篇文章讨论 AI 发展。",
            "body_text": "AI 正文内容完整，可被 FTS 检索命中。",
            "body_ready": True,
            "published_at": "2026-01-01T00:00:00+08:00",
            "source_name": "测试源",
            "tags": ["AI"],
            "related_topics": ["AI"],
            "metadata": {},
        }
    )

    config = {
        "indexing": {"content_index_db_path": str(db_path)},
        "semantic_search": {
            "enable_fts": True,
            "enable_embedding": False,
            "embedding_provider": "disabled",
            "rerank_top_k": 40,
            "final_top_k": 20,
        },
    }

    result = hybrid_search_content_objects(
        create_studio_db_path=db_path,
        query="ai",
        query_terms=["ai"],
        topic_intent={"topic": "ai", "entities": ["ai"], "topic_facets": []},
        config=config,
    )

    object_uids = [str(item.get("object_uid") or "") for item in list(result.get("items") or [])]
    assert "OBJ-LEGACY-AI" in object_uids
