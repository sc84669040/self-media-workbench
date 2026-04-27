from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from create_studio_store import CreateStudioStore  # noqa: E402
from fts_search_service import rebuild_fts_index, search_content_objects_fts  # noqa: E402


def _build_store(tmp_path: Path) -> tuple[CreateStudioStore, Path]:
    db_path = tmp_path / "create_studio.db"
    store = CreateStudioStore(db_path)
    store.initialize()
    return store, db_path


def test_fts_search_can_build_index_and_return_relevant_objects(tmp_path):
    store, db_path = _build_store(tmp_path)
    store.upsert_content_object(
        {
            "object_uid": "OBJ-ANA-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://hermes-value",
            "title": "Hermes 的价值判断",
            "summary": "解释 Hermes 在内容生产链路中的价值。",
            "body_text": "Hermes 的价值不只是提效，而是把检索、证据和写作编排成稳定链路。",
            "body_ready": True,
            "source_name": "Hermes",
            "tags": ["创作台", "编排层"],
            "related_topics": ["Hermes", "内容生产链路"],
            "metadata": {"event_packet_refs": ["EVT-001"]},
        }
    )
    store.upsert_content_object(
        {
            "object_uid": "OBJ-NOISE-001",
            "source_kind": "raw_article",
            "platform": "wechat",
            "source_ref": "raw://noise",
            "title": "无关样本",
            "summary": "这条内容和主题关系不大。",
            "body_text": "这里主要是无关背景。",
            "body_ready": True,
            "source_name": "Noise",
            "tags": ["背景"],
            "related_topics": ["杂项"],
        }
    )

    result = search_content_objects_fts(
        create_studio_db_path=db_path,
        query="Hermes 的价值",
        query_terms=["Hermes 价值", "内容生产链路"],
        limit=5,
    )

    assert result["ok"] is True
    assert result["fts_refreshed"] is True
    assert result["count"] >= 1
    top = result["items"][0]
    assert top["object_uid"] == "OBJ-ANA-001"
    assert "Hermes" in top["title"]
    assert "title" in top["field_hits"] or "summary" in top["field_hits"]
    assert "正文完整" in top["why_relevant"]


def test_fts_search_supports_body_ready_and_source_kind_filters(tmp_path):
    store, db_path = _build_store(tmp_path)
    store.upsert_content_object(
        {
            "object_uid": "OBJ-ANA-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://ai-workflow",
            "title": "AI 编排层分析卡",
            "summary": "解释 AI 编排层为什么重要。",
            "body_text": "正文已经齐全，可以直接引用。",
            "body_ready": True,
            "source_name": "知识库",
        }
    )
    store.upsert_content_object(
        {
            "object_uid": "OBJ-RAW-001",
            "source_kind": "wechat_raw_item",
            "platform": "wechat",
            "source_ref": "raw://ai-workflow",
            "title": "AI 编排层元数据",
            "summary": "只有摘要，正文待补。",
            "body_text": "",
            "body_ready": False,
            "source_name": "公众号",
        }
    )

    rebuild_fts_index(db_path)
    result = search_content_objects_fts(
        create_studio_db_path=db_path,
        query="AI 编排层",
        limit=5,
        body_ready_only=True,
        source_kinds=["analysis_card"],
    )

    assert result["ok"] is True
    assert result["count"] == 1
    assert result["items"][0]["object_uid"] == "OBJ-ANA-001"
    assert result["items"][0]["source_kind"] == "analysis_card"


def test_fts_search_auto_refreshes_when_content_objects_change(tmp_path):
    store, db_path = _build_store(tmp_path)
    store.upsert_content_object(
        {
            "object_uid": "OBJ-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://one",
            "title": "Topic intent first pass",
            "summary": "Start with topic understanding.",
            "body_text": "The first pass wires topic intent into the workflow.",
            "body_ready": True,
            "source_name": "知识库",
        }
    )

    first = search_content_objects_fts(
        create_studio_db_path=db_path,
        query="topic intent",
        query_terms=["topic", "intent"],
        limit=5,
    )
    assert first["count"] == 1

    store.upsert_content_object(
        {
            "object_uid": "OBJ-002",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://two",
            "title": "Topic intent full text recall",
            "summary": "Connect local index into creation retrieval.",
            "body_text": "Full text recall lets the topic entry reach the right material.",
            "body_ready": True,
            "source_name": "知识库",
        }
    )

    second = search_content_objects_fts(
        create_studio_db_path=db_path,
        query="topic intent",
        query_terms=["topic", "intent"],
        limit=5,
    )

    assert second["fts_refreshed"] is True
    assert second["count"] == 2
    assert {item["object_uid"] for item in second["items"]} == {"OBJ-001", "OBJ-002"}


def test_fts_search_supports_chinese_phrase_queries(tmp_path):
    store, db_path = _build_store(tmp_path)
    store.upsert_content_object(
        {
            "object_uid": "OBJ-CLAUDE-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://claude-code-guide",
            "title": "Claude Code 使用方法",
            "summary": "整理 Claude Code 的上手路径与典型场景。",
            "body_text": "这篇内容专门讨论 Claude Code 的使用方法、命令习惯和适用边界。",
            "body_ready": True,
            "source_name": "知识库",
            "related_topics": ["Claude Code", "使用方法"],
        }
    )

    result = search_content_objects_fts(
        create_studio_db_path=db_path,
        query="Claude Code 使用方法",
        query_terms=["Claude", "Code", "使用方法"],
        limit=5,
    )

    assert result["ok"] is True
    assert result["count"] == 1
    assert result["items"][0]["object_uid"] == "OBJ-CLAUDE-001"
