from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from content_object_models import (  # noqa: E402
    build_content_object_uid,
    content_object_from_markdown_document,
    content_object_from_raw_item,
    content_object_from_transcript_payload,
)


def test_content_object_uid_is_stable_for_same_source_identity():
    first_uid = build_content_object_uid(
        source_kind="analysis_card",
        platform="knowledge_base",
        source_ref="note://analysis/001",
        canonical_url="https://example.com/analysis/001",
    )
    second_uid = build_content_object_uid(
        source_kind="analysis_card",
        platform="knowledge_base",
        source_ref="note://analysis/001",
        canonical_url="https://example.com/analysis/001",
    )

    assert first_uid == second_uid
    assert first_uid.startswith("CO-")


def test_raw_item_content_object_keeps_metadata_only_item_as_not_ready():
    obj = content_object_from_raw_item(
        {
            "id": 101,
            "platform": "wechat",
            "source_handle": "nighthawk-feed",
            "item_id": "wx-001",
            "title": "夜鹰主链路切到 metadata-only",
            "content": "这里只是一个很短的元数据摘要。",
            "url": "https://example.com/wx-001",
            "published_at": "2026-04-20T09:00:00+08:00",
            "body_status": "pending",
            "metrics_json": {
                "source_kind": "official_feed",
                "body_fetch_ok": False,
                "related_topics": ["正文补抓"],
            },
        }
    )

    assert obj.source_kind == "official_feed"
    assert obj.source_ref == "raw_item:wechat:wx-001"
    assert obj.body_ready is False
    assert obj.related_topics == ["正文补抓"]
    assert obj.metadata["body_status"] == "pending"


def test_raw_item_content_object_marks_hydrated_body_as_ready():
    obj = content_object_from_raw_item(
        {
            "id": 202,
            "platform": "wechat",
            "source_handle": "official-account",
            "item_id": "wx-002",
            "title": "创作台独立索引库落地说明",
            "content": "这里已经是完整正文，不再只是摘要。它说明了为什么资料理解层需要独立于编排层存在。",
            "url": "https://example.com/wx-002",
            "published_at": "2026-04-20T10:00:00+08:00",
            "body_status": "success",
            "metrics_json": {
                "source_kind": "official_feed",
                "body_fetch_ok": True,
                "tags": ["创作台", "索引库"],
            },
        }
    )

    assert obj.body_ready is True
    assert obj.canonical_url == "https://example.com/wx-002"
    assert obj.tags == ["创作台", "索引库"]
    assert obj.content_hash


def test_x_raw_item_with_real_post_text_counts_as_ready_body():
    obj = content_object_from_raw_item(
        {
            "id": 303,
            "platform": "x",
            "source_handle": "openai",
            "item_id": "x-001",
            "title": "Claude 与 AI 工作流讨论",
            "content": "这是一条已经包含完整核心表达的 X 帖子内容，不应该因为没有单独补抓正文就被排除在正文池外。",
            "url": "https://x.com/openai/status/x-001",
            "published_at": "2026-04-21T09:00:00+08:00",
            "body_status": "none",
            "metrics_json": {
                "source_kind": "x_raw_item",
                "body_fetch_ok": None,
                "tags": ["AI"],
            },
        }
    )

    assert obj.platform == "x"
    assert obj.body_ready is True


def test_markdown_document_content_object_parses_frontmatter_and_body():
    markdown_text = """---
title: 创作台分析卡
platform: knowledge_base
canonical_url: https://example.com/cards/001
summary: 这是分析卡摘要。
tags:
  - 创作台
  - 分析卡
related_topics:
  - 语义检索
source_name: Hermes
---
# 创作台分析卡

这是一张完整分析卡，正文已经齐全，可以直接进入统一索引。
"""
    obj = content_object_from_markdown_document(
        "/sample-vault/analysis-cards/create-studio-analysis-card.md",
        markdown_text,
    )

    assert obj.source_kind == "analysis_card"
    assert obj.platform == "knowledge_base"
    assert obj.title == "创作台分析卡"
    assert obj.summary == "这是分析卡摘要。"
    assert obj.body_ready is True
    assert obj.tags == ["创作台", "分析卡"]
    assert obj.related_topics == ["语义检索"]


def test_transcript_payload_content_object_becomes_searchable_object():
    obj = content_object_from_transcript_payload(
        {
            "platform": "youtube",
            "video_id": "yt-001",
            "url": "https://youtube.com/watch?v=yt-001",
            "title": "AI 工作流视频",
            "transcript_text": "这里是一段完整字幕，足够作为后续检索和引用的正文基础。",
            "transcript_language": "zh-Hans",
            "source_name": "Hermes Channel",
            "related_topics": ["AI 工作流"],
        }
    )

    assert obj.source_kind == "transcript"
    assert obj.platform == "youtube"
    assert obj.source_ref == "yt-001"
    assert obj.body_ready is True
    assert obj.metadata["transcript_language"] == "zh-Hans"
    assert obj.related_topics == ["AI 工作流"]
