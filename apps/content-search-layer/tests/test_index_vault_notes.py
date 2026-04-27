from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from create_studio_store import CreateStudioStore  # noqa: E402
from index_vault_notes import sync_vault_notes_to_create_studio  # noqa: E402


def _write_file(path: Path, content: str, *, mtime_ns: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    os.utime(path, ns=(mtime_ns, mtime_ns))


def _write_config(path: Path, vault_roots: list[Path], create_studio_db_path: Path) -> None:
    payload = {
        "knowledge_sources": {
            "vault_roots": [str(root) for root in vault_roots],
            "analysis_card_globs": [str(root / "分析卡片" / "**" / "*.md") for root in vault_roots],
            "source_note_globs": [str(root / "**" / "*.md") for root in vault_roots],
        },
        "indexing": {
            "content_index_db_path": str(create_studio_db_path),
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _fetch_all_objects(db_path: Path) -> list[sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            """
            SELECT source_kind, title, source_ref, tags_json, related_topics_json, metadata_json, body_ready, body_text
            FROM content_objects
            ORDER BY source_kind ASC, title ASC
            """
        ).fetchall()


def test_sync_vault_notes_indexes_analysis_cards_and_source_notes_from_config(tmp_path):
    vault_a = tmp_path / "vault-a"
    vault_b = tmp_path / "vault-b"
    create_studio_db = tmp_path / "create_studio.db"
    config_path = tmp_path / "config" / "create-studio.yaml"

    _write_file(
        vault_a / "分析卡片" / "AI工作流.analysis.md",
        """---
title: "AI 工作流分析卡"
source_name: "Hermes"
source_url: "https://example.com/cards/ai-workflow"
published_at: "2026-04-20T09:00:00+08:00"
related_topics:
  - "AI 工作流"
---

# 分析卡片：AI 工作流分析卡

## 1. 一句话摘要
这是给创作台用的一张分析卡。

## 4. 标签
- 创作台
- AI工作流
""",
        mtime_ns=1_710_000_000_000_000_000,
    )
    _write_file(
        vault_a / "原文" / "创作台笔记.md",
        """---
title: "创作台原文笔记"
source_url: "https://example.com/notes/create-studio"
tags:
  - 原文
related_topics:
  - 语义检索
---

# 创作台原文笔记

这里是原文内容，后面会被统一进入创作台索引。
""",
        mtime_ns=1_710_000_000_100_000_000,
    )
    _write_file(
        vault_b / "研究" / "热点事件接入.md",
        """# 热点事件接入

创作台之后要允许从热点事件直接进入创作任务，所以知识对象层需要提前预留接入位。
""",
        mtime_ns=1_710_000_000_200_000_000,
    )
    _write_config(config_path, [vault_a, vault_b], create_studio_db)

    result = sync_vault_notes_to_create_studio(config_path=config_path, full=True)

    assert result["ok"] is True
    assert result["metrics"]["upserted"] == 3
    assert result["metrics"]["analysis_cards"] == 1
    assert result["metrics"]["source_notes"] == 2
    assert result["metrics"]["cluster_hint_objects"] == 2

    store = CreateStudioStore(create_studio_db)
    status = store.get_status()
    assert status["table_counts"]["content_objects"] == 3

    rows = _fetch_all_objects(create_studio_db)
    by_title = {str(row["title"]): row for row in rows}

    analysis_card = by_title["AI 工作流分析卡"]
    analysis_metadata = json.loads(analysis_card["metadata_json"])
    assert analysis_card["source_kind"] == "analysis_card"
    assert analysis_card["body_ready"] == 1
    assert json.loads(analysis_card["tags_json"]) == ["创作台", "AI工作流"]
    assert json.loads(analysis_card["related_topics_json"]) == ["AI 工作流"]
    assert analysis_metadata["cluster_hints"]["source_kind"] == "analysis_card"
    assert analysis_metadata["event_packet_refs"] == []

    source_note = by_title["创作台原文笔记"]
    note_metadata = json.loads(source_note["metadata_json"])
    assert source_note["source_kind"] == "source_note"
    assert source_note["body_ready"] == 1
    assert json.loads(source_note["related_topics_json"]) == ["语义检索"]
    assert note_metadata["cluster_ready"] is False

    plain_note = by_title["热点事件接入"]
    assert plain_note["source_kind"] == "source_note"
    assert plain_note["body_ready"] == 1


def test_sync_vault_notes_uses_incremental_watermark_across_multiple_roots(tmp_path):
    vault_root = tmp_path / "vault"
    create_studio_db = tmp_path / "create_studio.db"
    config_path = tmp_path / "config" / "create-studio.yaml"

    _write_file(
        vault_root / "原文" / "第一篇.md",
        "# 第一篇\n\n这是第一篇笔记。",
        mtime_ns=1_710_000_001_000_000_000,
    )
    _write_config(config_path, [vault_root], create_studio_db)

    first = sync_vault_notes_to_create_studio(config_path=config_path, full=True)
    assert first["metrics"]["upserted"] == 1

    _write_file(
        vault_root / "原文" / "第一篇.md",
        "# 第一篇\n\n这是第一篇笔记，现在正文被扩充了。",
        mtime_ns=1_710_000_002_000_000_000,
    )
    _write_file(
        vault_root / "分析卡片" / "第二篇.analysis.md",
        """---
title: "第二篇分析卡"
related_topics:
  - "热点事件"
---

# 分析卡片：第二篇分析卡

## 1. 一句话摘要
这是第二篇分析卡。
""",
        mtime_ns=1_710_000_003_000_000_000,
    )

    second = sync_vault_notes_to_create_studio(config_path=config_path, full=False)

    assert second["metrics"]["scanned"] == 2
    assert second["metrics"]["upserted"] == 2
    assert second["metrics"]["analysis_cards"] == 1
    assert second["metrics"]["source_notes"] == 1

    rows = _fetch_all_objects(create_studio_db)
    assert len(rows) == 2
    titles = {str(row["title"]): row for row in rows}
    assert "正文被扩充" in str(titles["第一篇"]["body_text"] or "")
    assert titles["第一篇"]["source_kind"] == "source_note"
    assert titles["第二篇分析卡"]["source_kind"] == "analysis_card"
