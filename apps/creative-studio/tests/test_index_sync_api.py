from __future__ import annotations

import importlib
import json
import sqlite3
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
WEB_DIR = ROOT / "web"
for path in (SCRIPTS_DIR, WEB_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _request(base_url: str, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(f"{base_url}{path}", data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _write_sync_fixture_config(tmp_path: Path) -> Path:
    vault_root = tmp_path / "vault"
    create_studio_db = tmp_path / "create_studio.db"
    nighthawk_db = tmp_path / "event_radar.db"
    config_path = tmp_path / "config" / "create-studio.yaml"

    vault_root.mkdir(parents=True, exist_ok=True)
    (vault_root / "分析卡片").mkdir(parents=True, exist_ok=True)
    (vault_root / "原文").mkdir(parents=True, exist_ok=True)

    (vault_root / "分析卡片" / "AI趋势.analysis.md").write_text(
        """---
title: "AI 趋势分析卡"
related_topics:
  - "AI 趋势"
---

# 分析卡片：AI 趋势分析卡

## 1. 一句话摘要
这里是一张可进入统一索引的分析卡。
""",
        encoding="utf-8",
    )
    (vault_root / "原文" / "创作笔记.md").write_text(
        """# 创作笔记

这是一篇会进入统一索引并被切块的原文笔记。""" * 30,
        encoding="utf-8",
    )

    with sqlite3.connect(nighthawk_db) as conn:
        conn.execute(
            """
            CREATE TABLE raw_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              platform TEXT NOT NULL,
              source_handle TEXT,
              item_id TEXT,
              title TEXT,
              content TEXT,
              url TEXT,
              published_at TEXT,
              metrics_json TEXT,
              fetched_at TEXT,
              body_status TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw_items(
              platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at, body_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "wechat",
                "official-account",
                "wx-sync-001",
                "同步测试原文",
                "这是一篇会被同步到创作台索引并进一步切块的数据库正文。" * 40,
                "https://example.com/wx-sync-001",
                "2026-04-20T09:00:00+08:00",
                json.dumps({"source_kind": "official_feed", "body_fetch_ok": True}, ensure_ascii=False),
                "2026-04-20T09:10:00+08:00",
                "success",
            ),
        )
        conn.commit()

    config_payload = {
        "knowledge_sources": {
            "vault_roots": [str(vault_root)],
            "analysis_card_globs": [str(vault_root / "分析卡片" / "**" / "*.md")],
            "source_note_globs": [str(vault_root / "**" / "*.md")],
        },
        "database_sources": {
            "nighthawk_db_path": str(nighthawk_db),
        },
        "creation_workspace": {
            "data_root": str(tmp_path / "creation"),
        },
        "indexing": {
            "content_index_db_path": str(create_studio_db),
            "chunk_size": 200,
            "chunk_overlap": 40,
        },
    }
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config_payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path


def test_index_sync_api_runs_full_pipeline_and_exposes_summary(monkeypatch, tmp_path):
    config_path = _write_sync_fixture_config(tmp_path)
    monkeypatch.setenv("CREATE_STUDIO_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("CONTENT_SEARCH_CREATION_DATA_ROOT", str(tmp_path / "creation"))

    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        sync_status, sync_payload = _request(base_url, "POST", "/api/create/index/sync", {})
        status_code, status_payload = _request(base_url, "GET", "/api/create/index/status")
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()

    assert sync_status == 200
    assert sync_payload["ok"] is True
    assert sync_payload["index_sync_summary"]["status"] == "completed"
    assert sync_payload["index_sync_summary"]["totals"]["new_objects"] >= 3
    assert sync_payload["index_sync_summary"]["totals"]["new_chunks"] >= 2
    assert any(phase["name"] == "全文检索索引刷新" for phase in sync_payload["index_sync_summary"]["phases"])
    assert sync_payload["phase_results"]["fts"]["ok"] is True
    assert sync_payload["phase_results"]["fts"]["indexed_count"] >= 3

    assert status_code == 200
    assert status_payload["index_sync_summary"]["status"] == "completed"
    assert status_payload["table_counts"]["content_objects"] >= 3
    assert status_payload["table_counts"]["content_chunks"] >= 2
    assert status_payload["latest_sync_run"]["status"] == "completed"
