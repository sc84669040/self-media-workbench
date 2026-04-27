from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from create_studio_store import CreateStudioStore  # noqa: E402
from index_raw_items import sync_raw_items_to_create_studio  # noqa: E402


def _init_nighthawk_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
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
            CREATE TABLE event_candidates (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_type TEXT,
              title TEXT,
              summary TEXT,
              heat_score REAL DEFAULT 0,
              novelty_score REAL DEFAULT 0,
              confidence REAL DEFAULT 0,
              status TEXT DEFAULT 'new'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE event_evidence (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_id INTEGER NOT NULL,
              raw_item_id INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def _fetch_content_object_row(db_path: Path, source_ref: str) -> sqlite3.Row:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT source_ref, title, body_text, body_ready, related_topics_json, metadata_json
            FROM content_objects
            WHERE source_ref = ?
            """,
            (source_ref,),
        ).fetchone()
    assert row is not None
    return row


def test_sync_raw_items_writes_content_objects_and_event_links(tmp_path):
    nighthawk_db = tmp_path / "event_radar.db"
    create_studio_db = tmp_path / "create_studio.db"
    _init_nighthawk_db(nighthawk_db)

    with sqlite3.connect(nighthawk_db) as conn:
        conn.execute(
            """
            INSERT INTO raw_items(
              platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at, body_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "wechat",
                "official-account",
                "wx-100",
                "这是一个热点事件原文",
                "正文已经补全，适合作为创作和热点聚合的底层证据。",
                "https://example.com/wx-100",
                "2026-04-20T09:00:00+08:00",
                json.dumps(
                    {
                        "source_kind": "official_feed",
                        "body_fetch_ok": True,
                        "related_topics": ["AI 工作流", "热点事件"],
                        "source_name": "官方号",
                    },
                    ensure_ascii=False,
                ),
                "2026-04-20T09:30:00+08:00",
                "success",
            ),
        )
        conn.execute(
            """
            INSERT INTO event_candidates(event_type, title, summary, heat_score, novelty_score, confidence, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("product_release", "AI 工作流新动态", "这是一个待聚合事件。", 7.5, 6.0, 0.82, "candidate"),
        )
        conn.execute(
            "INSERT INTO event_evidence(event_id, raw_item_id) VALUES (?, ?)",
            (1, 1),
        )
        conn.commit()

    result = sync_raw_items_to_create_studio(
        nighthawk_db_path=nighthawk_db,
        create_studio_db_path=create_studio_db,
        full=True,
    )

    assert result["ok"] is True
    assert result["metrics"]["scanned"] == 1
    assert result["metrics"]["upserted"] == 1
    assert result["metrics"]["body_ready"] == 1
    assert result["metrics"]["event_linked"] == 1

    store = CreateStudioStore(create_studio_db)
    status = store.get_status()
    assert status["table_counts"]["content_objects"] == 1
    assert status["latest_sync_run"]["status"] == "completed"

    row = _fetch_content_object_row(create_studio_db, "raw_item:wechat:wx-100")
    metadata = json.loads(row["metadata_json"])
    related_topics = json.loads(row["related_topics_json"])

    assert row["title"] == "这是一个热点事件原文"
    assert row["body_ready"] == 1
    assert "热点聚合的底层证据" in row["body_text"]
    assert related_topics == ["AI 工作流", "热点事件"]
    assert metadata["body_status"] == "success"
    assert metadata["event_packet_refs"] == []
    assert metadata["cluster_ready"] is True
    assert metadata["event_candidate_ids"] == [1]
    assert metadata["event_links"][0]["title"] == "AI 工作流新动态"


def test_sync_raw_items_uses_incremental_watermark_and_updates_existing_object(tmp_path):
    nighthawk_db = tmp_path / "event_radar.db"
    create_studio_db = tmp_path / "create_studio.db"
    _init_nighthawk_db(nighthawk_db)

    with sqlite3.connect(nighthawk_db) as conn:
        conn.execute(
            """
            INSERT INTO raw_items(
              platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at, body_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "wechat",
                "official-account",
                "wx-200",
                "最初只有摘要",
                "这里只有很短的摘要。",
                "https://example.com/wx-200",
                "2026-04-20T08:00:00+08:00",
                json.dumps({"source_kind": "official_feed", "body_fetch_ok": False}, ensure_ascii=False),
                "2026-04-20T08:05:00+08:00",
                "pending",
            ),
        )
        conn.commit()

    first = sync_raw_items_to_create_studio(
        nighthawk_db_path=nighthawk_db,
        create_studio_db_path=create_studio_db,
        full=True,
    )
    assert first["metrics"]["metadata_only"] == 1

    with sqlite3.connect(nighthawk_db) as conn:
        conn.execute(
            """
            UPDATE raw_items
            SET content = ?, metrics_json = ?, fetched_at = ?, body_status = ?
            WHERE item_id = ?
            """,
            (
                "现在正文已经补齐了，可以直接拿来做统一索引和后续召回。",
                json.dumps({"source_kind": "official_feed", "body_fetch_ok": True}, ensure_ascii=False),
                "2026-04-20T10:00:00+08:00",
                "success",
                "wx-200",
            ),
        )
        conn.execute(
            """
            INSERT INTO raw_items(
              platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at, body_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "youtube",
                "channel-a",
                "yt-201",
                "YouTube transcript",
                "这是一段完整字幕，可直接进入统一对象层。",
                "https://example.com/yt-201",
                "2026-04-20T10:10:00+08:00",
                json.dumps(
                    {
                        "source_kind": "youtube_transcript",
                        "body_fetch_ok": True,
                        "transcript_language": "zh-Hans",
                    },
                    ensure_ascii=False,
                ),
                "2026-04-20T10:15:00+08:00",
                "success",
            ),
        )
        conn.commit()

    second = sync_raw_items_to_create_studio(
        nighthawk_db_path=nighthawk_db,
        create_studio_db_path=create_studio_db,
        full=False,
    )

    assert second["metrics"]["scanned"] == 2
    assert second["metrics"]["upserted"] == 2
    assert second["metrics"]["body_ready"] == 2

    store = CreateStudioStore(create_studio_db)
    status = store.get_status()
    assert status["table_counts"]["content_objects"] == 2

    updated_row = _fetch_content_object_row(create_studio_db, "raw_item:wechat:wx-200")
    transcript_row = _fetch_content_object_row(create_studio_db, "raw_item:youtube:yt-201")

    assert updated_row["body_ready"] == 1
    assert "正文已经补齐" in updated_row["body_text"]
    assert transcript_row["body_ready"] == 1
