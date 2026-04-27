from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from content_chunker import chunk_content_objects_to_store, split_text_into_chunks  # noqa: E402
from create_studio_store import CreateStudioStore  # noqa: E402


def _fetch_chunk_rows(db_path: Path, object_uid: str) -> list[sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            """
            SELECT chunk_id, object_uid, chunk_index, chunk_text, token_estimate, metadata_json
            FROM content_chunks
            WHERE object_uid = ?
            ORDER BY chunk_index ASC
            """,
            (object_uid,),
        ).fetchall()


def test_split_text_into_chunks_creates_overlap_for_long_text():
    text = "第一段内容。" * 120
    chunks = split_text_into_chunks(text, chunk_size=120, chunk_overlap=20)

    assert len(chunks) >= 2
    assert chunks[0]["char_start"] == 0
    assert chunks[1]["char_start"] < chunks[0]["char_end"]
    assert chunks[0]["token_estimate"] > 0


def test_chunk_content_objects_writes_traceable_chunks_and_event_hints(tmp_path):
    create_studio_db = tmp_path / "create_studio.db"
    store = CreateStudioStore(create_studio_db)
    store.initialize()
    store.upsert_content_object(
        {
            "object_uid": "CO-ARTICLE-001",
            "source_kind": "source_note",
            "platform": "knowledge_base",
            "source_ref": "note://article-001",
            "canonical_url": "https://example.com/article-001",
            "title": "热点事件创作底稿",
            "summary": "长正文需要被切块。",
            "body_text": "这是一段用于测试的长正文。" * 160,
            "body_ready": True,
            "related_topics": ["热点事件", "创作台"],
            "metadata": {
                "event_candidate_ids": [11, 12],
                "event_packet_refs": ["EP-001"],
                "cluster_ready": True,
            },
            "content_hash": "hash-article-001",
        }
    )

    result = chunk_content_objects_to_store(
        create_studio_db_path=create_studio_db,
        chunk_size=240,
        chunk_overlap=40,
        full=True,
    )

    assert result["ok"] is True
    assert result["metrics"]["eligible_objects"] == 1
    assert result["metrics"]["rechunked_objects"] == 1
    assert result["metrics"]["new_chunks"] >= 2
    assert result["metrics"]["objects_with_event_links"] == 1

    rows = _fetch_chunk_rows(create_studio_db, "CO-ARTICLE-001")
    assert len(rows) >= 2
    first_metadata = json.loads(rows[0]["metadata_json"])
    assert rows[0]["object_uid"] == "CO-ARTICLE-001"
    assert rows[0]["chunk_id"].startswith("CO-ARTICLE-001::chunk::")
    assert first_metadata["content_hash"] == "hash-article-001"
    assert first_metadata["chunk_size"] == 240
    assert first_metadata["chunk_overlap"] == 40
    assert first_metadata["related_topics"] == ["热点事件", "创作台"]
    assert first_metadata["event_candidate_ids"] == [11, 12]
    assert first_metadata["event_packet_refs"] == ["EP-001"]
    assert first_metadata["cluster_ready"] is True


def test_chunk_content_objects_incrementally_rechunks_changed_body(tmp_path):
    create_studio_db = tmp_path / "create_studio.db"
    store = CreateStudioStore(create_studio_db)
    store.initialize()
    store.upsert_content_object(
        {
            "object_uid": "CO-ARTICLE-002",
            "source_kind": "transcript",
            "platform": "youtube",
            "source_ref": "yt-002",
            "title": "第一版字幕",
            "body_text": "字幕内容。" * 90,
            "body_ready": True,
            "content_hash": "hash-v1",
        }
    )

    first = chunk_content_objects_to_store(
        create_studio_db_path=create_studio_db,
        chunk_size=180,
        chunk_overlap=30,
        full=True,
    )
    first_rows = _fetch_chunk_rows(create_studio_db, "CO-ARTICLE-002")

    assert first["metrics"]["rechunked_objects"] == 1
    assert len(first_rows) >= 2

    store.upsert_content_object(
        {
            "object_uid": "CO-ARTICLE-002",
            "source_kind": "transcript",
            "platform": "youtube",
            "source_ref": "yt-002",
            "title": "第二版字幕",
            "body_text": "字幕内容已更新。" * 120,
            "body_ready": True,
            "content_hash": "hash-v2",
        }
    )

    second = chunk_content_objects_to_store(
        create_studio_db_path=create_studio_db,
        chunk_size=180,
        chunk_overlap=30,
        full=False,
    )
    second_rows = _fetch_chunk_rows(create_studio_db, "CO-ARTICLE-002")
    latest_metadata = json.loads(second_rows[0]["metadata_json"])

    assert second["metrics"]["rechunked_objects"] == 1
    assert second["metrics"]["skipped_objects"] == 0
    assert latest_metadata["content_hash"] == "hash-v2"
    assert any("已更新" in str(row["chunk_text"] or "") for row in second_rows)
    assert len(second_rows) >= len(first_rows)
