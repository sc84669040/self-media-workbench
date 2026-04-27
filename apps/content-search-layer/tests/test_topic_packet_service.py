from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from create_studio_store import CreateStudioStore  # noqa: E402
from topic_packet_service import create_topic_packet, get_topic_packet_detail, run_topic_search  # noqa: E402


def _build_config(tmp_path: Path) -> dict:
    return {
        "indexing": {
            "content_index_db_path": str(tmp_path / "create_studio.db"),
        },
        "semantic_search": {
            "enable_fts": True,
            "enable_embedding": True,
            "embedding_provider": "mock",
            "embedding_model": "mock-semantic-v1",
            "rerank_top_k": 20,
            "final_top_k": 8,
        },
    }


def _seed_store(db_path: Path) -> None:
    store = CreateStudioStore(db_path)
    store.initialize()
    store.upsert_content_object(
        {
            "object_uid": "OBJ-HERMES-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://hermes",
            "title": "Hermes 的价值判断",
            "summary": "解释 Hermes 在内容生产链路中的价值。",
            "body_text": "Hermes 把检索、证据、写作和热点接入编排成稳定工作流。",
            "body_ready": True,
            "published_at": "2026-04-20T09:00:00+08:00",
            "source_name": "Hermes",
            "tags": ["创作台", "编排层"],
            "related_topics": ["Hermes", "内容生产链路"],
        }
    )


def test_topic_search_returns_intent_and_results(tmp_path):
    config = _build_config(tmp_path)
    _seed_store(Path(config["indexing"]["content_index_db_path"]))

    result = run_topic_search({"topic": "Hermes 的价值"}, config=config)

    assert result["ok"] is True
    assert result["topic_intent"]["entities"] == ["Hermes"]
    assert result["search_result"]["results"]
    assert result["search_result"]["results"][0]["source_id"] == "OBJ-HERMES-001"


def test_topic_packet_can_be_created_and_loaded_with_exports(tmp_path):
    config = _build_config(tmp_path)
    db_path = Path(config["indexing"]["content_index_db_path"])
    _seed_store(db_path)

    created = create_topic_packet({"topic": "Hermes 的价值"}, config=config)
    packet_id = created["topic_packet"]["packet_id"]
    loaded = get_topic_packet_detail(packet_id, config=config)

    assert created["ok"] is True
    assert created["topic_packet"]["topic"] == "Hermes 的价值"
    assert created["topic_packet"]["results"][0]["source_id"] == "OBJ-HERMES-001"
    assert created["topic_packet"]["articles"][0]["body_text"].startswith("Hermes")
    assert "Topic Packet" in created["exports"]["markdown"]
    assert "Hermes 的价值判断" in created["exports"]["json"]
    assert loaded["topic_packet"]["packet_id"] == packet_id
    assert loaded["topic_packet"]["articles"][0]["title"] == "Hermes 的价值判断"
    assert "Hermes 的价值" in loaded["exports"]["markdown"]
