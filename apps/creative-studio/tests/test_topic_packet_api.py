from __future__ import annotations

import importlib
import json
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

from create_studio_store import CreateStudioStore  # noqa: E402


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


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config" / "create-studio.yaml"
    payload = {
        "database_sources": {
            "nighthawk_db_path": str(tmp_path / "event_radar.db"),
        },
        "creation_workspace": {
            "data_root": str(tmp_path / "creation"),
        },
        "indexing": {
            "content_index_db_path": str(tmp_path / "create_studio.db"),
            "chunk_size": 200,
            "chunk_overlap": 40,
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
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config_path


def test_topic_packet_api_can_search_and_persist_packet(monkeypatch, tmp_path):
    config_path = _write_config(tmp_path)
    store = CreateStudioStore(tmp_path / "create_studio.db")
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

    monkeypatch.setenv("CREATE_STUDIO_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("CONTENT_SEARCH_CREATION_DATA_ROOT", str(tmp_path / "creation"))

    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)

    def fail_external_search(_payload: dict) -> dict:
        raise AssertionError("topic packet flow should prefer local retrieval in this test")

    monkeypatch.setattr(dashboard, "search_candidates", fail_external_search)

    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        search_status, search_payload = _request(
            base_url,
            "POST",
            "/api/create/topic-search",
            {"topic": "Hermes 的价值"},
        )
        create_status, create_payload = _request(
            base_url,
            "POST",
            "/api/create/topic-packets",
            {"topic": "Hermes 的价值"},
        )
        packet_id = create_payload["topic_packet"]["packet_id"]
        get_status, get_payload = _request(
            base_url,
            "GET",
            f"/api/create/topic-packets/{packet_id}",
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()

    assert search_status == 200
    assert search_payload["topic_intent"]["entities"] == ["Hermes"]
    assert search_payload["search_result"]["results"][0]["source_id"] == "OBJ-HERMES-001"

    assert create_status == 200
    assert create_payload["topic_packet"]["topic"] == "Hermes 的价值"
    assert create_payload["topic_packet"]["results"][0]["source_id"] == "OBJ-HERMES-001"
    assert "Topic Packet" in create_payload["exports"]["markdown"]

    assert get_status == 200
    assert get_payload["topic_packet"]["packet_id"] == packet_id
    assert get_payload["topic_packet"]["articles"][0]["body_text"].startswith("Hermes")
    assert "Hermes 的价值判断" in get_payload["exports"]["json"]
def test_topic_packet_api_can_convert_packet_to_creation_task(monkeypatch, tmp_path):
    config_path = _write_config(tmp_path)
    store = CreateStudioStore(tmp_path / "create_studio.db")
    store.initialize()
    store.upsert_content_object(
        {
            "object_uid": "OBJ-HERMES-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "note://hermes",
            "title": "Hermes value judgement",
            "summary": "Explain why Hermes is valuable in the creative workflow.",
            "body_text": "Hermes turns retrieval, evidence, and writing into a stable workflow.",
            "body_ready": True,
            "published_at": "2026-04-20T09:00:00+08:00",
            "source_name": "Hermes",
            "tags": ["creative-studio", "orchestration"],
            "related_topics": ["Hermes", "workflow"],
        }
    )

    monkeypatch.setenv("CREATE_STUDIO_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("CONTENT_SEARCH_CREATION_DATA_ROOT", str(tmp_path / "creation"))

    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)

    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        create_status, create_payload = _request(
            base_url,
            "POST",
            "/api/create/topic-packets",
            {"topic": "Hermes value"},
        )
        packet_id = create_payload["topic_packet"]["packet_id"]
        convert_status, convert_payload = _request(
            base_url,
            "POST",
            f"/api/create/packets/{packet_id}/to-task",
            {},
        )
        task_id = convert_payload["creation_task"]["id"]
        bundle_status, bundle_payload = _request(
            base_url,
            "GET",
            f"/api/create/tasks/{task_id}",
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()

    assert create_status == 200
    assert convert_status == 200
    assert convert_payload["creation_task"]["id"].startswith("CT-")
    assert convert_payload["creation_task"]["trigger_type"] == "topic_packet"
    assert convert_payload["creation_task"]["metadata"]["source_packet"]["packet_id"] == packet_id
    assert convert_payload["bundle"]["retrieval_batch"]["results"][0]["source_id"] == "OBJ-HERMES-001"
    assert f"task_id={task_id}" in convert_payload["next_url"]
    assert bundle_status == 200
    assert bundle_payload["task"]["id"] == task_id
    assert bundle_payload["retrieval_batch"]["results"][0]["classification"] == "primary"
