from __future__ import annotations

import importlib
import json
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
WEB_DIR = ROOT / "web"
for path in (SCRIPTS_DIR, WEB_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from create_studio_store import CreateStudioStore, REQUIRED_TABLES, SCHEMA_VERSION  # noqa: E402


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


@pytest.fixture()
def create_studio_store(tmp_path):
    return CreateStudioStore(tmp_path / "create_studio.db")


@pytest.fixture()
def create_studio_api_server(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_SEARCH_CREATION_DATA_ROOT", str(tmp_path / "creation"))
    monkeypatch.setenv("CREATE_STUDIO_INDEX_DB_PATH", str(tmp_path / "create_studio.db"))
    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield base_url
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_create_studio_store_initializes_schema_and_reports_status(create_studio_store):
    status_before = create_studio_store.get_status()

    assert status_before["ok"] is True
    assert status_before["db_exists"] is False
    assert status_before["initialized"] is False
    assert status_before["missing_tables"] == list(REQUIRED_TABLES)

    status_after = create_studio_store.initialize()

    assert status_after["db_exists"] is True
    assert status_after["initialized"] is True
    assert status_after["missing_tables"] == []
    assert status_after["schema_version"] == SCHEMA_VERSION
    assert status_after["latest_sync_run"] is None
    assert status_after["table_counts"]["create_studio_meta"] >= 3
    for table_name in REQUIRED_TABLES:
        if table_name == "create_studio_meta":
            continue
        assert status_after["table_counts"][table_name] == 0


def test_create_studio_store_persists_core_records_and_sync_run(create_studio_store):
    create_studio_store.initialize()

    create_studio_store.upsert_content_object(
        {
            "object_uid": "OBJ-001",
            "source_kind": "analysis_card",
            "platform": "knowledge_base",
            "source_ref": "card://obj-001",
            "canonical_url": "https://example.com/obj-001",
            "title": "创作台独立存储骨架",
            "summary": "验证资料理解层可以独立持久化。",
            "body_text": "先把索引存储和编排存储拆开，后续检索和聚合才好演进。",
            "body_ready": True,
            "published_at": "2026-04-20T09:00:00+08:00",
            "source_name": "Hermes",
            "tags": ["创作台", "存储"],
            "related_topics": ["语义检索"],
            "metadata": {"confidence": "high"},
        }
    )
    create_studio_store.replace_content_chunks(
        "OBJ-001",
        [
            {"chunk_text": "第一段正文", "token_estimate": 12},
            {"chunk_text": "第二段正文", "token_estimate": 15},
        ],
    )
    create_studio_store.save_topic_packet(
        {
            "packet_id": "TP-001",
            "topic": "创作台为什么要拆独立索引库",
            "status": "draft",
            "query_text": "创作台 独立索引库",
            "packet": {"summary": "topic packet"},
        }
    )
    create_studio_store.save_event_packet(
        {
            "packet_id": "EP-001",
            "event_key": "event-001",
            "title": "独立索引库初始化",
            "status": "confirmed",
            "packet": {"summary": "event packet"},
        }
    )
    sync_run = create_studio_store.start_sync_run("knowledge_base", "bootstrap", {"seen": 1})
    create_studio_store.finish_sync_run(sync_run["run_id"], "completed", {"seen": 2, "written": 1})

    status = create_studio_store.get_status()

    assert status["initialized"] is True
    assert status["table_counts"]["content_objects"] == 1
    assert status["table_counts"]["content_chunks"] == 2
    assert status["table_counts"]["topic_packets"] == 1
    assert status["table_counts"]["event_packets"] == 1
    assert status["table_counts"]["sync_runs"] == 1
    assert status["latest_sync_run"]["source_name"] == "knowledge_base"
    assert status["latest_sync_run"]["phase"] == "bootstrap"
    assert status["latest_sync_run"]["status"] == "completed"
    assert status["latest_sync_run"]["metrics"]["written"] == 1


def test_create_index_api_reports_status_and_bootstrap(create_studio_api_server):
    status_code, before = _request(create_studio_api_server, "GET", "/api/create/index/status")

    assert status_code == 200
    assert before["ok"] is True
    assert before["db_exists"] is False
    assert before["initialized"] is False

    bootstrap_status, bootstrap_payload = _request(
        create_studio_api_server,
        "POST",
        "/api/create/index/bootstrap",
        {},
    )

    assert bootstrap_status == 200
    assert bootstrap_payload["initialized"] is True
    assert bootstrap_payload["missing_tables"] == []
    assert bootstrap_payload["message"] == "创作台独立索引库已初始化"

    refreshed_status, after = _request(create_studio_api_server, "GET", "/api/create/index/status")

    assert refreshed_status == 200
    assert after["db_exists"] is True
    assert after["initialized"] is True
    for table_name in REQUIRED_TABLES:
        assert table_name in after["table_counts"]
