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


def test_creation_api_uses_hybrid_rerank_when_mock_embedding_enabled(monkeypatch, tmp_path):
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
    store.upsert_content_object(
        {
            "object_uid": "OBJ-RAW-001",
            "source_kind": "raw_article",
            "platform": "wechat",
            "source_ref": "raw://workflow",
            "title": "内容工作流案例",
            "summary": "补充案例。",
            "body_text": "这条资料更多是补充案例，不是主判断。",
            "body_ready": True,
            "published_at": "2026-04-19T09:00:00+08:00",
            "source_name": "公众号",
        }
    )

    monkeypatch.setenv("CREATE_STUDIO_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("CONTENT_SEARCH_CREATION_DATA_ROOT", str(tmp_path / "creation"))

    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)

    def fail_external_search(_payload: dict) -> dict:
        raise AssertionError("hybrid local retrieval should not fall back to external search in this test")

    monkeypatch.setattr(dashboard, "search_candidates", fail_external_search)

    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        create_status, created = _request(
            base_url,
            "POST",
            "/api/create/tasks",
            {
                "trigger_type": "manual_topic",
                "topic": "Hermes 的价值",
                "platform": "wechat",
                "audience": "AI 创作者",
                "goal": "形成可直接进入写作的资料包",
                "source_scope": ["analysis_cards", "raw_articles"],
            },
        )
        task_id = created["task"]["id"]
        run_status, run_payload = _request(
            base_url,
            "POST",
            f"/api/create/tasks/{task_id}/retrieval/run",
            {"filters": {"limit": 5}},
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()

    assert create_status == 200
    assert run_status == 200
    assert run_payload["retrieval_batch"]["results"][0]["source_id"] == "OBJ-HERMES-001"
    assert run_payload["retrieval_batch"]["results"][0]["metrics"]["embedding_score"] > 0
    assert run_payload["retrieval_batch"]["results"][0]["metrics"]["search_explain"]["structured"]["matched_entities"] == ["hermes"]
    assert "hermes" in run_payload["retrieval_batch"]["results"][0]["why_pick"].lower()
