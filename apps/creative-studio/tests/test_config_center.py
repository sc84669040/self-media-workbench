from __future__ import annotations

import importlib
import json
import threading
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest


@pytest.fixture()
def config_center_server(monkeypatch, tmp_path):
    local_config = tmp_path / "local.yaml"
    env_file = tmp_path / ".env"
    source_dir = tmp_path / "sources"
    monkeypatch.setenv("SELF_MEDIA_CONFIG_PATH", str(local_config))

    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)
    monkeypatch.setattr(dashboard, "LOCAL_CONFIG_PATH", local_config)
    monkeypatch.setattr(dashboard, "ENV_PATH", env_file)
    monkeypatch.setattr(dashboard, "MONITORING_ROOT", source_dir)
    dashboard._clear_runtime_config_caches()

    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield {"base_url": base_url, "source_dir": source_dir}
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def _request(server: dict | str, method: str, path: str, payload: dict | None = None) -> tuple[int, dict | str]:
    base_url = server["base_url"] if isinstance(server, dict) else server
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(f"{base_url}{path}", data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=5) as resp:
            body = resp.read().decode("utf-8")
            if (resp.headers.get("Content-Type") or "").startswith("application/json"):
                return resp.status, json.loads(body)
            return resp.status, body
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        return exc.code, json.loads(body)


def test_portal_and_config_center_pages_are_served(config_center_server):
    status, body = _request(config_center_server, "GET", "/")
    assert status == 200
    assert "内容情报台" in body
    assert "创作前台" in body
    assert "配置中心" in body

    status, body = _request(config_center_server, "GET", "/config")
    assert status == 200
    assert "配置向导" in body
    assert "小白配置" in body
    assert "夜鹰抓取与主题库" in body
    assert "paths.event_radar_db_path" in body
    assert "/api/config/local" in body
    assert "YAML 高级编辑器" not in body
    assert "环境变量" not in body

    status, body = _request(config_center_server, "GET", "/search")
    assert status == 200
    assert "入口页" in body
    assert "创作前台" in body
    assert "配置中心" in body

    status, body = _request(config_center_server, "GET", "/create")
    assert status == 200
    assert "入口页" in body
    assert "内容情报台" in body
    assert "配置中心" in body


def test_config_center_save_reload_and_create_config_effect(config_center_server):
    status, payload = _request(config_center_server, "GET", "/api/config/local")
    assert status == 200
    assert isinstance(payload, dict)
    config = payload["config"]
    config["services"]["creative_studio"]["port"] = 19991
    config["creative_studio"]["nighthawk"]["enable_upstream_raw_sync"] = True
    config["creative_studio"]["writing"]["provider"] = "mock"
    config["creative_studio"]["writing"]["model"] = "unit-test-writer"

    status, saved = _request(
        config_center_server,
        "POST",
        "/api/config/local",
        {"config": config, "env_values": {"CREATE_STUDIO_WRITING_API_KEY": "test-secret"}},
    )
    assert status == 200
    assert isinstance(saved, dict)
    assert saved["ok"] is True
    assert saved["config"]["services"]["creative_studio"]["port"] == 19991
    assert any(item["name"] == "env.CREATE_STUDIO_WRITING_API_KEY" and item["ok"] for item in saved["checks"])

    status, create_config = _request(config_center_server, "GET", "/api/create/config")
    assert status == 200
    assert isinstance(create_config, dict)
    assert create_config["nighthawk"]["enable_upstream_raw_sync"] is True
    assert create_config["writing"]["provider"] == "mock"
    assert create_config["writing"]["model"] == "unit-test-writer"


def test_config_center_save_syncs_source_files(config_center_server):
    status, payload = _request(config_center_server, "GET", "/api/config/local")
    assert status == 200
    assert isinstance(payload, dict)
    config = payload["config"]
    config.setdefault("sources", {})
    config["sources"]["feeds"] = [
        {
            "id": "openai-blog",
            "name": "OpenAI Blog",
            "url": "https://openai.com/news/rss.xml",
            "enabled": True,
        }
    ]
    config["sources"]["youtube_channels"] = [
        {
            "id": "openai",
            "name": "OpenAI",
            "url": "https://www.youtube.com/@OpenAI",
            "enabled": True,
        }
    ]
    config["sources"]["x_accounts"] = ["openai", "sama"]

    status, saved = _request(config_center_server, "POST", "/api/config/local", {"config": config})
    assert status == 200
    assert isinstance(saved, dict)
    assert saved["ok"] is True

    source_dir = config_center_server["source_dir"]
    feeds_payload = json.loads((source_dir / "a-stage-feed-sources.json").read_text(encoding="utf-8"))
    youtube_payload = json.loads((source_dir / "a-stage-youtube-channels.json").read_text(encoding="utf-8"))
    x_payload = json.loads((source_dir / "a-stage-x-sources.json").read_text(encoding="utf-8"))
    assert feeds_payload["sources"][0]["id"] == "openai-blog"
    assert feeds_payload["sources"][0]["url"] == "https://openai.com/news/rss.xml"
    assert youtube_payload["channels"][0]["id"] == "openai"
    assert youtube_payload["channels"][0]["url"] == "https://www.youtube.com/@OpenAI"
    assert x_payload["accounts"] == ["openai", "sama"]


def test_config_center_imports_yaml_template(config_center_server):
    yaml_text = """
services:
  creative_studio:
    host: 127.0.0.1
    port: 19091
creative_studio:
  autofill:
    provider: mock
    model: imported-autofill
"""
    status, payload = _request(config_center_server, "POST", "/api/config/import", {"yaml": yaml_text})
    assert status == 200
    assert isinstance(payload, dict)
    assert payload["config"]["services"]["creative_studio"]["port"] == 19091
    assert payload["config"]["creative_studio"]["autofill"]["model"] == "imported-autofill"
