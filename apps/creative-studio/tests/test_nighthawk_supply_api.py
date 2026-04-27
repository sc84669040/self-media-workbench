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

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
WEB_DIR = ROOT / "web"
for path in (SCRIPTS_DIR, WEB_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _seed_nighthawk_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE raw_items (
              id INTEGER PRIMARY KEY,
              platform TEXT,
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
        cur.execute(
            """
            CREATE TABLE event_candidates (
              id INTEGER PRIMARY KEY,
              title TEXT,
              summary TEXT,
              event_type TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE event_evidence (
              id INTEGER PRIMARY KEY,
              event_id INTEGER,
              raw_item_id INTEGER,
              role TEXT
            )
            """
        )

        cur.execute(
            """
            INSERT INTO raw_items(id, platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at, body_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                101,
                "wechat",
                "HyperAI",
                "item-101",
                "NightHawk 正文一",
                "这是一段已经抓到正文的 NightHawk 材料，可以直接进入创作编排。",
                "https://example.com/101",
                "2026-04-21T01:00:00+00:00",
                json.dumps(
                    {
                        "source_kind": "official_feed",
                        "tags": ["AI", "创作台"],
                        "related_topics": ["NightHawk", "创作编排"],
                    },
                    ensure_ascii=False,
                ),
                "2026-04-21T01:05:00+00:00",
                "success",
            ),
        )
        cur.execute(
            """
            INSERT INTO raw_items(id, platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at, body_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                102,
                "wechat",
                "HyperAI",
                "item-102",
                "NightHawk 正文二",
                "这条暂时只有 metadata，还没有完整正文。",
                "https://example.com/102",
                "2026-04-21 08:30:00",
                json.dumps(
                    {
                        "source_kind": "official_feed",
                        "tags": ["热点事件"],
                        "related_topics": ["NightHawk"],
                    },
                    ensure_ascii=False,
                ),
                "2026-04-21 08:40:00",
                "pending",
            ),
        )
        cur.execute(
            """
            INSERT INTO event_candidates(id, title, summary, event_type)
            VALUES (1, '事件一', '这是一个热点事件', 'news')
            """
        )
        cur.execute(
            """
            INSERT INTO event_evidence(id, event_id, raw_item_id, role)
            VALUES (1, 1, 101, 'primary')
            """
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def nighthawk_api_server(monkeypatch, tmp_path):
    db_path = tmp_path / "event_radar.db"
    _seed_nighthawk_db(db_path)
    monkeypatch.setenv("CONTENT_SEARCH_CREATION_DATA_ROOT", str(tmp_path / "creation"))
    monkeypatch.setenv("CREATE_STUDIO_NIGHTHAWK_DB_PATH", str(db_path))
    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_PROVIDER", "mock")
    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_CACHE_DB_PATH", str(tmp_path / "translation_cache.db"))
    monkeypatch.setenv("CREATE_STUDIO_ENABLE_UPSTREAM_RAW_SYNC", "0")

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


def test_create_nighthawk_profile_api_returns_supply_summary(nighthawk_api_server):
    status, payload = _request(nighthawk_api_server, "GET", "/api/create/nighthawk/profile")

    assert status == 200
    assert payload["ok"] is True
    assert payload["raw_items_count"] == 2
    assert payload["body_ready_count"] == 1
    assert payload["event_linked_count"] == 1
    assert payload["supports"]["full_body"] is True
    assert payload["supports"]["event_linking"] is True
    assert payload["frontend_ready"]["suggested_entry_path"] == "/create/nighthawk"


def test_nighthawk_page_is_served_as_independent_entry(nighthawk_api_server):
    req = Request(f"{nighthawk_api_server}/create/nighthawk", method="GET")

    with urlopen(req, timeout=5) as resp:
      body = resp.read().decode("utf-8")
      content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "NightHawk 正文资料池" in body
    assert "/api/create/nighthawk/profile" in body
    assert "/api/create/nighthawk/items" in body
    assert "/api/create/nighthawk/creation-packet" in body
    assert "/api/create/nighthawk/to-task" in body
    assert "/create/events/" in body
    assert "/create/studio" in body
    assert "/create/workspace" in body


def test_event_page_is_served_as_independent_entry(nighthawk_api_server):
    req = Request(f"{nighthawk_api_server}/create/events/1", method="GET")

    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "/api/events/" in body
    assert "/api/create/nighthawk/to-task" in body
    assert "/api/create/event-packets" in body
    assert "/create/nighthawk?event_id=" in body
    assert "/create/topic" in body
    assert "/create/studio" in body
    assert "/create/workspace" in body


def test_create_nighthawk_items_and_detail_api_return_body_data(nighthawk_api_server):
    status, listing = _request(nighthawk_api_server, "GET", "/api/create/nighthawk/items?limit=10&page=1")
    detail_status, detail = _request(nighthawk_api_server, "GET", "/api/create/nighthawk/items/101")

    assert status == 200
    assert listing["total"] == 2
    assert listing["items"][0]["raw_item_id"] in {101, 102}
    assert "display_title" in listing["items"][0]
    assert detail_status == 200
    assert detail["item"]["raw_item_id"] == 101
    assert "display_body_text" in detail["item"]
    assert "NightHawk 材料" in detail["item"]["body_text"]


def test_event_detail_api_returns_related_nighthawk_items(nighthawk_api_server):
    status, payload = _request(nighthawk_api_server, "GET", "/api/events/1")

    assert status == 200
    assert payload["ok"] is True
    assert payload["event"]["event_id"] == 1
    assert payload["related_items"][0]["raw_item_id"] == 101
    assert payload["creation_entry"]["raw_item_ids"] == [101]


def test_nighthawk_items_api_supports_body_ready_filter_and_sort(nighthawk_api_server):
    status, listing = _request(
        nighthawk_api_server,
        "GET",
        "/api/create/nighthawk/items?limit=10&page=1&body_ready_only=1&sort_by=heat",
    )

    assert status == 200
    assert listing["total"] == 1
    assert len(listing["items"]) == 1
    assert listing["items"][0]["raw_item_id"] == 101
    assert listing["filters"]["body_ready_only"] is True
    assert listing["filters"]["sort_by"] == "heat"


def test_create_nighthawk_creation_packet_api_builds_source_driven_packet(nighthawk_api_server):
    status, payload = _request(
        nighthawk_api_server,
        "POST",
        "/api/create/nighthawk/creation-packet",
        {
            "raw_item_ids": [101, 102],
            "topic": "NightHawk 正文如何接入创作台",
            "platform": "wechat",
            "style_notes": ["讲人话"],
        },
    )

    assert status == 200
    assert payload["ok"] is True
    assert len(payload["selected_items"]) == 2
    assert payload["profile_hint"]["body_ready_count"] == 1
    packet = payload["creation_packet"]
    assert packet["creation_intent"]["creation_mode"] == "source_driven"
    assert packet["source_trace"]["source_type"] == "nighthawk_raw_items"
    assert packet["source_trace"]["raw_item_ids"] == [101, 102]
    assert packet["evidence_pack"]["summary"]["primary_count"] == 1


def test_create_event_packet_api_builds_event_packet(nighthawk_api_server):
    status, payload = _request(
        nighthawk_api_server,
        "POST",
        "/api/create/event-packets",
        {
            "event_id": 1,
            "topic": "NightHawk 事件如何直接进入创作",
        },
    )

    assert status == 200
    assert payload["ok"] is True
    packet = payload["event_packet"]
    assert packet["packet_type"] == "event"
    assert packet["event_id"] == 1
    assert packet["summary"]["article_count"] == 1
    assert packet["articles"][0]["event_packet_refs"] == [packet["packet_id"]]


def test_event_packet_detail_page_and_api_are_served(nighthawk_api_server):
    create_status, create_payload = _request(
        nighthawk_api_server,
        "POST",
        "/api/create/event-packets",
        {"event_id": 1},
    )
    assert create_status == 200
    packet_id = create_payload["event_packet"]["packet_id"]

    status, payload = _request(nighthawk_api_server, "GET", f"/api/create/event-packets/{packet_id}")
    assert status == 200
    assert payload["event_packet"]["packet_id"] == packet_id
    assert payload["exports"]["markdown"].startswith("# Event Packet:")

    req = Request(f"{nighthawk_api_server}/create/event-packets/{packet_id}", method="GET")
    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "事件资料包详情页" in body
    assert "/api/create/event-packets/" in body
    assert "/api/create/packets/" in body
    assert "/create/events/" in body
    assert "/create/workspace" in body


def test_event_packet_can_convert_directly_to_creation_task(nighthawk_api_server):
    create_status, create_payload = _request(
        nighthawk_api_server,
        "POST",
        "/api/create/event-packets",
        {"event_id": 1},
    )
    assert create_status == 200
    packet_id = create_payload["event_packet"]["packet_id"]

    status, payload = _request(
        nighthawk_api_server,
        "POST",
        f"/api/create/packets/{packet_id}/to-task",
        {},
    )

    assert status == 200
    assert payload["ok"] is True
    assert payload["packet_type"] == "event"
    assert payload["creation_task"]["trigger_type"] == "event_packet"
    assert payload["creation_task"]["topic"] == "事件一"
    assert payload["bundle"]["retrieval_batch"]["status"] == "retrieved"


def test_create_nighthawk_to_task_api_creates_creation_task(nighthawk_api_server):
    status, payload = _request(
        nighthawk_api_server,
        "POST",
        "/api/create/nighthawk/to-task",
        {
            "raw_item_ids": [101, 102],
            "topic": "NightHawk 直接进入编排",
            "platform": "wechat",
        },
    )

    assert status == 200
    assert payload["ok"] is True
    assert payload["creation_task"]["trigger_type"] == "nighthawk_sources"
    assert payload["creation_task"]["status"] == "retrieval_ready"
    assert payload["bundle"]["retrieval_batch"]["results"][0]["decision"] == "keep"
    assert "source=nighthawk" in payload["next_url"]


def test_nighthawk_profile_syncs_recent_raw_items_from_upstream(monkeypatch, tmp_path):
    db_path = tmp_path / "event_radar.db"
    _seed_nighthawk_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM raw_items WHERE id = 102")
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_PROVIDER", "mock")
    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_CACHE_DB_PATH", str(tmp_path / "translation_cache.db"))
    monkeypatch.setenv("CREATE_STUDIO_ENABLE_UPSTREAM_RAW_SYNC", "1")

    import nighthawk_supply_service  # noqa: WPS433

    service = importlib.reload(nighthawk_supply_service)
    service.ENABLE_UPSTREAM_RAW_SYNC = True
    service._RAW_ITEMS_SYNC_STATE["last_attempt_at"] = 0.0
    service._RAW_ITEMS_SYNC_STATE["last_summary"] = {}

    def _fake_fetch(path: str, params: dict | None = None) -> dict:
        if path == "/api/db/overview":
            return {
                "ok": True,
                "raw_items_count": 2,
                "latest_published_at": "2026-04-21 09:00:00",
                "latest_fetched_at": "2026-04-21 09:05:00",
            }
        if path == "/api/db/recent-items":
            return {
                "ok": True,
                "items": [
                    {
                        "id": 102,
                        "platform": "wechat",
                        "source_handle": "HyperAI",
                        "item_id": "item-102",
                        "title": "NightHawk 正文二",
                        "content": "这条暂时只有 metadata，还没有完整正文。",
                        "url": "https://example.com/102",
                        "published_at": "2026-04-21 08:30:00",
                        "fetched_at": "2026-04-21 08:40:00",
                        "metrics": {
                            "source_kind": "official_feed",
                            "metadata_only": True,
                            "body_fetch_skipped": True,
                        },
                    }
                ],
                "total": 2,
                "page": int((params or {}).get("page") or 1),
                "page_size": int((params or {}).get("limit") or 100),
                "total_pages": 1,
            }
        raise AssertionError(path)

    monkeypatch.setattr(service, "_fetch_upstream_json", _fake_fetch)

    payload = service.get_nighthawk_supply_profile(db_path=db_path)

    assert payload["raw_items_count"] == 2
    assert payload["mirror_sync"]["ok"] is True
    assert payload["mirror_sync"]["inserted"] == 1

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT title FROM raw_items WHERE id = 102").fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "NightHawk 正文二"
