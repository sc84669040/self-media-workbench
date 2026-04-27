from __future__ import annotations

import importlib
import json
import sqlite3
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
WEB_DIR = ROOT / "web"
for path in (SCRIPTS_DIR, WEB_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def test_create_nighthawk_items_convert_utc_time_to_beijing(monkeypatch, tmp_path):
    db_path = tmp_path / "event_radar.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
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
        conn.execute(
            """
            INSERT INTO raw_items(
              id, platform, source_handle, item_id, title, content, url,
              published_at, metrics_json, fetched_at, body_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                101,
                "x",
                "openai",
                "tweet-101",
                "UTC source item",
                "English body",
                "https://example.com/utc-101",
                "2026-04-22T15:48:37+00:00",
                json.dumps({"likes": 10}),
                "2026-04-22T15:50:00+00:00",
                "success",
            ),
        )
        conn.commit()
    finally:
        conn.close()

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
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        with urlopen(
            Request(f"{base_url}/api/create/nighthawk/items?limit=10&page=1&body_ready_only=1&sort_by=time", method="GET"),
            timeout=5,
        ) as resp:
            listing = json.loads(resp.read().decode("utf-8"))
        with urlopen(Request(f"{base_url}/api/create/nighthawk/items/101", method="GET"), timeout=5) as resp:
            detail = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()

    assert listing["items"][0]["published_at"] == "2026-04-22 23:48:37"
    assert listing["items"][0]["fetched_at"] == "2026-04-22 23:50:00"
    assert detail["item"]["published_at"] == "2026-04-22 23:48:37"
    assert detail["item"]["fetched_at"] == "2026-04-22 23:50:00"
