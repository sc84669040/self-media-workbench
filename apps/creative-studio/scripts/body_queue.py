#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "event_radar.db"


def fetch_scalar(cur: sqlite3.Cursor, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = cur.execute(sql, params).fetchone()
    return int((row or [0])[0] or 0)


def collect_queue_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    cur = conn.cursor()
    status_rows = cur.execute(
        "SELECT COALESCE(body_status, 'none') AS status, COUNT(*) FROM raw_items GROUP BY COALESCE(body_status, 'none') ORDER BY COUNT(*) DESC, status ASC"
    ).fetchall()
    platform_rows = cur.execute(
        "SELECT platform, COUNT(*) FROM raw_items WHERE body_status IN ('pending','in_progress','timeout','failed') GROUP BY platform ORDER BY COUNT(*) DESC, platform ASC"
    ).fetchall()
    source_rows = cur.execute(
        """
        SELECT COALESCE(source_handle, '') AS source_handle, body_status, COUNT(*) AS cnt
        FROM raw_items
        WHERE body_status IN ('pending','timeout','failed')
        GROUP BY COALESCE(source_handle, ''), body_status
        ORDER BY cnt DESC, source_handle ASC
        LIMIT 20
        """
    ).fetchall()
    next_retry_rows = cur.execute(
        """
        SELECT id, platform, source_handle, title, body_next_retry_at, body_attempts, body_error
        FROM raw_items
        WHERE body_status='pending' AND body_next_retry_at IS NOT NULL AND body_next_retry_at != '' AND body_next_retry_at > datetime('now')
        ORDER BY body_next_retry_at ASC, id ASC
        LIMIT 10
        """
    ).fetchall()

    return {
        "ok": True,
        "db_path": str(DB_PATH),
        "totals": {
            "raw_items": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items"),
            "body_pending": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='pending'"),
            "body_due_now": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='pending' AND (body_next_retry_at IS NULL OR body_next_retry_at='' OR body_next_retry_at <= datetime('now'))"),
            "body_retry_waiting": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='pending' AND body_next_retry_at IS NOT NULL AND body_next_retry_at != '' AND body_next_retry_at > datetime('now')"),
            "body_in_progress": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='in_progress'"),
            "body_success": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='success'"),
            "body_timeout": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='timeout'"),
            "body_failed": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='failed'"),
            "body_skipped": fetch_scalar(cur, "SELECT COUNT(*) FROM raw_items WHERE body_status='skipped'"),
        },
        "by_status": [{"body_status": str(status or 'none'), "count": int(count or 0)} for status, count in status_rows],
        "by_platform": [{"platform": str(platform or ''), "count": int(count or 0)} for platform, count in platform_rows],
        "hot_sources": [
            {"source_handle": str(source_handle or ''), "body_status": str(status or ''), "count": int(count or 0)}
            for source_handle, status, count in source_rows
        ],
        "next_retry_items": [
            {
                "id": int(row[0] or 0),
                "platform": str(row[1] or ''),
                "source_handle": str(row[2] or ''),
                "title": str(row[3] or ''),
                "body_next_retry_at": str(row[4] or ''),
                "body_attempts": int(row[5] or 0),
                "body_error": str(row[6] or ''),
            }
            for row in next_retry_rows
        ],
        "sql_examples": {
            "body_status_breakdown": "SELECT COALESCE(body_status, 'none') AS body_status, COUNT(*) FROM raw_items GROUP BY COALESCE(body_status, 'none') ORDER BY COUNT(*) DESC;",
            "due_now": "SELECT id, platform, source_handle, title, body_priority, body_next_retry_at FROM raw_items WHERE body_status='pending' AND (body_next_retry_at IS NULL OR body_next_retry_at='' OR body_next_retry_at <= datetime('now')) ORDER BY body_priority DESC, fetched_at DESC LIMIT 50;",
            "retry_waiting": "SELECT id, platform, source_handle, title, body_next_retry_at, body_attempts FROM raw_items WHERE body_status='pending' AND body_next_retry_at > datetime('now') ORDER BY body_next_retry_at ASC LIMIT 50;",
            "problem_sources": "SELECT source_handle, body_status, COUNT(*) AS cnt FROM raw_items WHERE body_status IN ('pending','timeout','failed') GROUP BY source_handle, body_status ORDER BY cnt DESC LIMIT 30;",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NightHawk body queue observability")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(DB_PATH)
    try:
        payload = collect_queue_stats(conn)
    finally:
        conn.close()

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    totals = payload.get("totals") or {}
    print("NightHawk body queue")
    print(f"- raw_items: {totals.get('raw_items', 0)}")
    print(f"- pending: {totals.get('body_pending', 0)}")
    print(f"- due_now: {totals.get('body_due_now', 0)}")
    print(f"- retry_waiting: {totals.get('body_retry_waiting', 0)}")
    print(f"- in_progress: {totals.get('body_in_progress', 0)}")
    print(f"- success: {totals.get('body_success', 0)}")
    print(f"- timeout: {totals.get('body_timeout', 0)}")
    print(f"- failed: {totals.get('body_failed', 0)}")
    print(f"- skipped: {totals.get('body_skipped', 0)}")
    print("\nTop hot sources:")
    for row in (payload.get("hot_sources") or [])[:10]:
        print(f"- {row['source_handle'] or '-'} | {row['body_status']} | {row['count']}")


if __name__ == "__main__":
    main()
