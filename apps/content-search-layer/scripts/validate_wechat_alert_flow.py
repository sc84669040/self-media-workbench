#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from runtime_config import event_radar_db_path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = event_radar_db_path()


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    raw_count = int(cur.execute("select count(*) from raw_items where platform='wechat'").fetchone()[0])
    alert_count = int(cur.execute("select count(*) from alert_queue where platform='wechat'").fetchone()[0])
    status_rows = [tuple(r) for r in cur.execute("select notify_status, count(*) from alert_queue where platform='wechat' group by notify_status order by 1")]
    level_rows = [tuple(r) for r in cur.execute("select recommend_level, count(*) from alert_queue where platform='wechat' group by recommend_level order by 1")]
    recent_strong = [
        dict(r)
        for r in cur.execute(
            """
            select id, source_handle, title, notify_status, notify_attempts, notify_error, notified_at
            from alert_queue
            where platform='wechat' and recommend_level='strong'
            order by id desc
            limit 10
            """
        )
    ]
    candidate_rows = [
        dict(r)
        for r in cur.execute(
            """
            select id, source_handle, title, score, published_at
            from alert_queue
            where platform='wechat' and notify_status='candidate'
            order by score desc, id desc
            limit 10
            """
        )
    ]
    conn.close()

    checks = {
        "wechat_raw_items_exists": raw_count > 0,
        "wechat_alert_queue_exists": alert_count > 0,
        "candidate_layer_exists": any(status == "candidate" and count > 0 for status, count in status_rows),
        "strong_recommendation_exists": any(level == "strong" and count > 0 for level, count in level_rows),
        "strong_sent_or_pending_exists": any(item.get("notify_status") in {"sent", "pending"} for item in recent_strong),
    }

    result = {
        "ok": all(checks.values()),
        "checks": checks,
        "raw_count": raw_count,
        "alert_count": alert_count,
        "status_breakdown": status_rows,
        "level_breakdown": level_rows,
        "recent_strong": recent_strong,
        "top_candidates": candidate_rows,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
