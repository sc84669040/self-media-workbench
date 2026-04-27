#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from event_radar_mirror import mirror_event_radar_db
from sync_topics_v2 import (
    DB_PATH,
    ensure_topic_pipeline_schema,
    finish_run,
    load_state,
    run_full_rebuild,
    run_incremental,
    save_state,
    start_run,
)

DEFAULT_INCREMENTAL_INTERVAL_MIN = 15
DEFAULT_FULL_REBUILD_INTERVAL_HOURS = 24
DEFAULT_WINDOW_DAYS = 7


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    print(f"[{now_text()}] {message}", flush=True)


def run_sync(mode: str, window_days: int) -> dict[str, object]:
    with sqlite3.connect(DB_PATH) as conn:
        ensure_topic_pipeline_schema(conn)
        state = load_state(conn)
        run_id = start_run(
            conn,
            run_mode=mode,
            last_raw_item_id=int(state.get("last_raw_item_id") or 0),
            notes={"window_days": int(window_days)},
        )
        try:
            if mode == "full-rebuild":
                result = run_full_rebuild(conn, window_days=window_days)
            else:
                result = run_incremental(conn, state=state, window_days=window_days)
            save_state(
                conn,
                last_raw_item_id=int(result.get("last_raw_item_id") or 0),
                run_mode=mode,
                notes={"window_days": int(window_days)},
            )
            finish_run(
                conn,
                run_id=run_id,
                status="success",
                topics_upserted=int(result.get("topics_upserted") or 0),
                topics_deactivated=int(result.get("topics_deactivated") or 0),
                articles_linked=int(result.get("articles_linked") or 0),
                notes={"window_days": int(window_days), "unmatched_rows": int(result.get("unmatched_rows") or 0)},
            )
            conn.commit()
            return result
        except Exception as exc:  # noqa: BLE001
            finish_run(
                conn,
                run_id=run_id,
                status="failed",
                topics_upserted=0,
                topics_deactivated=0,
                articles_linked=0,
                notes={"window_days": int(window_days), "error": str(exc)},
            )
            conn.commit()
            raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--incremental-interval-min", type=int, default=DEFAULT_INCREMENTAL_INTERVAL_MIN)
    parser.add_argument("--full-rebuild-interval-hours", type=int, default=DEFAULT_FULL_REBUILD_INTERVAL_HOURS)
    parser.add_argument("--run-once", action="store_true")
    args = parser.parse_args()

    window_days = max(1, int(args.window_days or DEFAULT_WINDOW_DAYS))
    incremental_interval_sec = max(60, int(args.incremental_interval_min or DEFAULT_INCREMENTAL_INTERVAL_MIN) * 60)
    full_rebuild_interval_sec = max(incremental_interval_sec, int(args.full_rebuild_interval_hours or DEFAULT_FULL_REBUILD_INTERVAL_HOURS) * 3600)
    last_full_rebuild_at = 0.0

    log(f"topic pipeline daemon started: db={DB_PATH}")
    while True:
        now_ts = time.time()
        mode = "incremental"
        if last_full_rebuild_at <= 0 or now_ts - last_full_rebuild_at >= full_rebuild_interval_sec:
            mode = "full-rebuild"

        try:
            result = run_sync(mode, window_days)
            log(
                json.dumps(
                    {
                        "mode": mode,
                        "topics_upserted": int(result.get("topics_upserted") or 0),
                        "topics_deactivated": int(result.get("topics_deactivated") or 0),
                        "articles_linked": int(result.get("articles_linked") or 0),
                        "raw_items_scanned": int(result.get("raw_items_scanned") or 0),
                    },
                    ensure_ascii=False,
                )
            )
            mirror_event_radar_db(Path(DB_PATH), logger=log)
            if mode == "full-rebuild":
                last_full_rebuild_at = now_ts
        except Exception as exc:  # noqa: BLE001
            log(f"topic pipeline {mode} failed: {exc}")

        if args.run_once:
            return
        time.sleep(incremental_interval_sec)


if __name__ == "__main__":
    main()
