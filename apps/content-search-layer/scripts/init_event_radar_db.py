#!/usr/bin/env python3
"""初始化 event radar SQLite 数据库（免费、本地、零依赖）。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from runtime_config import event_radar_db_path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = event_radar_db_path()


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              platform TEXT NOT NULL,
              handle TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              priority INTEGER NOT NULL DEFAULT 5,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(platform, handle)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              platform TEXT NOT NULL,
              source_handle TEXT,
              item_id TEXT,
              title TEXT,
              content TEXT,
              url TEXT,
              published_at TEXT,
              metrics_json TEXT,
              fetched_at TEXT DEFAULT (datetime('now')),
              UNIQUE(platform, item_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS event_candidates (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_type TEXT,
              title TEXT,
              summary TEXT,
              heat_score REAL DEFAULT 0,
              novelty_score REAL DEFAULT 0,
              confidence REAL DEFAULT 0,
              status TEXT DEFAULT 'new',
              created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS event_evidence (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_id INTEGER NOT NULL,
              raw_item_id INTEGER NOT NULL,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(event_id, raw_item_id)
            )
            """
        )

        conn.commit()
    finally:
        conn.close()


def main() -> None:
    init_db(DB_PATH)
    print(f"OK: {DB_PATH}")


if __name__ == "__main__":
    main()
