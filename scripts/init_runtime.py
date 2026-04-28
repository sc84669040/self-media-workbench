from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from self_media_config import REPO_ROOT, get_config, get_path


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def ensure_event_db(path: Path, profile: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
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
        conn.execute(
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
              body_status TEXT DEFAULT 'none',
              body_attempts INTEGER NOT NULL DEFAULT 0,
              body_error TEXT DEFAULT '',
              body_fetched_at TEXT,
              body_priority INTEGER NOT NULL DEFAULT 0,
              body_next_retry_at TEXT,
              UNIQUE(platform, item_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_queue (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              raw_item_id INTEGER,
              platform TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'candidate',
              created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_api_cache_topics (
              topic_id INTEGER PRIMARY KEY,
              topic_key TEXT NOT NULL DEFAULT '',
              title TEXT NOT NULL DEFAULT '',
              summary TEXT NOT NULL DEFAULT '',
              topic_type TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT '',
              first_seen_at TEXT NOT NULL DEFAULT '',
              last_seen_at TEXT NOT NULL DEFAULT '',
              importance_score REAL NOT NULL DEFAULT 0,
              impact_score REAL NOT NULL DEFAULT 0,
              creation_potential_score REAL NOT NULL DEFAULT 0,
              overall_score REAL NOT NULL DEFAULT 0,
              evidence_event_count INTEGER NOT NULL DEFAULT 0,
              evidence_article_count INTEGER NOT NULL DEFAULT 0,
              evidence_source_count INTEGER NOT NULL DEFAULT 0,
              evidence_platform_count INTEGER NOT NULL DEFAULT 0,
              card_summary TEXT NOT NULL DEFAULT '',
              primary_platforms_json TEXT NOT NULL DEFAULT '[]',
              primary_entities_json TEXT NOT NULL DEFAULT '[]',
              risk_flags_json TEXT NOT NULL DEFAULT '[]',
              topic_json TEXT NOT NULL DEFAULT '{}',
              synced_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_api_cache_details (
              topic_id INTEGER PRIMARY KEY,
              topic_json TEXT NOT NULL DEFAULT '{}',
              articles_json TEXT NOT NULL DEFAULT '[]',
              article_pagination_json TEXT NOT NULL DEFAULT '{}',
              events_json TEXT NOT NULL DEFAULT '[]',
              event_pagination_json TEXT NOT NULL DEFAULT '{}',
              synced_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topics (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              topic_key TEXT NOT NULL,
              title TEXT NOT NULL DEFAULT '',
              summary TEXT NOT NULL DEFAULT '',
              topic_type TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'ready',
              time_window_hours INTEGER NOT NULL DEFAULT 72,
              first_seen_at TEXT,
              last_seen_at TEXT,
              primary_platforms_json TEXT NOT NULL DEFAULT '[]',
              primary_entities_json TEXT NOT NULL DEFAULT '[]',
              risk_flags_json TEXT NOT NULL DEFAULT '[]',
              is_active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT DEFAULT (datetime('now')),
              updated_at TEXT DEFAULT (datetime('now')),
              UNIQUE(topic_key, topic_type)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_scores (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              topic_id INTEGER NOT NULL,
              importance_score REAL NOT NULL DEFAULT 0,
              impact_score REAL NOT NULL DEFAULT 0,
              creation_potential_score REAL NOT NULL DEFAULT 0,
              overall_score REAL NOT NULL DEFAULT 0,
              evidence_event_count INTEGER NOT NULL DEFAULT 0,
              evidence_article_count INTEGER NOT NULL DEFAULT 0,
              evidence_source_count INTEGER NOT NULL DEFAULT 0,
              evidence_platform_count INTEGER NOT NULL DEFAULT 0,
              trend_status TEXT NOT NULL DEFAULT 'steady',
              recommended_angles_json TEXT NOT NULL DEFAULT '[]',
              recommended_formats_json TEXT NOT NULL DEFAULT '[]',
              emotion_points_json TEXT NOT NULL DEFAULT '[]',
              debate_points_json TEXT NOT NULL DEFAULT '[]',
              card_summary TEXT NOT NULL DEFAULT '',
              generated_at TEXT DEFAULT (datetime('now')),
              FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_articles (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              topic_id INTEGER NOT NULL,
              raw_item_id INTEGER NOT NULL UNIQUE,
              article_title TEXT,
              platform TEXT,
              source_name TEXT,
              canonical_url TEXT,
              published_at TEXT,
              confidence REAL NOT NULL DEFAULT 1,
              created_at TEXT DEFAULT (datetime('now')),
              updated_at TEXT DEFAULT (datetime('now')),
              FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE,
              FOREIGN KEY (raw_item_id) REFERENCES raw_items(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              topic_id INTEGER NOT NULL,
              event_id INTEGER NOT NULL,
              confidence REAL NOT NULL DEFAULT 1,
              created_at TEXT DEFAULT (datetime('now')),
              updated_at TEXT DEFAULT (datetime('now')),
              UNIQUE(topic_id, event_id),
              FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE,
              FOREIGN KEY (event_id) REFERENCES event_candidates(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_scores_topic_id ON topic_scores(topic_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_articles_topic_id ON topic_articles(topic_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_events_topic_id ON topic_events(topic_id)")

        if profile == "sample":
            timestamp = now_iso()
            raw_cur = conn.execute(
                """
                INSERT OR IGNORE INTO raw_items(
                  platform, source_handle, item_id, title, content, url, published_at, metrics_json, body_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "sample",
                    "self-media-demo",
                    "sample-001",
                    "Sample topic: local-first content workbench",
                    "This sample item proves the fresh clone can render searchable material without private accounts.",
                    "https://example.com/self-media-demo",
                    timestamp,
                    json.dumps({"sample": True}),
                    "none",
                ),
            )
            raw_row = conn.execute(
                "SELECT id FROM raw_items WHERE platform='sample' AND item_id='sample-001' LIMIT 1"
            ).fetchone()
            raw_item_id = int((raw_row or [raw_cur.lastrowid or 1])[0] or 1)
            topic = {
                "id": 1,
                "topic_key": "sample-local-first-workbench",
                "title": "Sample: Local-first content workbench",
                "summary": "A demo topic generated during runtime initialization.",
                "topic_type": "sample",
                "status": "confirmed",
                "first_seen_at": timestamp,
                "last_seen_at": timestamp,
                "importance_score": 0.7,
                "impact_score": 0.6,
                "creation_potential_score": 0.8,
                "overall_score": 0.75,
                "evidence_event_count": 1,
                "evidence_article_count": 1,
                "evidence_source_count": 1,
                "evidence_platform_count": 1,
                "card_summary": "Sample topic for first-run verification.",
                "primary_platforms": ["sample"],
                "primary_entities": ["self-media-workbench"],
                "risk_flags": [],
            }
            topic_cur = conn.execute(
                """
                INSERT INTO topics(
                  topic_key, title, summary, topic_type, status, time_window_hours,
                  first_seen_at, last_seen_at, primary_platforms_json, primary_entities_json,
                  risk_flags_json, is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(topic_key, topic_type) DO UPDATE SET
                  title=excluded.title,
                  summary=excluded.summary,
                  status=excluded.status,
                  time_window_hours=excluded.time_window_hours,
                  first_seen_at=excluded.first_seen_at,
                  last_seen_at=excluded.last_seen_at,
                  primary_platforms_json=excluded.primary_platforms_json,
                  primary_entities_json=excluded.primary_entities_json,
                  risk_flags_json=excluded.risk_flags_json,
                  is_active=1,
                  updated_at=excluded.updated_at
                """,
                (
                    topic["topic_key"],
                    topic["title"],
                    topic["summary"],
                    "article_theme_v2",
                    "ready",
                    72,
                    timestamp,
                    timestamp,
                    json.dumps(topic["primary_platforms"]),
                    json.dumps(topic["primary_entities"]),
                    json.dumps(topic["risk_flags"]),
                    timestamp,
                    timestamp,
                ),
            )
            topic_row = conn.execute(
                "SELECT id FROM topics WHERE topic_key=? AND topic_type='article_theme_v2' LIMIT 1",
                (topic["topic_key"],),
            ).fetchone()
            topic_id = int((topic_row or [topic_cur.lastrowid or 1])[0] or 1)
            topic["id"] = topic_id
            conn.execute(
                """
                INSERT INTO topic_scores(
                  topic_id, importance_score, impact_score, creation_potential_score, overall_score,
                  evidence_event_count, evidence_article_count, evidence_source_count, evidence_platform_count,
                  trend_status, recommended_angles_json, recommended_formats_json, emotion_points_json,
                  debate_points_json, card_summary, generated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    topic_id,
                    topic["importance_score"],
                    topic["impact_score"],
                    topic["creation_potential_score"],
                    topic["overall_score"],
                    topic["evidence_event_count"],
                    topic["evidence_article_count"],
                    topic["evidence_source_count"],
                    topic["evidence_platform_count"],
                    "steady",
                    json.dumps(["Explain the local-first deployment value"]),
                    json.dumps(["brief", "longform"]),
                    json.dumps([]),
                    json.dumps([]),
                    topic["card_summary"],
                    timestamp,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO topic_articles(
                  topic_id, raw_item_id, article_title, platform, source_name, canonical_url, published_at, confidence, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    topic_id,
                    raw_item_id,
                    "Sample topic: local-first content workbench",
                    "sample",
                    "self-media-demo",
                    "https://example.com/self-media-demo",
                    timestamp,
                    1,
                    timestamp,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO topic_api_cache_topics(
                  topic_id, topic_key, title, summary, topic_type, status, first_seen_at, last_seen_at,
                  importance_score, impact_score, creation_potential_score, overall_score,
                  evidence_event_count, evidence_article_count, evidence_source_count, evidence_platform_count,
                  card_summary, primary_platforms_json, primary_entities_json, risk_flags_json, topic_json, synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    topic["id"],
                    topic["topic_key"],
                    topic["title"],
                    topic["summary"],
                    topic["topic_type"],
                    topic["status"],
                    topic["first_seen_at"],
                    topic["last_seen_at"],
                    topic["importance_score"],
                    topic["impact_score"],
                    topic["creation_potential_score"],
                    topic["overall_score"],
                    topic["evidence_event_count"],
                    topic["evidence_article_count"],
                    topic["evidence_source_count"],
                    topic["evidence_platform_count"],
                    topic["card_summary"],
                    json.dumps(topic["primary_platforms"]),
                    json.dumps(topic["primary_entities"]),
                    json.dumps(topic["risk_flags"]),
                    json.dumps(topic),
                    timestamp,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO topic_api_cache_details(
                  topic_id, topic_json, articles_json, article_pagination_json, events_json, event_pagination_json, synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    1,
                    json.dumps(topic),
                    json.dumps(
                        [
                            {
                                "id": "sample-001",
                                "title": "Sample topic: local-first content workbench",
                                "url": "https://example.com/self-media-demo",
                                "summary": "A sample article available immediately after initialization.",
                                "platform": "sample",
                            }
                        ]
                    ),
                    json.dumps({"page": 1, "limit": 20, "total": 1}),
                    json.dumps([]),
                    json.dumps({"page": 1, "limit": 20, "total": 0}),
                    timestamp,
                ),
            )
        conn.commit()


def ensure_sample_vault(path: Path) -> None:
    note_dir = path / "notes"
    card_dir = path / "analysis-cards"
    note_dir.mkdir(parents=True, exist_ok=True)
    card_dir.mkdir(parents=True, exist_ok=True)
    (note_dir / "sample-note.md").write_text(
        "# Sample note\n\nThis note is generated locally and can be indexed by Creative Studio.\n",
        encoding="utf-8",
    )
    (card_dir / "sample-card.md").write_text(
        "# Sample analysis card\n\n- Source: local sample\n- Purpose: fresh clone verification\n",
        encoding="utf-8",
    )


def ensure_source_files(config: dict) -> None:
    sources = dict(config.get("sources") or {})
    source_dir = REPO_ROOT / "config" / "sources"
    source_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        "a-stage-wechat-sources.json": {"version": 1, "sources": sources.get("wechat_sources") or []},
        "a-stage-youtube-channels.json": {"version": 1, "channels": sources.get("youtube_channels") or []},
        "a-stage-bilibili-sources.json": {"version": 1, "sources": sources.get("bilibili_sources") or []},
        "a-stage-douyin-sources.json": {"version": 1, "sources": sources.get("douyin_sources") or []},
        "a-stage-feishu-sources.json": {"version": 1, "sources": sources.get("feishu_sources") or []},
        "a-stage-x-sources.json": {"version": 1, "accounts": sources.get("x_accounts") or []},
    }
    for name, payload in payloads.items():
        target = source_dir / name
        has_entries = any(isinstance(value, list) and value for value in payload.values())
        if target.exists() and not has_entries:
            continue
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="sample", choices=["sample", "empty"])
    args = parser.parse_args()

    config = get_config()
    runtime = get_path(config, "paths.runtime_dir")
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "logs").mkdir(parents=True, exist_ok=True)
    (runtime / "fetched").mkdir(parents=True, exist_ok=True)
    get_path(config, "paths.creation_data_root").mkdir(parents=True, exist_ok=True)

    ensure_event_db(get_path(config, "paths.event_radar_db_path"), args.profile)
    ensure_event_db(get_path(config, "paths.event_radar_mirror_db_path", str(runtime / "event_radar.mirror.db")), "empty")
    ensure_sample_vault(get_path(config, "paths.sample_vault_path"))
    ensure_source_files(config)

    print(f"Runtime initialized at {runtime}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
