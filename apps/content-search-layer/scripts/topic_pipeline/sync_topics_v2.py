#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = BASE_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from runtime_config import event_radar_db_path  # noqa: E402

DB_PATH = event_radar_db_path()
PIPELINE_NAME = "topic_v2"
TOPIC_TYPE = "article_theme_v2"

from build_topics_v2 import DEFAULT_WINDOW_DAYS, classify_raw_item, fetch_candidate_rows


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def ensure_topic_pipeline_schema(conn: sqlite3.Connection) -> None:
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_articles_topic_id ON topic_articles(topic_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_articles_published_at ON topic_articles(published_at DESC)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS topic_pipeline_state (
          pipeline_name TEXT PRIMARY KEY,
          last_raw_item_id INTEGER NOT NULL DEFAULT 0,
          last_run_mode TEXT,
          last_run_at TEXT,
          notes_json TEXT DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS topic_generation_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          pipeline_name TEXT NOT NULL,
          run_mode TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'running',
          last_raw_item_id INTEGER NOT NULL DEFAULT 0,
          topics_upserted INTEGER NOT NULL DEFAULT 0,
          topics_deactivated INTEGER NOT NULL DEFAULT 0,
          articles_linked INTEGER NOT NULL DEFAULT 0,
          created_at TEXT DEFAULT (datetime('now')),
          finished_at TEXT,
          notes_json TEXT DEFAULT '{}'
        )
        """
    )
    conn.commit()


def fetch_incremental_rows(conn: sqlite3.Connection, last_raw_item_id: int) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
          id,
          platform,
          source_handle,
          item_id,
          title,
          content,
          url,
          published_at,
          metrics_json,
          fetched_at,
          body_status
        FROM raw_items
        WHERE id > ?
        ORDER BY id ASC
        """,
        (max(0, int(last_raw_item_id)),),
    ).fetchall()
    return [dict(row) for row in rows]


def load_state(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        "SELECT pipeline_name, last_raw_item_id, last_run_mode, last_run_at, notes_json FROM topic_pipeline_state WHERE pipeline_name=?",
        (PIPELINE_NAME,),
    ).fetchone()
    if not row:
        return {"last_raw_item_id": 0}
    notes = {}
    try:
        notes = json.loads(row[4] or "{}")
    except Exception:
        notes = {}
    return {
        "last_raw_item_id": int(row[1] or 0),
        "last_run_mode": str(row[2] or "").strip(),
        "last_run_at": str(row[3] or "").strip(),
        "notes": notes,
    }


def save_state(conn: sqlite3.Connection, *, last_raw_item_id: int, run_mode: str, notes: dict[str, Any] | None = None) -> None:
    conn.execute(
        """
        INSERT INTO topic_pipeline_state (pipeline_name, last_raw_item_id, last_run_mode, last_run_at, notes_json)
        VALUES (?, ?, ?, datetime('now'), ?)
        ON CONFLICT(pipeline_name) DO UPDATE SET
          last_raw_item_id=excluded.last_raw_item_id,
          last_run_mode=excluded.last_run_mode,
          last_run_at=datetime('now'),
          notes_json=excluded.notes_json
        """,
        (PIPELINE_NAME, max(0, int(last_raw_item_id)), run_mode, _json_dump(notes or {})),
    )


def start_run(conn: sqlite3.Connection, *, run_mode: str, last_raw_item_id: int, notes: dict[str, Any] | None = None) -> int:
    cur = conn.execute(
        """
        INSERT INTO topic_generation_runs (pipeline_name, run_mode, status, last_raw_item_id, notes_json)
        VALUES (?, ?, 'running', ?, ?)
        """,
        (PIPELINE_NAME, run_mode, max(0, int(last_raw_item_id)), _json_dump(notes or {})),
    )
    return int(cur.lastrowid or 0)


def finish_run(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    status: str,
    topics_upserted: int,
    topics_deactivated: int,
    articles_linked: int,
    notes: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        UPDATE topic_generation_runs
        SET status=?,
            topics_upserted=?,
            topics_deactivated=?,
            articles_linked=?,
            finished_at=datetime('now'),
            notes_json=?
        WHERE id=?
        """,
        (status, topics_upserted, topics_deactivated, articles_linked, _json_dump(notes or {}), int(run_id)),
    )


def classify_rows(rows: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], int]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmatched = 0
    for row in rows:
        classified = classify_raw_item(row)
        if not classified:
            unmatched += 1
            continue
        grouped[str(classified["topic_key"])].append(classified)
    return grouped, unmatched


def _unique_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[int] = set()
    output: list[dict[str, Any]] = []
    for article in articles:
        raw_item_id = int(article.get("raw_item_id") or 0)
        if raw_item_id <= 0 or raw_item_id in seen:
            continue
        seen.add(raw_item_id)
        output.append(article)
    return output


def summarize_topic(topic_title: str, articles: list[dict[str, Any]]) -> tuple[str, str]:
    unique_articles = _unique_articles(articles)
    article_count = len(unique_articles)
    source_count = len({str(item.get("source_name") or "").strip() for item in unique_articles if str(item.get("source_name") or "").strip()})
    platform_count = len({str(item.get("platform") or "").strip() for item in unique_articles if str(item.get("platform") or "").strip()})
    sample_titles = [str(item.get("title") or "").strip() for item in unique_articles[:3] if str(item.get("title") or "").strip()]
    summary = f"{topic_title} 近窗内聚合 {article_count} 篇文章，覆盖 {source_count} 个信源、{platform_count} 个平台。"
    if sample_titles:
        summary += " 代表文章包括：" + "；".join(sample_titles[:2])
    card_summary = summary
    return summary[:400], card_summary[:240]


def build_score_payload(topic_title: str, articles: list[dict[str, Any]]) -> dict[str, Any]:
    unique_articles = _unique_articles(articles)
    article_count = len(unique_articles)
    platforms = [str(item.get("platform") or "").strip() for item in unique_articles if str(item.get("platform") or "").strip()]
    platform_count = len(set(platforms))
    source_count = len({str(item.get("source_name") or "").strip() for item in unique_articles if str(item.get("source_name") or "").strip()})
    latest_titles = [str(item.get("title") or "").strip() for item in unique_articles[:3] if str(item.get("title") or "").strip()]

    importance = min(100.0, 30.0 + article_count * 4.0 + source_count * 3.0)
    impact = min(100.0, 25.0 + article_count * 3.0 + platform_count * 8.0)
    creation = 35.0
    lower = topic_title.lower()
    if any(token in lower for token in ["使用方法", "提示词", "案例", "评测", "用例", "自动化"]):
        creation += 18.0
    if any(token in lower for token in ["风险", "争议", "资本", "发布", "升级"]):
        creation += 10.0
    creation = min(100.0, creation + min(article_count, 8) * 3.0)
    overall = round(importance * 0.35 + impact * 0.30 + creation * 0.35, 2)

    trend_status = "rising" if article_count >= 6 else "steady"
    recommended_angles = latest_titles[:3]
    recommended_formats = ["brief"] if article_count < 5 else ["brief", "analysis"]
    if any(token in topic_title for token in ["案例", "用例", "自动化"]):
        recommended_formats.append("case-study")
    if any(token in topic_title for token in ["风险", "争议"]):
        recommended_formats.append("commentary")

    emotion_points: list[str] = []
    debate_points: list[str] = []
    if "风险" in topic_title or "争议" in topic_title:
        emotion_points.append("争议感")
        debate_points.append("是否值得大规模采用")
    if "发布" in topic_title or "升级" in topic_title:
        emotion_points.append("新鲜感")
    if "使用方法" in topic_title or "提示词" in topic_title:
        emotion_points.append("上手欲望")

    return {
        "importance_score": round(importance, 2),
        "impact_score": round(impact, 2),
        "creation_potential_score": round(creation, 2),
        "overall_score": overall,
        "evidence_event_count": 0,
        "evidence_article_count": article_count,
        "evidence_source_count": source_count,
        "evidence_platform_count": platform_count,
        "trend_status": trend_status,
        "recommended_angles_json": _json_dump(recommended_angles),
        "recommended_formats_json": _json_dump(sorted(set(recommended_formats))),
        "emotion_points_json": _json_dump(emotion_points),
        "debate_points_json": _json_dump(debate_points),
    }


def resolve_topic_record(
    conn: sqlite3.Connection,
    *,
    topic_key: str,
    topic_title: str,
    articles: list[dict[str, Any]],
    time_window_hours: int,
) -> int:
    unique_articles = _unique_articles(articles)
    summary, card_summary = summarize_topic(topic_title, unique_articles)
    platforms = sorted({str(item.get("platform") or "").strip() for item in unique_articles if str(item.get("platform") or "").strip()})
    subject_labels = sorted({str(item.get("subject_label") or "").strip() for item in unique_articles if str(item.get("subject_label") or "").strip()})
    risk_flags = sorted(
        {
            "风险争议"
            for item in unique_articles
            if str(item.get("facet_key") or "").strip() == "security"
        }
    )
    published_values = [str(item.get("published_at") or "").strip() for item in unique_articles if str(item.get("published_at") or "").strip()]
    first_seen_at = min(published_values) if published_values else ""
    last_seen_at = max(published_values) if published_values else ""

    row = conn.execute(
        """
        SELECT id
        FROM topics
        WHERE topic_key=? AND topic_type=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (topic_key, TOPIC_TYPE),
    ).fetchone()
    if row:
        topic_id = int(row[0])
        conn.execute(
            """
            UPDATE topics
            SET title=?,
                summary=?,
                status='ready',
                time_window_hours=?,
                first_seen_at=?,
                last_seen_at=?,
                primary_platforms_json=?,
                primary_entities_json=?,
                risk_flags_json=?,
                is_active=1,
                updated_at=datetime('now')
            WHERE id=?
            """,
            (
                topic_title,
                summary,
                max(24, int(time_window_hours)),
                first_seen_at or None,
                last_seen_at or None,
                _json_dump(platforms),
                _json_dump(subject_labels),
                _json_dump(risk_flags),
                topic_id,
            ),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO topics (
                topic_key, title, summary, topic_type, status, time_window_hours,
                first_seen_at, last_seen_at, primary_platforms_json, primary_entities_json,
                risk_flags_json, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
            """,
            (
                topic_key,
                topic_title,
                summary,
                TOPIC_TYPE,
                max(24, int(time_window_hours)),
                first_seen_at or None,
                last_seen_at or None,
                _json_dump(platforms),
                _json_dump(subject_labels),
                _json_dump(risk_flags),
            ),
        )
        topic_id = int(cur.lastrowid or 0)

    score_payload = build_score_payload(topic_title, unique_articles)
    conn.execute(
        """
        INSERT INTO topic_scores (
            topic_id,
            importance_score,
            impact_score,
            creation_potential_score,
            overall_score,
            evidence_event_count,
            evidence_article_count,
            evidence_source_count,
            evidence_platform_count,
            trend_status,
            recommended_angles_json,
            recommended_formats_json,
            emotion_points_json,
            debate_points_json,
            card_summary,
            generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            topic_id,
            score_payload["importance_score"],
            score_payload["impact_score"],
            score_payload["creation_potential_score"],
            score_payload["overall_score"],
            score_payload["evidence_event_count"],
            score_payload["evidence_article_count"],
            score_payload["evidence_source_count"],
            score_payload["evidence_platform_count"],
            score_payload["trend_status"],
            score_payload["recommended_angles_json"],
            score_payload["recommended_formats_json"],
            score_payload["emotion_points_json"],
            score_payload["debate_points_json"],
            card_summary,
        ),
    )
    return topic_id


def link_articles(conn: sqlite3.Connection, *, topic_id: int, articles: list[dict[str, Any]]) -> int:
    linked = 0
    for article in _unique_articles(articles):
        raw_item_id = int(article.get("raw_item_id") or 0)
        if raw_item_id <= 0:
            continue
        conn.execute(
            """
            INSERT INTO topic_articles (
                topic_id, raw_item_id, article_title, platform, source_name, canonical_url, published_at, confidence, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
            ON CONFLICT(raw_item_id) DO UPDATE SET
                topic_id=excluded.topic_id,
                article_title=excluded.article_title,
                platform=excluded.platform,
                source_name=excluded.source_name,
                canonical_url=excluded.canonical_url,
                published_at=excluded.published_at,
                confidence=excluded.confidence,
                updated_at=datetime('now')
            """,
            (
                topic_id,
                raw_item_id,
                str(article.get("title") or "").strip(),
                str(article.get("platform") or "").strip(),
                str(article.get("source_name") or "").strip(),
                str(article.get("canonical_url") or "").strip(),
                str(article.get("published_at") or "").strip(),
            ),
        )
        linked += 1
    return linked


def deactivate_legacy_topics(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "UPDATE topics SET is_active=0, updated_at=datetime('now') WHERE COALESCE(topic_type, '') <> ? AND COALESCE(is_active, 1) <> 0",
        (TOPIC_TYPE,),
    )
    return int(cur.rowcount or 0)


def deactivate_missing_v2_topics(conn: sqlite3.Connection, active_keys: set[str]) -> int:
    rows = conn.execute(
        "SELECT id, topic_key FROM topics WHERE topic_type=? AND COALESCE(is_active, 1) = 1",
        (TOPIC_TYPE,),
    ).fetchall()
    deactivate_ids = [int(row[0]) for row in rows if str(row[1] or "") not in active_keys]
    if not deactivate_ids:
        return 0
    placeholders = ",".join("?" for _ in deactivate_ids)
    conn.execute(
        f"UPDATE topics SET is_active=0, updated_at=datetime('now') WHERE id IN ({placeholders})",
        deactivate_ids,
    )
    return len(deactivate_ids)


def collect_max_raw_item_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id") or 0) for row in rows), default=0)


def run_full_rebuild(conn: sqlite3.Connection, *, window_days: int) -> dict[str, Any]:
    rows = fetch_candidate_rows(conn, window_days)
    grouped, unmatched = classify_rows(rows)
    active_keys = set(grouped.keys())
    existing_v2_ids = [int(row[0]) for row in conn.execute("SELECT id FROM topics WHERE topic_type=?", (TOPIC_TYPE,)).fetchall()]
    if existing_v2_ids:
        placeholders = ",".join("?" for _ in existing_v2_ids)
        conn.execute(f"DELETE FROM topic_articles WHERE topic_id IN ({placeholders})", existing_v2_ids)
        conn.execute(f"DELETE FROM topic_events WHERE topic_id IN ({placeholders})", existing_v2_ids)

    topics_upserted = 0
    articles_linked = 0
    for topic_key, articles in grouped.items():
        unique_articles = _unique_articles(articles)
        if len(unique_articles) < 2:
            continue
        topic_title = str(unique_articles[0].get("topic_title") or topic_key)
        topic_id = resolve_topic_record(
            conn,
            topic_key=topic_key,
            topic_title=topic_title,
            articles=unique_articles,
            time_window_hours=window_days * 24,
        )
        topics_upserted += 1
        articles_linked += link_articles(conn, topic_id=topic_id, articles=unique_articles)

    deactivated = deactivate_missing_v2_topics(conn, active_keys)
    deactivated += deactivate_legacy_topics(conn)
    return {
        "mode": "full-rebuild",
        "window_days": window_days,
        "raw_items_scanned": len(rows),
        "topics_upserted": topics_upserted,
        "topics_deactivated": deactivated,
        "articles_linked": articles_linked,
        "unmatched_rows": unmatched,
        "last_raw_item_id": collect_max_raw_item_id(rows),
    }


def run_incremental(conn: sqlite3.Connection, *, state: dict[str, Any], window_days: int) -> dict[str, Any]:
    rows = fetch_incremental_rows(conn, int(state.get("last_raw_item_id") or 0))
    grouped, unmatched = classify_rows(rows)
    topics_upserted = 0
    articles_linked = 0

    for topic_key, articles in grouped.items():
        unique_articles = _unique_articles(articles)
        if not unique_articles:
            continue
        topic_title = str(unique_articles[0].get("topic_title") or topic_key)
        topic_id = resolve_topic_record(
            conn,
            topic_key=topic_key,
            topic_title=topic_title,
            articles=unique_articles,
            time_window_hours=window_days * 24,
        )
        topics_upserted += 1
        articles_linked += link_articles(conn, topic_id=topic_id, articles=unique_articles)

    return {
        "mode": "incremental",
        "raw_items_scanned": len(rows),
        "topics_upserted": topics_upserted,
        "topics_deactivated": 0,
        "articles_linked": articles_linked,
        "unmatched_rows": unmatched,
        "last_raw_item_id": collect_max_raw_item_id(rows) or int(state.get("last_raw_item_id") or 0),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["incremental", "full-rebuild"], default="incremental")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with sqlite3.connect(DB_PATH) as conn:
        ensure_topic_pipeline_schema(conn)
        state = load_state(conn)

        if args.mode == "full-rebuild":
            result = run_full_rebuild(conn, window_days=max(1, int(args.window_days or DEFAULT_WINDOW_DAYS)))
        else:
            result = run_incremental(conn, state=state, window_days=max(1, int(args.window_days or DEFAULT_WINDOW_DAYS)))

        if args.dry_run:
            conn.rollback()
            print(json.dumps({"ok": True, "dry_run": True, **result}, ensure_ascii=False, indent=2))
            return

        run_id = start_run(
            conn,
            run_mode=str(result["mode"]),
            last_raw_item_id=int(state.get("last_raw_item_id") or 0),
            notes={"window_days": max(1, int(args.window_days or DEFAULT_WINDOW_DAYS))},
        )
        save_state(
            conn,
            last_raw_item_id=int(result["last_raw_item_id"] or 0),
            run_mode=str(result["mode"]),
            notes={"window_days": max(1, int(args.window_days or DEFAULT_WINDOW_DAYS))},
        )
        finish_run(
            conn,
            run_id=run_id,
            status="success",
            topics_upserted=int(result["topics_upserted"] or 0),
            topics_deactivated=int(result["topics_deactivated"] or 0),
            articles_linked=int(result["articles_linked"] or 0),
            notes={"window_days": max(1, int(args.window_days or DEFAULT_WINDOW_DAYS)), "unmatched_rows": int(result["unmatched_rows"] or 0)},
        )
        conn.commit()

    print(json.dumps({"ok": True, **result}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
