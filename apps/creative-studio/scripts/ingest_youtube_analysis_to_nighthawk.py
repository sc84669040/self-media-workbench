#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "event_radar.db"


def ensure_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
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
        CREATE TABLE IF NOT EXISTS alert_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          platform TEXT NOT NULL,
          item_id TEXT NOT NULL,
          source_handle TEXT,
          title TEXT,
          url TEXT,
          published_at TEXT,
          fetched_at TEXT,
          recommend_level TEXT NOT NULL,
          score REAL NOT NULL DEFAULT 0,
          reason_json TEXT DEFAULT '{}',
          event_fingerprint TEXT,
          notify_status TEXT NOT NULL DEFAULT 'skipped',
          notify_error TEXT DEFAULT '',
          notify_attempts INTEGER NOT NULL DEFAULT 0,
          notified_at TEXT,
          created_at TEXT DEFAULT (datetime('now')),
          updated_at TEXT DEFAULT (datetime('now')),
          obsidian_note_path TEXT DEFAULT '',
          obsidian_note_title TEXT DEFAULT '',
          UNIQUE(platform, item_id)
        )
        """
    )
    conn.commit()


def load_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def extract_video_id(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        host = (parsed.netloc or "").lower()
        if "youtu.be" in host:
            return parsed.path.strip("/")
        query_id = (parse_qs(parsed.query).get("v") or [""])[0].strip()
        if query_id:
            return query_id
        parts = [part for part in parsed.path.split("/") if part]
        if "shorts" in parts:
            idx = parts.index("shorts")
            if idx + 1 < len(parts):
                return parts[idx + 1]
    except Exception:
        return ""
    return ""


def normalize_item_id(payload: dict[str, Any]) -> str:
    item_id = str(payload.get("video_id") or "").strip()
    if item_id:
        return item_id
    url = str(payload.get("source_url") or payload.get("url") or "").strip()
    extracted = extract_video_id(url)
    if extracted:
        return extracted
    title = str(payload.get("title") or "").strip()
    return hashlib.md5(f"youtube|{url}|{title}".encode("utf-8")).hexdigest()  # noqa: S324


def build_content(payload: dict[str, Any]) -> str:
    parts = [
        str(payload.get("summary") or "").strip(),
        "\n".join(str(x).strip() for x in (payload.get("core_points") or []) if str(x).strip()),
        str(payload.get("transcript_excerpt") or "").strip(),
    ]
    return "\n\n".join([p for p in parts if p]).strip()


def build_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "platform": "youtube",
        "slug": str(payload.get("slug") or "").strip(),
        "content_type": str(payload.get("content_type") or "").strip(),
        "confidence": str(payload.get("confidence") or "").strip(),
        "recommendation": str(payload.get("recommendation") or "").strip(),
        "recommendation_reason": str(payload.get("recommendation_reason") or "").strip(),
        "nighthawk_action": str(payload.get("nighthawk_action") or "").strip(),
        "review_needed": bool(payload.get("review_needed", True)),
        "route_bucket": str(payload.get("route_bucket") or "").strip(),
        "transcript_language": str(payload.get("transcript_language") or "").strip(),
        "transcript_kind": str(payload.get("transcript_kind") or "").strip(),
        "related_topics": payload.get("related_topics") or [],
        "facts_and_data": payload.get("facts_and_data") or [],
        "tags": payload.get("tags") or [],
        "golden_quotes": payload.get("golden_quotes") or [],
        "angles": payload.get("angles") or [],
        "title_candidates": payload.get("title_candidates") or [],
        "extended_questions": payload.get("extended_questions") or [],
        "risk_notes": payload.get("risk_notes") or [],
        "creation_suggestion": str(payload.get("creation_suggestion") or "").strip(),
    }


def map_notify_status(recommendation: str, action: str) -> str:
    rec = str(recommendation or "").strip().lower()
    act = str(action or "").strip().lower()
    if act == "discard":
        return "skipped"
    if rec == "strong":
        return "pending"
    if rec == "watch":
        return "candidate"
    if act == "archive":
        return "skipped"
    return "skipped"


def map_score(payload: dict[str, Any]) -> float:
    raw_score = payload.get("score")
    if raw_score is not None:
        try:
            return float(raw_score)
        except Exception:
            pass
    recommendation = str(payload.get("recommendation") or "").strip().lower()
    confidence = str(payload.get("confidence") or "").strip().lower()
    base = {"strong": 9.2, "watch": 6.4, "skip": 2.0}.get(recommendation, 4.0)
    if confidence == "high":
        base += 0.4
    elif confidence == "low":
        base -= 0.6
    return round(base, 2)


def fingerprint(payload: dict[str, Any]) -> str:
    text = " ".join(
        [
            str(payload.get("title") or ""),
            str(payload.get("summary") or ""),
            " ".join(str(x) for x in (payload.get("core_points") or [])[:3]),
        ]
    ).strip()
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def cleanup_legacy_duplicates(conn: sqlite3.Connection, canonical_item_id: str, source_url: str) -> dict[str, int]:
    if not canonical_item_id or not source_url:
        return {"raw_deleted": 0, "alert_deleted": 0}
    cur = conn.cursor()
    raw_deleted = cur.execute(
        """
        DELETE FROM raw_items
        WHERE platform='youtube' AND url=? AND item_id<>?
        """,
        (source_url, canonical_item_id),
    ).rowcount
    alert_deleted = cur.execute(
        """
        DELETE FROM alert_queue
        WHERE platform='youtube' AND url=? AND item_id<>?
        """,
        (source_url, canonical_item_id),
    ).rowcount
    return {"raw_deleted": int(raw_deleted or 0), "alert_deleted": int(alert_deleted or 0)}


def upsert_raw_item(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
    item_id = normalize_item_id(payload)
    source_name = str(payload.get("source_name") or "").strip()
    title = str(payload.get("title") or "").strip()
    source_url = str(payload.get("source_url") or payload.get("url") or "").strip()
    published_at = str(payload.get("published_at") or "").strip()
    content = build_content(payload)
    metrics_json = json.dumps(build_metrics(payload), ensure_ascii=False)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO raw_items(
          platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at
        ) VALUES ('youtube', ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (source_name, item_id, title, content, source_url, published_at, metrics_json),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            UPDATE raw_items
            SET source_handle=?, title=?, content=?, url=?, published_at=?, metrics_json=?, fetched_at=datetime('now')
            WHERE platform='youtube' AND item_id=?
            """,
            (source_name, title, content, source_url, published_at, metrics_json, item_id),
        )


def upsert_alert_queue(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
    item_id = normalize_item_id(payload)
    source_name = str(payload.get("source_name") or "").strip()
    title = str(payload.get("title") or "").strip()
    source_url = str(payload.get("source_url") or payload.get("url") or "").strip()
    published_at = str(payload.get("published_at") or "").strip()
    recommendation = str(payload.get("recommendation") or "watch").strip().lower()
    action = str(payload.get("nighthawk_action") or "candidate").strip().lower()
    notify_status = map_notify_status(recommendation, action)
    score = map_score(payload)
    reasons = {
        "reasons": [
            str(payload.get("recommendation_reason") or "待补充推荐理由。"),
            f"route_bucket={str(payload.get('route_bucket') or '').strip() or 'youtube-watch'}",
            f"confidence={str(payload.get('confidence') or '').strip() or 'unknown'}",
        ]
    }
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alert_queue(
          platform, item_id, source_handle, title, url, published_at, fetched_at,
          recommend_level, score, reason_json, event_fingerprint, notify_status, notify_error,
          created_at, updated_at
        ) VALUES ('youtube', ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, '', datetime('now'), datetime('now'))
        ON CONFLICT(platform, item_id) DO UPDATE SET
          source_handle=excluded.source_handle,
          title=excluded.title,
          url=excluded.url,
          published_at=excluded.published_at,
          fetched_at=datetime('now'),
          recommend_level=excluded.recommend_level,
          score=excluded.score,
          reason_json=excluded.reason_json,
          event_fingerprint=excluded.event_fingerprint,
          notify_status=excluded.notify_status,
          updated_at=datetime('now')
        """,
        (
            item_id,
            source_name,
            title,
            source_url,
            published_at,
            recommendation,
            score,
            json.dumps(reasons, ensure_ascii=False),
            fingerprint(payload),
            notify_status,
        ),
    )


def ingest(path: Path) -> dict[str, Any]:
    payload = load_payload(path)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_tables(conn)
        item_id = normalize_item_id(payload)
        source_url = str(payload.get("source_url") or payload.get("url") or "").strip()
        cleanup = cleanup_legacy_duplicates(conn, item_id, source_url)
        upsert_raw_item(conn, payload)
        upsert_alert_queue(conn, payload)
        conn.commit()
        row = conn.execute(
            "SELECT platform, item_id, recommend_level, score, notify_status FROM alert_queue WHERE platform='youtube' AND item_id=?",
            (item_id,),
        ).fetchone()
        return {
            "ok": True,
            "platform": row[0],
            "item_id": row[1],
            "recommend_level": row[2],
            "score": row[3],
            "notify_status": row[4],
            "cleanup": cleanup,
            "db_path": str(DB_PATH),
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest normalized YouTube analysis JSON into NightHawk raw_items + alert_queue")
    parser.add_argument("input", help="Normalized JSON file path from youtube_analysis_card.py --json-output")
    args = parser.parse_args()
    result = ingest(Path(args.input))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
