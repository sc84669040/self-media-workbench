#!/usr/bin/env python3
"""content-search-layer 采集守护进程（V1）

目标：
- 每轮按平台采集账号内容，写入 SQLite raw_items
- 轮询间隔支持随机区间（默认 10~15 分钟）
- 架构可扩展到更多平台（当前先实现 X）
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
import time
from functools import lru_cache
from abc import ABC, abstractmethod
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from shutil import which
from typing import Any

import yaml

from runtime_config import config as root_config, event_radar_db_path
from workspace_paths import VENV_ROOT, resolve_skill_project_root
from event_radar_mirror import mirror_event_radar_db
from wechat_collector import WeChatCollector, load_wechat_sources
from youtube_collector import fetch_recent_videos, fetch_transcript, load_youtube_channels
from feishu_collector import FeishuCollector, load_feishu_sources
from douyin_collector import fetch_recent_videos as fetch_recent_douyin_videos, load_douyin_sources

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = event_radar_db_path()
TWITTER_TIMEOUT_SEC = int(os.environ.get("TWITTER_TIMEOUT_SEC", "45"))
DEFAULT_FETCH_LIMIT = 8
CONTENT_FETCH_HUB_CLI_PATH = resolve_skill_project_root("content-fetch-hub") / "scripts" / "fetch_content_cli.py"
DB_DATETIME_NORMALIZED = False
DB_TIMEOUT_SEC = int(os.environ.get("EVENT_RADAR_DB_TIMEOUT_SEC", "30"))
ALERT_RULES_PATH = BASE_DIR / "config" / "alert-rules.yaml"
SQLITE_BUSY_TIMEOUT_MS = max(1000, DB_TIMEOUT_SEC * 1000)
BODY_QUEUE_NORMALIZE_ON_COLLECT = os.environ.get("EVENT_RADAR_NORMALIZE_BODY_QUEUE_ON_COLLECT", "0").strip().lower() in {"1", "true", "yes", "on"}


def configure_sqlite_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA synchronous = NORMAL")
    except Exception:
        pass
    return conn


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_datetime_text(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        if raw.isdigit():
            if len(raw) == 8:
                dt = datetime.strptime(raw, "%Y%m%d")
                return dt.strftime("%Y-%m-%d 00:00:00")
            if len(raw) >= 10:
                dt = datetime.fromtimestamp(int(raw[:10]))
                return dt.strftime("%Y-%m-%d %H:%M:%S")
        if len(raw) == 10 and raw.count("-") == 2:
            dt = datetime.strptime(raw, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d 00:00:00")
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        return raw


def normalize_existing_db_datetimes(conn: sqlite3.Connection) -> int:
    updated = 0
    cur = conn.cursor()
    tasks = [
        ("raw_items", "id", ["published_at", "fetched_at", "body_fetched_at"]),
        ("alert_queue", "id", ["published_at", "fetched_at", "notified_at", "created_at", "updated_at"]),
    ]

    for table, pk, cols in tasks:
        try:
            rows = cur.execute(f"SELECT {pk}, {', '.join(cols)} FROM {table}").fetchall()
        except sqlite3.OperationalError:
            continue
        for row in rows:
            row_id = row[0]
            changed_pairs: list[tuple[str, str]] = []
            for idx, col in enumerate(cols, start=1):
                old = str(row[idx] or "").strip()
                if not old:
                    continue
                new = normalize_datetime_text(old)
                if new and new != old:
                    changed_pairs.append((col, new))
            if not changed_pairs:
                continue
            set_sql = ", ".join([f"{name}=?" for name, _ in changed_pairs])
            values = [value for _, value in changed_pairs]
            values.append(row_id)
            cur.execute(f"UPDATE {table} SET {set_sql} WHERE {pk}=?", values)
            updated += 1

    if updated > 0:
        conn.commit()
    return updated


def fetch_via_content_fetch_hub(url: str) -> dict[str, Any]:
    target_url = str(url or "").strip()
    if not target_url:
        return {"ok": False, "error": "url-empty"}
    if not CONTENT_FETCH_HUB_CLI_PATH.exists():
        return {"ok": False, "error": f"hub-cli-missing: {CONTENT_FETCH_HUB_CLI_PATH}"}

    cmd = ["python3", str(CONTENT_FETCH_HUB_CLI_PATH), target_url, "--json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=240, check=False)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "hub-failed").strip()
        return {"ok": False, "error": err}

    try:
        data = json.loads(proc.stdout or "{}")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"hub-json-error: {exc}"}

    item = (data.get("results") or [{}])[0]
    if not item.get("ok"):
        return {"ok": False, "error": str(item.get("error") or "hub-empty-result")}
    return item


def log(msg: str) -> None:
    print(f"[{now_ts()}] {msg}", flush=True)


@lru_cache(maxsize=1)
def load_alert_rule_hydration_signals() -> dict[str, Any]:
    default = {
        "source_boosts": {},
        "media_source_penalty_handles": [],
        "strong_keywords": [],
        "deep_value_keywords": [],
    }
    if not ALERT_RULES_PATH.exists():
        return default
    try:
        data = yaml.safe_load(ALERT_RULES_PATH.read_text(encoding="utf-8")) or {}
        rules = data.get("rules") or {}
        return {
            "source_boosts": rules.get("source_boosts") or {},
            "media_source_penalty_handles": rules.get("media_source_penalty_handles") or [],
            "strong_keywords": rules.get("strong_keywords") or [],
            "deep_value_keywords": rules.get("deep_value_keywords") or [],
        }
    except Exception:
        return default


@lru_cache(maxsize=1)
def load_wsl_proxy_env() -> dict[str, str]:
    keys = {
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "no_proxy",
        "NO_PROXY",
    }
    return {key: value for key in keys if (value := os.environ.get(key))}


def load_agent_reach_twitter_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")

    # 让 systemd 服务态也能拿到 WSL 代理环境。
    proxy_env = load_wsl_proxy_env()
    env.update(proxy_env)
    if not env.get("TWITTER_PROXY"):
        env["TWITTER_PROXY"] = (
            env.get("HTTPS_PROXY")
            or env.get("https_proxy")
            or env.get("HTTP_PROXY")
            or env.get("http_proxy")
            or "http://172.27.16.1:7890"
        )

    cfg = root_config()
    credentials = dict(cfg.get("credentials") or {})
    auth_env = str(credentials.get("twitter_auth_token_env") or "TWITTER_AUTH_TOKEN").strip()
    ct0_env = str(credentials.get("twitter_ct0_env") or "TWITTER_CT0").strip()
    if auth_env and os.environ.get(auth_env) and not env.get("TWITTER_AUTH_TOKEN"):
        env["TWITTER_AUTH_TOKEN"] = str(os.environ.get(auth_env) or "")
    if ct0_env and os.environ.get(ct0_env) and not env.get("TWITTER_CT0"):
        env["TWITTER_CT0"] = str(os.environ.get(ct0_env) or "")
    return env


def resolve_twitter_bin() -> str:
    cfg = root_config()
    env_bin = str(os.environ.get("TWITTER_BIN") or ((cfg.get("external_tools") or {}).get("twitter_bin")) or "").strip()
    if env_bin and Path(env_bin).exists():
        return env_bin

    current_python_bin = Path(sys.executable).resolve().parent / "Scripts" / "twitter.exe"
    if current_python_bin.exists():
        return str(current_python_bin)

    # 优先使用 WSL Linux 原生 venv，避免 /mnt/c 上 Python 包导入卡死。
    cmd = which("twitter")
    if cmd:
        return cmd

    raise RuntimeError("未找到 twitter 命令，请安装 twitter-cli 或设置 TWITTER_BIN")


def run_twitter_command(cmd: list[str]) -> dict[str, Any]:
    cmd = [resolve_twitter_bin(), *cmd[1:]]
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=load_agent_reach_twitter_env(),
            timeout=TWITTER_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"twitter-cli 超时（>{TWITTER_TIMEOUT_SEC}s）") from exc

    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout).strip())

    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"twitter-cli 输出不是合法 JSON：{exc}") from exc

    if not payload.get("ok", False):
        error = payload.get("error", {})
        raise RuntimeError(error.get("message") or "twitter-cli 执行失败")

    return payload


def _sync_platform_accounts(conn: sqlite3.Connection, platform: str, items: list[dict[str, Any]]) -> None:
    cur = conn.cursor()
    seen_ids = {str(item.get('id') or '').strip() for item in items if str(item.get('id') or '').strip()}
    for item in items:
        item_id = str(item.get('id') or '').strip()
        if not item_id:
            continue
        cur.execute(
            """
            INSERT INTO accounts(platform, handle, enabled, priority, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(platform, handle) DO UPDATE SET
              enabled=excluded.enabled,
              priority=excluded.priority
            """,
            (platform, item_id, 1 if item.get('enabled', True) else 0, int(item.get('priority') or 5)),
        )
    if seen_ids:
        placeholders = ",".join("?" for _ in seen_ids)
        cur.execute(
            f"UPDATE accounts SET enabled=0 WHERE platform=? AND handle NOT IN ({placeholders})",
            (platform, *tuple(sorted(seen_ids))),
        )
    conn.commit()


def sync_wechat_sources(conn: sqlite3.Connection) -> None:
    _sync_platform_accounts(conn, 'wechat', load_wechat_sources())


def sync_youtube_channels(conn: sqlite3.Connection) -> None:
    _sync_platform_accounts(conn, 'youtube', load_youtube_channels(enabled_only=False))


def sync_feishu_sources(conn: sqlite3.Connection) -> None:
    _sync_platform_accounts(conn, 'feishu', load_feishu_sources(enabled_only=False))


def sync_douyin_sources(conn: sqlite3.Connection) -> None:
    _sync_platform_accounts(conn, 'douyin', load_douyin_sources(enabled_only=False))


def ensure_raw_items_body_queue_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    existing = {str(row[1]) for row in cur.execute("PRAGMA table_info(raw_items)").fetchall()}
    alters: list[str] = []
    if "body_status" not in existing:
        alters.append("ALTER TABLE raw_items ADD COLUMN body_status TEXT DEFAULT 'none'")
    if "body_attempts" not in existing:
        alters.append("ALTER TABLE raw_items ADD COLUMN body_attempts INTEGER NOT NULL DEFAULT 0")
    if "body_error" not in existing:
        alters.append("ALTER TABLE raw_items ADD COLUMN body_error TEXT DEFAULT ''")
    if "body_fetched_at" not in existing:
        alters.append("ALTER TABLE raw_items ADD COLUMN body_fetched_at TEXT")
    if "body_priority" not in existing:
        alters.append("ALTER TABLE raw_items ADD COLUMN body_priority INTEGER NOT NULL DEFAULT 0")
    if "body_next_retry_at" not in existing:
        alters.append("ALTER TABLE raw_items ADD COLUMN body_next_retry_at TEXT")
    for sql in alters:
        cur.execute(sql)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_items_body_status_priority ON raw_items(body_status, body_priority DESC, fetched_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_items_body_retry ON raw_items(body_status, body_next_retry_at, body_priority DESC, fetched_at DESC)")
    if alters:
        conn.commit()


def ensure_tables(conn: sqlite3.Connection) -> None:
    global DB_DATETIME_NORMALIZED
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
          UNIQUE(platform, item_id)
        )
        """
    )
    conn.commit()
    ensure_raw_items_body_queue_columns(conn)

    if not DB_DATETIME_NORMALIZED:
        changed = normalize_existing_db_datetimes(conn)
        if changed > 0:
            log(f"[db] datetime normalized rows={changed}")
        DB_DATETIME_NORMALIZED = True


def get_body_queue_stats(conn: sqlite3.Connection) -> dict[str, int]:
    cur = conn.cursor()
    stats = {
        "body_none": 0,
        "body_pending": 0,
        "body_in_progress": 0,
        "body_success": 0,
        "body_failed": 0,
        "body_timeout": 0,
        "body_skipped": 0,
        "body_retry_waiting": 0,
        "body_due_now": 0,
    }
    try:
        rows = cur.execute(
            "SELECT body_status, COUNT(*) FROM raw_items GROUP BY body_status"
        ).fetchall()
        for status, count in rows:
            key = f"body_{str(status or 'none').strip() or 'none'}"
            if key in stats:
                stats[key] = int(count or 0)
        stats["body_retry_waiting"] = int(
            cur.execute(
                "SELECT COUNT(*) FROM raw_items WHERE body_status='pending' AND body_next_retry_at IS NOT NULL AND body_next_retry_at != '' AND body_next_retry_at > datetime('now')"
            ).fetchone()[0]
        )
        stats["body_due_now"] = int(
            cur.execute(
                "SELECT COUNT(*) FROM raw_items WHERE body_status='pending' AND (body_next_retry_at IS NULL OR body_next_retry_at='' OR body_next_retry_at <= datetime('now'))"
            ).fetchone()[0]
        )
    except Exception:
        pass
    return stats


class SourceCollector(ABC):
    platform: str

    @abstractmethod
    def collect_for_handle(self, handle: str, limit: int) -> list[dict[str, Any]]:
        raise NotImplementedError


class XCollector(SourceCollector):
    platform = "x"

    def collect_for_handle(self, handle: str, limit: int) -> list[dict[str, Any]]:
        payload = run_twitter_command(["twitter", "user-posts", handle, "-n", str(limit), "--json"])
        items = payload.get("data", []) or []
        out: list[dict[str, Any]] = []
        for item in items:
            author = item.get("author") or {}
            screen_name = author.get("screenName") or author.get("username") or handle
            tweet_id = str(item.get("id") or "").strip()
            text = str(item.get("text") or "")
            created_at = item.get("createdAtISO") or item.get("createdAtLocal") or item.get("createdAt") or ""
            if not tweet_id:
                raw_key = f"{screen_name}|{created_at}|{text[:200]}"
                tweet_id = hashlib.md5(raw_key.encode("utf-8")).hexdigest()  # noqa: S324
            url = f"https://x.com/{screen_name}/status/{tweet_id}" if tweet_id else ""
            metrics = item.get("metrics") or {}
            likes = int(metrics.get("likes") or 0)
            retweets = int(metrics.get("retweets") or 0)
            replies = int(metrics.get("replies") or 0)
            quotes = int(metrics.get("quotes") or 0)
            views = int(metrics.get("views") or 0)
            engagement_score = likes * 1.0 + retweets * 2.0 + quotes * 2.0 + replies * 0.8
            metrics_payload = {
                "likes": likes,
                "retweets": retweets,
                "replies": replies,
                "quotes": quotes,
                "views": views,
                "engagement_score": round(engagement_score, 2),
                "lang": (item.get("lang") or "").lower(),
            }
            title = text.replace("\n", " ").strip()[:140]
            out.append(
                {
                    "platform": "x",
                    "source_handle": handle,
                    "item_id": tweet_id,
                    "title": title,
                    "content": text,
                    "url": url,
                    "published_at": str(created_at),
                    "metrics_json": json.dumps(metrics_payload, ensure_ascii=False),
                }
            )
        return out


class YouTubeCollector(SourceCollector):
    platform = "youtube"

    def __init__(self) -> None:
        self.channels = {str(item.get('id') or '').strip(): item for item in load_youtube_channels(enabled_only=False)}

    def collect_for_handle(self, handle: str, limit: int) -> list[dict[str, Any]]:
        channel = self.channels.get(str(handle or '').strip())
        if not channel:
            raise RuntimeError(f"youtube channel not found: {handle}")

        videos = fetch_recent_videos(channel, limit=limit)
        out: list[dict[str, Any]] = []
        for video in videos:
            video_url = str(video.get('url') or '').strip()
            hub_item = fetch_via_content_fetch_hub(video_url) if video_url else {"ok": False, "error": "video-url-empty"}
            transcript_language = ""
            transcript_kind = ""
            transcript_error = str(hub_item.get('error') or '') if not hub_item.get('ok') else ''
            content = ""
            final_title = str(video.get('title') or '').strip()
            fetch_method = str(channel.get('fetch_method') or 'channel_videos')
            transcript_available = False

            if hub_item.get('ok'):
                content = str(hub_item.get('content_markdown') or '').strip()[:20000]
                final_title = str(hub_item.get('title') or final_title).strip()
                transcript_available = bool(content)
                fetch_method = str((hub_item.get('meta') or {}).get('fetch_method') or 'content_fetch_hub')
            else:
                transcript = fetch_transcript(video_url)
                transcript_text = str(transcript.get('text') or '').strip()
                content = transcript_text[:12000]
                transcript_available = bool(transcript.get('available'))
                transcript_language = str(transcript.get('language') or '')
                transcript_kind = str(transcript.get('kind') or '')
                transcript_error = str(transcript.get('error') or transcript_error or '')
                fetch_method = 'yt_dlp_fallback'

            metrics_payload = {
                "channel_id": str(video.get('channel_id') or ''),
                "channel_name": str(video.get('channel_name') or ''),
                "duration_sec": int(video.get('duration_sec') or 0),
                "view_count": int(video.get('view_count') or 0),
                "transcript_available": transcript_available,
                "transcript_language": transcript_language,
                "transcript_kind": transcript_kind,
                "transcript_error": transcript_error,
                "category": str(channel.get('category') or ''),
                "fetch_method": fetch_method,
            }
            content_parts = [
                final_title,
                content,
            ]
            merged_content = "\n\n".join([part for part in content_parts if part]).strip()
            out.append(
                {
                    "platform": "youtube",
                    "source_handle": str(channel.get('name') or handle),
                    "item_id": str(video.get('video_id') or ''),
                    "title": final_title,
                    "content": merged_content,
                    "url": video_url,
                    "published_at": str(video.get('published_at') or '').strip(),
                    "metrics_json": json.dumps(metrics_payload, ensure_ascii=False),
                }
            )
        return out


class DouyinCollector(SourceCollector):
    platform = "douyin"

    def __init__(self) -> None:
        self.sources = {str(item.get('id') or '').strip(): item for item in load_douyin_sources(enabled_only=False)}

    def collect_for_handle(self, handle: str, limit: int) -> list[dict[str, Any]]:
        source = self.sources.get(str(handle or '').strip())
        if not source:
            raise RuntimeError(f"douyin source not found: {handle}")

        videos = fetch_recent_douyin_videos(source, limit=limit)
        out: list[dict[str, Any]] = []
        for video in videos:
            video_url = str(video.get('url') or '').strip()
            hub_item = fetch_via_content_fetch_hub(video_url) if video_url else {"ok": False, "error": "video-url-empty"}
            meta = hub_item.get('meta') or {}
            transcript_language = str(meta.get('transcript_language') or '').strip()
            transcript_kind = str(meta.get('transcript_source') or '').strip()
            transcript_error = str(hub_item.get('error') or '') if not hub_item.get('ok') else ''
            content = ''
            final_title = str(video.get('title') or '').strip() or str(source.get('name') or handle)
            fetch_method = str(source.get('fetch_method') or 'profile_playlist')
            transcript_available = False

            if hub_item.get('ok'):
                content = str(hub_item.get('content_markdown') or '').strip()[:20000]
                final_title = str(hub_item.get('title') or final_title).strip()
                transcript_available = bool(meta.get('transcript_available'))
                fetch_method = str(meta.get('fetch_method') or 'content_fetch_hub')
            else:
                transcript_error = transcript_error or 'content-fetch-hub-failed'

            item_id = str(video.get('video_id') or meta.get('video_id') or '').strip()
            if not item_id:
                raw_key = f"douyin|{video_url}|{final_title}"
                item_id = hashlib.md5(raw_key.encode('utf-8')).hexdigest()  # noqa: S324

            metrics_payload = {
                "source_id": str(source.get('id') or ''),
                "source_name": str(source.get('name') or ''),
                "uploader": str(video.get('uploader') or ''),
                "duration_sec": int(video.get('duration_sec') or 0),
                "view_count": int(video.get('view_count') or 0),
                "transcript_available": transcript_available,
                "transcript_language": transcript_language,
                "transcript_kind": transcript_kind,
                "transcript_error": transcript_error,
                "category": str(source.get('category') or ''),
                "fetch_method": fetch_method,
            }
            content_parts = [
                final_title,
                content,
            ]
            merged_content = "\n\n".join([part for part in content_parts if part]).strip()
            out.append(
                {
                    "platform": "douyin",
                    "source_handle": str(source.get('name') or handle),
                    "item_id": item_id,
                    "title": final_title,
                    "content": merged_content,
                    "url": video_url,
                    "published_at": str(video.get('published_at') or '').strip(),
                    "metrics_json": json.dumps(metrics_payload, ensure_ascii=False),
                }
            )
        return out


def get_collectors() -> dict[str, SourceCollector]:
    # 后续新增渠道时，在这里注册即可（如 feed/github/reddit/youtube）
    return {
        "x": XCollector(),
        "wechat": WeChatCollector(logger=log),
        "youtube": YouTubeCollector(),
        "douyin": DouyinCollector(),
        "feishu": FeishuCollector(logger=log),
    }


def fetch_enabled_accounts(conn: sqlite3.Connection, platform: str) -> list[tuple[str, int]]:
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT handle, priority FROM accounts WHERE platform=? AND enabled=1 ORDER BY priority DESC, id ASC",
        (platform,),
    ).fetchall()
    return [(str(r[0]), int(r[1] or 0)) for r in rows]


def parse_metrics_json_safe(metrics_json: str) -> dict[str, Any]:
    try:
        data = json.loads(metrics_json or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def normalize_text_for_match(*parts: Any) -> str:
    text = " ".join([str(part or "") for part in parts]).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def has_alert_queue_priority(conn: sqlite3.Connection, platform: str, item_id: str) -> tuple[bool, str]:
    if not str(platform or "").strip() or not str(item_id or "").strip():
        return False, "alert-queue-miss"
    row = conn.execute(
        "SELECT recommend_level, notify_status, score FROM alert_queue WHERE platform=? AND item_id=? ORDER BY id DESC LIMIT 1",
        (platform, item_id),
    ).fetchone()
    if not row:
        return False, "alert-queue-miss"
    recommend_level = str(row[0] or "").strip().lower()
    notify_status = str(row[1] or "").strip().lower()
    score = float(row[2] or 0.0)
    if notify_status == "candidate":
        return True, f"alert-candidate:{recommend_level or 'unknown'}"
    if recommend_level == "strong":
        return True, f"alert-strong:{notify_status or 'unknown'}"
    if score >= 8.5:
        return True, f"alert-score:{score:.1f}"
    return False, "alert-low-priority"


def compute_body_candidate(conn: sqlite3.Connection, platform: str, source_handle: str, item_id: str, title: str, url: str, metrics: dict[str, Any]) -> tuple[bool, int, str]:
    if platform != "wechat":
        return False, 0, "platform-not-supported"
    if not str(url or "").strip():
        return False, 0, "url-empty"
    if not bool(metrics.get("metadata_only") or metrics.get("body_fetch_skipped")):
        return False, 0, "already-hydrated-or-not-metadata-only"

    alert_priority, alert_reason = has_alert_queue_priority(conn, platform, item_id)
    if alert_priority:
        return True, 320, alert_reason

    signals = load_alert_rule_hydration_signals()
    source_boosts = signals.get("source_boosts") or {}
    penalty_handles = {str(x) for x in (signals.get("media_source_penalty_handles") or [])}
    strong_keywords = [str(x).lower() for x in (signals.get("strong_keywords") or []) if str(x).strip()]
    deep_keywords = [str(x).lower() for x in (signals.get("deep_value_keywords") or []) if str(x).strip()]

    haystack = normalize_text_for_match(title, source_handle, metrics.get("category"))
    matched_strong = [kw for kw in strong_keywords if kw in haystack]
    matched_deep = [kw for kw in deep_keywords if kw in haystack]
    source_boost = float(source_boosts.get(source_handle, 0.0) or 0.0)
    penalized_media = source_handle in penalty_handles

    if matched_deep:
        return True, 260, f"deep-keyword:{matched_deep[0]}"
    if matched_strong and source_boost > 0:
        return True, 240, f"strong+boost:{matched_strong[0]}"
    if matched_strong and not penalized_media:
        return True, 220, f"strong-keyword:{matched_strong[0]}"
    if source_boost >= 1.5 and not penalized_media:
        return True, 180, f"boosted-source:{source_handle}"
    return False, 0, "low-signal"


def next_body_status(existing_status: str, wants_body_hydration: bool) -> str:
    status = str(existing_status or "").strip().lower()
    if not wants_body_hydration:
        return status or "none"
    if status in {"success", "pending", "in_progress", "failed", "timeout", "skipped"}:
        return status
    return "pending"


def normalize_body_queue_states(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, platform, source_handle, item_id, title, url, metrics_json, body_status FROM raw_items WHERE platform='wechat' AND body_status IN ('pending','none')"
    ).fetchall()
    changed = 0
    for row in rows:
        row_id, platform, source_handle, item_id, title, url, metrics_json, current_status = row
        metrics = parse_metrics_json_safe(str(metrics_json or "{}"))
        should_enqueue, priority, reason = compute_body_candidate(
            conn,
            str(platform or ""),
            str(source_handle or ""),
            str(item_id or ""),
            str(title or ""),
            str(url or ""),
            metrics,
        )
        desired_status = 'pending' if should_enqueue else 'none'
        metrics['body_candidate_reason'] = reason
        metrics_json_new = json.dumps(metrics, ensure_ascii=False)
        if str(current_status or '') == desired_status:
            cur.execute(
                "UPDATE raw_items SET body_priority=?, metrics_json=? WHERE id=? AND (COALESCE(body_priority,0)<>? OR COALESCE(metrics_json,'')<>?)",
                (priority, metrics_json_new, row_id, priority, metrics_json_new),
            )
            changed += cur.rowcount
            continue
        cur.execute("UPDATE raw_items SET body_status=?, body_priority=?, metrics_json=? WHERE id=?", (desired_status, priority, metrics_json_new, row_id))
        changed += cur.rowcount
    if changed > 0:
        conn.commit()
    return changed


def upsert_raw_item(conn: sqlite3.Connection, item: dict[str, Any]) -> str:
    cur = conn.cursor()
    platform = item.get("platform") or ""
    source_handle = item.get("source_handle") or ""
    item_id = item.get("item_id") or ""
    title = item.get("title") or ""
    content = item.get("content") or ""
    url = item.get("url") or ""
    published_at = normalize_datetime_text(item.get("published_at") or "")
    metrics_json = item.get("metrics_json") or "{}"
    metrics = parse_metrics_json_safe(metrics_json)
    wants_body_hydration, body_priority, candidate_reason = compute_body_candidate(conn, platform, source_handle, item_id, title, url, metrics)
    metrics["body_candidate_reason"] = candidate_reason
    metrics_json = json.dumps(metrics, ensure_ascii=False)
    initial_body_status = "pending" if wants_body_hydration else "none"

    cur.execute(
        """
        INSERT OR IGNORE INTO raw_items(
          platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at,
          body_status, body_attempts, body_error, body_fetched_at, body_priority
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, 0, '', NULL, ?)
        """,
        (
            platform,
            source_handle,
            item_id,
            title,
            content,
            url,
            published_at,
            metrics_json,
            initial_body_status,
            body_priority,
        ),
    )
    if cur.rowcount > 0:
        return "inserted"

    existing_row = cur.execute(
        "SELECT content, body_status, body_priority FROM raw_items WHERE platform=? AND item_id=?",
        (platform, item_id),
    ).fetchone()
    existing_content = str(existing_row[0] or "") if existing_row else ""
    existing_body_status = str(existing_row[1] or "") if existing_row else ""
    existing_body_priority = int(existing_row[2] or 0) if existing_row else 0
    next_status = next_body_status(existing_body_status, wants_body_hydration)
    next_priority = max(existing_body_priority, body_priority)

    content_to_store = content
    if wants_body_hydration and existing_body_status == "success" and existing_content:
        content_to_store = existing_content

    cur.execute(
        """
        UPDATE raw_items
        SET source_handle=?, title=?, content=?, url=?, published_at=?, metrics_json=?, fetched_at=datetime('now'),
            body_status=?, body_priority=?
        WHERE platform=? AND item_id=?
        """,
        (
            source_handle,
            title,
            content_to_store,
            url,
            published_at,
            metrics_json,
            next_status,
            next_priority,
            platform,
            item_id,
        ),
    )
    if cur.rowcount > 0:
        return "refreshed"
    return "noop"


def collect_once(fetch_limit: int = DEFAULT_FETCH_LIMIT, platforms: list[str] | None = None) -> dict[str, Any]:
    started_at = time.time()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = configure_sqlite_connection(sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT_SEC))
    try:
        ensure_tables(conn)
        sync_wechat_sources(conn)
        sync_youtube_channels(conn)
        sync_douyin_sources(conn)
        sync_feishu_sources(conn)
        if BODY_QUEUE_NORMALIZE_ON_COLLECT:
            normalized_queue_rows = normalize_body_queue_states(conn)
            if normalized_queue_rows > 0:
                log(f"[body-queue] normalized rows={normalized_queue_rows}")
        collectors = get_collectors()
        target_platforms = platforms or list(collectors.keys())

        inserted_total = 0
        refreshed_total = 0
        fetched_total = 0
        account_total = 0
        errors: list[str] = []

        for platform in target_platforms:
            collector = collectors.get(platform)
            if not collector:
                log(f"[skip] 平台 {platform} 暂未注册采集器")
                continue

            accounts = fetch_enabled_accounts(conn, platform)
            account_total += len(accounts)
            log(f"[{platform}] 启用账号 {len(accounts)} 个")

            for idx, (handle, _) in enumerate(accounts, start=1):
                try:
                    items = collector.collect_for_handle(handle, fetch_limit)
                    fetched_total += len(items)
                    inserted = 0
                    refreshed = 0
                    for item in items:
                        status = upsert_raw_item(conn, item)
                        if status == "inserted":
                            inserted += 1
                            inserted_total += 1
                        elif status == "refreshed":
                            refreshed += 1
                            refreshed_total += 1
                    conn.commit()
                    log(f"[{platform}] {idx}/{len(accounts)} @{handle}: fetched={len(items)} inserted={inserted} refreshed={refreshed}")
                except Exception as exc:  # noqa: BLE001
                    msg = f"[{platform}] @{handle} 失败: {exc}"
                    errors.append(msg)
                    log(msg)

        body_stats = get_body_queue_stats(conn)
        duration_sec = round(time.time() - started_at, 2)
        summary = {
            "ok": True,
            "service": "collector",
            "platforms": ",".join(target_platforms),
            "account_total": account_total,
            "fetched_total": fetched_total,
            "inserted_total": inserted_total,
            "refreshed_total": refreshed_total,
            "error_count": len(errors),
            "duration_sec": duration_sec,
            **body_stats,
            "errors": errors,
        }
        if inserted_total > 0 or refreshed_total > 0:
            summary["mirror"] = mirror_event_radar_db(DB_PATH, logger=log)
        log(
            f"[summary] service=collector platforms={summary['platforms']} duration_sec={duration_sec} accounts={account_total} fetched={fetched_total} inserted={inserted_total} refreshed={refreshed_total} errors={len(errors)} body_due_now={body_stats['body_due_now']} body_retry_waiting={body_stats['body_retry_waiting']} body_in_progress={body_stats['body_in_progress']} body_success={body_stats['body_success']}"
        )
        return summary
    finally:
        conn.close()


def run_daemon(min_sec: int, max_sec: int, fetch_limit: int, platforms: list[str] | None = None) -> None:
    if min_sec < 1 or max_sec < 1 or min_sec > max_sec:
        raise ValueError("间隔参数不合法")

    log(f"collector daemon started: interval={min_sec}-{max_sec}s fetch_limit={fetch_limit} platforms={platforms or list(get_collectors().keys())}")
    while True:
        try:
            collect_once(fetch_limit=fetch_limit, platforms=platforms)
        except Exception as exc:  # noqa: BLE001
            log(f"[fatal-loop-error] {exc}")

        sleep_sec = random.randint(min_sec, max_sec)
        log(f"sleep {sleep_sec}s")
        time.sleep(sleep_sec)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="只执行一轮采集")
    parser.add_argument("--min-sec", type=int, default=600, help="最小轮询间隔（秒）")
    parser.add_argument("--max-sec", type=int, default=900, help="最大轮询间隔（秒）")
    parser.add_argument("--fetch-limit", type=int, default=DEFAULT_FETCH_LIMIT, help="每账号每轮抓取条数")
    parser.add_argument("--platforms", nargs="*", default=[], help="采集平台列表，默认全部已注册平台")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target_platforms = args.platforms or None
    if args.once:
        collect_once(fetch_limit=args.fetch_limit, platforms=target_platforms)
        return
    run_daemon(args.min_sec, args.max_sec, args.fetch_limit, platforms=target_platforms)


if __name__ == "__main__":
    main()
