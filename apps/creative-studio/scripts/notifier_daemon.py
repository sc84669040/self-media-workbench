#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import sqlite3
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from runtime_config import config as root_config, event_radar_db_path, sample_vault_path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = event_radar_db_path()
ALERT_RULES_PATH = BASE_DIR / "config" / "alert-rules.yaml"
UTC = timezone.utc
DB_TIMEOUT_SEC = int(os.environ.get("EVENT_RADAR_DB_TIMEOUT_SEC", "30"))
SQLITE_BUSY_TIMEOUT_MS = max(1000, DB_TIMEOUT_SEC * 1000)


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


def log(msg: str) -> None:
    print(f"[{now_ts()}] {msg}", flush=True)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
        if "T" in raw or "+" in raw:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except Exception:
        return None


def clean_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[@#]\w+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_fingerprint(text: str) -> str:
    text = clean_text(text).lower()
    tokens = re.findall(r"[\u4e00-\u9fff]{2,}|[a-z0-9]+", text)
    tokens = [t for t in tokens if len(t) >= 2][:12]
    base = " ".join(tokens)[:120] or text[:120]
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def slugify_filename(value: str) -> str:
    value = unicodedata.normalize("NFKC", str(value or "")).strip()
    value = re.sub(r"[\\/:*?\"<>|]", "-", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:80] or "untitled"


@dataclass
class TelegramConfig:
    enabled: bool
    chat_id: str
    bot_token: str
    account_id: str
    openclaw_config: str
    proxy: str


@dataclass
class KnowledgeBaseConfig:
    enabled: bool
    vault_path: str
    folder: str


@dataclass
class RuleConfig:
    strong_threshold: float
    watch_threshold: float
    bootstrap_skip_older_than_hours: int
    notify_cooldown_minutes: int
    cluster_window_minutes: int
    scan_limit: int
    send_limit_per_cycle: int
    failed_retry_limit: int
    strong_keywords: list[str]
    deep_value_keywords: list[str]
    short_term_keywords: list[str]
    source_boosts: dict[str, float]
    no_strong_news_keywords: list[str]
    media_source_penalty_handles: list[str]
    media_source_penalty_score: float
    min_title_len_for_strong: int
    min_text_len_for_strong: int


@dataclass
class AlertDecision:
    recommend_level: str
    score: float
    reasons: list[str]
    fingerprint: str


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_rules() -> tuple[TelegramConfig, KnowledgeBaseConfig, RuleConfig]:
    data = yaml.safe_load(ALERT_RULES_PATH.read_text(encoding="utf-8")) or {}
    data = deep_merge(data, dict(root_config().get("notifications") or {}))
    tg = data.get("telegram") or {}
    kb = data.get("knowledge_base") or {}
    rules = data.get("rules") or {}

    root = root_config()
    token_env = str((root.get("credentials") or {}).get("telegram_bot_token_env") or "NIGHTHAWK_TELEGRAM_BOT_TOKEN").strip()
    bot_token = os.environ.get(token_env, "").strip()
    account_id = str(tg.get("account_id") or "").strip()
    openclaw_config = str(tg.get("openclaw_config") or "").strip()
    proxy_env = str(tg.get("proxy_env") or "NIGHTHAWK_TELEGRAM_PROXY").strip()
    proxy = os.environ.get(proxy_env, "").strip()
    if openclaw_config and Path(openclaw_config).exists():
        try:
            obj = json.loads(Path(openclaw_config).read_text(encoding="utf-8-sig"))
            channels = obj.get("channels") or {}
            telegram = channels.get("telegram") or {}
            accounts = telegram.get("accounts") or {}
            account = accounts.get(account_id) or accounts.get(telegram.get("defaultAccount") or "") or {}
            if not bot_token:
                bot_token = str(account.get("botToken") or "").strip()
            if not proxy:
                proxy = str(telegram.get("proxy") or "").strip()
        except Exception as exc:  # noqa: BLE001
            log(f"[warn] load telegram config failed: {exc}")

    return (
        TelegramConfig(
            enabled=bool(tg.get("enabled", True)),
            chat_id=str(tg.get("chat_id") or "").strip(),
            bot_token=bot_token,
            account_id=account_id,
            openclaw_config=openclaw_config,
            proxy=proxy,
        ),
        KnowledgeBaseConfig(
            enabled=bool(kb.get("enabled", True)),
            vault_path=str(kb.get("vault_path") or sample_vault_path()).strip(),
            folder=str(kb.get("folder") or "强推荐").strip() or "强推荐",
        ),
        RuleConfig(
            strong_threshold=float(rules.get("strong_threshold", 8.5)),
            watch_threshold=float(rules.get("watch_threshold", 5.0)),
            bootstrap_skip_older_than_hours=int(rules.get("bootstrap_skip_older_than_hours", 6)),
            notify_cooldown_minutes=int(rules.get("notify_cooldown_minutes", 10)),
            cluster_window_minutes=int(rules.get("cluster_window_minutes", 180)),
            scan_limit=int(rules.get("scan_limit", 100)),
            send_limit_per_cycle=int(rules.get("send_limit_per_cycle", 1)),
            failed_retry_limit=int(rules.get("failed_retry_limit", 3)),
            strong_keywords=[str(x).strip() for x in (rules.get("strong_keywords") or []) if str(x).strip()],
            deep_value_keywords=[str(x).strip() for x in (rules.get("deep_value_keywords") or []) if str(x).strip()],
            short_term_keywords=[str(x).strip() for x in (rules.get("short_term_keywords") or []) if str(x).strip()],
            source_boosts={str(k): float(v) for k, v in (rules.get("source_boosts") or {}).items()},
            no_strong_news_keywords=[str(x).strip() for x in (rules.get("no_strong_news_keywords") or []) if str(x).strip()],
            media_source_penalty_handles=[str(x).strip() for x in (rules.get("media_source_penalty_handles") or []) if str(x).strip()],
            media_source_penalty_score=float(rules.get("media_source_penalty_score", 1.6)),
            min_title_len_for_strong=int(rules.get("min_title_len_for_strong", 20)),
            min_text_len_for_strong=int(rules.get("min_text_len_for_strong", 140)),
        ),
    )


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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_runtime (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    try:
        cur.execute("ALTER TABLE alert_queue ADD COLUMN obsidian_note_path TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE alert_queue ADD COLUMN obsidian_note_title TEXT DEFAULT ''")
    except Exception:
        pass
    conn.commit()


def get_runtime_value(conn: sqlite3.Connection, key: str) -> str:
    cur = conn.cursor()
    row = cur.execute("SELECT value FROM alert_runtime WHERE key=?", (key,)).fetchone()
    return str((row[0] if row else "") or "")


def set_runtime_value(conn: sqlite3.Connection, key: str, value: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alert_runtime(key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
        """,
        (key, value),
    )
    conn.commit()


def get_bootstrap_raw_id(conn: sqlite3.Connection) -> int:
    existing = get_runtime_value(conn, "bootstrap_raw_id")
    if existing:
        try:
            return int(existing)
        except Exception:
            pass
    cur = conn.cursor()
    max_raw_id = int((cur.execute("SELECT COALESCE(MAX(id), 0) FROM raw_items").fetchone()[0]) or 0)
    set_runtime_value(conn, "bootstrap_raw_id", str(max_raw_id))
    log(f"[bootstrap] set bootstrap_raw_id={max_raw_id}")
    return max_raw_id


def get_account_priority(conn: sqlite3.Connection, platform: str, handle: str) -> int:
    cur = conn.cursor()
    row = cur.execute(
        "SELECT priority FROM accounts WHERE platform=? AND LOWER(handle)=LOWER(?) LIMIT 1",
        (platform, handle),
    ).fetchone()
    return int((row[0] if row else 0) or 0)


def compute_score(item: sqlite3.Row, conn: sqlite3.Connection, rules: RuleConfig) -> AlertDecision:
    platform = str(item["platform"] or "").strip().lower()
    title = str(item["title"] or "")
    content = str(item["content"] or "")
    source_handle = str(item["source_handle"] or "")
    text = f"{title}\n{content}"
    text_lower = text.lower()
    title_lower = title.lower()
    cleaned_title = clean_text(title)
    cleaned_text = clean_text(text)
    reasons: list[str] = []
    score = 0.0
    force_no_strong = False

    priority = get_account_priority(conn, platform, source_handle)
    if priority >= 9:
        score += 3.0
        reasons.append(f"高优先账号(priority={priority})")
    elif priority >= 7:
        score += 2.0
        reasons.append(f"重点账号(priority={priority})")
    elif priority >= 5:
        score += 1.0

    source_boost = float(rules.source_boosts.get(source_handle, 0.0))
    if source_boost > 0:
        score += source_boost
        reasons.append(f"重点来源(@{source_handle})")

    if source_handle in set(rules.media_source_penalty_handles):
        score -= max(0.0, rules.media_source_penalty_score)
        reasons.append(f"媒体账号降权(@{source_handle})")

    published_at = parse_dt(str(item["published_at"] or "")) or parse_dt(str(item["fetched_at"] or ""))
    if published_at is not None:
        age_hours = (datetime.now(UTC) - published_at).total_seconds() / 3600
        if age_hours <= 6:
            score += 2.5
            reasons.append("强时效(<=6h)")
        elif age_hours <= 24:
            score += 1.5
            reasons.append("近期内容(<=24h)")

    hit_keywords = [kw for kw in rules.strong_keywords if kw.lower() in text_lower][:3]
    if hit_keywords:
        score += min(4.0, 1.5 * len(hit_keywords))
        reasons.append("关键词命中：" + "、".join(hit_keywords))

    deep_hits = [kw for kw in rules.deep_value_keywords if kw.lower() in text_lower][:3]
    if deep_hits:
        score += min(3.5, 1.6 * len(deep_hits))
        reasons.append("深度分析加权：" + "、".join(deep_hits))

    short_hits = [kw for kw in rules.short_term_keywords if kw.lower() in text_lower][:3]
    if short_hits:
        score -= min(3.0, 1.2 * len(short_hits))
        reasons.append("短期快讯降权：" + "、".join(short_hits))

    no_strong_news_hits = [kw for kw in rules.no_strong_news_keywords if kw.lower() in text_lower][:3]
    if no_strong_news_hits:
        score -= 2.2
        force_no_strong = True
        reasons.append("时效新闻降级（不进强推荐）：" + "、".join(no_strong_news_hits))

    metrics = {}
    try:
        metrics = json.loads(str(item["metrics_json"] or "{}")) or {}
    except Exception:
        metrics = {}
    engagement = float(metrics.get("engagement_score") or 0)
    likes = int(metrics.get("likes") or 0)
    retweets = int(metrics.get("retweets") or 0)
    views = int(metrics.get("views") or 0)
    if engagement >= 1500 or likes >= 1000 or views >= 50000:
        score += 1.8
        reasons.append("互动显著偏高")
    elif engagement >= 500 or likes >= 300 or views >= 10000:
        score += 1.0
        reasons.append("互动较高")
    elif engagement >= 150 or likes >= 100:
        score += 0.5
        reasons.append("互动有热度")
    if retweets >= 80:
        score += 0.6
        reasons.append("转发传播明显")

    if len(cleaned_title) < max(1, rules.min_title_len_for_strong):
        score -= 1.8
        force_no_strong = True
        reasons.append(f"标题过短({len(cleaned_title)}<{rules.min_title_len_for_strong})")

    if len(cleaned_text) < max(1, rules.min_text_len_for_strong):
        score -= 1.2
        force_no_strong = True
        reasons.append(f"内容过短({len(cleaned_text)}<{rules.min_text_len_for_strong})")

    if platform == "wechat":
        body_fetch_ok = bool(metrics.get("body_fetch_ok"))
        source_kind = str(metrics.get("source_kind") or "").strip().lower()
        category = str(metrics.get("category") or "").strip().lower()
        content_len = len(clean_text(content))

        if body_fetch_ok:
            score += 0.6
            reasons.append("正文抓取成功")
        if source_kind in {"official_feed", "official_atom"}:
            score += 0.4
            reasons.append("稳定来源")
        if category in {"ai_analysis", "tech_analysis"}:
            score += 0.8
            reasons.append("分析类来源加权")
        elif category in {"ai_news", "tech_news"}:
            score += 0.3

        if content_len >= 2500:
            score += 1.2
            reasons.append("长文深度加权")
        elif content_len >= 1200:
            score += 0.7
            reasons.append("正文较完整")
        elif content_len < 180:
            score -= 1.0
            reasons.append("正文偏短降权")

        if any(keyword in title_lower for keyword in ["早报", "日报", "周报", "速递", "简报", "快讯"]):
            score -= 0.8
            reasons.append("资讯汇总型轻降权")
        if any(keyword in title_lower for keyword in ["拆解", "复盘", "实测", "对比", "详解", "深度"]):
            score += 0.8
            reasons.append("深度标题加权")

    fingerprint = normalize_fingerprint(text)
    if score >= rules.strong_threshold:
        level = "strong"
    elif score >= rules.watch_threshold:
        level = "watch"
    else:
        level = "skip"

    if force_no_strong and level == "strong":
        level = "watch"
    return AlertDecision(level, round(score, 2), reasons or ["未命中强推荐规则"], fingerprint)


def should_skip_bootstrap(item: sqlite3.Row, rules: RuleConfig) -> bool:
    baseline = parse_dt(str(item["fetched_at"] or "")) or parse_dt(str(item["published_at"] or ""))
    if baseline is None:
        return False
    age_hours = (datetime.now(UTC) - baseline).total_seconds() / 3600
    return age_hours > rules.bootstrap_skip_older_than_hours


def recent_duplicate_exists(conn: sqlite3.Connection, fingerprint: str, rules: RuleConfig) -> bool:
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT 1
        FROM alert_queue
        WHERE event_fingerprint=?
          AND notify_status IN ('pending', 'sent', 'suppressed')
          AND created_at >= datetime('now', ?)
        LIMIT 1
        """,
        (fingerprint, f"-{rules.cluster_window_minutes} minutes"),
    ).fetchone()
    return row is not None


def enqueue_new_candidates(conn: sqlite3.Connection, rules: RuleConfig) -> dict[str, int]:
    cur = conn.cursor()
    bootstrap_raw_id = get_bootstrap_raw_id(conn)
    rows = cur.execute(
        """
        SELECT r.id, r.platform, r.source_handle, r.item_id, r.title, r.content, r.url, r.published_at, r.fetched_at, r.metrics_json
        FROM raw_items r
        LEFT JOIN alert_queue q ON q.platform = r.platform AND q.item_id = r.item_id
        WHERE q.id IS NULL AND r.id > ?
        ORDER BY r.id ASC
        LIMIT ?
        """,
        (bootstrap_raw_id, rules.scan_limit),
    ).fetchall()
    inserted = pending = candidate = skipped = suppressed = 0
    max_seen_raw_id = bootstrap_raw_id
    for row in rows:
        decision = compute_score(row, conn, rules)
        notify_status = "skipped"
        notify_error = ""
        if decision.recommend_level == "strong":
            if should_skip_bootstrap(row, rules):
                notify_error = "bootstrap_history_skip"
            elif recent_duplicate_exists(conn, decision.fingerprint, rules):
                notify_status = "suppressed"
                notify_error = "cluster_dedup"
                suppressed += 1
            else:
                notify_status = "pending"
                pending += 1
        elif decision.recommend_level == "watch":
            notify_status = "candidate"
            candidate += 1
        else:
            skipped += 1
        max_seen_raw_id = max(max_seen_raw_id, int(row["id"]))
        cur.execute(
            """
            INSERT INTO alert_queue(
              platform, item_id, source_handle, title, url, published_at, fetched_at,
              recommend_level, score, reason_json, event_fingerprint, notify_status, notify_error,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            (
                row["platform"], row["item_id"], row["source_handle"], row["title"], row["url"],
                row["published_at"], row["fetched_at"], decision.recommend_level, decision.score,
                json.dumps({"reasons": decision.reasons}, ensure_ascii=False), decision.fingerprint,
                notify_status, notify_error,
            ),
        )
        inserted += 1
    if max_seen_raw_id > bootstrap_raw_id:
        set_runtime_value(conn, "bootstrap_raw_id", str(max_seen_raw_id))
    conn.commit()
    return {
        "scanned": len(rows),
        "inserted": inserted,
        "pending": pending,
        "candidate": candidate,
        "skipped": skipped,
        "suppressed": suppressed,
    }


def telegram_enabled(tg: TelegramConfig) -> bool:
    return bool(tg.enabled and tg.chat_id and tg.bot_token)


def format_source_label(platform: str, source_handle: str) -> str:
    platform_norm = str(platform or "").strip().lower()
    handle = str(source_handle or "").strip()
    if not handle:
        return "(未知来源)"
    if platform_norm == "x":
        return f"@{handle}"
    return handle


def format_message(row: sqlite3.Row) -> str:
    reasons = []
    try:
        payload = json.loads(str(row["reason_json"] or "{}")) or {}
        reasons = payload.get("reasons") or []
    except Exception:
        reasons = []
    reason_text = "；".join(reasons[:3]) if reasons else "命中强推荐规则"
    source_label = format_source_label(str(row["platform"] or ""), str(row["source_handle"] or ""))
    parts = [
        "【夜鹰强推荐】",
        f"来源：{source_label}",
        f"评分：{row['score']}",
        f"标题：{row['title'] or '(无标题)'}",
        f"理由：{reason_text}",
    ]
    if row["published_at"]:
        parts.append(f"发布时间：{row['published_at']}")
    if row["url"]:
        parts.append(f"链接：{row['url']}")
    return "\n".join(parts)


def save_to_knowledge_base(kb: KnowledgeBaseConfig, row: sqlite3.Row) -> tuple[str, str]:
    if not kb.enabled:
        return "", ""
    vault = Path(kb.vault_path)
    folder = vault / kb.folder
    folder.mkdir(parents=True, exist_ok=True)
    title = str(row["title"] or row["source_handle"] or row["item_id"] or "强推荐").strip()
    source_label = format_source_label(str(row["platform"] or ""), str(row["source_handle"] or ""))
    note_title = f"[{str(row['platform']).upper()}] {source_label} - {slugify_filename(title)}"
    filename = f"{slugify_filename(note_title)}.md"
    path = folder / filename
    reasons = []
    try:
        payload = json.loads(str(row["reason_json"] or "{}")) or {}
        reasons = payload.get("reasons") or []
    except Exception:
        reasons = []
    body_lines = [
        "---",
        f"source_handle: {row['source_handle']}",
        f"platform: {row['platform']}",
        f"item_id: {row['item_id']}",
        f"score: {row['score']}",
        f"recommend_level: {row['recommend_level']}",
        f"published_at: {row['published_at'] or ''}",
        f"notified_at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"url: {row['url'] or ''}",
        "tags:",
        "  - 强推荐",
        "  - NightHawk",
        "---",
        "",
        f"# {title}",
        "",
        f"- 来源：{source_label}",
        f"- 平台：{row['platform']}",
        f"- 评分：{row['score']}",
        f"- 发布时间：{row['published_at'] or '-'}",
        f"- 链接：{row['url'] or '-'}",
        "",
        "## 推荐理由",
        "",
    ]
    if reasons:
        body_lines.extend([f"- {reason}" for reason in reasons])
    else:
        body_lines.append("- 命中强推荐规则")
    body_lines.extend([
        "",
        "## 正文",
        "",
        str(row["title"] or ""),
        "",
        str(row["content"] or ""),
        "",
    ])
    path.write_text("\n".join(body_lines), encoding="utf-8")
    return str(path), note_title


def _urlopen_ipv4_first(req: urllib.request.Request, timeout: int = 20, proxy: str = "") -> Any:
    # 某些环境会优先走 IPv6，若本机无 IPv6 出口会出现 [Errno 101] Network is unreachable。
    # 这里在调用 Telegram API 时优先使用 IPv4 解析；若配置了代理则复用代理，避免直连超时。
    original_getaddrinfo = socket.getaddrinfo

    def _patched_getaddrinfo(host: str, *args: Any, **kwargs: Any):
        result = original_getaddrinfo(host, *args, **kwargs)
        if host == "api.telegram.org":
            ipv4 = [item for item in result if item and item[0] == socket.AF_INET]
            return ipv4 or result
        return result

    socket.getaddrinfo = _patched_getaddrinfo
    try:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy}) if proxy else urllib.request.ProxyHandler({})
        )
        return opener.open(req, timeout=timeout)
    finally:
        socket.getaddrinfo = original_getaddrinfo


def send_telegram_message(tg: TelegramConfig, text: str) -> dict[str, Any]:
    data = urllib.parse.urlencode({
        "chat_id": tg.chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{tg.bot_token}/sendMessage",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            with _urlopen_ipv4_first(req, timeout=20, proxy=tg.proxy) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if payload.get("ok") is False:
                raise RuntimeError(f"telegram_api_error: {payload}")
            return payload
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            time.sleep(min(3, attempt))
            continue

    raise RuntimeError(f"telegram_send_failed_after_retries: {last_error}")


def recent_send_exists(conn: sqlite3.Connection, rules: RuleConfig) -> bool:
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT 1 FROM alert_queue
        WHERE notify_status='sent' AND notified_at >= datetime('now', ?)
        LIMIT 1
        """,
        (f"-{rules.notify_cooldown_minutes} minutes",),
    ).fetchone()
    return row is not None


def normalize_queue_states(conn: sqlite3.Connection, rules: RuleConfig) -> dict[str, int]:
    cur = conn.cursor()
    migrated_candidates = cur.execute(
        """
        UPDATE alert_queue
        SET notify_status='candidate', updated_at=datetime('now')
        WHERE recommend_level='watch' AND notify_status='skipped'
        """
    ).rowcount
    retried_failed = cur.execute(
        """
        UPDATE alert_queue
        SET notify_status='pending', notify_error='retry_scheduled', updated_at=datetime('now')
        WHERE recommend_level='strong'
          AND notify_status='failed'
          AND notify_attempts < ?
          AND updated_at <= datetime('now', ?)
        """,
        (max(1, rules.failed_retry_limit), f"-{rules.notify_cooldown_minutes} minutes"),
    ).rowcount
    conn.commit()
    return {"migrated_candidates": migrated_candidates, "retried_failed": retried_failed}


def backfill_strong_notes(conn: sqlite3.Connection, kb: KnowledgeBaseConfig, limit: int = 50) -> dict[str, int]:
    if not kb.enabled:
        return {"backfilled_notes": 0}

    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT q.*, r.content
        FROM alert_queue q
        LEFT JOIN raw_items r ON r.platform=q.platform AND r.item_id=q.item_id
        WHERE q.recommend_level='strong'
          AND (q.obsidian_note_path='' OR q.obsidian_note_path IS NULL)
        ORDER BY q.created_at ASC, q.id ASC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()

    backfilled = 0
    for row in rows:
        try:
            note_path, note_title = save_to_knowledge_base(kb, row)
            cur.execute(
                "UPDATE alert_queue SET obsidian_note_path=?, obsidian_note_title=?, updated_at=datetime('now') WHERE id=?",
                (note_path, note_title, row["id"]),
            )
            backfilled += 1
        except Exception as exc:  # noqa: BLE001
            log(f"[kb] backfill failed item={row['item_id']}: {exc}")

    conn.commit()
    return {"backfilled_notes": backfilled}


def _current_summary_slot(now_local: datetime | None = None) -> str:
    now_local = now_local or datetime.now()
    hour = now_local.hour
    if hour == 10:
        return now_local.strftime("%Y-%m-%d-10")
    if hour == 22:
        return now_local.strftime("%Y-%m-%d-22")
    return ""


def should_send_scheduled_summary(conn: sqlite3.Connection, now_local: datetime | None = None) -> tuple[bool, str]:
    slot = _current_summary_slot(now_local)
    if not slot:
        return False, ""
    last_slot = get_runtime_value(conn, "summary_last_slot")
    if last_slot == slot:
        return False, slot
    return True, slot


def collect_12h_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    cur = conn.cursor()
    platform_rows = cur.execute(
        """
        SELECT platform, COUNT(*)
        FROM raw_items
        WHERE fetched_at >= datetime('now', '-12 hours')
        GROUP BY platform
        ORDER BY COUNT(*) DESC, platform ASC
        """
    ).fetchall()
    platform_counts = {str(row[0] or "unknown"): int(row[1] or 0) for row in platform_rows}
    alert_rows = cur.execute(
        """
        SELECT recommend_level, COUNT(*)
        FROM alert_queue
        WHERE created_at >= datetime('now', '-12 hours')
        GROUP BY recommend_level
        """
    ).fetchall()
    alert_counts = {str(row[0] or "unknown"): int(row[1] or 0) for row in alert_rows}
    status_rows = cur.execute(
        """
        SELECT notify_status, COUNT(*)
        FROM alert_queue
        WHERE created_at >= datetime('now', '-12 hours')
        GROUP BY notify_status
        """
    ).fetchall()
    status_counts = {str(row[0] or "unknown"): int(row[1] or 0) for row in status_rows}
    body_rows = cur.execute(
        """
        SELECT body_status, COUNT(*)
        FROM raw_items
        WHERE fetched_at >= datetime('now', '-12 hours')
        GROUP BY body_status
        """
    ).fetchall()
    body_counts = {str(row[0] or "none"): int(row[1] or 0) for row in body_rows}
    total_fetched = int(cur.execute("SELECT COUNT(*) FROM raw_items WHERE fetched_at >= datetime('now', '-12 hours')").fetchone()[0])
    return {
        "total_fetched": total_fetched,
        "platform_counts": platform_counts,
        "strong_count": int(alert_counts.get("strong", 0)),
        "watch_count": int(alert_counts.get("watch", 0)),
        "skip_count": int(alert_counts.get("skip", 0)),
        "pending_count": int(status_counts.get("pending", 0)),
        "candidate_count": int(status_counts.get("candidate", 0)),
        "suppressed_count": int(status_counts.get("suppressed", 0)),
        "failed_count": int(status_counts.get("failed", 0)),
        "body_success_count": int(body_counts.get("success", 0)),
        "body_pending_count": int(body_counts.get("pending", 0)),
        "body_failed_count": int(body_counts.get("failed", 0) + body_counts.get("timeout", 0)),
    }


def format_scheduled_summary(summary12h: dict[str, Any], duration_sec: float) -> str:
    platform_text = "，".join([f"{k} {v}" for k, v in summary12h["platform_counts"].items()]) or "无"
    end_local = datetime.now()
    start_local = end_local - timedelta(hours=12)
    return "\n".join(
        [
            "夜鹰 12 小时简报",
            f"- 时间：{start_local.strftime('%m-%d %H:%M')} ~ {end_local.strftime('%m-%d %H:%M')}",
            f"- 采集：共 {summary12h['total_fetched']} 条（{platform_text}）",
            f"- 评分：强推荐 {summary12h['strong_count']}，观察 {summary12h['watch_count']}，跳过 {summary12h['skip_count']}，抑制 {summary12h['suppressed_count']}",
            f"- 正文：成功 {summary12h['body_success_count']}，待抓取 {summary12h['body_pending_count']}，失败/超时 {summary12h['body_failed_count']}",
            f"- 队列：待通知 {summary12h['pending_count']}，候选 {summary12h['candidate_count']}，失败 {summary12h['failed_count']}",
            f"- 本轮耗时：{duration_sec} 秒",
        ]
    )


def process_pending_alerts(conn: sqlite3.Connection, tg: TelegramConfig, kb: KnowledgeBaseConfig, rules: RuleConfig, summary: dict[str, Any]) -> dict[str, int]:
    del kb, rules
    if not telegram_enabled(tg):
        log("[warn] telegram not configured; scheduled summary skipped")
        return {"sent": 0, "failed": 0, "pending": 0}

    should_send, slot = should_send_scheduled_summary(conn)
    if not should_send:
        return {"sent": 0, "failed": 0, "pending": 0}

    summary12h = collect_12h_summary(conn)
    try:
        send_telegram_message(tg, format_scheduled_summary(summary12h, float(summary.get("duration_sec", 0))))
        set_runtime_value(conn, "summary_last_slot", slot)
        log(f"[notify] scheduled-summary sent slot={slot}")
        return {"sent": 1, "failed": 0, "pending": summary12h.get("pending_count", 0)}
    except Exception as exc:  # noqa: BLE001
        log(f"[notify] scheduled-summary failed slot={slot}: {exc}")
        return {"sent": 0, "failed": 1, "pending": summary12h.get("pending_count", 0)}


def get_notify_queue_stats(conn: sqlite3.Connection) -> dict[str, int]:
    cur = conn.cursor()
    return {
        "notify_pending_total": int(cur.execute("SELECT COUNT(*) FROM alert_queue WHERE notify_status='pending'").fetchone()[0]),
        "notify_candidate_total": int(cur.execute("SELECT COUNT(*) FROM alert_queue WHERE notify_status='candidate'").fetchone()[0]),
        "notify_sent_total": int(cur.execute("SELECT COUNT(*) FROM alert_queue WHERE notify_status='sent'").fetchone()[0]),
        "notify_failed_total": int(cur.execute("SELECT COUNT(*) FROM alert_queue WHERE notify_status='failed'").fetchone()[0]),
        "strong_total": int(cur.execute("SELECT COUNT(*) FROM alert_queue WHERE recommend_level='strong'").fetchone()[0]),
        "watch_total": int(cur.execute("SELECT COUNT(*) FROM alert_queue WHERE recommend_level='watch'").fetchone()[0]),
    }


def run_once() -> dict[str, Any]:
    started_at = time.time()
    tg, kb, rules = load_rules()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = configure_sqlite_connection(sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT_SEC))
    conn.row_factory = sqlite3.Row
    try:
        ensure_tables(conn)
        queue_summary = normalize_queue_states(conn, rules)
        enqueue_summary = enqueue_new_candidates(conn, rules)
        backfill_summary = backfill_strong_notes(conn, kb, limit=80)
        queue_stats = get_notify_queue_stats(conn)
        duration_sec = round(time.time() - started_at, 2)
        summary = {"ok": True, "service": "notifier", "duration_sec": duration_sec, **queue_summary, **enqueue_summary, **backfill_summary, **queue_stats}
        notify_summary = process_pending_alerts(conn, tg, kb, rules, summary)
        summary.update(notify_summary)
        queue_stats_after_notify = get_notify_queue_stats(conn)
        summary.update(queue_stats_after_notify)
        log(
            f"[summary] service=notifier duration_sec={duration_sec} scanned={summary['scanned']} pending_new={summary['pending']} candidate_new={summary['candidate']} skipped={summary['skipped']} suppressed={summary['suppressed']} retried_failed={summary['retried_failed']} migrated_candidates={summary['migrated_candidates']} backfilled_notes={summary['backfilled_notes']} sent={summary['sent']} failed={summary['failed']} notify_pending_total={summary['notify_pending_total']} notify_candidate_total={summary['notify_candidate_total']} notify_failed_total={summary['notify_failed_total']} strong_total={summary['strong_total']}"
        )
        return summary
    finally:
        conn.close()


def run_daemon(poll_sec: int) -> None:
    log(f"nighthawk notifier started: poll_sec={poll_sec}")
    while True:
        try:
            run_once()
        except Exception as exc:  # noqa: BLE001
            log(f"[fatal-loop-error] {exc}")
        time.sleep(poll_sec)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-sec", type=int, default=120)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.once:
        run_once()
        return
    run_daemon(args.poll_sec)


if __name__ == "__main__":
    main()
