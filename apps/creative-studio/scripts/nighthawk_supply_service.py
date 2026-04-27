from __future__ import annotations

from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from content_object_models import content_object_from_raw_item
from create_studio_config import load_create_studio_config
from creation_models import (
    CreationIntent,
    CreationPacket,
    DEFAULT_CREATION_PACKET_VERSION,
    EvidencePack,
    NarrativePlan,
    ensure_list,
)
from topic_intent_service import build_topic_intent
from translation_service import enrich_raw_item_translations
from runtime_config import service_base_url
try:
    from zoneinfo import ZoneInfo
except Exception:  # noqa: BLE001
    ZoneInfo = None


DEFAULT_NIGHTHAWK_UPSTREAM_BASE = service_base_url("content_search_layer", 8787)
NIGHTHAWK_UPSTREAM_BASE = str(
    os.environ.get("CREATE_STUDIO_NIGHTHAWK_UPSTREAM_BASE") or DEFAULT_NIGHTHAWK_UPSTREAM_BASE
).strip() or DEFAULT_NIGHTHAWK_UPSTREAM_BASE
BEIJING_TZ = ZoneInfo("Asia/Shanghai") if ZoneInfo is not None else timezone(timedelta(hours=8))
ENABLE_UPSTREAM_RAW_SYNC = str(
    os.environ.get("CREATE_STUDIO_ENABLE_UPSTREAM_RAW_SYNC", "0")
).strip().lower() not in {"0", "false", "no", "off"}
RAW_ITEMS_SYNC_TTL_SEC = max(15, int(os.environ.get("CREATE_STUDIO_RAW_SYNC_TTL_SEC", "90") or "90"))
RAW_ITEMS_SYNC_PAGE_SIZE = max(20, min(int(os.environ.get("CREATE_STUDIO_RAW_SYNC_PAGE_SIZE", "100") or "100"), 100))
RAW_ITEMS_SYNC_MAX_PAGES = max(1, min(int(os.environ.get("CREATE_STUDIO_RAW_SYNC_MAX_PAGES", "8") or "8"), 50))
UPSTREAM_TIMEOUT_SEC = max(0.2, float(os.environ.get("CREATE_STUDIO_NIGHTHAWK_UPSTREAM_TIMEOUT_SEC", "2") or "2"))
_RAW_ITEMS_SYNC_STATE: dict[str, Any] = {"last_attempt_at": 0.0, "last_summary": {}}


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _sync_runtime_settings(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_create_studio_config(config_path=config_path)
    nighthawk = dict(config.get("nighthawk") or {})
    enabled = _parse_bool(
        os.environ.get("CREATE_STUDIO_ENABLE_UPSTREAM_RAW_SYNC"),
        default=bool(nighthawk.get("enable_upstream_raw_sync", ENABLE_UPSTREAM_RAW_SYNC)),
    )
    base_url = str(
        os.environ.get("CREATE_STUDIO_NIGHTHAWK_UPSTREAM_BASE")
        or nighthawk.get("upstream_base_url")
        or DEFAULT_NIGHTHAWK_UPSTREAM_BASE
    ).strip() or DEFAULT_NIGHTHAWK_UPSTREAM_BASE
    try:
        timeout_sec = float(
            os.environ.get("CREATE_STUDIO_NIGHTHAWK_UPSTREAM_TIMEOUT_SEC")
            or nighthawk.get("upstream_timeout_sec")
            or UPSTREAM_TIMEOUT_SEC
        )
    except Exception:  # noqa: BLE001
        timeout_sec = UPSTREAM_TIMEOUT_SEC
    return {
        "enabled": enabled,
        "base_url": base_url,
        "timeout_sec": max(0.2, timeout_sec),
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return 0


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:  # noqa: BLE001
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _format_datetime_text(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        if raw.isdigit():
            if len(raw) == 8:
                dt = datetime.strptime(raw, "%Y%m%d")
                return dt.strftime("%Y-%m-%d 00:00:00")
            if len(raw) >= 10:
                dt = datetime.fromtimestamp(int(raw[:10]), tz=timezone.utc).astimezone(BEIJING_TZ).replace(tzinfo=None)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
        if len(raw) == 10 and raw.count("-") == 2:
            dt = datetime.strptime(raw, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d 00:00:00")
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(BEIJING_TZ).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is not None:
            dt = dt.astimezone(BEIJING_TZ).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        return raw


def _resolve_db_path(db_path: str | Path | None = None, config_path: str | Path | None = None) -> Path:
    if db_path:
        return Path(db_path).expanduser()
    config = load_create_studio_config(config_path=config_path)
    configured = str(((config.get("database_sources") or {}).get("nighthawk_db_path") or "")).strip()
    return Path(configured).expanduser()


def _fetch_upstream_json(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    query = urlencode({key: value for key, value in (params or {}).items() if value is not None})
    url = f"{NIGHTHAWK_UPSTREAM_BASE.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    with urlopen(url, timeout=UPSTREAM_TIMEOUT_SEC) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _ensure_raw_items_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_items (
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
        "CREATE INDEX IF NOT EXISTS idx_raw_items_platform_published_fetched ON raw_items(platform, published_at DESC, fetched_at DESC, id DESC)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_items_fetched_at ON raw_items(fetched_at DESC)")
    conn.commit()


def _derive_body_status(metrics: dict[str, Any]) -> str:
    if metrics.get("body_fetch_ok") is True:
        return "success"
    if str(metrics.get("body_fetch_error") or "").strip():
        return "failed"
    if metrics.get("body_fetch_skipped") or metrics.get("metadata_only"):
        return "none"
    return ""


def _upsert_raw_items_from_upstream(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> dict[str, int]:
    _ensure_raw_items_table(conn)
    inserted = 0
    updated = 0
    for item in items:
        raw_item_id = _safe_int(item.get("id"))
        if raw_item_id <= 0:
            continue
        metrics = dict(item.get("metrics") or {})
        payload = (
            raw_item_id,
            str(item.get("platform") or "").strip(),
            str(item.get("source_handle") or "").strip(),
            str(item.get("item_id") or "").strip(),
            str(item.get("title") or "").strip(),
            str(item.get("content") or "").strip(),
            str(item.get("url") or "").strip(),
            str(item.get("published_at") or "").strip(),
            json.dumps(metrics, ensure_ascii=False),
            str(item.get("fetched_at") or "").strip(),
            _derive_body_status(metrics),
        )
        existing = conn.execute("SELECT 1 FROM raw_items WHERE id=? LIMIT 1", (raw_item_id,)).fetchone()
        conn.execute(
            """
            INSERT INTO raw_items(
              id, platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at, body_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              platform=excluded.platform,
              source_handle=excluded.source_handle,
              item_id=excluded.item_id,
              title=excluded.title,
              content=excluded.content,
              url=excluded.url,
              published_at=excluded.published_at,
              metrics_json=excluded.metrics_json,
              fetched_at=excluded.fetched_at,
              body_status=CASE
                WHEN COALESCE(excluded.body_status, '') != '' THEN excluded.body_status
                ELSE raw_items.body_status
              END
            """,
            payload,
        )
        if existing:
            updated += 1
        else:
            inserted += 1
    conn.commit()
    return {"inserted": inserted, "updated": updated}


def _get_local_raw_items_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(conn, "raw_items"):
        return {"count": 0, "latest_at": ""}
    row = conn.execute(
        "SELECT COUNT(*) AS count, MAX(COALESCE(published_at, fetched_at)) AS latest_at FROM raw_items"
    ).fetchone()
    return {
        "count": _safe_int(row["count"] if row else 0),
        "latest_at": str((row["latest_at"] if row else "") or "").strip(),
    }


def sync_recent_raw_items_from_upstream(
    *,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    force: bool = False,
    max_pages: int | None = None,
) -> dict[str, Any]:
    global NIGHTHAWK_UPSTREAM_BASE, UPSTREAM_TIMEOUT_SEC
    settings = _sync_runtime_settings(config_path=config_path)
    NIGHTHAWK_UPSTREAM_BASE = str(settings["base_url"])
    UPSTREAM_TIMEOUT_SEC = float(settings["timeout_sec"])
    if not settings["enabled"]:
        return {
            "ok": True,
            "disabled": True,
            "reason": "upstream_sync_disabled",
            "db_path": str(_resolve_db_path(db_path=db_path, config_path=config_path)),
            "upstream_base": NIGHTHAWK_UPSTREAM_BASE,
            "inserted": 0,
            "updated": 0,
            "pages_scanned": 0,
            "items_seen": 0,
            "before": {},
            "after": {},
        }
    now = time.time()
    if not force and (now - float(_RAW_ITEMS_SYNC_STATE.get("last_attempt_at") or 0.0)) < RAW_ITEMS_SYNC_TTL_SEC:
        cached = dict(_RAW_ITEMS_SYNC_STATE.get("last_summary") or {})
        if cached:
            cached["throttled"] = True
            return cached

    resolved_db_path = _resolve_db_path(db_path=db_path, config_path=config_path)
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {
        "ok": False,
        "db_path": str(resolved_db_path),
        "upstream_base": NIGHTHAWK_UPSTREAM_BASE,
        "upstream_timeout_sec": UPSTREAM_TIMEOUT_SEC,
        "inserted": 0,
        "updated": 0,
        "pages_scanned": 0,
        "items_seen": 0,
        "before": {},
        "after": {},
    }
    try:
        upstream_overview = _fetch_upstream_json("/api/db/overview")
        with sqlite3.connect(resolved_db_path) as conn:
            conn.row_factory = sqlite3.Row
            before = _get_local_raw_items_summary(conn)
            summary["before"] = before
            latest_before = str(before.get("latest_at") or "").strip()
            pages_to_scan = max_pages or RAW_ITEMS_SYNC_MAX_PAGES
            for page in range(1, pages_to_scan + 1):
                payload = _fetch_upstream_json(
                    "/api/db/recent-items",
                    {"limit": RAW_ITEMS_SYNC_PAGE_SIZE, "page": page},
                )
                items = list(payload.get("items") or [])
                if not items:
                    break
                stats = _upsert_raw_items_from_upstream(conn, items)
                summary["inserted"] += stats["inserted"]
                summary["updated"] += stats["updated"]
                summary["pages_scanned"] += 1
                summary["items_seen"] += len(items)
                if latest_before:
                    item_times = [
                        str(item.get("published_at") or item.get("fetched_at") or "").strip()
                        for item in items
                        if str(item.get("published_at") or item.get("fetched_at") or "").strip()
                    ]
                    if item_times and max(item_times) <= latest_before and summary["inserted"] == 0:
                        break
            summary["after"] = _get_local_raw_items_summary(conn)
        summary["upstream"] = {
            "raw_items_count": _safe_int(upstream_overview.get("raw_items_count")),
            "latest_published_at": str(upstream_overview.get("latest_published_at") or "").strip(),
            "latest_fetched_at": str(upstream_overview.get("latest_fetched_at") or "").strip(),
        }
        summary["ok"] = True
    except Exception as exc:  # noqa: BLE001
        summary["error"] = str(exc)

    _RAW_ITEMS_SYNC_STATE["last_attempt_at"] = now
    _RAW_ITEMS_SYNC_STATE["last_summary"] = dict(summary)
    return summary


def _load_event_links(conn: sqlite3.Connection) -> dict[int, list[dict[str, Any]]]:
    if not _table_exists(conn, "event_candidates") or not _table_exists(conn, "event_evidence"):
        return {}

    event_candidate_columns = _column_names(conn, "event_candidates")
    event_evidence_columns = _column_names(conn, "event_evidence")
    role_expr = "ee.role AS evidence_role" if "role" in event_evidence_columns else "'' AS evidence_role"
    event_type_expr = "ec.event_type AS event_type" if "event_type" in event_candidate_columns else "'' AS event_type"
    summary_expr = "ec.summary AS event_summary" if "summary" in event_candidate_columns else "'' AS event_summary"

    rows = conn.execute(
        f"""
        SELECT
          ee.raw_item_id,
          ec.id AS event_id,
          ec.title AS event_title,
          {event_type_expr},
          {summary_expr},
          {role_expr}
        FROM event_evidence ee
        JOIN event_candidates ec ON ec.id = ee.event_id
        ORDER BY ee.raw_item_id ASC, ec.id ASC
        """
    ).fetchall()

    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        raw_item_id = _safe_int(row["raw_item_id"])
        if raw_item_id <= 0:
            continue
        grouped[raw_item_id].append(
            {
                "event_id": _safe_int(row["event_id"]),
                "title": str(row["event_title"] or "").strip(),
                "event_type": str(row["event_type"] or "").strip(),
                "summary": str(row["event_summary"] or "").strip(),
                "role": str(row["evidence_role"] or "").strip(),
            }
        )
    return grouped


def get_nighthawk_supply_profile(
    *,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    recent_limit: int = 5,
) -> dict[str, Any]:
    resolved_db_path = _resolve_db_path(db_path=db_path, config_path=config_path)
    if not resolved_db_path.exists():
        raise FileNotFoundError(f"NightHawk DB not found: {resolved_db_path}")

    sync_summary = sync_recent_raw_items_from_upstream(
        db_path=resolved_db_path,
        config_path=config_path,
    )

    with sqlite3.connect(resolved_db_path) as conn:
        conn.row_factory = sqlite3.Row
        if not _table_exists(conn, "raw_items"):
            raise RuntimeError("NightHawk DB does not contain raw_items")

        event_links = _load_event_links(conn)
        rows = conn.execute(
            """
            SELECT id, platform, source_handle, title, content, published_at, fetched_at, metrics_json, body_status
            FROM raw_items
            ORDER BY COALESCE(published_at, '') DESC, fetched_at DESC, id DESC
            """
        ).fetchall()

    platform_counter: Counter[str] = Counter()
    platform_body_ready: Counter[str] = Counter()
    body_status_counter: Counter[str] = Counter()
    source_kind_counter: Counter[str] = Counter()
    related_topic_counter = 0
    tag_counter = 0
    body_ready_count = 0
    metadata_only_count = 0
    event_linked_count = 0
    recent_examples: list[dict[str, Any]] = []
    latest_published_at = ""
    latest_fetched_at = ""

    for index, row in enumerate(rows):
        payload = dict(row)
        obj = content_object_from_raw_item(payload)
        metrics = dict(obj.metadata or {})
        source_kind = str(obj.source_kind or "").strip() or "unknown"
        platform = str(obj.platform or "").strip() or "unknown"
        body_status = str(metrics.get("body_status") or payload.get("body_status") or "none").strip() or "none"
        links = event_links.get(_safe_int(payload.get("id")))

        platform_counter[platform] += 1
        body_status_counter[body_status] += 1
        source_kind_counter[source_kind] += 1
        if obj.body_ready:
            body_ready_count += 1
            platform_body_ready[platform] += 1
        else:
            metadata_only_count += 1
        if links:
            event_linked_count += 1
        if list(obj.related_topics or []):
            related_topic_counter += 1
        if list(obj.tags or []):
            tag_counter += 1
        if not latest_published_at and str(obj.published_at or "").strip():
            latest_published_at = str(obj.published_at or "").strip()
        if not latest_fetched_at and str(payload.get("fetched_at") or "").strip():
            latest_fetched_at = str(payload.get("fetched_at") or "").strip()

        if index < max(1, int(recent_limit or 5)):
            recent_examples.append(
                {
                    "raw_item_id": _safe_int(payload.get("id")),
                    "title": str(obj.title or "").strip(),
                    "platform": platform,
                    "source_kind": source_kind,
                    "body_ready": bool(obj.body_ready),
                    "has_event_links": bool(links),
                    "published_at": _format_datetime_text(obj.published_at),
                }
            )

    return {
        "ok": True,
        "db_path": str(resolved_db_path),
        "raw_items_count": len(rows),
        "latest_published_at": _format_datetime_text(latest_published_at),
        "latest_fetched_at": _format_datetime_text(latest_fetched_at),
        "body_ready_count": body_ready_count,
        "metadata_only_count": metadata_only_count,
        "event_linked_count": event_linked_count,
        "supports": {
            "full_body": body_ready_count > 0,
            "metadata_only": metadata_only_count > 0,
            "event_linking": event_linked_count > 0,
            "topic_hints": related_topic_counter > 0,
            "tag_hints": tag_counter > 0,
        },
        "platform_breakdown": [
            {
                "platform": platform,
                "count": count,
                "body_ready_count": platform_body_ready.get(platform, 0),
            }
            for platform, count in platform_counter.most_common()
        ],
        "body_status_breakdown": [
            {"body_status": body_status, "count": count}
            for body_status, count in body_status_counter.most_common()
        ],
        "source_kind_breakdown": [
            {"source_kind": source_kind, "count": count}
            for source_kind, count in source_kind_counter.most_common()
        ],
        "recent_examples": recent_examples,
        "frontend_ready": {
            "recommended_entry_mode": "independent_page",
            "suggested_entry_path": "/create/nighthawk",
        },
        "mirror_sync": sync_summary,
    }


def list_nighthawk_raw_items(
    *,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    limit: int = 20,
    page: int = 1,
    platform: str = "",
    keyword: str = "",
    body_ready_only: bool = False,
    sort_by: str = "time",
) -> dict[str, Any]:
    resolved_db_path = _resolve_db_path(db_path=db_path, config_path=config_path)
    if not resolved_db_path.exists():
        raise FileNotFoundError(f"NightHawk DB not found: {resolved_db_path}")

    sync_summary = sync_recent_raw_items_from_upstream(
        db_path=resolved_db_path,
        config_path=config_path,
    )

    with sqlite3.connect(resolved_db_path) as conn:
        conn.row_factory = sqlite3.Row
        if not _table_exists(conn, "raw_items"):
            raise RuntimeError("NightHawk DB does not contain raw_items")
        event_links_by_id = _load_event_links(conn)

        where_parts = ["1=1"]
        params: list[Any] = []
        normalized_platform = str(platform or "").strip()
        normalized_keyword = str(keyword or "").strip()
        if normalized_platform:
            where_parts.append("platform = ?")
            params.append(normalized_platform)
        if normalized_keyword:
            where_parts.append("(LOWER(title) LIKE LOWER(?) OR LOWER(content) LIKE LOWER(?) OR LOWER(source_handle) LIKE LOWER(?))")
            wildcard = f"%{normalized_keyword}%"
            params.extend([wildcard, wildcard, wildcard])
        page_size = max(1, min(int(limit or 20), 50))
        current_page = max(1, int(page or 1))
        normalized_sort_by = str(sort_by or "time").strip().lower() or "time"
        if normalized_sort_by == "heat":
            order_sql = """
            COALESCE(json_extract(metrics_json, '$.engagement_score'), json_extract(metrics_json, '$.heat_score'), 0) DESC,
            COALESCE(published_at, '') DESC,
            fetched_at DESC,
            id DESC
            """
        else:
            normalized_sort_by = "time"
            order_sql = "COALESCE(published_at, '') DESC, fetched_at DESC, id DESC"
        where_sql = " WHERE " + " AND ".join(where_parts)
        selected_rows: list[sqlite3.Row] = []
        if body_ready_only:
            rows = conn.execute(
                f"""
                SELECT
                  id,
                  platform,
                  source_handle,
                  item_id,
                  title,
                  content,
                  url,
                  published_at,
                  fetched_at,
                  metrics_json,
                  body_status
                FROM raw_items
                {where_sql}
                ORDER BY {order_sql}
                """,
                tuple(params),
            ).fetchall()
            total = 0
            offset = (current_page - 1) * page_size
            end = offset + page_size
            for row in rows:
                payload = dict(row)
                obj = content_object_from_raw_item(payload)
                if not obj.body_ready:
                    continue
                if total >= offset and len(selected_rows) < page_size:
                    selected_rows.append(row)
                total += 1
                if len(selected_rows) >= page_size and total >= end:
                    continue
        else:
            total = _safe_int(
                conn.execute(f"SELECT COUNT(*) FROM raw_items{where_sql}", tuple(params)).fetchone()[0]
            )
            total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
            if current_page > total_pages:
                current_page = total_pages
            offset = (current_page - 1) * page_size
            selected_rows = conn.execute(
                f"""
                SELECT
                  id,
                  platform,
                  source_handle,
                  item_id,
                  title,
                  content,
                  url,
                  published_at,
                  fetched_at,
                  metrics_json,
                  body_status
                FROM raw_items
                {where_sql}
                ORDER BY {order_sql}
                LIMIT ? OFFSET ?
                """,
                tuple([*params, page_size, offset]),
            ).fetchall()

    total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
    if current_page > total_pages:
        current_page = total_pages

    paged_items: list[dict[str, Any]] = []
    for row in selected_rows:
        payload = dict(row)
        obj = content_object_from_raw_item(payload)
        event_links = list(event_links_by_id.get(_safe_int(payload.get("id")), []))
        paged_items.append(
            enrich_raw_item_translations(
                {
                    "raw_item_id": _safe_int(payload.get("id")),
                    "title": str(obj.title or "").strip(),
                    "summary": str(obj.summary or "").strip(),
                    "content_preview": str(obj.body_text or "").strip()[:240],
                    "body_ready": bool(obj.body_ready),
                    "body_status": str((obj.metadata or {}).get("body_status") or payload.get("body_status") or "").strip(),
                    "platform": str(obj.platform or "").strip(),
                    "source_kind": str(obj.source_kind or "").strip(),
                    "source_name": str(obj.source_name or "").strip(),
                    "published_at": _format_datetime_text(obj.published_at),
                    "fetched_at": _format_datetime_text(payload.get("fetched_at")),
                    "canonical_url": str(obj.canonical_url or "").strip(),
                    "related_topics": list(obj.related_topics or []),
                    "tags": list(obj.tags or []),
                    "event_links": event_links,
                    "heat_score": float((obj.metadata or {}).get("engagement_score") or (obj.metadata or {}).get("heat_score") or 0),
                },
                include_body=False,
                config_path=config_path,
                allow_live_translate=False,
            )
        )

    return {
        "ok": True,
        "items": paged_items,
        "total": total,
        "page": current_page,
        "page_size": page_size,
        "total_pages": total_pages,
        "filters": {
            "platform": normalized_platform,
            "keyword": normalized_keyword,
            "body_ready_only": body_ready_only,
            "sort_by": normalized_sort_by,
        },
        "mirror_sync": sync_summary,
    }


def get_nighthawk_raw_item_detail(
    raw_item_id: int | str,
    *,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_id = _safe_int(raw_item_id)
    if normalized_id <= 0:
        raise ValueError("raw_item_id is required")
    resolved_db_path = _resolve_db_path(db_path=db_path, config_path=config_path)
    if not resolved_db_path.exists():
        raise FileNotFoundError(f"NightHawk DB not found: {resolved_db_path}")

    with sqlite3.connect(resolved_db_path) as conn:
        conn.row_factory = sqlite3.Row
        if not _table_exists(conn, "raw_items"):
            raise RuntimeError("NightHawk DB does not contain raw_items")
        row = conn.execute(
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
              fetched_at,
              metrics_json,
              body_status
            FROM raw_items
            WHERE id = ?
            LIMIT 1
            """,
            (normalized_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"NightHawk raw_item not found: {normalized_id}")
        event_links_by_id = _load_event_links(conn)

    payload = dict(row)
    obj = content_object_from_raw_item(payload)
    return {
        "ok": True,
        "item": enrich_raw_item_translations(
            {
            "raw_item_id": normalized_id,
            "title": str(obj.title or "").strip(),
            "summary": str(obj.summary or "").strip(),
            "body_text": str(obj.body_text or "").strip(),
            "body_ready": bool(obj.body_ready),
            "body_status": str((obj.metadata or {}).get("body_status") or payload.get("body_status") or "").strip(),
            "platform": str(obj.platform or "").strip(),
            "source_kind": str(obj.source_kind or "").strip(),
            "source_name": str(obj.source_name or "").strip(),
            "source_ref": str(obj.source_ref or "").strip(),
            "published_at": _format_datetime_text(obj.published_at),
            "fetched_at": _format_datetime_text(payload.get("fetched_at")),
            "canonical_url": str(obj.canonical_url or "").strip(),
            "related_topics": list(obj.related_topics or []),
            "tags": list(obj.tags or []),
            "event_links": list(event_links_by_id.get(normalized_id, [])),
            "metadata": dict(obj.metadata or {}),
            },
            include_body=True,
            config_path=config_path,
        ),
    }


def get_event_detail(
    event_id: int | str,
    *,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_id = _safe_int(event_id)
    if normalized_id <= 0:
        raise ValueError("event_id is required")
    resolved_db_path = _resolve_db_path(db_path=db_path, config_path=config_path)
    if not resolved_db_path.exists():
        raise FileNotFoundError(f"NightHawk DB not found: {resolved_db_path}")

    with sqlite3.connect(resolved_db_path) as conn:
        conn.row_factory = sqlite3.Row
        if not _table_exists(conn, "event_candidates"):
            raise RuntimeError("NightHawk DB does not contain event_candidates")
        event_candidate_columns = _column_names(conn, "event_candidates")

        subject_expr = "ec.subject" if "subject" in event_candidate_columns else "'' AS subject"
        object_expr = "ec.object" if "object" in event_candidate_columns else "'' AS object"
        first_seen_expr = "ec.first_seen_at" if "first_seen_at" in event_candidate_columns else "'' AS first_seen_at"
        last_seen_expr = "ec.last_seen_at" if "last_seen_at" in event_candidate_columns else "'' AS last_seen_at"
        heat_expr = "ec.heat_score" if "heat_score" in event_candidate_columns else "0 AS heat_score"
        novelty_expr = "ec.novelty_score" if "novelty_score" in event_candidate_columns else "0 AS novelty_score"
        confidence_expr = "ec.confidence" if "confidence" in event_candidate_columns else "0 AS confidence"
        status_expr = "ec.status" if "status" in event_candidate_columns else "'' AS status"

        event_row = conn.execute(
            f"""
            SELECT
              ec.id,
              ec.event_type,
              ec.title,
              ec.summary,
              {subject_expr},
              {object_expr},
              {first_seen_expr},
              {last_seen_expr},
              {heat_expr},
              {novelty_expr},
              {confidence_expr},
              {status_expr}
            FROM event_candidates ec
            WHERE ec.id = ?
            LIMIT 1
            """,
            (normalized_id,),
        ).fetchone()
        if not event_row:
            raise ValueError(f"NightHawk event not found: {normalized_id}")

        topic_refs: list[dict[str, Any]] = []
        if _table_exists(conn, "topic_events") and _table_exists(conn, "topics"):
            topic_rows = conn.execute(
                """
                SELECT t.id, t.title, t.topic_key
                FROM topic_events te
                JOIN topics t ON t.id = te.topic_id
                WHERE te.event_id = ?
                ORDER BY t.id DESC
                """,
                (normalized_id,),
            ).fetchall()
            topic_refs = [
                {
                    "topic_id": _safe_int(row["id"]),
                    "title": str(row["title"] or "").strip(),
                    "topic_key": str(row["topic_key"] or "").strip(),
                }
                for row in topic_rows
            ]

        related_items: list[dict[str, Any]] = []
        if _table_exists(conn, "event_evidence") and _table_exists(conn, "raw_items"):
            rows = conn.execute(
                """
                SELECT
                  ee.role,
                  r.id,
                  r.platform,
                  r.source_handle,
                  r.item_id,
                  r.title,
                  r.content,
                  r.url,
                  r.published_at,
                  r.fetched_at,
                  r.metrics_json,
                  r.body_status
                FROM event_evidence ee
                JOIN raw_items r ON r.id = ee.raw_item_id
                WHERE ee.event_id = ?
                ORDER BY COALESCE(r.published_at, '') DESC, r.fetched_at DESC, r.id DESC
                """,
                (normalized_id,),
            ).fetchall()
            for row in rows:
                obj = content_object_from_raw_item(dict(row))
                related_items.append(
                    {
                        "raw_item_id": _safe_int(row["id"]),
                        "title": str(obj.title or "").strip(),
                        "summary": str(obj.summary or "").strip(),
                        "body_text": str(obj.body_text or "").strip(),
                        "body_ready": bool(obj.body_ready),
                        "platform": str(obj.platform or "").strip(),
                        "source_kind": str(obj.source_kind or "").strip(),
                        "source_name": str(obj.source_name or "").strip(),
                        "published_at": _format_datetime_text(obj.published_at),
                        "fetched_at": _format_datetime_text(row["fetched_at"]),
                        "canonical_url": str(obj.canonical_url or "").strip(),
                        "related_topics": list(obj.related_topics or []),
                        "tags": list(obj.tags or []),
                        "evidence_role": str(row["role"] or "").strip(),
                    }
                )

    event_payload = {
        "event_id": _safe_int(event_row["id"]),
        "event_type": str(event_row["event_type"] or "").strip(),
        "title": str(event_row["title"] or "").strip(),
        "summary": str(event_row["summary"] or "").strip(),
        "subject": str(event_row["subject"] or "").strip(),
        "object": str(event_row["object"] or "").strip(),
        "first_seen_at": _format_datetime_text(event_row["first_seen_at"]),
        "last_seen_at": _format_datetime_text(event_row["last_seen_at"]),
        "heat_score": float(event_row["heat_score"] or 0),
        "novelty_score": float(event_row["novelty_score"] or 0),
        "confidence": float(event_row["confidence"] or 0),
        "status": str(event_row["status"] or "").strip(),
    }
    return {
        "ok": True,
        "event": event_payload,
        "topics": topic_refs,
        "related_items": related_items,
        "creation_entry": {
            "raw_item_ids": [item["raw_item_id"] for item in related_items],
            "recommended_topic": event_payload["title"],
        },
    }


def _fetch_raw_items_by_id(conn: sqlite3.Connection, raw_item_ids: list[int]) -> list[sqlite3.Row]:
    unique_ids = [item_id for item_id in dict.fromkeys(raw_item_ids) if int(item_id) > 0]
    if not unique_ids:
        return []
    placeholders = ",".join("?" for _ in unique_ids)
    return conn.execute(
        f"""
        SELECT
          id,
          platform,
          source_handle,
          item_id,
          title,
          content,
          url,
          published_at,
          fetched_at,
          metrics_json,
          body_status
        FROM raw_items
        WHERE id IN ({placeholders})
        ORDER BY COALESCE(published_at, '') DESC, fetched_at DESC, id DESC
        """,
        tuple(unique_ids),
    ).fetchall()


def build_creation_packet_from_nighthawk_raw_items(
    raw_item_ids: list[int | str],
    *,
    payload: dict[str, Any] | None = None,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_ids = [_safe_int(item) for item in raw_item_ids]
    normalized_ids = [item for item in normalized_ids if item > 0]
    if not normalized_ids:
        raise ValueError("raw_item_ids is required")

    resolved_db_path = _resolve_db_path(db_path=db_path, config_path=config_path)
    if not resolved_db_path.exists():
        raise FileNotFoundError(f"NightHawk DB not found: {resolved_db_path}")

    payload = dict(payload or {})
    with sqlite3.connect(resolved_db_path) as conn:
        conn.row_factory = sqlite3.Row
        if not _table_exists(conn, "raw_items"):
            raise RuntimeError("NightHawk DB does not contain raw_items")
        rows = _fetch_raw_items_by_id(conn, normalized_ids)
        event_links_by_id = _load_event_links(conn)

    row_map = {int(row["id"]): row for row in rows}
    missing_ids = [item_id for item_id in normalized_ids if item_id not in row_map]
    if missing_ids:
        raise ValueError(f"NightHawk raw_items not found: {', '.join(str(item) for item in missing_ids)}")

    selected_objects: list[dict[str, Any]] = []
    all_event_ids: list[int] = []
    primary_ids = {_safe_int(item) for item in list(payload.get("primary_raw_item_ids") or []) if _safe_int(item) > 0}

    for item_id in normalized_ids:
        row = row_map[item_id]
        obj = content_object_from_raw_item(dict(row))
        event_links = list(event_links_by_id.get(item_id, []))
        metadata = dict(obj.metadata or {})
        metadata["event_links"] = event_links
        metadata["event_candidate_ids"] = [item["event_id"] for item in event_links if item.get("event_id")]
        obj.metadata = metadata
        all_event_ids.extend(metadata["event_candidate_ids"])
        selected_objects.append(
            {
                "raw_item_id": item_id,
                "object_uid": str(obj.object_uid),
                "title": str(obj.title or "").strip(),
                "summary": str(obj.summary or "").strip(),
                "body_text": str(obj.body_text or "").strip(),
                "body_ready": bool(obj.body_ready),
                "platform": str(obj.platform or "").strip(),
                "source_kind": str(obj.source_kind or "").strip(),
                "source_name": str(obj.source_name or "").strip(),
                "published_at": _format_datetime_text(obj.published_at),
                "fetched_at": _format_datetime_text(row["fetched_at"]),
                "canonical_url": str(obj.canonical_url or "").strip(),
                "tags": list(obj.tags or []),
                "related_topics": list(obj.related_topics or []),
                "event_links": event_links,
            }
        )

    body_ready_items = [item for item in selected_objects if item["body_ready"]]
    if not primary_ids:
        if body_ready_items:
            primary_ids = {int(body_ready_items[0]["raw_item_id"])}
        else:
            primary_ids = {int(selected_objects[0]["raw_item_id"])}

    topic = str(payload.get("topic") or "").strip() or str(selected_objects[0]["title"] or "").strip()
    angle = str(payload.get("angle") or "").strip() or f"从 NightHawk 采集正文切入：{topic}"
    audience = str(payload.get("audience") or "").strip() or "内容创作者"
    goal = str(payload.get("goal") or "").strip() or "把 NightHawk 正文整理成可编排创作材料"
    platform = str(payload.get("platform") or "").strip() or "wechat"
    article_archetype = str(payload.get("article_archetype") or "").strip() or (
        "event_commentary" if all_event_ids else "phenomenon_analysis"
    )

    topic_intent = build_topic_intent(topic).to_dict()
    creation_intent = CreationIntent(
        trigger_type="nighthawk_raw_items",
        topic=topic,
        platform=platform,
        audience=audience,
        goal=goal,
        creation_mode="source_driven",
        angle=angle,
        article_archetype=article_archetype,
        primary_output=[str(payload.get("primary_output") or "long_article").strip() or "long_article"],
        optional_followups=ensure_list(payload.get("optional_followups")),
        source_scope=["raw_articles"],
        style_notes=ensure_list(payload.get("style_notes")),
        banned_patterns=ensure_list(payload.get("banned_patterns")),
        topic_intent=topic_intent,
        metadata={
            "selected_raw_item_ids": normalized_ids,
            "body_ready_count": len(body_ready_items),
        },
    ).to_dict()

    citations: list[dict[str, Any]] = []
    for index, item in enumerate(selected_objects, start=1):
        is_primary = int(item["raw_item_id"]) in primary_ids
        citations.append(
            {
                "citation_id": f"NH-{index}",
                "source_id": str(item["object_uid"]),
                "raw_item_id": int(item["raw_item_id"]),
                "title": item["title"],
                "source_name": item["source_name"],
                "usable_excerpt": item["body_text"][:240] if item["body_text"] else item["summary"],
                "summary": item["summary"],
                "body_ready": item["body_ready"],
                "usage_scope": "must_use" if is_primary else "supporting",
                "recommend_usage": "core_claim" if is_primary else "supporting_case",
                "event_candidate_ids": [link["event_id"] for link in item["event_links"] if link.get("event_id")],
            }
        )

    evidence_pack = EvidencePack(
        retrieval_batch_id="",
        citation_list_id="",
        citations=citations,
        result_ids=[str(item["object_uid"]) for item in selected_objects],
        source_scope=["raw_articles"],
        summary={
            "total_results": len(selected_objects),
            "body_ready_count": len(body_ready_items),
            "event_linked_count": sum(1 for item in selected_objects if item["event_links"]),
            "primary_count": sum(1 for item in citations if item["usage_scope"] == "must_use"),
        },
    ).to_dict()

    top_titles = [item["title"] for item in selected_objects[:3] if item["title"]]
    hook_candidates = [item["summary"] for item in selected_objects if item["summary"]][:3]
    narrative_plan = NarrativePlan(
        outline_packet_id="",
        core_judgement=str(payload.get("core_judgement") or "").strip() or (
            f"NightHawk 已经抓到足够的正文原料，可以围绕“{topic}”组织成一轮可写的创作判断。"
        ),
        angle=angle,
        content_template=str(payload.get("content_template") or "").strip() or "source_driven_brief",
        hook_candidates=hook_candidates,
        title_candidates=top_titles,
        outline=[
            {
                "section": "为什么这批 NightHawk 正文值得看",
                "goal": "先交代这批材料给出的主要判断",
                "citation_refs": [item["citation_id"] for item in citations if item["usage_scope"] == "must_use"],
            },
            {
                "section": "正文里真正能支撑判断的证据",
                "goal": "把主证据和辅助证据分开讲",
                "citation_refs": [item["citation_id"] for item in citations],
            },
            {
                "section": "这批材料更适合往哪种创作走",
                "goal": "为后续编排和 writer 选择留下清晰方向",
                "citation_refs": [item["citation_id"] for item in citations[:2]],
            },
        ],
        recommended_opening=hook_candidates[0] if hook_candidates else topic,
        recommended_sections=[
            "source_value",
            "evidence_layers",
            "next_creation_direction",
        ],
        risks=[
            *(
                ["some_items_body_not_ready"]
                if any(not item["body_ready"] for item in selected_objects)
                else []
            ),
            *(
                ["event_link_needs_followup_confirmation"]
                if all_event_ids
                else []
            ),
        ],
    ).to_dict()

    packet = CreationPacket(
        packet_kind="creation_packet",
        version=DEFAULT_CREATION_PACKET_VERSION,
        task_id="",
        creation_intent=creation_intent,
        evidence_pack=evidence_pack,
        narrative_plan=narrative_plan,
        downstream_targets=[],
        source_trace={
            "source_type": "nighthawk_raw_items",
            "db_path": str(resolved_db_path),
            "raw_item_ids": normalized_ids,
            "event_candidate_ids": list(dict.fromkeys(all_event_ids)),
        },
        metadata={
            "selection_mode": "manual_source_selection",
            "selected_count": len(selected_objects),
        },
    ).to_dict()

    return {
        "ok": True,
        "creation_packet": packet,
        "selected_items": selected_objects,
        "profile_hint": {
            "body_ready_count": len(body_ready_items),
            "event_linked_count": sum(1 for item in selected_objects if item["event_links"]),
        },
    }
