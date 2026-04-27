from __future__ import annotations

from collections import Counter, defaultdict
import json
from pathlib import Path
import sqlite3
from typing import Any

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


def _resolve_db_path(db_path: str | Path | None = None, config_path: str | Path | None = None) -> Path:
    if db_path:
        return Path(db_path).expanduser()
    config = load_create_studio_config(config_path=config_path)
    configured = str(((config.get("database_sources") or {}).get("nighthawk_db_path") or "")).strip()
    return Path(configured).expanduser()


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

        if index < max(1, int(recent_limit or 5)):
            recent_examples.append(
                {
                    "raw_item_id": _safe_int(payload.get("id")),
                    "title": str(obj.title or "").strip(),
                    "platform": platform,
                    "source_kind": source_kind,
                    "body_ready": bool(obj.body_ready),
                    "has_event_links": bool(links),
                    "published_at": str(obj.published_at or "").strip(),
                }
            )

    return {
        "ok": True,
        "db_path": str(resolved_db_path),
        "raw_items_count": len(rows),
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

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        obj = content_object_from_raw_item(payload)
        if body_ready_only and not obj.body_ready:
            continue
        event_links = list(event_links_by_id.get(_safe_int(payload.get("id")), []))
        items.append(
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
                "published_at": str(obj.published_at or "").strip(),
                "canonical_url": str(obj.canonical_url or "").strip(),
                "related_topics": list(obj.related_topics or []),
                "tags": list(obj.tags or []),
                "event_links": event_links,
                "heat_score": float((obj.metadata or {}).get("engagement_score") or (obj.metadata or {}).get("heat_score") or 0),
            }
        )

    total = len(items)
    total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
    if current_page > total_pages:
        current_page = total_pages
    offset = (current_page - 1) * page_size
    paged_items = items[offset: offset + page_size]

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
        "item": {
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
            "published_at": str(obj.published_at or "").strip(),
            "canonical_url": str(obj.canonical_url or "").strip(),
            "related_topics": list(obj.related_topics or []),
            "tags": list(obj.tags or []),
            "event_links": list(event_links_by_id.get(normalized_id, [])),
            "metadata": dict(obj.metadata or {}),
        },
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
                        "published_at": str(obj.published_at or "").strip(),
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
        "first_seen_at": str(event_row["first_seen_at"] or "").strip(),
        "last_seen_at": str(event_row["last_seen_at"] or "").strip(),
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
                "published_at": str(obj.published_at or "").strip(),
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
