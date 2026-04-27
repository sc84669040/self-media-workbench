from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from create_studio_config import load_create_studio_config
from create_studio_store import CreateStudioStore
from creation_models import build_id
from nighthawk_supply_service import get_event_detail
from topic_intent_service import build_topic_intent


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _ensure_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _parse_json(value: Any, default: Any) -> Any:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        return default


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _render_event_packet_markdown(packet: dict[str, Any]) -> str:
    event = dict(packet.get("event") or {})
    summary = dict(packet.get("summary") or {})
    lines = [
        f"# Event Packet: {packet.get('title') or event.get('title') or 'Untitled Event'}",
        "",
        f"- Packet ID: {packet.get('packet_id') or ''}",
        f"- Event ID: {event.get('event_id') or packet.get('event_id') or ''}",
        f"- Generated At: {packet.get('generated_at') or ''}",
        f"- Event Type: {event.get('event_type') or '-'}",
        f"- Confidence: {event.get('confidence') if event.get('confidence') is not None else '-'}",
        f"- Related Articles: {summary.get('article_count') or len(list(packet.get('articles') or []))}",
        "",
        "## Event Summary",
        f"- Title: {event.get('title') or packet.get('title') or '-'}",
        f"- Summary: {event.get('summary') or '-'}",
        f"- Subjects: {', '.join(_ensure_list([event.get('subject'), event.get('object')])) or '-'}",
        "",
        "## Related Topics",
    ]
    topics = list(packet.get("topics") or [])
    if topics:
        for item in topics:
            lines.append(f"- {item.get('title') or item.get('topic_key') or item.get('topic_id')}")
    else:
        lines.append("- None")

    lines.extend(["", "## Event Evidence"])
    for index, item in enumerate(list(packet.get("articles") or []), start=1):
        lines.extend(
            [
                "",
                f"### {index}. {item.get('title') or item.get('source_id') or 'Untitled'}",
                f"- Role: {item.get('evidence_role') or '-'}",
                f"- Source: {item.get('source') or item.get('source_name') or '-'} / {item.get('content_type') or '-'}",
                f"- Published At: {item.get('published_at') or '-'}",
                f"- Why Pick: {item.get('why_pick') or '-'}",
                f"- Summary: {item.get('summary') or item.get('body_preview') or '-'}",
                f"- URL: {item.get('canonical_url') or item.get('url') or '-'}",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def _packet_exports(packet: dict[str, Any]) -> dict[str, str]:
    return {
        "markdown": _render_event_packet_markdown(packet),
        "json": json.dumps(packet, ensure_ascii=False, indent=2, sort_keys=True),
    }


def create_event_packet(
    payload: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_config = dict(config or load_create_studio_config())
    event_id = int(payload.get("event_id") or 0)
    if event_id <= 0:
        raise ValueError("event_id is required")

    detail_payload = get_event_detail(event_id, config_path=(resolved_config.get("_meta") or {}).get("config_path"))
    event = dict(detail_payload.get("event") or {})
    topics = list(detail_payload.get("topics") or [])
    related_items = list(detail_payload.get("related_items") or [])

    title = _normalize_text(payload.get("title") or event.get("title")) or f"Event {event_id}"
    topic = _normalize_text(payload.get("topic") or title)
    topic_intent = build_topic_intent(topic).to_dict()
    packet_id = _normalize_text(payload.get("packet_id")) or build_id("EP")
    packet_status = _normalize_text(payload.get("status")) or "ready"

    articles: list[dict[str, Any]] = []
    body_ready_count = 0
    for item in related_items:
        raw_item_id = int(item.get("raw_item_id") or 0)
        body_ready = bool(item.get("body_ready"))
        if body_ready:
            body_ready_count += 1
        articles.append(
            {
                "source_id": (
                    f"RAW-{raw_item_id}"
                    if raw_item_id > 0
                    else _normalize_text(item.get("canonical_url") or item.get("title"))
                ),
                "raw_item_id": raw_item_id,
                "title": _normalize_text(item.get("title")),
                "summary": _normalize_text(item.get("summary")),
                "body_text": _normalize_text(item.get("body_text")),
                "body_preview": _normalize_text(item.get("body_text"))[:320],
                "canonical_url": _normalize_text(item.get("canonical_url")),
                "source": _normalize_text(item.get("source_name")) or "NightHawk",
                "platform": _normalize_text(item.get("platform")),
                "content_type": _normalize_text(item.get("source_kind")) or "raw_article",
                "published_at": _normalize_text(item.get("published_at")),
                "body_ready": body_ready,
                "evidence_role": _normalize_text(item.get("evidence_role")) or "secondary",
                "why_pick": "这条正文被 NightHawk 事件聚合命中，可直接作为事件主证据或辅证。",
                "related_topics": list(item.get("related_topics") or []),
                "tags": list(item.get("tags") or []),
                "event_packet_refs": [packet_id],
                "event_candidate_ids": [event_id],
            }
        )

    packet = {
        "packet_id": packet_id,
        "packet_type": "event",
        "event_id": event_id,
        "event_key": _normalize_text(payload.get("event_key") or f"event::{event_id}") or f"event::{event_id}",
        "title": title,
        "topic": topic,
        "status": packet_status,
        "generated_at": _now_iso(),
        "event": event,
        "topics": topics,
        "topic_intent": topic_intent,
        "entities": [item for item in _ensure_list([event.get("subject"), event.get("object")]) if item],
        "summary": {
            "article_count": len(articles),
            "body_ready_count": body_ready_count,
            "topic_count": len(topics),
            "primary_count": len([item for item in articles if item.get("evidence_role") == "primary"]),
            "supporting_count": len([item for item in articles if item.get("evidence_role") != "primary"]),
        },
        "articles": articles,
        "creation_entry": {
            "recommended_topic": topic,
            "raw_item_ids": [int(item.get("raw_item_id") or 0) for item in articles if int(item.get("raw_item_id") or 0) > 0],
            "recommended_path": f"/create/events/{event_id}",
        },
    }

    create_studio_db_path = _normalize_text(((resolved_config.get("indexing") or {}).get("content_index_db_path")))
    if not create_studio_db_path:
        raise ValueError("create studio index db path is missing from config")
    store = CreateStudioStore(create_studio_db_path)
    store.initialize()
    record = store.save_event_packet(
        {
            "packet_id": packet_id,
            "event_key": _normalize_text(packet.get("event_key")),
            "title": title,
            "status": packet_status,
            "packet": packet,
        }
    )

    full_packet = {
        **packet,
        "created_at": _normalize_text(record.get("created_at")),
        "updated_at": _normalize_text(record.get("updated_at")),
    }
    return {
        "ok": True,
        "event_packet": full_packet,
        "record": record,
        "exports": _packet_exports(full_packet),
    }


def get_latest_event_packet_ref(
    event_id: int | str,
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_id = int(event_id or 0)
    if normalized_id <= 0:
        raise ValueError("event_id is required")

    resolved_config = dict(config or load_create_studio_config())
    create_studio_db_path = _normalize_text(((resolved_config.get("indexing") or {}).get("content_index_db_path")))
    if not create_studio_db_path:
        raise ValueError("create studio index db path is missing from config")
    db_path = Path(create_studio_db_path)
    if not db_path.exists():
        return {"ok": True, "exists": False, "packet": None}

    with _connect(db_path) as conn:
        table_row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='event_packets' LIMIT 1"
        ).fetchone()
        if not table_row:
            return {"ok": True, "exists": False, "packet": None}
        row = conn.execute(
            """
            SELECT packet_id, title, status, created_at, updated_at
            FROM event_packets
            WHERE event_key = ?
            ORDER BY updated_at DESC, packet_id DESC
            LIMIT 1
            """,
            (f"event::{normalized_id}",),
        ).fetchone()
    if not row:
        return {"ok": True, "exists": False, "packet": None}

    return {
        "ok": True,
        "exists": True,
        "packet": {
            "packet_id": _normalize_text(row["packet_id"]),
            "title": _normalize_text(row["title"]),
            "status": _normalize_text(row["status"]),
            "created_at": _normalize_text(row["created_at"]),
            "updated_at": _normalize_text(row["updated_at"]),
        },
    }


def get_event_packet_detail(packet_id: str, *, config: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved_config = dict(config or load_create_studio_config())
    create_studio_db_path = _normalize_text(((resolved_config.get("indexing") or {}).get("content_index_db_path")))
    if not create_studio_db_path:
        raise ValueError("create studio index db path is missing from config")

    with _connect(create_studio_db_path) as conn:
        row = conn.execute(
            """
            SELECT packet_id, event_key, title, status, packet_json, created_at, updated_at
            FROM event_packets
            WHERE packet_id = ?
            LIMIT 1
            """,
            (_normalize_text(packet_id),),
        ).fetchone()
    if not row:
        raise ValueError(f"event packet not found: {packet_id}")

    packet = _parse_json(row["packet_json"], {})
    if not isinstance(packet, dict):
        packet = {}
    packet.setdefault("packet_id", _normalize_text(row["packet_id"]))
    packet.setdefault("event_key", _normalize_text(row["event_key"]))
    packet.setdefault("title", _normalize_text(row["title"]))
    packet.setdefault("status", _normalize_text(row["status"]))
    packet.setdefault("created_at", _normalize_text(row["created_at"]))
    packet.setdefault("updated_at", _normalize_text(row["updated_at"]))

    return {
        "ok": True,
        "event_packet": packet,
        "exports": _packet_exports(packet),
    }
