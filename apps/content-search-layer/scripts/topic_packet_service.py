from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from create_search_resolver import search_creation_candidates
from create_studio_config import load_create_studio_config
from create_studio_store import CreateStudioStore
from creation_models import build_id
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


def _normalize_filters(payload: dict[str, Any]) -> dict[str, Any]:
    filters = dict(payload.get("filters") or {})
    source_scope = _ensure_list(payload.get("source_scope"))
    if source_scope:
        filters["source_scope"] = source_scope
    if "limit" in payload and "limit" not in filters:
        filters["limit"] = payload.get("limit")
    return filters


def _topic_query_terms(topic_intent: dict[str, Any], topic: str) -> list[str]:
    expanded_queries = _ensure_list(topic_intent.get("expanded_queries"))
    if expanded_queries:
        return expanded_queries
    return [topic] if topic else []


def run_topic_search(
    payload: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
    external_search_callable=None,
) -> dict[str, Any]:
    resolved_config = dict(config or load_create_studio_config())
    topic = _normalize_text(payload.get("topic"))
    if not topic:
        raise ValueError("topic is required")

    topic_intent = build_topic_intent(topic).to_dict()
    filters = _normalize_filters(payload)
    search_payload = {
        "query": _normalize_text(topic_intent.get("normalized_topic")) or topic,
        "query_terms": _topic_query_terms(topic_intent, topic),
        "topic_intent": topic_intent,
        "filters": filters,
        "source_scope": _ensure_list(filters.get("source_scope")),
    }
    search_result = search_creation_candidates(
        search_payload,
        external_search_callable=external_search_callable,
        config=resolved_config,
    )
    return {
        "ok": True,
        "topic": topic,
        "topic_intent": topic_intent,
        "search_result": search_result,
    }


def _result_digest(item: dict[str, Any]) -> dict[str, Any]:
    metrics = dict(item.get("metrics") or {})
    raw = dict(item.get("raw") or {})
    return {
        "source_id": _normalize_text(item.get("source_id") or item.get("id")),
        "title": _normalize_text(item.get("title")),
        "summary": _normalize_text(item.get("summary")),
        "why_pick": _normalize_text(item.get("why_pick")),
        "source": _normalize_text(item.get("source")),
        "channel": _normalize_text(item.get("channel")),
        "published_at": _normalize_text(item.get("published_at")),
        "url": _normalize_text(item.get("url")),
        "content_type": _normalize_text(item.get("content_type")) or "other",
        "bucket": _normalize_text(item.get("bucket")) or "watch",
        "text_excerpt": _normalize_text(item.get("text"))[:320],
        "scores": {
            "relevance_score": metrics.get("relevance_score"),
            "fts_score": metrics.get("fts_score"),
            "embedding_score": metrics.get("embedding_score"),
            "structured_score": metrics.get("structured_score"),
        },
        "event_packet_refs": list(raw.get("event_packet_refs") or []),
        "event_candidate_ids": list(raw.get("event_candidate_ids") or []),
    }


def _packet_article(item: dict[str, Any]) -> dict[str, Any]:
    raw = dict(item.get("raw") or {})
    search_explain = dict((item.get("metrics") or {}).get("search_explain") or {})
    return {
        "source_id": _normalize_text(item.get("source_id") or item.get("id")),
        "title": _normalize_text(item.get("title")),
        "summary": _normalize_text(item.get("summary")),
        "body_text": _normalize_text(item.get("text")),
        "url": _normalize_text(item.get("url")),
        "source": _normalize_text(item.get("source")),
        "channel": _normalize_text(item.get("channel")),
        "content_type": _normalize_text(item.get("content_type")) or "other",
        "published_at": _normalize_text(item.get("published_at")),
        "why_pick": _normalize_text(item.get("why_pick")),
        "related_topics": list(raw.get("related_topics") or []),
        "tags": list(raw.get("tags") or []),
        "event_packet_refs": list(raw.get("event_packet_refs") or []),
        "event_candidate_ids": list(raw.get("event_candidate_ids") or []),
        "search_explain": search_explain,
    }


def _packet_summary(search_result: dict[str, Any], topic_intent: dict[str, Any]) -> dict[str, Any]:
    results = list(search_result.get("results") or [])
    grouped = dict(search_result.get("grouped") or {})
    top_titles = [_normalize_text(item.get("title")) for item in results[:5] if _normalize_text(item.get("title"))]
    return {
        "total_results": int(search_result.get("count") or len(results)),
        "strong_results": len(grouped.get("strong") or []),
        "watch_results": len(grouped.get("watch") or []),
        "top_titles": top_titles,
        "entities": _ensure_list(topic_intent.get("entities")),
        "facets": _ensure_list(topic_intent.get("topic_facets")),
    }


def _render_topic_packet_markdown(packet: dict[str, Any]) -> str:
    lines = [
        f"# Topic Packet: {packet.get('topic') or 'Untitled Topic'}",
        "",
        f"- Packet ID: {packet.get('packet_id') or ''}",
        f"- Generated At: {packet.get('generated_at') or ''}",
        f"- Query: {packet.get('query_text') or ''}",
        f"- Results: {((packet.get('summary') or {}).get('total_results') or 0)}",
        "",
        "## Topic Understanding",
        f"- Normalized Topic: {((packet.get('topic_intent') or {}).get('normalized_topic') or '')}",
        f"- Entities: {', '.join(_ensure_list((packet.get('topic_intent') or {}).get('entities'))) or '-'}",
        f"- Facets: {', '.join(_ensure_list((packet.get('topic_intent') or {}).get('topic_facets'))) or '-'}",
        "",
        "## Search Summary",
        f"- Strong Results: {((packet.get('summary') or {}).get('strong_results') or 0)}",
        f"- Watch Results: {((packet.get('summary') or {}).get('watch_results') or 0)}",
        "",
        "## Result Digests",
    ]
    for index, item in enumerate(list(packet.get("results") or []), start=1):
        lines.extend(
            [
                "",
                f"### {index}. {item.get('title') or item.get('source_id') or 'Untitled'}",
                f"- Source: {item.get('source') or '-'} / {item.get('content_type') or '-'}",
                f"- Published At: {item.get('published_at') or '-'}",
                f"- Why Pick: {item.get('why_pick') or '-'}",
                f"- Summary: {item.get('summary') or item.get('text_excerpt') or '-'}",
                f"- URL: {item.get('url') or '-'}",
            ]
        )
        event_refs = list(item.get("event_packet_refs") or [])
        if event_refs:
            lines.append(f"- Event Refs: {', '.join(event_refs)}")
    return "\n".join(lines).strip() + "\n"


def _packet_exports(packet: dict[str, Any]) -> dict[str, str]:
    return {
        "markdown": _render_topic_packet_markdown(packet),
        "json": json.dumps(packet, ensure_ascii=False, indent=2, sort_keys=True),
    }


def create_topic_packet(
    payload: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
    external_search_callable=None,
) -> dict[str, Any]:
    resolved_config = dict(config or load_create_studio_config())
    search_payload = dict(payload.get("topic_search") or {})
    if not search_payload:
        search_payload = dict(payload)
    search_response = run_topic_search(
        search_payload,
        config=resolved_config,
        external_search_callable=external_search_callable,
    )
    topic = _normalize_text(search_response.get("topic"))
    topic_intent = dict(search_response.get("topic_intent") or {})
    search_result = dict(search_response.get("search_result") or {})
    packet_id = _normalize_text(payload.get("packet_id")) or build_id("TP")
    packet_status = _normalize_text(payload.get("status")) or "ready"
    packet = {
        "packet_id": packet_id,
        "packet_type": "topic",
        "topic": topic,
        "status": packet_status,
        "query_text": _normalize_text(search_result.get("query")) or topic,
        "topic_intent": topic_intent,
        "summary": _packet_summary(search_result, topic_intent),
        "results": [_result_digest(item) for item in list(search_result.get("results") or [])],
        "articles": [_packet_article(item) for item in list(search_result.get("results") or [])],
        "query_explain": dict(search_result.get("query_explain") or {}),
        "search_observability": dict(search_result.get("observability") or {}),
        "filters": dict(search_result.get("filters") or {}),
        "generated_at": _now_iso(),
    }

    create_studio_db_path = _normalize_text(((resolved_config.get("indexing") or {}).get("content_index_db_path")))
    if not create_studio_db_path:
        raise ValueError("create studio index db path is missing from config")
    store = CreateStudioStore(create_studio_db_path)
    store.initialize()
    record = store.save_topic_packet(
        {
            "packet_id": packet_id,
            "topic": topic,
            "status": packet_status,
            "query_text": packet["query_text"],
            "packet": packet,
        }
    )
    return {
        "ok": True,
        "topic_packet": {
            **packet,
            "created_at": _normalize_text(record.get("created_at")),
            "updated_at": _normalize_text(record.get("updated_at")),
        },
        "record": record,
        "exports": _packet_exports(
            {
                **packet,
                "created_at": _normalize_text(record.get("created_at")),
                "updated_at": _normalize_text(record.get("updated_at")),
            }
        ),
    }


def get_topic_packet_detail(packet_id: str, *, config: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved_config = dict(config or load_create_studio_config())
    create_studio_db_path = _normalize_text(((resolved_config.get("indexing") or {}).get("content_index_db_path")))
    if not create_studio_db_path:
        raise ValueError("create studio index db path is missing from config")

    with _connect(create_studio_db_path) as conn:
        row = conn.execute(
            """
            SELECT packet_id, topic, status, query_text, packet_json, created_at, updated_at
            FROM topic_packets
            WHERE packet_id = ?
            LIMIT 1
            """,
            (_normalize_text(packet_id),),
        ).fetchone()
    if not row:
        raise ValueError(f"topic packet not found: {packet_id}")

    packet = _parse_json(row["packet_json"], {})
    if not isinstance(packet, dict):
        packet = {}
    packet.setdefault("packet_id", _normalize_text(row["packet_id"]))
    packet.setdefault("topic", _normalize_text(row["topic"]))
    packet.setdefault("status", _normalize_text(row["status"]))
    packet.setdefault("query_text", _normalize_text(row["query_text"]))
    packet.setdefault("created_at", _normalize_text(row["created_at"]))
    packet.setdefault("updated_at", _normalize_text(row["updated_at"]))

    return {
        "ok": True,
        "topic_packet": packet,
        "exports": _packet_exports(packet),
    }
