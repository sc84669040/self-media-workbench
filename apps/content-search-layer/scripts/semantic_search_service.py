from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from embedding_provider import load_embedding_provider
from fts_search_service import search_content_objects_fts


def _parse_json(value: Any, default: Any) -> Any:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        return default


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_iso_to_timestamp(value: Any) -> float:
    text = _normalize_text(value)
    if not text:
        return 0.0
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:  # noqa: BLE001
        return 0.0


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


def _source_quality_score(source_kind: str) -> float:
    normalized = _normalize_text(source_kind).lower()
    if normalized == "analysis_card":
        return 1.0
    if normalized in {"source_note", "knowledge_note"}:
        return 0.8
    if normalized == "transcript":
        return 0.65
    return 0.5


def _freshness_score(published_at: str, newest_ts: float, oldest_ts: float) -> float:
    current_ts = _parse_iso_to_timestamp(published_at)
    if current_ts <= 0 or newest_ts <= 0 or oldest_ts <= 0 or newest_ts <= oldest_ts:
        return 0.5
    return max(0.0, min(1.0, (current_ts - oldest_ts) / max(newest_ts - oldest_ts, 1.0)))


def _object_to_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "object_uid": _normalize_text(row["object_uid"]),
        "source_kind": _normalize_text(row["source_kind"]),
        "platform": _normalize_text(row["platform"]),
        "source_ref": _normalize_text(row["source_ref"]),
        "canonical_url": _normalize_text(row["canonical_url"]),
        "title": _normalize_text(row["title"]),
        "summary": _normalize_text(row["summary"]),
        "body_text": _normalize_text(row["body_text"]),
        "body_ready": bool(row["body_ready"]),
        "published_at": _normalize_text(row["published_at"]),
        "source_name": _normalize_text(row["source_name"]),
        "tags": _parse_json(row["tags_json"], []),
        "related_topics": _parse_json(row["related_topics_json"], []),
        "metadata": _parse_json(row["metadata_json"], {}),
    }


def _load_content_objects(create_studio_db_path: str | Path, limit: int) -> list[dict[str, Any]]:
    normalized_limit = max(20, int(limit or 0))
    with _connect(create_studio_db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              object_uid,
              source_kind,
              platform,
              source_ref,
              canonical_url,
              title,
              summary,
              body_text,
              body_ready,
              published_at,
              source_name,
              tags_json,
              related_topics_json,
              metadata_json
            FROM content_objects
            ORDER BY COALESCE(published_at, '') DESC, updated_at DESC, object_uid ASC
            LIMIT ?
            """,
            (normalized_limit,),
        ).fetchall()
    return [_object_to_payload(row) for row in rows]


def _load_content_objects_by_ids(create_studio_db_path: str | Path, object_uids: list[str]) -> list[dict[str, Any]]:
    normalized_ids = [str(item or "").strip() for item in object_uids if str(item or "").strip()]
    if not normalized_ids:
        return []

    placeholders = ",".join("?" for _ in normalized_ids)
    with _connect(create_studio_db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
              object_uid,
              source_kind,
              platform,
              source_ref,
              canonical_url,
              title,
              summary,
              body_text,
              body_ready,
              published_at,
              source_name,
              tags_json,
              related_topics_json,
              metadata_json
            FROM content_objects
            WHERE object_uid IN ({placeholders})
            """,
            tuple(normalized_ids),
        ).fetchall()
    return [_object_to_payload(row) for row in rows]


def _structured_score(item: dict[str, Any], *, query_terms: list[str], topic_intent: dict[str, Any], newest_ts: float, oldest_ts: float) -> tuple[float, dict[str, Any]]:
    lower_blob = " ".join(
        [
            _normalize_text(item.get("title")).lower(),
            _normalize_text(item.get("summary")).lower(),
            _normalize_text(item.get("body_text")).lower(),
            " ".join(str(part).strip().lower() for part in list(item.get("related_topics") or []) if str(part).strip()),
            " ".join(str(part).strip().lower() for part in list(item.get("tags") or []) if str(part).strip()),
        ]
    ).strip()

    normalized_query_terms = [str(term or "").strip().lower() for term in query_terms if str(term or "").strip()]
    entities = [str(term or "").strip().lower() for term in list(topic_intent.get("entities") or []) if str(term or "").strip()]
    facets = [str(term or "").strip().lower() for term in list(topic_intent.get("topic_facets") or []) if str(term or "").strip()]

    term_hits = [term for term in normalized_query_terms if term in lower_blob]
    entity_hits = [term for term in entities if term in lower_blob]
    facet_hits = [term for term in facets if term in lower_blob]
    body_ready_bonus = 1.0 if item.get("body_ready") else 0.0
    source_quality = _source_quality_score(str(item.get("source_kind") or ""))
    freshness = _freshness_score(str(item.get("published_at") or ""), newest_ts, oldest_ts)

    score = (
        0.35 * (len(term_hits) / max(len(normalized_query_terms), 1) if normalized_query_terms else 0.0)
        + 0.15 * (len(entity_hits) / max(len(entities), 1) if entities else 0.0)
        + 0.15 * (len(facet_hits) / max(len(facets), 1) if facets else 0.0)
        + 0.15 * body_ready_bonus
        + 0.12 * source_quality
        + 0.08 * freshness
    )
    explain = {
        "matched_query_terms": term_hits[:8],
        "matched_entities": entity_hits[:8],
        "matched_facets": facet_hits[:8],
        "body_ready_bonus": body_ready_bonus,
        "source_quality": round(source_quality, 4),
        "freshness": round(freshness, 4),
    }
    return round(score, 4), explain


def hybrid_search_content_objects(
    *,
    create_studio_db_path: str | Path,
    query: str,
    query_terms: list[str],
    topic_intent: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    semantic_search = dict(config.get("semantic_search") or {})
    rerank_top_k = max(5, _safe_int(semantic_search.get("rerank_top_k"), 40))
    final_top_k = max(1, _safe_int(semantic_search.get("final_top_k"), 20))

    all_objects = _load_content_objects(create_studio_db_path, limit=max(rerank_top_k * 4, 120))
    object_map = {str(item.get("object_uid") or ""): item for item in all_objects}

    fts_result = search_content_objects_fts(
        create_studio_db_path=create_studio_db_path,
        query=query,
        query_terms=query_terms,
        limit=rerank_top_k,
        body_ready_only=False,
        source_kinds=[],
    )
    fts_rank_scores = {
        str(item.get("object_uid") or ""): round(1.0 / (index + 1), 4)
        for index, item in enumerate(list(fts_result.get("items") or []))
        if str(item.get("object_uid") or "").strip()
    }

    embedding_provider = load_embedding_provider(config)
    embedding_result = embedding_provider.search(
        query=query,
        query_terms=query_terms,
        topic_intent=topic_intent,
        objects=all_objects,
        top_k=rerank_top_k,
    )
    embedding_scores = {
        str(item.get("object_uid") or ""): float(item.get("embedding_score") or 0.0)
        for item in list(embedding_result.get("items") or [])
        if str(item.get("object_uid") or "").strip()
    }
    embedding_explains = {
        str(item.get("object_uid") or ""): dict(item.get("explain") or {})
        for item in list(embedding_result.get("items") or [])
        if str(item.get("object_uid") or "").strip()
    }

    candidate_ids = {key for key in [*fts_rank_scores.keys(), *embedding_scores.keys()] if key}
    if not candidate_ids and not all_objects:
        return {
            "ok": True,
            "items": [],
            "fts_result": fts_result,
            "embedding_result": embedding_result,
            "query_explain": {
                "query": query,
                "query_terms": query_terms,
                "topic_intent": topic_intent,
                "embedding_provider": embedding_provider.provider_id,
            },
        }

    if not candidate_ids:
        candidate_ids = {str(item.get("object_uid") or "") for item in all_objects[:final_top_k] if str(item.get("object_uid") or "").strip()}

    missing_candidate_ids = [object_uid for object_uid in candidate_ids if object_uid and object_uid not in object_map]
    if missing_candidate_ids:
        for item in _load_content_objects_by_ids(create_studio_db_path, missing_candidate_ids):
            object_uid = str(item.get("object_uid") or "")
            if object_uid:
                object_map[object_uid] = item

    timestamps = [_parse_iso_to_timestamp(item.get("published_at")) for item in all_objects if _parse_iso_to_timestamp(item.get("published_at")) > 0]
    newest_ts = max(timestamps) if timestamps else 0.0
    oldest_ts = min(timestamps) if timestamps else 0.0

    reranked_items: list[dict[str, Any]] = []
    for object_uid in candidate_ids:
        base_item = dict(object_map.get(object_uid) or {})
        if not base_item:
            continue
        structured_score, structured_explain = _structured_score(
            base_item,
            query_terms=query_terms,
            topic_intent=topic_intent,
            newest_ts=newest_ts,
            oldest_ts=oldest_ts,
        )
        fts_score = float(fts_rank_scores.get(object_uid, 0.0))
        embedding_score = float(embedding_scores.get(object_uid, 0.0))
        if embedding_provider.enabled:
            final_score = 0.45 * fts_score + 0.30 * embedding_score + 0.25 * structured_score
        else:
            final_score = 0.70 * fts_score + 0.30 * structured_score

        base_item["search_scores"] = {
            "final_score": round(final_score, 4),
            "fts_score": round(fts_score, 4),
            "embedding_score": round(embedding_score, 4),
            "structured_score": round(structured_score, 4),
        }
        base_item["search_explain"] = {
            "fts": {
                "matched_via_fts": object_uid in fts_rank_scores,
                "fts_refreshed": bool(fts_result.get("fts_refreshed")),
            },
            "embedding": dict(embedding_explains.get(object_uid) or {"provider": embedding_provider.provider_id}),
            "structured": structured_explain,
        }
        reranked_items.append(base_item)

    reranked_items.sort(
        key=lambda item: (
            -float((item.get("search_scores") or {}).get("final_score") or 0.0),
            -int(bool(item.get("body_ready"))),
            -_parse_iso_to_timestamp(item.get("published_at")),
            str(item.get("object_uid") or ""),
        )
    )

    return {
        "ok": True,
        "items": reranked_items[:final_top_k],
        "fts_result": fts_result,
        "embedding_result": embedding_result,
        "query_explain": {
            "query": query,
            "query_terms": query_terms,
            "topic_intent": topic_intent,
            "embedding_provider": embedding_provider.provider_id,
            "embedding_enabled": bool(embedding_provider.enabled),
            "rerank_top_k": rerank_top_k,
            "final_top_k": final_top_k,
            "fts_match_query": str(fts_result.get("match_query") or ""),
        },
    }
