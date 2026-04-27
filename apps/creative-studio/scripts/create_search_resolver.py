from __future__ import annotations

from pathlib import Path
from typing import Any

from create_studio_config import load_create_studio_config
from semantic_search_service import hybrid_search_content_objects

EMPTY_CANDIDATE_PACK = {
    "ok": True,
    "provider": "create_studio",
    "providers": ["create_studio"],
    "query": "",
    "mode": "create_studio_fts",
    "count": 0,
    "results": [],
    "grouped": {"strong": [], "watch": [], "skip": []},
    "counts": {"strong": 0, "watch": 0, "skip": 0},
    "searched_handles": [],
    "success_handles": [],
    "filters": {},
    "errors": [],
    "error_count": 0,
    "observability": {},
    "per_provider": {"create_studio": {"count": 0}},
    "schema_version": "candidate-pack-v1",
    "message": "创作台本地索引暂无可用结果",
}


def _normalize_limit(params: dict[str, Any], config: dict[str, Any]) -> int:
    filters = dict(params.get("filters") or {})
    semantic_search = dict(config.get("semantic_search") or {})
    try:
        requested = int(filters.get("limit") or semantic_search.get("final_top_k") or 20)
    except Exception:  # noqa: BLE001
        requested = 20
    return max(1, requested)


def _content_type_from_object(item: dict[str, Any]) -> str:
    source_kind = str(item.get("source_kind") or "").strip().lower()
    metadata = dict(item.get("metadata") or {})
    if source_kind == "analysis_card":
        return "analysis_card"
    if source_kind == "transcript" or str(metadata.get("transcript_text") or "").strip():
        return "transcript"
    if source_kind in {"source_note", "knowledge_note"}:
        return "source_note"
    return "raw_article"


def _bucket_from_result(item: dict[str, Any]) -> str:
    explain = dict(item.get("search_explain") or {})
    structured = dict(explain.get("structured") or {})
    if structured.get("matched_entities") or structured.get("matched_facets") or structured.get("matched_query_terms"):
        return "strong"
    scores = dict(item.get("search_scores") or {})
    if float(scores.get("final_score") or 0.0) >= 0.35:
        return "strong"
    return "watch"


def _candidate_from_fts_item(item: dict[str, Any]) -> dict[str, Any]:
    content_type = _content_type_from_object(item)
    body_text = str(item.get("body_text") or "").strip()
    summary = str(item.get("summary") or "").strip()
    canonical_url = str(item.get("canonical_url") or "").strip()
    source_name = str(item.get("source_name") or "").strip()
    metadata = dict(item.get("metadata") or {})

    raw_payload = dict(metadata)
    raw_payload.setdefault("source_kind", str(item.get("source_kind") or "").strip())
    raw_payload.setdefault("body_fetch_ok", bool(item.get("body_ready")))
    raw_payload.setdefault("content", body_text)
    raw_payload.setdefault("related_topics", list(item.get("related_topics") or []))
    raw_payload.setdefault("tags", list(item.get("tags") or []))
    raw_payload.setdefault("event_packet_refs", list(metadata.get("event_packet_refs") or []))
    raw_payload.setdefault("event_candidate_ids", list(metadata.get("event_candidate_ids") or []))
    raw_payload.setdefault("search_explain", dict(item.get("search_explain") or {}))

    scores = dict(item.get("search_scores") or {})
    explain = dict(item.get("search_explain") or {})
    structured = dict(explain.get("structured") or {})
    why_parts = []
    if structured.get("matched_entities"):
        why_parts.append(f"命中实体：{', '.join(structured.get('matched_entities') or [])}")
    if structured.get("matched_facets"):
        why_parts.append(f"命中主题维度：{', '.join(structured.get('matched_facets') or [])}")
    if not why_parts:
        why_parts.append("混合召回命中")
    return {
        "id": str(item.get("object_uid") or ""),
        "source_id": str(item.get("object_uid") or ""),
        "title": str(item.get("title") or "").strip(),
        "url": canonical_url,
        "channel": str(item.get("platform") or "").strip() or "create_studio",
        "source": source_name or str(item.get("platform") or "").strip() or str(item.get("source_kind") or "").strip(),
        "published_at": str(item.get("published_at") or "").strip(),
        "summary": summary,
        "why_pick": "；".join(why_parts),
        "bucket": _bucket_from_result(item),
        "content_type": content_type,
        "text": body_text or summary,
        "recommend_full_fetch": "no" if item.get("body_ready") else "yes",
        "matched_handle": "",
        "author": source_name,
        "created_at": str(item.get("published_at") or "").strip(),
        "lang": str(metadata.get("lang") or metadata.get("transcript_language") or "").strip(),
        "metrics": {
            "relevance_score": float(scores.get("final_score") or 0.0),
            "fts_score": float(scores.get("fts_score") or 0.0),
            "embedding_score": float(scores.get("embedding_score") or 0.0),
            "structured_score": float(scores.get("structured_score") or 0.0),
            "search_explain": explain,
        },
        "raw": raw_payload,
    }


def _fts_candidate_pack(params: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    semantic_search = dict(config.get("semantic_search") or {})
    query_terms = [str(item or "").strip() for item in list(params.get("query_terms") or []) if str(item or "").strip()]
    query = str(params.get("query") or "").strip()

    search_result = hybrid_search_content_objects(
        create_studio_db_path=str((config.get("indexing") or {}).get("content_index_db_path") or "").strip(),
        query=query,
        query_terms=query_terms,
        topic_intent=dict(params.get("topic_intent") or {}),
        config=config,
    )

    results = [_candidate_from_fts_item(item) for item in list(search_result.get("items") or [])]
    grouped = {
        "strong": [item for item in results if item.get("bucket") == "strong"],
        "watch": [item for item in results if item.get("bucket") == "watch"],
        "skip": [],
    }
    return {
        "ok": True,
        "provider": "create_studio_fts",
        "providers": ["create_studio_fts"],
        "query": query,
        "mode": "create_studio_fts",
        "count": len(results),
        "results": results,
        "grouped": grouped,
        "counts": {
            "strong": len(grouped["strong"]),
            "watch": len(grouped["watch"]),
            "skip": 0,
        },
        "searched_handles": [],
        "success_handles": [],
        "filters": dict(params.get("filters") or {}),
        "errors": [],
        "error_count": 0,
        "observability": {
            "fts_refreshed": bool((search_result.get("fts_result") or {}).get("fts_refreshed")),
            "match_query": str((search_result.get("fts_result") or {}).get("match_query") or ""),
            "topic_intent": dict(params.get("topic_intent") or {}),
            "embedding_provider": str((search_result.get("query_explain") or {}).get("embedding_provider") or "disabled"),
        },
        "per_provider": {"create_studio_fts": {"count": len(results)}},
        "schema_version": "candidate-pack-v1",
        "message": f"创作台混合召回 {len(results)} 条结果",
        "query_explain": {
            "normalized_query": query,
            "query_terms": query_terms,
            "match_query": str((search_result.get("fts_result") or {}).get("match_query") or ""),
            "final_top_k": int(semantic_search.get("final_top_k") or 0),
            "embedding_provider": str((search_result.get("query_explain") or {}).get("embedding_provider") or "disabled"),
            "embedding_enabled": bool((search_result.get("query_explain") or {}).get("embedding_enabled")),
        },
    }


def search_creation_candidates(
    params: dict[str, Any],
    *,
    external_search_callable=None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_config = dict(config or load_create_studio_config())
    semantic_search = dict(resolved_config.get("semantic_search") or {})
    indexing = dict(resolved_config.get("indexing") or {})
    enable_fts = bool(semantic_search.get("enable_fts"))
    db_path = str(indexing.get("content_index_db_path") or "").strip()

    if enable_fts and db_path and Path(db_path).exists():
        try:
            return _fts_candidate_pack(params, resolved_config)
        except Exception:  # noqa: BLE001
            pass

    if callable(external_search_callable):
        return external_search_callable(params)

    payload = dict(EMPTY_CANDIDATE_PACK)
    payload["query"] = str(params.get("query") or "").strip()
    payload["filters"] = dict(params.get("filters") or {})
    return payload
