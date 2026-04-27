from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from creation_models import (
    ArticleDraft,
    CitationList,
    CreationIntent,
    CreationPacket,
    CreationTask,
    DEFAULT_CREATION_PACKET_VERSION,
    DEFAULT_WRITER_SKILL,
    EvidencePack,
    NarrativePlan,
    OutlinePacket,
    RetrievalBatch,
    WriterJob,
    build_id,
    ensure_list,
    gate_1_ready,
    now_iso,
)
from article_writer_service import generate_article_from_packet
from creation_autofill_service import generate_target_autofill
from creation_target_registry import resolve_creation_target
from creation_store import CreationStore
from topic_intent_service import build_topic_intent
from writer_adapter_registry import build_writer_ready_packet, resolve_writer_profile


class CreationValidationError(ValueError):
    pass


TASK_STATUSES_REQUIRING_GATE_1 = {
    "retrieval_ready",
    "retrieving",
    "citation_ready",
    "outline_ready",
    "writer_ready",
}

ALLOWED_WRITER_JOB_STATUSES = {"draft", "ready", "submitted", "completed", "failed"}

PLATFORM_PROVIDER_MAP = {
    "x": "x",
    "wechat": "wechat",
    "youtube": "youtube",
    "bilibili": "bilibili",
    "feed": "feed",
    "github": "github",
}

SOURCE_SCOPE_CONTENT_TYPE_MAP = {
    "analysis_cards": {"analysis_card"},
    "analysis_card": {"analysis_card"},
    "raw_articles": {"raw_article"},
    "raw_article": {"raw_article"},
    "transcripts": {"transcript"},
    "transcript": {"transcript"},
}
CONTENT_TYPE_SOURCE_SCOPE_MAP = {
    "analysis_card": "analysis_cards",
    "raw_article": "raw_articles",
    "transcript": "transcripts",
}

MIN_CITABLE_BODY_LENGTH = 8
SHORT_FORM_BODY_READY_PLATFORMS = {"x", "twitter"}


class CreationWorkspaceService:
    def __init__(self, data_root: str | Path | None = None):
        default_root = Path(__file__).resolve().parents[1] / "data" / "creation"
        configured_root = data_root or os.getenv("CONTENT_SEARCH_CREATION_DATA_ROOT") or default_root
        self.store = CreationStore(configured_root)

    def _serialize_bundle(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        retrieval_batch = self.get_retrieval_batch(task_id)
        citation_list = self.store.latest_by_task("citation_lists", task_id)
        outline_packet = self.store.latest_by_task("outline_packets", task_id)
        writer_jobs = self.store.find_by_task("writer_jobs", task_id)
        article_drafts = self.store.find_by_task("article_drafts", task_id)
        return {
            "ok": True,
            "workspace_name": "Creation Workspace",
            "task": task,
            "retrieval_batch": retrieval_batch,
            "citation_list": citation_list,
            "outline_packet": outline_packet,
            "writer_jobs": writer_jobs,
            "article_drafts": article_drafts,
            "gates": self._build_gate_report(task_id),
        }

    def _save_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["updated_at"] = now_iso()
        self.store.save("tasks", payload)
        return payload

    def _get_imported_materials(self, task_id: str) -> list[dict[str, Any]]:
        retrieval_batch = self.get_retrieval_batch(task_id) or {}
        results = list(retrieval_batch.get("results") or [])
        kept = []
        for item in results:
            decision = str(item.get("decision") or "").strip().lower()
            classification = str(item.get("classification") or "").strip().lower()
            if decision in {"keep", "primary", "supporting"} or classification in {"primary", "secondary", "supporting"}:
                kept.append(dict(item))
        return kept or [dict(item) for item in results]

    def _build_topic_intent_payload(self, task: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        topic = str(payload.get("topic") or task.get("topic") or "").strip()
        if not topic:
            return {
                "topic": "",
                "normalized_topic": "",
                "topic_facets": [],
                "entities": [],
                "expanded_queries": [],
            }
        return build_topic_intent(topic).to_dict()

    def _normalize_query_terms(self, task: dict[str, Any], payload: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
        query_terms = ensure_list(payload.get("query_terms"))
        if query_terms:
            return query_terms, self._build_topic_intent_payload(task, payload)
        topic_intent = self._build_topic_intent_payload(task, payload)
        expanded_queries = ensure_list(topic_intent.get("expanded_queries"))
        if expanded_queries:
            return expanded_queries, topic_intent
        fallback = [str(task.get("topic") or "").strip(), str(task.get("angle") or "").strip()]
        return [item for item in fallback if item], topic_intent

    def _resolve_platform_provider(self, task: dict[str, Any], payload: dict[str, Any]) -> str:
        platform = str(payload.get("platform") or task.get("platform") or "").strip().lower()
        return PLATFORM_PROVIDER_MAP.get(platform, "")

    def _resolve_source_scope(self, task: dict[str, Any], payload: dict[str, Any], filters: dict[str, Any]) -> list[str]:
        candidates = ensure_list(payload.get("source_scope"))
        if not candidates:
            candidates = ensure_list(filters.get("source_scope"))
        if not candidates:
            candidates = ensure_list(task.get("source_scope"))
        resolved: list[str] = []
        for item in candidates:
            normalized = str(item or "").strip().lower()
            if not normalized or normalized in resolved:
                continue
            resolved.append(normalized)
        return resolved

    def _apply_source_scope(self, results: list[dict[str, Any]], source_scope: list[str]) -> list[dict[str, Any]]:
        allowed_types: set[str] = set()
        for scope in source_scope:
            allowed_types.update(SOURCE_SCOPE_CONTENT_TYPE_MAP.get(scope, set()))
        if not allowed_types:
            return results
        return [item for item in results if str(item.get("content_type") or "").strip().lower() in allowed_types]

    def _normalize_nested_payload(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))

    def _resolve_result_text(self, item: dict[str, Any], raw: dict[str, Any]) -> str:
        candidates = [
            item.get("text"),
            item.get("content"),
            raw.get("content"),
            raw.get("transcript_text"),
            raw.get("transcript_excerpt"),
        ]
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text:
                return text
        return ""

    def _resolve_raw_body_text(self, raw: dict[str, Any]) -> str:
        for candidate in [raw.get("content"), raw.get("transcript_text"), raw.get("transcript_excerpt")]:
            text = str(candidate or "").strip()
            if text:
                return text
        return ""

    def _resolve_material_platform(self, item: dict[str, Any], raw: dict[str, Any]) -> str:
        for candidate in [
            item.get("channel"),
            item.get("platform"),
            raw.get("platform"),
            raw.get("channel"),
            item.get("source"),
            raw.get("source_name"),
        ]:
            normalized = str(candidate or "").strip().lower()
            if normalized:
                return normalized
        return ""

    def _resolve_material_body_state(self, item: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
        direct_body_text = str(item.get("body_text") or "").strip() or self._resolve_raw_body_text(raw)
        resolved_text = self._resolve_result_text(item, raw)
        platform = self._resolve_material_platform(item, raw)
        effective_body_text = direct_body_text
        body_ready = bool(effective_body_text)

        # Short-form platforms such as X should treat the post text itself as a
        # usable body when it is already long enough to cite.
        if not body_ready and platform in SHORT_FORM_BODY_READY_PLATFORMS and len(resolved_text) >= MIN_CITABLE_BODY_LENGTH:
            effective_body_text = resolved_text
            body_ready = True

        return {
            "platform": platform,
            "resolved_text": resolved_text,
            "body_text": effective_body_text,
            "body_ready": body_ready,
            "recommend_full_fetch": "" if body_ready else "yes",
            "body_status": "ready" if body_ready else "missing",
        }

    def _normalize_retrieval_result_body_flags(self, item: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(item or {})
        raw = self._normalize_nested_payload(normalized.get("raw"))
        body_state = self._resolve_material_body_state(normalized, raw)
        platform = str(body_state.get("platform") or "").strip().lower()
        current_recommend = str(normalized.get("recommend_full_fetch") or "").strip().lower()

        # Only fix stale short-form records that were previously marked with the
        # WeChat-style full-fetch rule even though the post text itself is already
        # enough to cite. Keep existing strict rules for other platforms.
        if platform in SHORT_FORM_BODY_READY_PLATFORMS and bool(body_state.get("body_ready")):
            normalized["text"] = str(body_state.get("body_text") or body_state.get("resolved_text") or "").strip()
            normalized["recommend_full_fetch"] = ""
            raw["content"] = str(body_state.get("body_text") or "").strip()
            raw["body_fetch_ok"] = True
            raw["body_status"] = "ready"
            raw["platform"] = platform
        elif current_recommend in {"", "body_missing"} and not str(raw.get("content") or "").strip():
            # Keep lightweight normalization for records that never explicitly
            # required a strict full fetch but are missing synchronized raw flags.
            raw["body_fetch_ok"] = bool(body_state.get("body_ready"))
            raw["body_status"] = str(body_state.get("body_status") or "missing")
            if body_state.get("platform"):
                raw["platform"] = str(body_state.get("platform") or "").strip()
            if body_state.get("body_ready") and not str(normalized.get("text") or "").strip():
                normalized["text"] = str(body_state.get("body_text") or body_state.get("resolved_text") or "").strip()
        normalized["raw"] = raw
        return normalized

    def _normalize_retrieval_batch_results(self, batch: dict[str, Any]) -> dict[str, Any]:
        normalized_batch = dict(batch or {})
        original_results = list(normalized_batch.get("results") or [])
        normalized_results = [self._normalize_retrieval_result_body_flags(item) for item in original_results]
        if normalized_results != original_results:
            normalized_batch["results"] = normalized_results
            return self._save_retrieval_batch(normalized_batch)
        return normalized_batch

    def _primary_requires_full_fetch(self, item: dict[str, Any]) -> bool:
        return str(item.get("recommend_full_fetch") or "").strip().lower() == "yes"

    def _evaluate_primary_citation_source(self, item: dict[str, Any]) -> dict[str, Any]:
        source_id = str(item.get("source_id") or "").strip() or "未命名来源"
        raw = self._normalize_nested_payload(item.get("raw"))
        raw_body_text = self._resolve_raw_body_text(raw)
        resolved_text = self._resolve_result_text(item, raw)
        if self._primary_requires_full_fetch(item):
            if raw.get("body_fetch_ok") is False:
                return {"pass": False, "reason": f"{source_id}：正文未就绪"}
            if not raw_body_text:
                return {"pass": False, "reason": f"{source_id}：缺少可引用正文"}
            if len(raw_body_text) < MIN_CITABLE_BODY_LENGTH:
                return {"pass": False, "reason": f"{source_id}：正文过短"}
            return {"pass": True, "quote_text": raw_body_text}
        if not resolved_text:
            return {"pass": False, "reason": f"{source_id}：缺少可引用正文"}
        return {"pass": True, "quote_text": raw_body_text or resolved_text}

    def _collect_primary_citation_candidates(
        self,
        results: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        primary_candidates = [
            item
            for item in list(results or [])
            if str(item.get("decision") or "") == "keep" and str(item.get("classification") or "") == "primary"
        ]
        citable_candidates: list[dict[str, Any]] = []
        invalid_reasons: list[str] = []
        for item in primary_candidates:
            evaluation = self._evaluate_primary_citation_source(item)
            if evaluation.get("pass"):
                next_item = dict(item)
                next_item["_quote_text"] = evaluation.get("quote_text") or ""
                citable_candidates.append(next_item)
            else:
                invalid_reasons.append(str(evaluation.get("reason") or "").strip())
        return primary_candidates, citable_candidates, [reason for reason in invalid_reasons if reason]

    def _normalize_search_result(self, item: dict[str, Any], index: int) -> dict[str, Any]:
        source_id = str(
            item.get("source_id") or item.get("id") or item.get("note_id") or item.get("url") or f"SRC-{index + 1:03d}"
        ).strip()
        raw = self._normalize_nested_payload(item.get("raw"))
        return {
            "source_id": source_id,
            "title": str(item.get("title") or source_id).strip(),
            "url": str(item.get("url") or "").strip(),
            "source": str(item.get("source") or item.get("channel") or "").strip(),
            "channel": str(item.get("channel") or "").strip(),
            "published_at": str(item.get("published_at") or item.get("created_at") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
            "why_pick": str(item.get("why_pick") or item.get("reason") or "").strip(),
            "bucket": str(item.get("bucket") or "").strip(),
            "content_type": str(item.get("content_type") or "other").strip() or "other",
            "text": self._resolve_result_text(item, raw),
            "recommend_full_fetch": str(item.get("recommend_full_fetch") or "").strip(),
            "matched_handle": str(item.get("matched_handle") or "").strip(),
            "author": str(item.get("author") or raw.get("author") or "").strip(),
            "created_at": str(item.get("created_at") or "").strip(),
            "lang": str(item.get("lang") or raw.get("transcript_language") or "").strip(),
            "metrics": self._normalize_nested_payload(item.get("metrics")),
            "raw": raw,
            "decision": str(item.get("decision") or "").strip(),
            "decision_reason": str(item.get("decision_reason") or "").strip(),
            "classification": str(item.get("classification") or "").strip(),
            "classification_reason": str(item.get("classification_reason") or "").strip(),
        }

    def _infer_source_scope_from_packet(self, packet: dict[str, Any]) -> list[str]:
        resolved: list[str] = []
        for article in list(packet.get("articles") or []):
            content_type = str(article.get("content_type") or "").strip().lower()
            scope = CONTENT_TYPE_SOURCE_SCOPE_MAP.get(content_type, "")
            if scope and scope not in resolved:
                resolved.append(scope)
        return resolved

    def _collect_packet_event_refs(self, packet: dict[str, Any]) -> list[str]:
        resolved: list[str] = []
        for article in list(packet.get("articles") or []):
            for item in list(article.get("event_packet_refs") or []):
                ref = str(item or "").strip()
                if ref and ref not in resolved:
                    resolved.append(ref)
        return resolved

    def _build_packet_task_metadata(self, packet: dict[str, Any]) -> dict[str, Any]:
        packet_id = str(packet.get("packet_id") or "").strip()
        packet_title = str(packet.get("topic") or packet.get("title") or "").strip()
        summary = dict(packet.get("summary") or {})
        return {
            "source_packet": {
                "packet_id": packet_id,
                "packet_type": str(packet.get("packet_type") or "topic").strip() or "topic",
                "topic": packet_title,
                "status": str(packet.get("status") or "").strip(),
                "generated_at": str(packet.get("generated_at") or packet.get("created_at") or "").strip(),
            },
            "packet_preview": {
                "query_text": str(packet.get("query_text") or packet_title).strip(),
                "top_titles": list(summary.get("top_titles") or []),
                "total_results": int(summary.get("total_results") or len(list(packet.get("articles") or []))),
                "strong_results": int(summary.get("strong_results") or 0),
                "watch_results": int(summary.get("watch_results") or 0),
            },
            "topic_intent": self._normalize_nested_payload(packet.get("topic_intent")),
            "event_packet_refs": self._collect_packet_event_refs(packet),
        }

    def _pick_primary_packet_article_index(self, articles: list[dict[str, Any]]) -> int:
        for index, article in enumerate(articles):
            if str(article.get("evidence_role") or "").strip().lower() == "primary":
                return index
        for index, article in enumerate(articles):
            body_text = str(article.get("body_text") or "").strip()
            if len(body_text) >= MIN_CITABLE_BODY_LENGTH:
                return index
        return 0

    def _packet_retrieval_result(
        self,
        packet: dict[str, Any],
        digest_map: dict[str, dict[str, Any]],
        article: dict[str, Any],
        index: int,
        primary_index: int,
    ) -> dict[str, Any]:
        source_id = str(article.get("source_id") or f"PKT-{index + 1:03d}").strip() or f"PKT-{index + 1:03d}"
        digest = dict(digest_map.get(source_id) or {})
        raw_payload = {
            "content": str(article.get("body_text") or "").strip(),
            "channel": str(article.get("channel") or digest.get("channel") or "").strip(),
        }
        body_state = self._resolve_material_body_state(
            {
                **digest,
                **article,
                "text": str(article.get("body_text") or digest.get("text_excerpt") or article.get("summary") or "").strip(),
                "channel": str(article.get("channel") or digest.get("channel") or "").strip(),
            },
            raw_payload,
        )
        body_text = str(body_state.get("body_text") or "").strip()
        article_count = max(len(digest_map), len(list(packet.get("articles") or [])))
        keep_cutoff = 1 if article_count <= 1 else 2
        if index == primary_index:
            decision = "keep"
            classification = "primary"
            decision_reason = "从资料包转入任务时默认保留为主资料"
            classification_reason = "资料包中的首要支撑材料"
        elif index < keep_cutoff:
            decision = "keep"
            classification = "secondary"
            decision_reason = "从资料包转入任务时默认保留为辅助资料"
            classification_reason = "资料包中的补充材料"
        else:
            decision = "exclude"
            classification = "secondary"
            decision_reason = "资料包转任务时先收起低优先级候选"
            classification_reason = "后备材料"
        return {
            "source_id": source_id,
            "title": str(article.get("title") or digest.get("title") or source_id).strip(),
            "url": str(article.get("url") or digest.get("url") or "").strip(),
            "source": str(article.get("source") or digest.get("source") or "").strip(),
            "channel": str(article.get("channel") or digest.get("channel") or "").strip(),
            "published_at": str(article.get("published_at") or digest.get("published_at") or "").strip(),
            "summary": str(article.get("summary") or digest.get("summary") or "").strip(),
            "why_pick": str(article.get("why_pick") or digest.get("why_pick") or "").strip(),
            "bucket": str(digest.get("bucket") or ("strong" if decision == "keep" else "watch")).strip() or "watch",
            "content_type": str(article.get("content_type") or digest.get("content_type") or "other").strip() or "other",
            "text": body_text or str(body_state.get("resolved_text") or digest.get("text_excerpt") or article.get("summary") or "").strip(),
            "recommend_full_fetch": str(body_state.get("recommend_full_fetch") or "").strip(),
            "matched_handle": "",
            "author": "",
            "created_at": str(packet.get("generated_at") or now_iso()).strip(),
            "lang": "",
            "metrics": {
                **dict(digest.get("scores") or {}),
                "search_explain": self._normalize_nested_payload(article.get("search_explain")),
            },
            "raw": {
                "content": body_text,
                "tags": list(article.get("tags") or []),
                "related_topics": list(article.get("related_topics") or []),
                "event_packet_refs": list(article.get("event_packet_refs") or []),
                "event_candidate_ids": list(article.get("event_candidate_ids") or []),
                "body_fetch_ok": bool(body_state.get("body_ready")),
                "body_status": str(body_state.get("body_status") or "missing"),
                "platform": str(body_state.get("platform") or "").strip(),
                "source_packet_id": str(packet.get("packet_id") or "").strip(),
                "source_packet_type": str(packet.get("packet_type") or "topic").strip() or "topic",
            },
            "decision": decision,
            "decision_reason": decision_reason,
            "classification": classification,
            "classification_reason": classification_reason,
        }

    def _nighthawk_selected_item_to_retrieval_result(
        self,
        item: dict[str, Any],
        primary_raw_item_ids: set[int],
    ) -> dict[str, Any]:
        body_state = self._resolve_material_body_state(
            {
                "body_text": str(item.get("body_text") or "").strip(),
                "text": str(item.get("body_text") or item.get("summary") or "").strip(),
                "summary": str(item.get("summary") or "").strip(),
                "channel": str(item.get("platform") or "").strip(),
                "platform": str(item.get("platform") or "").strip(),
                "source": str(item.get("source_name") or "").strip(),
            },
            {
                "content": str(item.get("body_text") or "").strip(),
                "platform": str(item.get("platform") or "").strip(),
            },
        )
        raw_item_id = int(item.get("raw_item_id") or 0)
        return {
            "source_id": str(item.get("object_uid") or item.get("raw_item_id") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "url": str(item.get("canonical_url") or "").strip(),
            "source": str(item.get("source_name") or "").strip() or "NightHawk",
            "channel": str(item.get("platform") or "").strip(),
            "published_at": str(item.get("published_at") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
            "why_pick": "从 NightHawk 正文资料池直接送入创作",
            "bucket": "raw_articles",
            "content_type": str(item.get("source_kind") or "raw_article").strip() or "raw_article",
            "text": str(body_state.get("body_text") or body_state.get("resolved_text") or "").strip(),
            "recommend_full_fetch": str(body_state.get("recommend_full_fetch") or "").strip(),
            "decision": "keep",
            "decision_reason": "用户从 NightHawk 正文池手动保留",
            "classification": "primary" if raw_item_id in primary_raw_item_ids else "secondary",
            "classification_reason": "主资料" if raw_item_id in primary_raw_item_ids else "辅助资料",
            "raw": {
                "body_fetch_ok": bool(body_state.get("body_ready")),
                "content": str(body_state.get("body_text") or "").strip(),
                "body_status": str(body_state.get("body_status") or "missing"),
                "platform": str(body_state.get("platform") or "").strip(),
                "source_kind": str(item.get("source_kind") or "").strip(),
                "raw_item_id": raw_item_id,
                "event_links": list(item.get("event_links") or []),
            },
        }

    def _build_gate_report(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            not_found = {"pass": False, "reasons": [f"任务不存在：{task_id}"]}
            return {
                "gate_1_ready": not_found,
                "gate_2_retrieval_ready": not_found,
                "gate_3_citation_ready": not_found,
                "gate_4_outline_ready": not_found,
                "gate_5_writer_ready": not_found,
            }
        return {
            "gate_1_ready": self.validate_creation_task_gate(task),
            "gate_2_retrieval_ready": self.validate_retrieval_gate(task_id),
            "gate_3_citation_ready": self.validate_citation_gate(task_id),
            "gate_4_outline_ready": self.validate_outline_gate(task_id),
            "gate_5_writer_ready": self.validate_writer_gate(task_id),
        }

    def _get_retrieval_batch(self, task_id: str, batch_id: str | None = None) -> dict[str, Any]:
        batch = self.store.get("retrieval_batches", batch_id) if batch_id else self.store.latest_by_task("retrieval_batches", task_id)
        if not batch or str(batch.get("creation_task_id") or "") != task_id:
            raise CreationValidationError(f"检索批次不存在：{task_id}")
        return self._normalize_retrieval_batch_results(batch)

    def _save_retrieval_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        batch["updated_at"] = now_iso()
        self.store.save("retrieval_batches", batch)
        task = self.get_task(str(batch.get("creation_task_id") or "").strip())
        if task:
            gate = self.validate_retrieval_gate(task["id"])
            task["status"] = "retrieval_ready" if gate.get("pass") else "retrieving"
            self._save_task(task)
        return batch

    def _update_result_in_batch(
        self,
        task_id: str,
        batch_id: str,
        source_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        batch = dict(self._get_retrieval_batch(task_id, batch_id))
        normalized_source_id = str(source_id or "").strip()
        updated_results = []
        found = False
        for item in list(batch.get("results") or []):
            next_item = dict(item)
            if str(next_item.get("source_id") or "").strip() == normalized_source_id:
                next_item.update(updates)
                found = True
            updated_results.append(next_item)
        if not found:
            raise CreationValidationError(f"检索结果不存在：{normalized_source_id}")
        batch["results"] = updated_results
        return self._save_retrieval_batch(batch)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        return self.store.get("tasks", task_id)

    def list_tasks(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.store.list("tasks", limit=limit)

    def delete_task(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        for object_type in ["retrieval_batches", "citation_lists", "outline_packets", "writer_jobs", "article_drafts"]:
            self.store.delete_by_task(object_type, task_id)
        self.store.delete_writer_packets(task_id)
        self.store.delete_article_drafts(task_id)
        self.store.delete("tasks", task_id)
        return {"ok": True, "task_id": task_id}

    def autofill_task_target(self, task_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        requested_target_id = str(
            payload.get("creation_target_id")
            or ((task.get("metadata") or {}).get("creation_target_id"))
            or "khazix-wechat"
        ).strip() or "khazix-wechat"
        target = resolve_creation_target(requested_target_id)
        materials = self._get_imported_materials(task_id)
        if not materials:
            raise CreationValidationError("当前素材包里还没有可用于智能填写的文章")

        autofill_result = generate_target_autofill(
            task=task,
            materials=materials,
            target=target,
        )
        fields = dict(autofill_result.get("fields") or {})
        metadata = dict(task.get("metadata") or {})
        metadata["creation_target_id"] = target["id"]
        metadata["creation_target_autofill"] = {
            "target_id": target["id"],
            "generated_at": str(autofill_result.get("generated_at") or now_iso()).strip(),
            "source": str(autofill_result.get("source") or "").strip(),
            "provider": str(autofill_result.get("provider") or "").strip(),
            "model": str(autofill_result.get("model") or "").strip(),
            "warning": str(autofill_result.get("warning") or "").strip(),
            "material_count": int(autofill_result.get("material_count") or len(materials)),
        }
        updated_task = self.update_task(
            task_id,
            {
                "angle": str(fields.get("angle") or "").strip(),
                "topic_reason": str(fields.get("topic_reason") or "").strip(),
                "opening_hook": str(fields.get("opening_hook") or "").strip(),
                "personal_observations": str(fields.get("personal_observations") or "").strip(),
                "hkr_focus": ensure_list(fields.get("hkr_focus")),
                "creation_target_id": target["id"],
                "preferred_writer_skill": str(target.get("writer_skill") or DEFAULT_WRITER_SKILL).strip() or DEFAULT_WRITER_SKILL,
                "style_notes": ensure_list(fields.get("user_voice_notes")),
                "banned_patterns": ensure_list(fields.get("banned_patterns")),
                "metadata": metadata,
            },
        )

        latest_writer_job = self.get_latest_writer_job(task_id)
        if latest_writer_job:
            latest_writer_job = dict(latest_writer_job)
            latest_writer_job["article_archetype"] = str(fields.get("article_archetype") or "").strip()
            latest_writer_job["optional_followups"] = ensure_list(fields.get("optional_followups"))
            latest_writer_job["user_voice_notes"] = ensure_list(fields.get("user_voice_notes"))
            latest_writer_job["banned_patterns"] = ensure_list(fields.get("banned_patterns"))
            latest_writer_job["writer_skill"] = str(target.get("writer_skill") or latest_writer_job.get("writer_skill") or DEFAULT_WRITER_SKILL).strip() or DEFAULT_WRITER_SKILL
            latest_writer_job["updated_at"] = now_iso()
            self.store.save("writer_jobs", latest_writer_job)

        return {
            "ok": True,
            "task": updated_task,
            "autofill": autofill_result,
            "bundle": self.get_task_bundle(task_id),
        }

    def get_retrieval_batch(self, task_id: str) -> dict[str, Any] | None:
        batch = self.store.latest_by_task("retrieval_batches", task_id)
        if not batch:
            return None
        return self._normalize_retrieval_batch_results(batch)

    def get_retrieval_view(self, task_id: str) -> dict[str, Any]:
        retrieval_batch = self.get_retrieval_batch(task_id)
        if not retrieval_batch:
            raise CreationValidationError(f"检索批次不存在：{task_id}")
        return {"ok": True, "retrieval_batch": retrieval_batch, "gates": self._build_gate_report(task_id)}

    def get_primary_source_fetch_candidate(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        batch = self._get_retrieval_batch(task_id)
        results = list(batch.get("results") or [])
        primary_candidates, citable_primary_candidates, invalid_primary_reasons = self._collect_primary_citation_candidates(results)
        if not primary_candidates:
            raise CreationValidationError("Gate 2 校验失败：缺少可生成引用的主资料")
        if citable_primary_candidates:
            raise CreationValidationError("Gate 3 已通过：主资料正文已就绪，无需补正文")

        for item in primary_candidates:
            if not self._primary_requires_full_fetch(item):
                continue
            evaluation = self._evaluate_primary_citation_source(item)
            if evaluation.get("pass"):
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            return {
                "ok": True,
                "source_id": str(item.get("source_id") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "url": url,
                "reason": str(evaluation.get("reason") or "").strip(),
                "gates": self._build_gate_report(task_id),
            }

        if invalid_primary_reasons:
            raise CreationValidationError(f"Gate 3 校验失败：{'；'.join(invalid_primary_reasons)}")
        raise CreationValidationError("Gate 3 校验失败：缺少可补正文的主资料链接")

    def get_retrieval_result_fetch_candidate(self, task_id: str, batch_id: str, source_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        batch = self._get_retrieval_batch(task_id, batch_id)
        normalized_source_id = str(source_id or "").strip()
        if not normalized_source_id:
            raise CreationValidationError("缺少 source_id，无法发起补正文")

        target_item: dict[str, Any] | None = None
        for item in list(batch.get("results") or []):
            if str(item.get("source_id") or "").strip() == normalized_source_id:
                target_item = dict(item)
                break
        if not target_item:
            raise CreationValidationError(f"检索结果不存在：{normalized_source_id}")

        raw = self._normalize_nested_payload(target_item.get("raw"))
        raw_body_text = self._resolve_raw_body_text(raw)
        resolved_text = self._resolve_result_text(target_item, raw)
        requires_full_fetch = self._primary_requires_full_fetch(target_item)
        if requires_full_fetch:
            evaluation = self._evaluate_primary_citation_source(target_item)
            if evaluation.get("pass"):
                raise CreationValidationError("正文已就绪，无需再抓取")
            reason = str(evaluation.get("reason") or "正文未就绪").strip() or "正文未就绪"
        else:
            if raw_body_text or resolved_text:
                raise CreationValidationError("正文已就绪，无需再抓取")
            reason = f"{normalized_source_id}：缺少可用正文"

        url = str(target_item.get("url") or "").strip()
        if not url:
            raise CreationValidationError("当前素材缺少原文链接，无法发起抓正文")

        return {
            "ok": True,
            "task_id": task_id,
            "batch_id": batch_id,
            "source_id": normalized_source_id,
            "title": str(target_item.get("title") or normalized_source_id).strip(),
            "url": url,
            "reason": reason,
            "gates": self._build_gate_report(task_id),
        }

    def get_citation_list(self, task_id: str) -> dict[str, Any] | None:
        return self.store.latest_by_task("citation_lists", task_id)

    def get_outline_packet(self, task_id: str) -> dict[str, Any] | None:
        return self.store.latest_by_task("outline_packets", task_id)

    def get_latest_writer_job(self, task_id: str) -> dict[str, Any] | None:
        return self.store.latest_by_task("writer_jobs", task_id)

    def validate_creation_task_gate(self, task: dict[str, Any]) -> dict[str, Any]:
        reasons = [
            f"缺少必填字段：{field}"
            for field in ["topic", "platform", "audience", "goal"]
            if not str(task.get(field) or "").strip()
        ]
        return {"pass": not reasons, "reasons": reasons}

    def validate_retrieval_gate(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            return {"pass": False, "reasons": [f"任务不存在：{task_id}"]}
        trigger_type = str(task.get("trigger_type") or "").strip()
        reasons = []
        batch = self.get_retrieval_batch(task_id)
        results = list((batch or {}).get("results") or [])
        if not results:
            reasons.append("尚未形成检索候选")
        primary_candidates, citable_primary_candidates, invalid_primary_reasons = self._collect_primary_citation_candidates(results)
        has_excluded = any(str(item.get("decision") or "") == "exclude" for item in results)
        if not primary_candidates:
            reasons.append("缺少主资料")
        elif not citable_primary_candidates:
            reasons.append("缺少可生成引用的主资料")
            reasons.extend(invalid_primary_reasons)
        if not has_excluded and trigger_type != "nighthawk_sources":
            reasons.append("缺少剔除结果")
        return {"pass": not reasons, "reasons": reasons}

    def validate_citation_gate(self, task_id: str) -> dict[str, Any]:
        citation_list = self.get_citation_list(task_id)
        reasons = []
        citations = list((citation_list or {}).get("citations") or [])
        if not citations:
            reasons.append("引用清单为空")
        return {"pass": not reasons, "reasons": reasons}

    def validate_outline_gate(self, task_id: str) -> dict[str, Any]:
        outline_packet = self.get_outline_packet(task_id)
        reasons = []
        outline = list((outline_packet or {}).get("outline") or [])
        if not outline:
            reasons.append("框架包为空")
        return {"pass": not reasons, "reasons": reasons}

    def validate_writer_gate(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            return {"pass": False, "reasons": [f"任务不存在：{task_id}"]}
        reasons = list(self.validate_creation_task_gate(task).get("reasons") or [])
        reasons.extend(self.validate_citation_gate(task_id).get("reasons") or [])
        reasons.extend(self.validate_outline_gate(task_id).get("reasons") or [])
        return {"pass": not reasons, "reasons": reasons}

    def _require_gate(self, gate_name: str, gate_result: dict[str, Any]) -> None:
        if gate_result.get("pass"):
            return
        raise CreationValidationError(f"{gate_name} 校验失败：{'；'.join(gate_result.get('reasons') or [])}")

    def _resolve_preferred_writer_skill(
        self,
        payload: dict[str, Any] | None = None,
        task: dict[str, Any] | None = None,
    ) -> str:
        payload = payload or {}
        task = task or {}
        task_metadata = dict(task.get("metadata") or {})
        preferred = str(
            payload.get("preferred_writer_skill")
            or payload.get("writer_skill")
            or task_metadata.get("preferred_writer_skill")
            or DEFAULT_WRITER_SKILL
        ).strip()
        return preferred or DEFAULT_WRITER_SKILL

    def _merge_preferred_writer_metadata(
        self,
        metadata: dict[str, Any] | None,
        *,
        preferred_writer_skill: str,
    ) -> dict[str, Any]:
        merged = dict(metadata or {})
        merged["preferred_writer_skill"] = str(preferred_writer_skill or DEFAULT_WRITER_SKILL).strip() or DEFAULT_WRITER_SKILL
        return merged

    def _merge_creation_target_inputs(
        self,
        metadata: dict[str, Any] | None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        merged = dict(metadata or {})
        payload = payload or {}
        target_inputs = dict(merged.get("creation_target_inputs") or {})
        for key in ["topic_reason", "opening_hook", "creation_target_id", "personal_observations", "hkr_focus", "target_word_count"]:
            if key in payload:
                if key == "hkr_focus":
                    target_inputs[key] = ensure_list(payload.get(key))
                else:
                    target_inputs[key] = str(payload.get(key) or "").strip()
        if target_inputs:
            merged["creation_target_inputs"] = target_inputs
        return merged

    def create_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        created_at = now_iso()
        preferred_writer_skill = self._resolve_preferred_writer_skill(payload)
        task = CreationTask(
            id=build_id("CT"),
            trigger_type=str(payload.get("trigger_type") or "manual_topic").strip() or "manual_topic",
            topic=str(payload.get("topic") or "").strip(),
            platform=str(payload.get("platform") or "wechat").strip() or "wechat",
            audience=str(payload.get("audience") or "").strip(),
            goal=str(payload.get("goal") or "").strip(),
            angle=str(payload.get("angle") or "").strip(),
            style_notes=ensure_list(payload.get("style_notes")),
            banned_patterns=ensure_list(payload.get("banned_patterns")),
            source_scope=ensure_list(payload.get("source_scope")),
            status=str(payload.get("status") or "draft").strip() or "draft",
            metadata=self._merge_preferred_writer_metadata(
                payload.get("metadata"),
                preferred_writer_skill=preferred_writer_skill,
            ),
            created_at=created_at,
            updated_at=created_at,
        ).to_dict()
        self.store.save("tasks", task)

        retrieval_batch = RetrievalBatch(
            id=build_id("RB"),
            creation_task_id=task["id"],
            query_terms=[task["topic"]] if task["topic"] else [],
            created_at=created_at,
            updated_at=created_at,
        ).to_dict()
        citation_list = CitationList(
            id=build_id("CL"),
            creation_task_id=task["id"],
            created_at=created_at,
            updated_at=created_at,
        ).to_dict()
        outline_packet = OutlinePacket(
            id=build_id("OP"),
            creation_task_id=task["id"],
            angle=task.get("angle") or task.get("topic") or "",
            created_at=created_at,
            updated_at=created_at,
        ).to_dict()
        writer_job = WriterJob(
            id=build_id("WJ"),
            creation_task_id=task["id"],
            writer_skill=preferred_writer_skill,
            citation_list_id=citation_list["id"],
            outline_packet_id=outline_packet["id"],
            banned_patterns=task.get("banned_patterns") or [],
            created_at=created_at,
            updated_at=created_at,
        ).to_dict()

        self.store.save("retrieval_batches", retrieval_batch)
        self.store.save("citation_lists", citation_list)
        self.store.save("outline_packets", outline_packet)
        self.store.save("writer_jobs", writer_job)
        return self._serialize_bundle(task["id"])

    def create_task_from_packet(self, packet: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        packet_type = str(packet.get("packet_type") or "topic").strip() or "topic"
        if packet_type not in {"topic", "event"}:
            raise CreationValidationError(f"暂不支持从 {packet_type} packet 直接转任务")

        topic = str(packet.get("topic") or packet.get("title") or "").strip()
        if not topic:
            raise CreationValidationError("资料包缺少可用标题，不能转任务")

        inferred_scope = self._infer_source_scope_from_packet(packet)
        metadata = self._build_packet_task_metadata(packet)
        provided_metadata = dict(payload.get("metadata") or {})
        metadata.update(provided_metadata)
        default_trigger_type = "event_packet" if packet_type == "event" else "topic_packet"
        default_goal = (
            "基于事件资料包沉淀正式创作任务，并继续进入引用 / 大纲 / Writer 链路"
            if packet_type == "event"
            else "基于资料包沉淀正式创作任务，并继续进入引用 / 大纲 / Writer 链路"
        )
        default_audience = (
            "希望快速把热点事件推进成可写文章的中文内容创作者"
            if packet_type == "event"
            else "希望快速进入创作编排的中文内容创作者"
        )
        created_bundle = self.create_task(
            {
                "trigger_type": str(payload.get("trigger_type") or default_trigger_type).strip() or default_trigger_type,
                "topic": topic,
                "platform": str(payload.get("platform") or "wechat").strip() or "wechat",
                "audience": str(payload.get("audience") or default_audience).strip() or default_audience,
                "goal": str(payload.get("goal") or default_goal).strip() or default_goal,
                "angle": str(payload.get("angle") or topic).strip() or topic,
                "style_notes": ensure_list(payload.get("style_notes")),
                "banned_patterns": ensure_list(payload.get("banned_patterns")),
                "source_scope": ensure_list(payload.get("source_scope")) or inferred_scope,
                "preferred_writer_skill": self._resolve_preferred_writer_skill(payload),
                "metadata": metadata,
            }
        )

        task_id = str(((created_bundle.get("task") or {}).get("id")) or "").strip()
        if not task_id:
            raise CreationValidationError("资料包转任务失败：未生成 task_id")

        retrieval_batch = self.get_retrieval_batch(task_id)
        if not retrieval_batch:
            raise CreationValidationError(f"资料包转任务失败：未找到检索批次 {task_id}")

        articles = list(packet.get("articles") or [])
        digest_map = {
            str(item.get("source_id") or "").strip(): dict(item)
            for item in list(packet.get("results") or [])
            if str(item.get("source_id") or "").strip()
        }
        primary_index = self._pick_primary_packet_article_index(articles) if articles else 0
        retrieval_batch["query_terms"] = ensure_list(((packet.get("topic_intent") or {}).get("expanded_queries"))) or [topic]
        retrieval_batch["topic_intent"] = self._normalize_nested_payload(packet.get("topic_intent"))
        retrieval_batch["filters"] = dict(packet.get("filters") or {})
        retrieval_batch["results"] = [
            self._packet_retrieval_result(packet, digest_map, article, index, primary_index)
            for index, article in enumerate(articles)
        ]
        retrieval_batch["status"] = "retrieved"
        retrieval_batch["notes"] = [f"由 {packet_type.title()} Packet 直接转换生成"]
        self._save_retrieval_batch(retrieval_batch)

        bundle = self.get_task_bundle(task_id)
        return {
            "ok": True,
            "packet_id": str(packet.get("packet_id") or "").strip(),
            "packet_type": packet_type,
            "creation_task": bundle.get("task") or {},
            "bundle": bundle,
            "next_url": (
                f"/create/workspace?task_id={task_id}"
                f"&source=packet&packet_id={str(packet.get('packet_id') or '').strip()}"
            ),
        }

    def create_task_from_nighthawk_sources(
        self,
        creation_packet: dict[str, Any],
        selected_items: list[dict[str, Any]],
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = payload or {}
        creation_intent = dict(creation_packet.get("creation_intent") or {})
        topic = str(creation_intent.get("topic") or payload.get("topic") or "").strip()
        if not topic:
            raise CreationValidationError("NightHawk source selection is missing topic")

        source_trace = dict(creation_packet.get("source_trace") or {})
        metadata = {
            "source_packet": {
                "packet_kind": str(creation_packet.get("packet_kind") or "creation_packet"),
                "source_type": str(source_trace.get("source_type") or "nighthawk_raw_items"),
                "raw_item_ids": list(source_trace.get("raw_item_ids") or []),
                "event_candidate_ids": list(source_trace.get("event_candidate_ids") or []),
            },
            "creation_packet_preview": {
                "topic": topic,
                "body_ready_count": int((((creation_packet.get("evidence_pack") or {}).get("summary") or {}).get("body_ready_count")) or 0),
                "selected_count": int((((creation_packet.get("metadata") or {}).get("selected_count")) or len(selected_items))),
            },
        }
        metadata.update(dict(payload.get("metadata") or {}))

        created_bundle = self.create_task(
            {
                "trigger_type": str(payload.get("trigger_type") or "nighthawk_sources").strip() or "nighthawk_sources",
                "topic": topic,
                "platform": str(payload.get("platform") or creation_intent.get("platform") or "wechat").strip() or "wechat",
                "audience": str(payload.get("audience") or creation_intent.get("audience") or "希望从正文直接进入创作编排的内容创作者").strip()
                or "希望从正文直接进入创作编排的内容创作者",
                "goal": str(payload.get("goal") or creation_intent.get("goal") or "把 NightHawk 正文整理成正式创作任务，并进入后续编排").strip()
                or "把 NightHawk 正文整理成正式创作任务，并进入后续编排",
                "angle": str(payload.get("angle") or creation_intent.get("angle") or topic).strip() or topic,
                "style_notes": ensure_list(payload.get("style_notes")) or ensure_list(creation_intent.get("style_notes")),
                "banned_patterns": ensure_list(payload.get("banned_patterns")) or ensure_list(creation_intent.get("banned_patterns")),
                "source_scope": ensure_list(payload.get("source_scope")) or ensure_list(creation_intent.get("source_scope")) or ["raw_articles"],
                "preferred_writer_skill": self._resolve_preferred_writer_skill(payload),
                "metadata": metadata,
            }
        )

        task_id = str(((created_bundle.get("task") or {}).get("id")) or "").strip()
        if not task_id:
            raise CreationValidationError("NightHawk source selection failed: missing task_id")

        retrieval_batch = self.get_retrieval_batch(task_id)
        if not retrieval_batch:
            raise CreationValidationError(f"NightHawk source selection failed: retrieval batch missing for {task_id}")

        primary_raw_item_ids = {
            int(item.get("raw_item_id") or 0)
            for item in list(((creation_packet.get("evidence_pack") or {}).get("citations")) or [])
            if str(item.get("usage_scope") or "").strip().lower() == "must_use" and int(item.get("raw_item_id") or 0) > 0
        }
        if not primary_raw_item_ids and selected_items:
            primary_raw_item_ids = {int(selected_items[0].get("raw_item_id") or 0)}

        retrieval_batch["query_terms"] = ensure_list(((creation_intent.get("topic_intent") or {}).get("expanded_queries"))) or [topic]
        retrieval_batch["topic_intent"] = self._normalize_nested_payload(creation_intent.get("topic_intent"))
        retrieval_batch["filters"] = {"source_scope": ["raw_articles"], "source_type": "nighthawk"}
        retrieval_batch["results"] = [
            self._nighthawk_selected_item_to_retrieval_result(item, primary_raw_item_ids)
            for item in selected_items
        ]
        retrieval_batch["status"] = "retrieved"
        retrieval_batch["notes"] = ["由 NightHawk 正文资料池直接创建"]
        self._save_retrieval_batch(retrieval_batch)

        bundle = self.get_task_bundle(task_id)
        return {
            "ok": True,
            "creation_task": bundle.get("task") or {},
            "bundle": bundle,
            "creation_packet": creation_packet,
            "next_url": f"/create/workspace?task_id={task_id}&source=nighthawk",
        }

    def update_task(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.get_task(task_id)
        if not current:
            raise CreationValidationError(f"任务不存在：{task_id}")
        updated = dict(current)
        for key in [
            "trigger_type",
            "topic",
            "platform",
            "audience",
            "goal",
            "angle",
            "status",
        ]:
            if key in payload:
                updated[key] = str(payload.get(key) or "").strip()
        for key in ["style_notes", "banned_patterns", "source_scope"]:
            if key in payload:
                updated[key] = ensure_list(payload.get(key))
        if "metadata" in payload:
            updated["metadata"] = dict(payload.get("metadata") or {})
        if "preferred_writer_skill" in payload or "writer_skill" in payload:
            preferred_writer_skill = self._resolve_preferred_writer_skill(payload, updated)
            updated["metadata"] = self._merge_preferred_writer_metadata(
                updated.get("metadata"),
                preferred_writer_skill=preferred_writer_skill,
            )
        if any(
            key in payload
            for key in ["topic_reason", "opening_hook", "creation_target_id", "personal_observations", "hkr_focus", "target_word_count"]
        ):
            updated["metadata"] = self._merge_creation_target_inputs(updated.get("metadata"), payload)
        desired_status = str(updated.get("status") or "").strip()
        if desired_status in TASK_STATUSES_REQUIRING_GATE_1:
            self._require_gate("Gate 1", self.validate_creation_task_gate(updated))
        return self._save_task(updated)

    def get_task_bundle(self, task_id: str) -> dict[str, Any]:
        return self._serialize_bundle(task_id)

    def run_retrieval(self, task_id: str, payload: dict[str, Any], search_callable) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        self._require_gate("Gate 1", self.validate_creation_task_gate(task))
        current = self.get_retrieval_batch(task_id)
        if not current:
            current = RetrievalBatch(id=build_id("RB"), creation_task_id=task_id).to_dict()
        query_terms, topic_intent = self._normalize_query_terms(task, payload)
        filters = dict(payload.get("filters") or {})
        provider = self._resolve_platform_provider(task, payload)
        source_scope = self._resolve_source_scope(task, payload, filters)
        if source_scope:
            filters["source_scope"] = list(source_scope)
        search_payload = {
            "query": " ".join(query_terms),
            "query_terms": query_terms,
            "filters": filters,
            "task_id": task_id,
            "topic": task.get("topic") or "",
            "topic_intent": topic_intent,
            "platform": str(task.get("platform") or "").strip(),
            "source_scope": source_scope,
        }
        if provider:
            search_payload["provider"] = provider
            search_payload["providers"] = [provider]
        raw_result = search_callable(search_payload) if callable(search_callable) else {"results": []}
        normalized_results = [
            self._normalize_search_result(item, index)
            for index, item in enumerate(list((raw_result or {}).get("results") or []))
        ]
        normalized_results = self._apply_source_scope(normalized_results, source_scope)
        updated = dict(current)
        updated["query_terms"] = query_terms
        updated["topic_intent"] = topic_intent
        updated["filters"] = filters
        updated["results"] = normalized_results
        updated["status"] = "retrieved"
        updated["notes"] = ensure_list(payload.get("notes"))
        self._save_retrieval_batch(updated)
        return {"ok": True, "retrieval_batch": updated, "gates": self._build_gate_report(task_id)}

    def mark_retrieval_keep(self, task_id: str, batch_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        batch = self._update_result_in_batch(
            task_id,
            batch_id,
            str(payload.get("source_id") or "").strip(),
            {
                "decision": "keep",
                "decision_reason": str(payload.get("reason") or "").strip(),
            },
        )
        return {"ok": True, "retrieval_batch": batch, "gates": self._build_gate_report(task_id)}

    def mark_retrieval_exclude(self, task_id: str, batch_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        batch = self._update_result_in_batch(
            task_id,
            batch_id,
            str(payload.get("source_id") or "").strip(),
            {
                "decision": "exclude",
                "decision_reason": str(payload.get("reason") or "").strip(),
            },
        )
        return {"ok": True, "retrieval_batch": batch, "gates": self._build_gate_report(task_id)}

    def classify_retrieval_result(self, task_id: str, batch_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        batch = self._update_result_in_batch(
            task_id,
            batch_id,
            str(payload.get("source_id") or "").strip(),
            {
                "classification": str(payload.get("classification") or "").strip(),
                "classification_reason": str(payload.get("reason") or "").strip(),
            },
        )
        return {"ok": True, "retrieval_batch": batch, "gates": self._build_gate_report(task_id)}

    def save_citation_list(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.store.latest_by_task("citation_lists", task_id)
        if not current:
            raise CreationValidationError(f"引用清单不存在：{task_id}")
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        self._require_gate("Gate 1", self.validate_creation_task_gate(task))
        updated = dict(current)
        updated["citations"] = list(payload.get("citations") or [])
        updated["status"] = str(payload.get("status") or "ready").strip() or "ready"
        updated["updated_at"] = now_iso()
        self.store.save("citation_lists", updated)
        task["status"] = "citation_ready"
        self._save_task(task)
        return updated

    def generate_citation_list(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        batch = self._get_retrieval_batch(task_id)
        primary_candidates, citable_primary_candidates, invalid_primary_reasons = self._collect_primary_citation_candidates(
            list(batch.get("results") or [])
        )
        if not primary_candidates:
            raise CreationValidationError("Gate 2 校验失败：缺少可生成引用的主资料")
        if not citable_primary_candidates:
            raise CreationValidationError(f"Gate 3 校验失败：{'；'.join(invalid_primary_reasons or ['缺少可引用正文'])}")
        current = self.get_citation_list(task_id)
        if not current:
            current = CitationList(id=build_id("CL"), creation_task_id=task_id).to_dict()
        citations = []
        for item in citable_primary_candidates:
            citations.append(
                {
                    "citation_id": build_id("C"),
                    "source_id": item.get("source_id") or "",
                    "title": item.get("title") or "",
                    "source": item.get("source") or "",
                    "url": item.get("url") or "",
                    "claim_type": "fact",
                    "normalized_claim": item.get("summary") or item.get("text") or item.get("title") or "",
                    "usage_scope": "support",
                    "quote_text": item.get("_quote_text") or item.get("text") or item.get("summary") or "",
                    "why_pick": item.get("why_pick") or "",
                }
            )
        updated = dict(current)
        updated["citations"] = citations
        updated["status"] = "ready"
        updated["updated_at"] = now_iso()
        self.store.save("citation_lists", updated)
        task["status"] = "citation_ready"
        self._save_task(task)
        return {"ok": True, "citation_list": updated, "gates": self._build_gate_report(task_id)}

    def update_citation(self, task_id: str, citation_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.get_citation_list(task_id)
        if not current:
            raise CreationValidationError(f"引用清单不存在：{task_id}")
        updated = dict(current)
        next_citations = []
        found = False
        for citation in list(current.get("citations") or []):
            next_item = dict(citation)
            if str(next_item.get("citation_id") or "") == citation_id:
                for key in ["normalized_claim", "usage_scope", "claim_type", "quote_text", "why_pick"]:
                    if key in payload:
                        next_item[key] = str(payload.get(key) or "").strip()
                found = True
            next_citations.append(next_item)
        if not found:
            raise CreationValidationError(f"引用不存在：{citation_id}")
        updated["citations"] = next_citations
        updated["updated_at"] = now_iso()
        self.store.save("citation_lists", updated)
        return {"ok": True, "citation_list": updated, "gates": self._build_gate_report(task_id)}

    def save_outline_packet(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.store.latest_by_task("outline_packets", task_id)
        if not current:
            raise CreationValidationError(f"框架包不存在：{task_id}")
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        self._require_gate("Gate 1", self.validate_creation_task_gate(task))
        updated = dict(current)
        updated["core_judgement"] = str(payload.get("core_judgement") or updated.get("core_judgement") or "").strip()
        updated["angle"] = str(payload.get("angle") or updated.get("angle") or "").strip()
        updated["content_template"] = str(payload.get("content_template") or updated.get("content_template") or "").strip()
        updated["hook_candidates"] = ensure_list(payload.get("hook_candidates")) or updated.get("hook_candidates") or []
        updated["title_candidates"] = ensure_list(payload.get("title_candidates")) or updated.get("title_candidates") or []
        updated["outline"] = list(payload.get("outline") or updated.get("outline") or [])
        updated["status"] = str(payload.get("status") or "ready").strip() or "ready"
        updated["updated_at"] = now_iso()
        self.store.save("outline_packets", updated)
        task["status"] = "outline_ready"
        self._save_task(task)
        return updated

    def generate_outline_packet(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        self._require_gate("Gate 3", self.validate_citation_gate(task_id))
        current = self.get_outline_packet(task_id)
        if not current:
            current = OutlinePacket(id=build_id("OP"), creation_task_id=task_id).to_dict()
        citation_list = self.get_citation_list(task_id) or {}
        citation_ids = [item.get("citation_id") for item in list(citation_list.get("citations") or []) if item.get("citation_id")]
        core_judgement = str(payload.get("core_judgement") or task.get("topic") or "").strip()
        angle = str(payload.get("angle") or task.get("angle") or task.get("topic") or "").strip()
        content_template = str(payload.get("content_template") or "方法论型").strip() or "方法论型"
        outline = [
            {
                "section_id": "S1",
                "heading": "先下判断",
                "summary": core_judgement or "先把核心判断说透。",
                "citation_refs": citation_ids[:1],
            },
            {
                "section_id": "S2",
                "heading": "拆问题",
                "summary": "解释为什么单模型输出不够稳定，以及编排层怎么补位。",
                "citation_refs": citation_ids[:1],
            },
            {
                "section_id": "S3",
                "heading": "给方法",
                "summary": "把检索、证据和写作拆开，才能形成可复用的创作链路。",
                "citation_refs": citation_ids[:1],
            },
        ]
        updated = dict(current)
        updated["core_judgement"] = core_judgement
        updated["angle"] = angle
        updated["content_template"] = content_template
        updated["hook_candidates"] = [f"别再把模型强弱，当成内容生产的全部差距。"]
        opening_hook = str(payload.get("opening_hook") or "").strip()
        if opening_hook:
            updated["hook_candidates"] = [opening_hook]
        updated["title_candidates"] = [
            f"{task.get('topic') or '创作编排层'}：真正值钱的不是模型，而是编排层",
            f"为什么内容团队最后拼的是编排层，而不是单次提示词",
        ]
        updated["outline"] = outline
        updated["status"] = "ready"
        updated["updated_at"] = now_iso()
        self.store.save("outline_packets", updated)
        task["status"] = "outline_ready"
        self._save_task(task)
        return {"ok": True, "outline_packet": updated, "gates": self._build_gate_report(task_id)}

    def bootstrap_packet_task(self, task_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"浠诲姟涓嶅瓨鍦細{task_id}")

        citation_payload = self.generate_citation_list(task_id)
        outline_payload = self.generate_outline_packet(
            task_id,
            {
                "core_judgement": str(payload.get("core_judgement") or task.get("topic") or "").strip(),
                "opening_hook": str(payload.get("opening_hook") or "").strip(),
                "content_template": str(payload.get("content_template") or "鏂规硶璁哄瀷").strip() or "鏂规硶璁哄瀷",
            },
        )
        return {
            "ok": True,
            "task_id": task_id,
            "citation_list": citation_payload.get("citation_list") or {},
            "outline_packet": outline_payload.get("outline_packet") or {},
            "bundle": self.get_task_bundle(task_id),
            "gates": self._build_gate_report(task_id),
        }

    def _build_creation_intent(self, task: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        return CreationIntent(
            trigger_type=str(task.get("trigger_type") or "").strip(),
            topic=str(task.get("topic") or "").strip(),
            platform=str(task.get("platform") or "").strip(),
            audience=str(task.get("audience") or "").strip(),
            goal=str(task.get("goal") or "").strip(),
            creation_mode="topic_driven",
            angle=str(payload.get("angle") or task.get("angle") or "").strip(),
            article_archetype=str(payload.get("article_archetype") or "").strip(),
            primary_output=ensure_list(payload.get("primary_output")) or ["long_article"],
            optional_followups=ensure_list(payload.get("optional_followups")),
            source_scope=ensure_list(task.get("source_scope")),
            style_notes=ensure_list(payload.get("user_voice_notes")) or ensure_list(task.get("style_notes")),
            banned_patterns=ensure_list(payload.get("banned_patterns")) or ensure_list(task.get("banned_patterns")),
            topic_intent=self._build_topic_intent_payload(task, payload),
            metadata={
                "task_status": str(task.get("status") or "").strip(),
                "task_metadata": dict(task.get("metadata") or {}),
            },
        ).to_dict()

    def _build_evidence_pack(
        self,
        task: dict[str, Any],
        retrieval_batch: dict[str, Any] | None,
        citation_list: dict[str, Any] | None,
    ) -> dict[str, Any]:
        citations = list((citation_list or {}).get("citations") or [])
        results = list((retrieval_batch or {}).get("results") or [])
        kept_results = [
            item
            for item in results
            if str(item.get("decision") or item.get("classification") or "").strip().lower() in {"keep", "primary", "supporting"}
        ]
        must_use_count = sum(
            1
            for citation in citations
            if str(citation.get("usage_scope") or "").strip().lower() == "must_use"
        )
        return EvidencePack(
            retrieval_batch_id=str((retrieval_batch or {}).get("id") or "").strip(),
            citation_list_id=str((citation_list or {}).get("id") or "").strip(),
            citations=citations,
            result_ids=[
                str(item.get("result_id") or item.get("id") or "").strip()
                for item in results
                if str(item.get("result_id") or item.get("id") or "").strip()
            ],
            source_scope=ensure_list(task.get("source_scope")),
            summary={
                "total_results": len(results),
                "kept_results": len(kept_results),
                "total_citations": len(citations),
                "must_use_citations": must_use_count,
            },
        ).to_dict()

    def _build_narrative_plan(
        self,
        outline_packet: dict[str, Any] | None,
        evidence_pack: dict[str, Any],
    ) -> dict[str, Any]:
        outline_packet = outline_packet or {}
        outline = list(outline_packet.get("outline") or [])
        risks: list[str] = []
        if not str(outline_packet.get("core_judgement") or "").strip():
            risks.append("core_judgement_missing")
        if int((evidence_pack.get("summary") or {}).get("must_use_citations") or 0) == 0:
            risks.append("must_use_citation_missing")

        recommended_sections = [
            str(item.get("section") or item.get("title") or "").strip()
            for item in outline
            if str(item.get("section") or item.get("title") or "").strip()
        ]
        recommended_opening = str(((outline_packet.get("hook_candidates") or [""])[0]) or "").strip()
        return NarrativePlan(
            outline_packet_id=str(outline_packet.get("id") or "").strip(),
            core_judgement=str(outline_packet.get("core_judgement") or "").strip(),
            angle=str(outline_packet.get("angle") or "").strip(),
            content_template=str(outline_packet.get("content_template") or "").strip(),
            hook_candidates=list(outline_packet.get("hook_candidates") or []),
            title_candidates=list(outline_packet.get("title_candidates") or []),
            outline=outline,
            recommended_opening=recommended_opening,
            recommended_sections=recommended_sections,
            risks=risks,
        ).to_dict()

    def _build_creation_packet(
        self,
        task: dict[str, Any],
        writer_job: dict[str, Any],
        citation_list: dict[str, Any],
        outline_packet: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        retrieval_batch = self.store.latest_by_task("retrieval_batches", task["id"])
        creation_intent = self._build_creation_intent(task, payload)
        evidence_pack = self._build_evidence_pack(task, retrieval_batch, citation_list)
        narrative_plan = self._build_narrative_plan(outline_packet, evidence_pack)
        return CreationPacket(
            packet_kind="creation_packet",
            version=DEFAULT_CREATION_PACKET_VERSION,
            task_id=str(task.get("id") or "").strip(),
            creation_intent=creation_intent,
            evidence_pack=evidence_pack,
            narrative_plan=narrative_plan,
            downstream_targets=[str(writer_job.get("writer_skill") or DEFAULT_WRITER_SKILL).strip()],
            source_trace={
                "source_type": "creation_task",
                "task_id": str(task.get("id") or "").strip(),
                "trigger_type": str(task.get("trigger_type") or "").strip(),
            },
            metadata={
                "writer_job_id": str(writer_job.get("id") or "").strip(),
                "citation_list_id": str(citation_list.get("id") or "").strip(),
                "outline_packet_id": str(outline_packet.get("id") or "").strip(),
            },
        ).to_dict()

    def generate_writer_job(self, task_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")
        self._require_gate("Gate 5", self.validate_writer_gate(task_id))
        citation_list = self.store.latest_by_task("citation_lists", task_id)
        outline_packet = self.store.latest_by_task("outline_packets", task_id)
        if not citation_list or not outline_packet:
            raise CreationValidationError("引用清单或框架包不存在，不能生成 writer 包")

        writer_job = WriterJob(
            id=build_id("WJ"),
            creation_task_id=task_id,
            writer_skill=self._resolve_preferred_writer_skill(payload, task),
            citation_list_id=citation_list["id"],
            outline_packet_id=outline_packet["id"],
            primary_output=ensure_list(payload.get("primary_output")) or ["long_article"],
            optional_followups=ensure_list(payload.get("optional_followups")),
            article_archetype=str(payload.get("article_archetype") or "").strip(),
            user_voice_notes=ensure_list(payload.get("user_voice_notes")) or task.get("style_notes") or [],
            banned_patterns=ensure_list(payload.get("banned_patterns")) or task.get("banned_patterns") or [],
            status="ready",
        ).to_dict()
        creation_packet = self._build_creation_packet(task, writer_job, citation_list, outline_packet, payload)
        writer_profile = resolve_writer_profile(writer_job["writer_skill"])
        packet = build_writer_ready_packet(
            task=task,
            citation_list=citation_list,
            outline_packet=outline_packet,
            writer_job=writer_job,
            creation_packet=creation_packet,
            writer_profile=writer_profile,
        )
        writer_job["packet_path"] = self.store.write_writer_packet(writer_job["id"], task_id, packet)
        writer_job["updated_at"] = now_iso()
        self.store.save("writer_jobs", writer_job)
        task["metadata"] = self._merge_preferred_writer_metadata(
            task.get("metadata"),
            preferred_writer_skill=str(writer_job.get("writer_skill") or DEFAULT_WRITER_SKILL),
        )
        task["status"] = "writer_ready"
        self._save_task(task)
        return writer_job

    def update_writer_job_status(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.get_latest_writer_job(task_id)
        if not current:
            raise CreationValidationError(f"WriterJob 不存在：{task_id}")

        next_status = str(payload.get("status") or "").strip()
        if next_status not in ALLOWED_WRITER_JOB_STATUSES:
            allowed = ", ".join(sorted(ALLOWED_WRITER_JOB_STATUSES))
            raise CreationValidationError(f"WriterJob 状态非法：{next_status}（允许：{allowed}）")

        updated = dict(current)
        updated["status"] = next_status
        updated["updated_at"] = now_iso()
        self.store.save("writer_jobs", updated)

        task = self.get_task(task_id)
        if task:
            task_status_map = {
                "draft": "draft",
                "ready": "writer_ready",
                "submitted": "writing",
                "completed": "done",
                "failed": "failed",
            }
            task["status"] = task_status_map.get(next_status, task.get("status") or "draft")
            self._save_task(task)
        return updated

    def get_writer_packet(self, task_id: str) -> dict[str, Any]:
        writer_job = self.get_latest_writer_job(task_id)
        if not writer_job:
            raise CreationValidationError(f"WriterJob 不存在：{task_id}")
        packet_path = Path(str(writer_job.get("packet_path") or "").strip())
        if not packet_path.exists():
            raise CreationValidationError(f"Writer 包不存在：{packet_path}")
        return json.loads(packet_path.read_text(encoding="utf-8"))

    def generate_article_draft(self, task_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        task = self.get_task(task_id)
        if not task:
            raise CreationValidationError(f"任务不存在：{task_id}")

        self.bootstrap_packet_task(task_id, payload)
        writer_job = self.generate_writer_job(task_id, payload)
        packet = self.get_writer_packet(task_id)
        stored_inputs = dict(((task.get("metadata") or {}).get("creation_target_inputs")) or {})
        packet["article_generation_options"] = {
            "target_word_count": str(payload.get("target_word_count") or stored_inputs.get("target_word_count") or "").strip(),
            "selected_revision_suggestions": ensure_list(payload.get("selected_revision_suggestions")),
            "regenerate_from_draft_id": str(payload.get("regenerate_from_draft_id") or "").strip(),
        }
        writer_profile = resolve_writer_profile(str(writer_job.get("writer_skill") or DEFAULT_WRITER_SKILL))
        article_payload = generate_article_from_packet(
            packet=packet,
            writer_profile=writer_profile,
        )
        draft = ArticleDraft(
            id=build_id("AD"),
            creation_task_id=task_id,
            writer_job_id=str(writer_job.get("id") or "").strip(),
            writer_skill=str(writer_job.get("writer_skill") or DEFAULT_WRITER_SKILL).strip(),
            title=str(article_payload.get("title") or task.get("topic") or "未命名文章").strip(),
            quality_report=dict(article_payload.get("quality_report") or {}),
            generation_source=str(article_payload.get("generation_source") or "").strip(),
            model=str(article_payload.get("model") or "").strip(),
            status="draft",
        ).to_dict()
        draft["article_path"] = self.store.write_article_draft(
            draft["id"],
            task_id,
            str(article_payload.get("article_markdown") or ""),
        )
        draft["updated_at"] = now_iso()
        self.store.save("article_drafts", draft)
        task["status"] = "writing"
        self._save_task(task)
        return {
            "ok": True,
            "article_draft": draft,
            "article_markdown": str(article_payload.get("article_markdown") or ""),
            "next_url": f"/create/write?draft_id={draft['id']}",
            "bundle": self.get_task_bundle(task_id),
        }

    def get_article_draft(self, draft_id: str) -> dict[str, Any]:
        draft = self.store.get("article_drafts", draft_id)
        if not draft:
            raise CreationValidationError(f"文章草稿不存在：{draft_id}")
        article_markdown = self.store.read_article_draft(str(draft.get("article_path") or ""))
        task = self.get_task(str(draft.get("creation_task_id") or "").strip())
        return {
            "ok": True,
            "article_draft": draft,
            "article_markdown": article_markdown,
            "task": task,
        }

    def save_article_draft(self, draft_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        draft = self.store.get("article_drafts", draft_id)
        if not draft:
            raise CreationValidationError(f"文章草稿不存在：{draft_id}")
        article_markdown = str(payload.get("article_markdown") or payload.get("content") or "")
        if not article_markdown.strip():
            raise CreationValidationError("文章正文不能为空")
        path = str(draft.get("article_path") or "").strip()
        if not path:
            path = self.store.write_article_draft(draft_id, str(draft.get("creation_task_id") or ""), article_markdown)
        else:
            Path(path).write_text(article_markdown, encoding="utf-8")
        updated = dict(draft)
        updated["article_path"] = path
        updated["title"] = str(payload.get("title") or updated.get("title") or "").strip()
        updated["status"] = str(payload.get("status") or updated.get("status") or "draft").strip() or "draft"
        updated["updated_at"] = now_iso()
        self.store.save("article_drafts", updated)
        return {"ok": True, "article_draft": updated, "article_markdown": article_markdown}

