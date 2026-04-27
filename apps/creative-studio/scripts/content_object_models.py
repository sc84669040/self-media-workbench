from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import yaml


BODY_READY_STATUSES = {"success", "ready", "completed", "complete", "hydrated"}
BODY_NOT_READY_STATUSES = {"none", "pending", "in_progress", "timeout", "failed", "metadata_only", "queued"}


def _ensure_text(value: Any) -> str:
    return str(value or "").strip()


def _ensure_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple | set):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _parse_json_like(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, dict | list):
        return value
    text = str(value).strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        return default


def _frontmatter_split(markdown_text: str) -> tuple[dict[str, Any], str]:
    text = str(markdown_text or "")
    if not text.startswith("---"):
        return {}, text.strip()

    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text.strip()

    closing_index = None
    for index in range(1, len(lines)):
        if lines[index].strip() == "---":
            closing_index = index
            break
    if closing_index is None:
        return {}, text.strip()

    frontmatter_text = "\n".join(lines[1:closing_index]).strip()
    body = "\n".join(lines[closing_index + 1 :]).strip()
    try:
        frontmatter = yaml.safe_load(frontmatter_text) or {}
    except Exception:  # noqa: BLE001
        frontmatter = {}
    if not isinstance(frontmatter, dict):
        frontmatter = {}
    return frontmatter, body


def _first_markdown_heading(body_text: str) -> str:
    for line in str(body_text or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip()
    return ""


def _extract_section_text(body_text: str, section_title: str) -> str:
    lines = str(body_text or "").splitlines()
    normalized_target = section_title.strip()
    capture = False
    captured: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("## "):
            heading = stripped[3:].strip()
            if capture:
                break
            capture = heading == normalized_target
            continue
        if capture:
            captured.append(line)
    return "\n".join(captured).strip()


def _extract_bullet_items(section_text: str) -> list[str]:
    items: list[str] = []
    for line in str(section_text or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            value = stripped[2:].strip()
            if value:
                items.append(value)
    return items


def _body_excerpt(body_text: str, limit: int = 180) -> str:
    compact = re.sub(r"\s+", " ", str(body_text or "")).strip()
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit].rstrip()}..."


def build_content_object_uid(
    *,
    source_kind: str,
    platform: str = "",
    source_ref: str = "",
    canonical_url: str = "",
    origin_path: str = "",
) -> str:
    stable_key = "||".join(
        [
            _ensure_text(source_kind).lower() or "unknown",
            _ensure_text(platform).lower(),
            _ensure_text(source_ref),
            _ensure_text(canonical_url),
            _ensure_text(origin_path),
        ]
    )
    digest = hashlib.sha1(stable_key.encode("utf-8")).hexdigest()[:20]
    return f"CO-{digest}"


def infer_body_ready(
    *,
    body_text: str = "",
    body_ready: Any = None,
    metadata: dict[str, Any] | None = None,
    source_kind: str = "",
    platform: str = "",
) -> bool:
    if isinstance(body_ready, bool):
        return body_ready

    metadata = dict(metadata or {})
    normalized_body = _ensure_text(body_text)
    transcript_text = _ensure_text(metadata.get("transcript_text"))
    candidate_body = normalized_body or transcript_text

    body_status = _ensure_text(metadata.get("body_status")).lower()
    body_fetch_ok = metadata.get("body_fetch_ok")

    if body_status in BODY_READY_STATUSES:
        return bool(candidate_body)
    if body_status in BODY_NOT_READY_STATUSES:
        normalized_platform = _ensure_text(platform).lower()
        if normalized_platform in {"x", "twitter"}:
            compact_body = re.sub(r"\s+", " ", candidate_body).strip()
            if len(compact_body) >= 24:
                return True
        return False
    if body_fetch_ok is False:
        return False
    if body_fetch_ok is True:
        return bool(candidate_body)

    if _ensure_text(source_kind).lower() in {"analysis_card", "knowledge_note", "transcript"}:
        return bool(candidate_body)
    return bool(candidate_body)


@dataclass
class ContentObject:
    object_uid: str
    source_kind: str
    platform: str = ""
    source_ref: str = ""
    canonical_url: str = ""
    title: str = ""
    summary: str = ""
    body_text: str = ""
    body_ready: bool = False
    published_at: str = ""
    source_name: str = ""
    tags: list[str] = field(default_factory=list)
    related_topics: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    origin_path: str = ""
    content_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_store_payload(self) -> dict[str, Any]:
        return self.to_dict()


def build_content_object(payload: dict[str, Any]) -> ContentObject:
    raw_metadata = _parse_json_like(payload.get("metadata"), {}) or {}
    metadata = dict(raw_metadata if isinstance(raw_metadata, dict) else {})
    source_kind = _ensure_text(payload.get("source_kind")) or "unknown"
    platform = _ensure_text(payload.get("platform"))
    source_ref = _ensure_text(payload.get("source_ref"))
    canonical_url = _ensure_text(payload.get("canonical_url"))
    origin_path = _ensure_text(payload.get("origin_path"))
    title = _ensure_text(payload.get("title"))
    body_text = _ensure_text(payload.get("body_text")) or _ensure_text(metadata.get("transcript_text"))
    summary = _ensure_text(payload.get("summary")) or _body_excerpt(body_text)

    object_uid = _ensure_text(payload.get("object_uid")) or build_content_object_uid(
        source_kind=source_kind,
        platform=platform,
        source_ref=source_ref,
        canonical_url=canonical_url,
        origin_path=origin_path,
    )

    content_hash = _ensure_text(payload.get("content_hash"))
    if not content_hash and body_text:
        content_hash = hashlib.sha1(body_text.encode("utf-8")).hexdigest()[:20]

    return ContentObject(
        object_uid=object_uid,
        source_kind=source_kind,
        platform=platform,
        source_ref=source_ref,
        canonical_url=canonical_url,
        title=title,
        summary=summary,
        body_text=body_text,
        body_ready=infer_body_ready(
            body_text=body_text,
            body_ready=payload.get("body_ready"),
            metadata=metadata,
            source_kind=source_kind,
            platform=platform,
        ),
        published_at=_ensure_text(payload.get("published_at")),
        source_name=_ensure_text(payload.get("source_name")),
        tags=_ensure_list(payload.get("tags")),
        related_topics=_ensure_list(payload.get("related_topics")),
        metadata=metadata,
        origin_path=origin_path,
        content_hash=content_hash,
    )


def content_object_from_raw_item(row: dict[str, Any]) -> ContentObject:
    metrics = _parse_json_like(row.get("metrics_json"), {}) or {}
    metadata = dict(metrics if isinstance(metrics, dict) else {})
    platform = _ensure_text(row.get("platform"))
    item_id = _ensure_text(row.get("item_id") or row.get("id"))
    source_handle = _ensure_text(row.get("source_handle"))
    title = _ensure_text(row.get("title"))
    content = _ensure_text(row.get("content")) or _ensure_text(metadata.get("content")) or _ensure_text(
        metadata.get("transcript_text")
    )
    source_kind = _ensure_text(metadata.get("source_kind")) or f"{platform or 'unknown'}_raw_item"
    source_ref = f"raw_item:{platform or 'unknown'}:{item_id or source_handle or title or 'unknown'}"

    if "body_status" not in metadata and row.get("body_status") is not None:
        metadata["body_status"] = row.get("body_status")
    if "source_handle" not in metadata and source_handle:
        metadata["source_handle"] = source_handle
    if "item_id" not in metadata and item_id:
        metadata["item_id"] = item_id
    if "raw_item_id" not in metadata and row.get("id") is not None:
        metadata["raw_item_id"] = row.get("id")

    summary = _body_excerpt(content or title)
    return build_content_object(
        {
            "source_kind": source_kind,
            "platform": platform,
            "source_ref": source_ref,
            "canonical_url": _ensure_text(row.get("url")) or _ensure_text(metadata.get("entry_url")),
            "title": title or summary or f"{platform} raw item",
            "summary": summary,
            "body_text": content,
            "published_at": _ensure_text(row.get("published_at")) or _ensure_text(row.get("fetched_at")),
            "source_name": _ensure_text(metadata.get("source_name")) or source_handle,
            "tags": _ensure_list(metadata.get("tags")),
            "related_topics": _ensure_list(metadata.get("related_topics")),
            "metadata": metadata,
        }
    )


def content_object_from_markdown_document(
    origin_path: str | Path,
    markdown_text: str,
    *,
    default_source_kind: str = "knowledge_note",
) -> ContentObject:
    path = Path(origin_path)
    frontmatter, body = _frontmatter_split(markdown_text)

    inferred_source_kind = _ensure_text(frontmatter.get("source_kind"))
    path_text = str(path).replace("\\", "/").lower()
    if not inferred_source_kind and (
        "分析卡片" in str(path) or "analysis-cards" in path_text or "analysis_cards" in path_text
    ):
        inferred_source_kind = "analysis_card"
    source_kind = inferred_source_kind or default_source_kind
    if source_kind == "knowledge_note":
        source_kind = "source_note"

    summary_section = _extract_section_text(body, "1. 一句话摘要")
    tags_section = _extract_section_text(body, "4. 标签")
    related_info_section = _extract_section_text(body, "0. 关联信息")
    extracted_tags = _extract_bullet_items(tags_section)

    title = (
        _ensure_text(frontmatter.get("title"))
        or _first_markdown_heading(body)
        or path.stem
    )
    summary = (
        _ensure_text(frontmatter.get("summary"))
        or _ensure_text(frontmatter.get("one_sentence_summary"))
        or _ensure_text(summary_section)
        or _body_excerpt(body)
    )
    metadata = dict(frontmatter)
    metadata.setdefault("origin_path", str(path))
    if related_info_section:
        metadata.setdefault("related_info_text", related_info_section)
    metadata.setdefault("event_packet_refs", list(metadata.get("event_packet_refs") or []))
    metadata.setdefault("cluster_ready", False)
    metadata.setdefault(
        "cluster_hints",
        {
            "related_topics": _ensure_list(frontmatter.get("related_topics") or frontmatter.get("topics")),
            "tags": _ensure_list(frontmatter.get("tags")) or extracted_tags,
            "source_kind": source_kind,
        },
    )

    return build_content_object(
        {
            "source_kind": source_kind,
            "platform": _ensure_text(frontmatter.get("platform")) or "knowledge_base",
            "source_ref": _ensure_text(frontmatter.get("source_ref")) or str(path),
            "canonical_url": (
                _ensure_text(frontmatter.get("canonical_url"))
                or _ensure_text(frontmatter.get("source_url"))
                or _ensure_text(frontmatter.get("url"))
            ),
            "title": title,
            "summary": summary,
            "body_text": body,
            "published_at": _ensure_text(frontmatter.get("published_at")) or _ensure_text(frontmatter.get("date")),
            "source_name": _ensure_text(frontmatter.get("source_name")) or _ensure_text(frontmatter.get("author")),
            "tags": _ensure_list(frontmatter.get("tags")) or extracted_tags,
            "related_topics": _ensure_list(frontmatter.get("related_topics") or frontmatter.get("topics")),
            "metadata": metadata,
            "origin_path": str(path),
        }
    )


def content_object_from_transcript_payload(payload: dict[str, Any]) -> ContentObject:
    transcript_text = _ensure_text(payload.get("transcript_text")) or _ensure_text(payload.get("body_text"))
    source_ref = _ensure_text(payload.get("source_ref")) or _ensure_text(payload.get("video_id")) or _ensure_text(
        payload.get("url")
    )
    metadata = dict(payload.get("metadata") or {})
    metadata.setdefault("transcript_language", _ensure_text(payload.get("transcript_language")))
    metadata.setdefault("transcript_kind", _ensure_text(payload.get("transcript_kind")))

    return build_content_object(
        {
            "source_kind": _ensure_text(payload.get("source_kind")) or "transcript",
            "platform": _ensure_text(payload.get("platform")) or "youtube",
            "source_ref": source_ref,
            "canonical_url": _ensure_text(payload.get("url")),
            "title": _ensure_text(payload.get("title")) or "Transcript",
            "summary": _ensure_text(payload.get("summary")) or _body_excerpt(transcript_text),
            "body_text": transcript_text,
            "published_at": _ensure_text(payload.get("published_at")),
            "source_name": _ensure_text(payload.get("source_name")) or _ensure_text(payload.get("channel")),
            "tags": _ensure_list(payload.get("tags")),
            "related_topics": _ensure_list(payload.get("related_topics")),
            "metadata": metadata,
            "origin_path": _ensure_text(payload.get("origin_path")),
        }
    )
