from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
import uuid

TASK_REQUIRED_FIELDS = ["topic", "platform", "audience", "goal"]
DEFAULT_WRITER_SKILL = "khazix-writer"
DEFAULT_CREATION_PACKET_VERSION = "1.0"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_id(prefix: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    suffix = uuid.uuid4().hex[:6].upper()
    return f"{prefix}-{stamp}-{suffix}"


def ensure_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


@dataclass
class CreationTask:
    id: str
    trigger_type: str
    topic: str
    platform: str
    audience: str
    goal: str
    angle: str = ""
    style_notes: list[str] = field(default_factory=list)
    banned_patterns: list[str] = field(default_factory=list)
    source_scope: list[str] = field(default_factory=list)
    status: str = "draft"
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RetrievalBatch:
    id: str
    creation_task_id: str
    query_terms: list[str] = field(default_factory=list)
    filters: dict[str, Any] = field(default_factory=dict)
    results: list[dict[str, Any]] = field(default_factory=list)
    status: str = "not_started"
    notes: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CitationList:
    id: str
    creation_task_id: str
    citations: list[dict[str, Any]] = field(default_factory=list)
    status: str = "draft"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class OutlinePacket:
    id: str
    creation_task_id: str
    core_judgement: str = ""
    angle: str = ""
    content_template: str = ""
    hook_candidates: list[str] = field(default_factory=list)
    title_candidates: list[str] = field(default_factory=list)
    outline: list[dict[str, Any]] = field(default_factory=list)
    status: str = "draft"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WriterJob:
    id: str
    creation_task_id: str
    writer_skill: str = DEFAULT_WRITER_SKILL
    citation_list_id: str = ""
    outline_packet_id: str = ""
    primary_output: list[str] = field(default_factory=lambda: ["long_article"])
    optional_followups: list[str] = field(default_factory=list)
    article_archetype: str = ""
    user_voice_notes: list[str] = field(default_factory=list)
    banned_patterns: list[str] = field(default_factory=list)
    packet_path: str = ""
    status: str = "draft"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ArticleDraft:
    id: str
    creation_task_id: str
    writer_job_id: str = ""
    writer_skill: str = DEFAULT_WRITER_SKILL
    title: str = ""
    article_path: str = ""
    quality_report: dict[str, Any] = field(default_factory=dict)
    generation_source: str = ""
    model: str = ""
    status: str = "draft"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CreationIntent:
    trigger_type: str
    topic: str
    platform: str
    audience: str
    goal: str
    creation_mode: str = "topic_driven"
    angle: str = ""
    article_archetype: str = ""
    primary_output: list[str] = field(default_factory=list)
    optional_followups: list[str] = field(default_factory=list)
    source_scope: list[str] = field(default_factory=list)
    style_notes: list[str] = field(default_factory=list)
    banned_patterns: list[str] = field(default_factory=list)
    topic_intent: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EvidencePack:
    retrieval_batch_id: str = ""
    citation_list_id: str = ""
    citations: list[dict[str, Any]] = field(default_factory=list)
    result_ids: list[str] = field(default_factory=list)
    source_scope: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class NarrativePlan:
    outline_packet_id: str = ""
    core_judgement: str = ""
    angle: str = ""
    content_template: str = ""
    hook_candidates: list[str] = field(default_factory=list)
    title_candidates: list[str] = field(default_factory=list)
    outline: list[dict[str, Any]] = field(default_factory=list)
    recommended_opening: str = ""
    recommended_sections: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CreationPacket:
    packet_kind: str
    version: str
    task_id: str
    generated_at: str = field(default_factory=now_iso)
    creation_intent: dict[str, Any] = field(default_factory=dict)
    evidence_pack: dict[str, Any] = field(default_factory=dict)
    narrative_plan: dict[str, Any] = field(default_factory=dict)
    downstream_targets: list[str] = field(default_factory=list)
    source_trace: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def gate_1_ready(task: dict[str, Any]) -> bool:
    return all(str(task.get(field) or "").strip() for field in TASK_REQUIRED_FIELDS)
