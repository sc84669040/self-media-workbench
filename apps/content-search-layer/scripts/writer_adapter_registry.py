from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_WRITER_ADAPTER_CONFIG_PATH = BASE_DIR / "config" / "writer-adapters.yaml"


def _default_registry() -> dict[str, Any]:
    return {
        "default_writer": "khazix-writer",
        "profiles": {
            "khazix-writer": {
                "skill": "khazix-writer",
                "display_name": "Khazix Writer",
                "writer_type": "longform_wechat",
                "description": "偏公众号长文写作，强调判断、结构和成熟表达。",
                "available": True,
                "adapter_name": "brief-first",
                "enabled_sections": [
                    "creation_intent",
                    "evidence_pack",
                    "narrative_plan",
                    "legacy_compat",
                ],
                "brief_template": {
                    "focus": "turn evidence into a publication-ready long-form article brief",
                    "must_include": [
                        "core_judgement",
                        "hook_candidates",
                        "must_use_citations",
                        "writer_voice_notes",
                    ],
                },
            },
            "generic-longform": {
                "skill": "generic-longform",
                "display_name": "Generic Longform",
                "writer_type": "general_longform",
                "description": "更通用的长文适配目标，适合后续接其他写作器或人工接手。",
                "available": True,
                "adapter_name": "balanced-brief",
                "enabled_sections": [
                    "creation_intent",
                    "evidence_pack",
                    "narrative_plan",
                    "legacy_compat",
                ],
                "brief_template": {
                    "focus": "turn evidence into a reusable long-form writing brief",
                    "must_include": [
                        "core_judgement",
                        "hook_candidates",
                        "must_use_citations",
                        "recommended_sections",
                    ],
                },
            },
        },
    }


def load_writer_adapter_registry(config_path: str | Path | None = None) -> dict[str, Any]:
    registry = _default_registry()
    resolved_config_path = Path(str(config_path or DEFAULT_WRITER_ADAPTER_CONFIG_PATH)).expanduser()
    if not resolved_config_path.exists():
        return registry

    try:
        payload = yaml.safe_load(resolved_config_path.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return registry

    profiles = dict(registry.get("profiles") or {})
    override_profiles = dict(payload.get("profiles") or {})
    profiles.update(override_profiles)
    registry.update(payload if isinstance(payload, dict) else {})
    registry["profiles"] = profiles
    return registry


def resolve_writer_profile(writer_skill: str, config_path: str | Path | None = None) -> dict[str, Any]:
    registry = load_writer_adapter_registry(config_path=config_path)
    profiles = dict(registry.get("profiles") or {})
    requested_skill = str(writer_skill or "").strip()
    default_skill = str(registry.get("default_writer") or "").strip()
    fallback_skill = requested_skill or default_skill or "khazix-writer"
    profile = dict(profiles.get(fallback_skill) or {})
    if not profile and default_skill:
        profile = dict(profiles.get(default_skill) or {})
    if not profile:
        profile = dict(_default_registry()["profiles"]["khazix-writer"])

    profile["skill"] = requested_skill or str(profile.get("skill") or fallback_skill or "khazix-writer")
    profile.setdefault("display_name", str(profile.get("skill") or fallback_skill or "Writer"))
    profile.setdefault("writer_type", "general_longform")
    profile.setdefault("description", "")
    profile.setdefault("available", True)
    profile.setdefault("adapter_name", "brief-first")
    profile.setdefault(
        "enabled_sections",
        ["creation_intent", "evidence_pack", "narrative_plan", "legacy_compat"],
    )
    return profile


def list_writer_profiles(config_path: str | Path | None = None) -> list[dict[str, Any]]:
    registry = load_writer_adapter_registry(config_path=config_path)
    profiles = dict(registry.get("profiles") or {})
    default_skill = str(registry.get("default_writer") or "").strip()
    items: list[dict[str, Any]] = []
    for key, raw_profile in profiles.items():
        profile = resolve_writer_profile(str(key), config_path=config_path)
        profile["is_default"] = profile["skill"] == default_skill
        items.append(profile)
    items.sort(key=lambda item: (not bool(item.get("is_default")), str(item.get("display_name") or item.get("skill") or "")))
    return items


def get_writer_registry_summary(config_path: str | Path | None = None) -> dict[str, Any]:
    registry = load_writer_adapter_registry(config_path=config_path)
    default_skill = str(registry.get("default_writer") or "").strip() or "khazix-writer"
    profiles = list_writer_profiles(config_path=config_path)
    return {
        "ok": True,
        "default_writer": default_skill,
        "writers": [
            {
                "skill": str(item.get("skill") or "").strip(),
                "display_name": str(item.get("display_name") or item.get("skill") or "").strip(),
                "writer_type": str(item.get("writer_type") or "").strip(),
                "description": str(item.get("description") or "").strip(),
                "adapter_name": str(item.get("adapter_name") or "").strip(),
                "enabled_sections": list(item.get("enabled_sections") or []),
                "available": bool(item.get("available")),
                "is_default": bool(item.get("is_default")),
            }
            for item in profiles
        ],
    }


def build_writer_ready_packet(
    *,
    task: dict[str, Any],
    citation_list: dict[str, Any],
    outline_packet: dict[str, Any],
    writer_job: dict[str, Any],
    creation_packet: dict[str, Any],
    writer_profile: dict[str, Any],
) -> dict[str, Any]:
    citations = list((citation_list or {}).get("citations") or [])
    must_use = [
        citation
        for citation in citations
        if str(citation.get("usage_scope") or "").strip().lower() == "must_use"
    ]
    brief = {
        "focus": ((writer_profile.get("brief_template") or {}).get("focus") or "").strip(),
        "core_judgement": str((outline_packet or {}).get("core_judgement") or "").strip(),
        "opening_hook": str((((outline_packet or {}).get("hook_candidates") or [""])[0]) or "").strip(),
        "must_use_citations": must_use,
        "writer_voice_notes": list(writer_job.get("user_voice_notes") or []),
        "article_archetype": str(writer_job.get("article_archetype") or "").strip(),
    }
    return {
        "workspace_name": "Creation Workspace",
        "writer_adapter": {
            "skill": str(writer_profile.get("skill") or writer_job.get("writer_skill") or "").strip(),
            "display_name": str(writer_profile.get("display_name") or writer_profile.get("skill") or "").strip(),
            "writer_type": str(writer_profile.get("writer_type") or "").strip(),
            "adapter_name": str(writer_profile.get("adapter_name") or "brief-first").strip(),
            "enabled_sections": list(writer_profile.get("enabled_sections") or []),
        },
        "creation_packet": creation_packet,
        "writer_ready_brief": brief,
        "task": task,
        "citation_list": citation_list,
        "outline_packet": outline_packet,
        "writer_job": writer_job,
    }
