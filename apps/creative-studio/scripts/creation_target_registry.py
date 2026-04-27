from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CREATION_TARGET_CONFIG_PATH = BASE_DIR / "config" / "creation-targets.yaml"


def _default_registry() -> dict[str, Any]:
    return {
        "default_target": "khazix-wechat",
        "targets": {
            "khazix-wechat": {
                "label": "Khazix 长文",
                "description": "面向 Khazix Writer 的公众号长文创作，强调判断、结构和证据组织。",
                "writer_skill": "khazix-writer",
                "ui_preset": "khazix_longform",
                "defaults": {
                    "content_template": "判断型长文",
                    "article_archetype": "judgement_longform",
                    "primary_output": ["long_article"],
                    "optional_followups": ["title_set", "cover_copy"],
                },
                "article_archetypes": [
                    {"value": "judgement_longform", "label": "判断型长文"},
                    {"value": "method_longform", "label": "方法论长文"},
                    {"value": "case_longform", "label": "案例拆解长文"},
                ],
                "primary_output_options": [
                    {"value": "long_article", "label": "完整长文"},
                ],
                "optional_followup_options": [
                    {"value": "title_set", "label": "标题备选"},
                    {"value": "cover_copy", "label": "封面文案"},
                    {"value": "oral_adaptation", "label": "口播改写提示"},
                ],
                "fields": [
                    {"key": "angle", "type": "textarea", "label": "切入角度", "placeholder": "这篇要从什么判断切进去"},
                    {"key": "article_archetype", "type": "select", "label": "文章类型"},
                    {"key": "user_voice_notes", "type": "textarea", "label": "表达要求", "placeholder": "写作语气、节奏、禁区"},
                    {"key": "banned_patterns", "type": "textarea", "label": "避免写法", "placeholder": "每行一个不希望出现的写法"},
                ],
            },
            "voiceover-script": {
                "label": "口播稿",
                "description": "面向后续口播或视频表达，强调节奏、段落推进和口语化表达。",
                "writer_skill": "generic-longform",
                "ui_preset": "voiceover_script",
                "defaults": {
                    "content_template": "口播脚本",
                    "article_archetype": "voiceover_explainer",
                    "primary_output": ["voiceover_script"],
                    "optional_followups": ["short_title", "clip_outline"],
                },
                "article_archetypes": [
                    {"value": "voiceover_explainer", "label": "解释型口播"},
                    {"value": "voiceover_commentary", "label": "评论型口播"},
                    {"value": "voiceover_story", "label": "叙事型口播"},
                ],
                "primary_output_options": [
                    {"value": "voiceover_script", "label": "口播主稿"},
                ],
                "optional_followup_options": [
                    {"value": "short_title", "label": "短标题"},
                    {"value": "clip_outline", "label": "分镜提纲"},
                    {"value": "hook_lines", "label": "开头钩子"},
                ],
                "fields": [
                    {"key": "angle", "type": "textarea", "label": "口播主线", "placeholder": "这条口播最想先说透什么"},
                    {"key": "article_archetype", "type": "select", "label": "口播类型"},
                    {"key": "user_voice_notes", "type": "textarea", "label": "口语要求", "placeholder": "希望更像什么说话方式"},
                    {"key": "banned_patterns", "type": "textarea", "label": "避免表达", "placeholder": "每行一个不希望出现的表达"},
                ],
            },
            "generic-longform": {
                "label": "通用长文",
                "description": "不绑定具体下游 writer，先生成一份通用的长文创作包。",
                "writer_skill": "generic-longform",
                "ui_preset": "generic_longform",
                "defaults": {
                    "content_template": "通用长文",
                    "article_archetype": "general_longform",
                    "primary_output": ["long_article"],
                    "optional_followups": ["title_set"],
                },
                "article_archetypes": [
                    {"value": "general_longform", "label": "通用长文"},
                    {"value": "research_note", "label": "研究型长文"},
                    {"value": "briefing", "label": "简报型长文"},
                ],
                "primary_output_options": [
                    {"value": "long_article", "label": "完整长文"},
                ],
                "optional_followup_options": [
                    {"value": "title_set", "label": "标题备选"},
                    {"value": "summary_note", "label": "摘要说明"},
                ],
                "fields": [
                    {"key": "angle", "type": "textarea", "label": "核心切角", "placeholder": "希望最后形成什么判断或结构"},
                    {"key": "article_archetype", "type": "select", "label": "产物类型"},
                    {"key": "user_voice_notes", "type": "textarea", "label": "表达备注", "placeholder": "希望保留的语气、结构要求"},
                    {"key": "banned_patterns", "type": "textarea", "label": "避免写法", "placeholder": "每行一个不希望出现的写法"},
                ],
            },
        },
    }


def load_creation_target_registry(config_path: str | Path | None = None) -> dict[str, Any]:
    registry = _default_registry()
    resolved = Path(str(config_path or DEFAULT_CREATION_TARGET_CONFIG_PATH)).expanduser()
    if not resolved.exists():
        return registry
    try:
        payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return registry

    targets = dict(registry.get("targets") or {})
    override_targets = dict(payload.get("targets") or {})
    for key, value in override_targets.items():
        merged = dict(targets.get(key) or {})
        if isinstance(value, dict):
            merged.update(value)
        else:
            merged = value
        targets[key] = merged
    registry.update(payload if isinstance(payload, dict) else {})
    registry["targets"] = targets
    return registry


def resolve_creation_target(target_id: str | None = None, config_path: str | Path | None = None) -> dict[str, Any]:
    registry = load_creation_target_registry(config_path=config_path)
    targets = dict(registry.get("targets") or {})
    default_target = str(registry.get("default_target") or "").strip() or "khazix-wechat"
    requested = str(target_id or "").strip()
    resolved_id = requested or default_target
    target = dict(targets.get(resolved_id) or {})
    if not target and default_target:
        resolved_id = default_target
        target = dict(targets.get(default_target) or {})
    if not target:
        resolved_id = "khazix-wechat"
        target = dict(_default_registry()["targets"]["khazix-wechat"])

    target["id"] = resolved_id
    target.setdefault("label", resolved_id)
    target.setdefault("description", "")
    target.setdefault("writer_skill", "generic-longform")
    target.setdefault("ui_preset", "generic_longform")
    target.setdefault("defaults", {})
    target.setdefault("article_archetypes", [])
    target.setdefault("primary_output_options", [])
    target.setdefault("optional_followup_options", [])
    target.setdefault("fields", [])
    target["is_default"] = resolved_id == default_target
    return target


def list_creation_targets(config_path: str | Path | None = None) -> list[dict[str, Any]]:
    registry = load_creation_target_registry(config_path=config_path)
    targets = dict(registry.get("targets") or {})
    default_target = str(registry.get("default_target") or "").strip() or "khazix-wechat"
    items = [resolve_creation_target(target_id=key, config_path=config_path) for key in targets]
    items.sort(key=lambda item: (not bool(item.get("is_default")), str(item.get("label") or item.get("id") or "")))
    if not items:
        return [resolve_creation_target(default_target, config_path=config_path)]
    return items


def get_creation_target_summary(config_path: str | Path | None = None) -> dict[str, Any]:
    registry = load_creation_target_registry(config_path=config_path)
    default_target = str(registry.get("default_target") or "").strip() or "khazix-wechat"
    return {
        "ok": True,
        "default_target": default_target,
        "targets": list_creation_targets(config_path=config_path),
    }
