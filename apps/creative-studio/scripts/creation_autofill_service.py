from __future__ import annotations

import json
import os
import re
import uuid
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from create_studio_config import load_create_studio_config
from creation_models import now_iso

DEFAULT_PROVIDER = "mock"
DEFAULT_TIMEOUT_SEC = 45.0
DEFAULT_MAX_MATERIALS = 18
DEFAULT_MAX_CHARS_PER_MATERIAL = 1800
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"

METHOD_KEYWORDS = (
    "方法",
    "教程",
    "提示词",
    "使用",
    "上手",
    "实操",
    "workflow",
    "guide",
    "how to",
    "best practice",
    "prompt",
)
CASE_KEYWORDS = (
    "实测",
    "体验",
    "评测",
    "测评",
    "案例",
    "演示",
    "demo",
    "design",
    "compare",
    "对比",
)
JUDGEMENT_KEYWORDS = (
    "发布",
    "融资",
    "趋势",
    "争议",
    "判断",
    "风险",
    "观察",
    "升级",
    "发布",
    "launch",
    "release",
)


def _compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        return [item.strip() for item in re.split(r"[\r\n]+", value) if item.strip()]
    return [value]


def _sanitize_multiline_list(items: Any, *, fallback: list[str], limit: int = 5) -> list[str]:
    normalized: list[str] = []
    for item in _coerce_list(items):
        text = _compact_text(item)
        if not text or text in normalized:
            continue
        normalized.append(text)
        if len(normalized) >= limit:
            break
    return normalized or list(fallback)


def _resolve_settings(config_path: str | None = None) -> dict[str, Any]:
    config = load_create_studio_config(config_path=config_path)
    autofill = dict(config.get("autofill") or {})
    writing = dict(config.get("writing") or {})
    explicit_provider = os.getenv("CREATE_STUDIO_AUTOFILL_PROVIDER")
    provider = str(explicit_provider or autofill.get("provider") or DEFAULT_PROVIDER).strip() or DEFAULT_PROVIDER
    model = str(os.getenv("CREATE_STUDIO_AUTOFILL_MODEL") or autofill.get("model") or "").strip()
    api_base = str(os.getenv("CREATE_STUDIO_AUTOFILL_API_BASE") or autofill.get("api_base") or DEFAULT_OPENAI_BASE_URL).strip() or DEFAULT_OPENAI_BASE_URL
    api_key_env = str(os.getenv("CREATE_STUDIO_AUTOFILL_API_KEY_ENV") or autofill.get("api_key_env") or "OPENAI_API_KEY").strip() or "OPENAI_API_KEY"
    api_key = str(os.getenv("CREATE_STUDIO_AUTOFILL_API_KEY") or autofill.get("api_key") or os.getenv(api_key_env) or "").strip()

    # If autofill is not configured separately, reuse the working writing model.
    # This keeps the UI simple: one model config powers both "智能填写" and "生成文章".
    if explicit_provider is None and provider == DEFAULT_PROVIDER and str(writing.get("provider") or "").strip() == "openai_compatible":
        provider = "openai_compatible"
        model = model or str(writing.get("model") or "").strip()
        api_base = str(writing.get("api_base") or api_base or DEFAULT_OPENAI_BASE_URL).strip()
        api_key_env = str(writing.get("api_key_env") or api_key_env or "OPENAI_API_KEY").strip()
        api_key = str(writing.get("api_key") or api_key or os.getenv(api_key_env) or "").strip()
    try:
        timeout_sec = float(str(os.getenv("CREATE_STUDIO_AUTOFILL_TIMEOUT_SEC") or autofill.get("timeout_sec") or DEFAULT_TIMEOUT_SEC).strip())
    except Exception:  # noqa: BLE001
        timeout_sec = DEFAULT_TIMEOUT_SEC
    try:
        max_materials = int(str(os.getenv("CREATE_STUDIO_AUTOFILL_MAX_MATERIALS") or autofill.get("max_materials") or DEFAULT_MAX_MATERIALS).strip())
    except Exception:  # noqa: BLE001
        max_materials = DEFAULT_MAX_MATERIALS
    try:
        max_chars = int(str(os.getenv("CREATE_STUDIO_AUTOFILL_MAX_CHARS_PER_MATERIAL") or autofill.get("max_chars_per_material") or DEFAULT_MAX_CHARS_PER_MATERIAL).strip())
    except Exception:  # noqa: BLE001
        max_chars = DEFAULT_MAX_CHARS_PER_MATERIAL
    return {
        "provider": provider,
        "model": model,
        "api_base": api_base.rstrip("/"),
        "api_key": api_key,
        "api_key_env": api_key_env,
        "timeout_sec": max(10.0, timeout_sec),
        "max_materials": max(3, max_materials),
        "max_chars_per_material": max(400, max_chars),
    }


def _clip_text(text: str, limit: int) -> str:
    raw = str(text or "").strip()
    if len(raw) <= limit:
        return raw
    return f"{raw[:limit].rstrip()}..."


def _prepare_materials(materials: list[dict[str, Any]], *, max_materials: int, max_chars_per_material: int) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for item in list(materials or [])[:max_materials]:
        prepared.append(
            {
                "title": _compact_text(item.get("title")),
                "summary": _clip_text(_compact_text(item.get("summary") or item.get("why_pick")), 320),
                "body_excerpt": _clip_text(_compact_text(item.get("body_text") or item.get("text") or item.get("summary")), max_chars_per_material),
                "source": _compact_text(item.get("source") or item.get("channel")),
                "published_at": _compact_text(item.get("published_at") or item.get("created_at")),
                "classification": _compact_text(item.get("classification")),
            }
        )
    return prepared


def _score_keywords(text: str, keywords: tuple[str, ...]) -> int:
    lower = text.lower()
    return sum(1 for item in keywords if item.lower() in lower)


def _infer_archetype(task: dict[str, Any], materials: list[dict[str, Any]]) -> str:
    corpus = " ".join(
        [
            _compact_text(task.get("topic")),
            _compact_text(task.get("goal")),
            *[
                " ".join(
                    [
                        _compact_text(item.get("title")),
                        _compact_text(item.get("summary")),
                        _compact_text(item.get("body_text") or item.get("text")),
                    ]
                )
                for item in materials
            ],
        ]
    )
    method_score = _score_keywords(corpus, METHOD_KEYWORDS)
    case_score = _score_keywords(corpus, CASE_KEYWORDS)
    judgement_score = _score_keywords(corpus, JUDGEMENT_KEYWORDS)
    if method_score >= max(case_score, judgement_score):
        return "method_longform"
    if case_score >= max(method_score, judgement_score):
        return "case_longform"
    return "judgement_longform"


def _heuristic_autofill(task: dict[str, Any], materials: list[dict[str, Any]], target: dict[str, Any]) -> dict[str, Any]:
    archetype = _infer_archetype(task, materials)
    titles = [str(item.get("title") or "").strip() for item in materials if str(item.get("title") or "").strip()]
    top_titles = "；".join(titles[:3])
    topic = str(task.get("topic") or "").strip() or "这组素材"
    if archetype == "method_longform":
        topic_reason = (
            f"{topic} 这批素材不只是资讯堆叠，它里面已经带出了能直接复用的方法和操作细节。"
            f"如果把这些一手案例和做法拎顺，最后很容易写成一篇既有信息量、又真能让读者带走东西的长文。"
        )
        angle = (
            f"别把 {topic} 只当成又一个新功能集合，更值得写的是它正在把零散技巧收拢成真正可复用的工作流。"
        )
        opening_hook = (
            f"这两天我反复看 {topic} 相关素材，真正打到我的不是谁又上了个新功能，而是很多人已经开始把它用成一套完整方法了。"
        )
    elif archetype == "case_longform":
        topic_reason = (
            f"{topic} 这批素材最有价值的地方，是它不是空讲概念，而是已经出现了足够具体的案例、实测和使用反馈。"
            f"把这些案例放在一起看，很适合写出一篇有画面感、有判断的拆解。"
        )
        angle = (
            f"{topic} 真正值得写的，不是它表面上看起来多炫，而是这些案例已经说明它开始从演示感走向真实使用场景。"
        )
        opening_hook = (
            f"我最近连续看了几篇和 {topic} 有关的案例，越看越觉得，这玩意儿已经不是拿来演示一下就完事的东西了。"
        )
    else:
        topic_reason = (
            f"{topic} 这批素材集中冒头，说明它已经不是一条零散快讯，而是一个值得下判断的议题。"
            f"它既有新变化，也有足够多的外部信号，适合写成一篇带判断力的长文。"
        )
        angle = (
            f"别把 {topic} 只当成一轮短期热闹，更值得写的是它正在暴露出下一阶段内容生产和产品竞争的真正差距。"
        )
        opening_hook = (
            f"这两天围绕 {topic} 的信息很多，但真正让我停下来的，不是又多了一条新闻，而是它背后那种更大的变化开始露头了。"
        )

    return {
        "article_archetype": archetype,
        "optional_followups": list((target.get("defaults") or {}).get("optional_followups") or []),
        "topic_reason": topic_reason,
        "angle": angle,
        "opening_hook": opening_hook,
        "personal_observations": (
            "暂无用户亲历补充。写作时只能使用素材里的公开信息，不要替用户编造亲身体验；"
            "如果需要第一人称，可以写成“我看到这批案例时的感受”，不要写成亲自参与。"
        ),
        "hkr_focus": ["happy", "knowledge", "resonance"],
        "user_voice_notes": [
            "像熟人聊天，不要报告腔",
            "先下判断，再展开证据和案例",
            f"优先用具体材料说话，必要时可引用：{top_titles}" if top_titles else "优先用具体材料说话，不要只做概念总结",
        ],
        "banned_patterns": [
            "宏大叙事开头",
            "标准 AI 套话和正确废话",
            "只有功能罗列，没有个人判断",
        ],
    }


def _sanitize_autofill_fields(raw: dict[str, Any], target: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    allowed_archetypes = {
        str(item.get("value") or "").strip()
        for item in list(target.get("article_archetypes") or [])
        if str(item.get("value") or "").strip()
    }
    allowed_followups = {
        str(item.get("value") or "").strip()
        for item in list(target.get("optional_followup_options") or [])
        if str(item.get("value") or "").strip()
    }
    article_archetype = str(raw.get("article_archetype") or "").strip()
    if article_archetype not in allowed_archetypes:
        article_archetype = str(fallback.get("article_archetype") or next(iter(allowed_archetypes), "judgement_longform")).strip()
    optional_followups = [
        item
        for item in [str(v or "").strip() for v in list(raw.get("optional_followups") or [])]
        if item in allowed_followups
    ]
    if not optional_followups:
        optional_followups = list(fallback.get("optional_followups") or [])
    allowed_hkr = {"happy", "knowledge", "resonance"}
    hkr_focus = [
        item
        for item in [str(v or "").strip() for v in _coerce_list(raw.get("hkr_focus"))]
        if item in allowed_hkr
    ]
    if not hkr_focus:
        hkr_focus = [
            item
            for item in [str(v or "").strip() for v in _coerce_list(fallback.get("hkr_focus"))]
            if item in allowed_hkr
        ] or ["happy", "knowledge", "resonance"]
    return {
        "article_archetype": article_archetype,
        "optional_followups": optional_followups,
        "topic_reason": _compact_text(raw.get("topic_reason") or fallback.get("topic_reason")),
        "angle": _compact_text(raw.get("angle") or fallback.get("angle")),
        "opening_hook": _compact_text(raw.get("opening_hook") or fallback.get("opening_hook")),
        "personal_observations": _compact_text(raw.get("personal_observations") or fallback.get("personal_observations")),
        "hkr_focus": hkr_focus,
        "user_voice_notes": _sanitize_multiline_list(
            raw.get("user_voice_notes"),
            fallback=list(fallback.get("user_voice_notes") or []),
        ),
        "banned_patterns": _sanitize_multiline_list(
            raw.get("banned_patterns"),
            fallback=list(fallback.get("banned_patterns") or []),
        ),
    }


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:  # noqa: BLE001
        pass
    match = re.search(r"\{[\s\S]*\}", raw)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(0))
        return payload if isinstance(payload, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _build_messages(
    task: dict[str, Any],
    target: dict[str, Any],
    prepared_materials: list[dict[str, Any]],
    *,
    variation_id: str,
) -> list[dict[str, str]]:
    prompt_payload = {
        "task": {
            "topic": str(task.get("topic") or "").strip(),
            "goal": str(task.get("goal") or "").strip(),
            "audience": str(task.get("audience") or "").strip(),
            "platform": str(task.get("platform") or "").strip(),
        },
        "target": {
            "id": str(target.get("id") or "").strip(),
            "label": str(target.get("label") or "").strip(),
            "writer_skill": str(target.get("writer_skill") or "").strip(),
            "article_archetypes": list(target.get("article_archetypes") or []),
            "optional_followup_options": list(target.get("optional_followup_options") or []),
        },
        "materials": prepared_materials,
        "variation_id": variation_id,
        "output_schema": {
            "article_archetype": "must be one of the allowed values",
            "optional_followups": "subset of allowed values",
            "topic_reason": "2-4 Chinese sentences",
            "angle": "1 paragraph Chinese judgement and cut-in angle",
            "opening_hook": "1-2 Chinese sentences, colloquial opening",
            "personal_observations": "Chinese notes about real observation; do not fabricate personal experience",
            "hkr_focus": ["subset of happy, knowledge, resonance"],
            "user_voice_notes": ["3-5 Chinese bullet-style lines"],
            "banned_patterns": ["3-5 Chinese bullet-style lines"],
        },
    }
    system_prompt = (
        "你在为 Khazix 长文模式做创作前置判断。"
        "目标不是写文章，而是帮编辑根据当前素材包，自动填出最适合进入写作的 brief 字段。"
        "请优先提炼：这题为什么值得写、真正该立什么判断、开头应该怎么起、需要保留什么表达要求、要避开什么写法。"
        "必须严格依据素材，不要编造。"
        "输出必须是 JSON 对象，不要输出任何解释。"
    )
    user_prompt = (
        "请读取下面这组素材，给出一版最适合 Khazix 长文模式的智能填写结果。\n"
        "如果素材更偏方法、案例、判断，请自动选择最合适的文章原型。\n"
        "这是一次重新生成，不要机械复用上一版措辞；请换一个切口、开头气口或表达侧重点，但不能偏离素材。\n"
        "输出字段必须完整。\n\n"
        f"{json.dumps(prompt_payload, ensure_ascii=False)}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _call_openai_compatible_llm(messages: list[dict[str, str]], settings: dict[str, Any]) -> dict[str, Any]:
    request = Request(
        f"{settings['api_base']}/chat/completions",
        method="POST",
        data=json.dumps(
            {
                "model": settings["model"],
                "temperature": 0.75,
                "response_format": {"type": "json_object"},
                "messages": messages,
            },
            ensure_ascii=False,
        ).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {settings['api_key']}",
            "User-Agent": "Mozilla/5.0 CreateStudio/1.0",
        },
    )
    try:
        with urlopen(request, timeout=settings["timeout_sec"]) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code in {401, 403}:
            raise ValueError(
                f"模型服务拒绝访问（HTTP {exc.code}）。这通常表示 API Key 无效、没有当前模型权限，"
                "或 API Base 与这把 Key 不匹配。"
            ) from exc
        raise ValueError(f"模型服务返回 HTTP {exc.code}，请检查 API Base、模型名称和服务状态。") from exc
    except URLError as exc:
        raise ValueError(f"无法连接模型服务：{exc.reason}") from exc
    choices = list(payload.get("choices") or [])
    message = dict((choices[0] or {}).get("message") or {}) if choices else {}
    content = message.get("content")
    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(str(item.get("text") or ""))
        content = "\n".join(text_parts)
    return _extract_json_object(str(content or ""))


def generate_target_autofill(
    *,
    task: dict[str, Any],
    materials: list[dict[str, Any]],
    target: dict[str, Any],
    config_path: str | None = None,
) -> dict[str, Any]:
    if str(target.get("ui_preset") or "").strip() != "khazix_longform":
        raise ValueError("当前只支持 Khazix 长文模式的智能填写")
    settings = _resolve_settings(config_path=config_path)
    prepared_materials = _prepare_materials(
        materials,
        max_materials=int(settings["max_materials"]),
        max_chars_per_material=int(settings["max_chars_per_material"]),
    )
    if not prepared_materials:
        raise ValueError("当前素材包里没有可用于智能填写的文章")

    fallback = _heuristic_autofill(task, materials, target)
    source = "mock"
    warning = ""
    raw_fields = dict(fallback)

    if (
        settings["provider"] == "openai_compatible"
        and settings["model"]
        and settings["api_key"]
    ):
        try:
            raw_fields = _call_openai_compatible_llm(
                _build_messages(task, target, prepared_materials, variation_id=uuid.uuid4().hex[:12]),
                settings,
            )
            source = "llm"
        except Exception as exc:  # noqa: BLE001
            warning = f"llm_failed:{exc}"
            raw_fields = dict(fallback)
            source = "mock"
    elif settings["provider"] != "mock":
        warning = "llm_not_configured"

    fields = _sanitize_autofill_fields(raw_fields, target, fallback)
    return {
        "ok": True,
        "source": source,
        "warning": warning,
        "generated_at": now_iso(),
        "target_id": str(target.get("id") or "").strip(),
        "material_count": len(materials),
        "model": str(settings.get("model") or "").strip(),
        "provider": str(settings.get("provider") or "").strip(),
        "fields": fields,
    }
