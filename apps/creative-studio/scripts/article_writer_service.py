from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from create_studio_config import load_create_studio_config
from creation_models import now_iso

DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_KHAZIX_SKILL_PATH = Path(os.getenv("KHAZIX_SKILL_PATH") or "__khazix_skill_not_configured__")

FORBIDDEN_WORDS = [
    "说白了",
    "意味着什么",
    "这意味着",
    "本质上",
    "换句话说",
    "不可否认",
    "综上所述",
    "总的来说",
    "值得注意的是",
    "不难发现",
    "让我们来看看",
    "接下来让我们",
    "在当今",
    "随着技术",
]
FORBIDDEN_PUNCTUATION = ["——", '"', '"', '"']


def _resolve_settings(config_path: str | None = None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    config = load_create_studio_config(config_path=config_path)
    writing = dict(config.get("writing") or {})
    if overrides:
        for key, value in overrides.items():
            if value is not None:
                writing[key] = value
    provider = str(os.getenv("CREATE_STUDIO_WRITING_PROVIDER") or writing.get("provider") or "disabled").strip()
    model = str(os.getenv("CREATE_STUDIO_WRITING_MODEL") or writing.get("model") or "").strip()
    api_base = str(os.getenv("CREATE_STUDIO_WRITING_API_BASE") or writing.get("api_base") or DEFAULT_OPENAI_BASE_URL).strip()
    api_key_env = str(os.getenv("CREATE_STUDIO_WRITING_API_KEY_ENV") or writing.get("api_key_env") or "OPENAI_API_KEY").strip()
    # A key saved in Create Studio should win over stale process env values.
    api_key = str(writing.get("api_key") or os.getenv("CREATE_STUDIO_WRITING_API_KEY") or os.getenv(api_key_env) or "").strip()
    try:
        timeout_sec = float(str(os.getenv("CREATE_STUDIO_WRITING_TIMEOUT_SEC") or writing.get("timeout_sec") or 120).strip())
    except Exception:  # noqa: BLE001
        timeout_sec = 120.0
    try:
        max_materials = int(str(os.getenv("CREATE_STUDIO_WRITING_MAX_MATERIALS") or writing.get("max_materials") or 18).strip())
    except Exception:  # noqa: BLE001
        max_materials = 18
    try:
        max_chars = int(str(os.getenv("CREATE_STUDIO_WRITING_MAX_CHARS_PER_MATERIAL") or writing.get("max_chars_per_material") or 2200).strip())
    except Exception:  # noqa: BLE001
        max_chars = 2200
    quality_raw = os.getenv("CREATE_STUDIO_WRITING_QUALITY_CHECK_ENABLED")
    quality_enabled = bool(writing.get("quality_check_enabled", True))
    if quality_raw is not None:
        quality_enabled = quality_raw.strip().lower() in {"1", "true", "yes", "on"}
    return {
        "provider": provider.lower() or "disabled",
        "model": model,
        "api_base": api_base.rstrip("/"),
        "api_key": api_key,
        "api_key_env": api_key_env,
        "timeout_sec": max(15.0, timeout_sec),
        "max_materials": max(1, max_materials),
        "max_chars_per_material": max(400, max_chars),
        "quality_check_enabled": quality_enabled,
    }


def _load_skill_prompt(writer_profile: dict[str, Any]) -> tuple[str, str]:
    configured = str(writer_profile.get("skill_prompt_path") or "").strip()
    path = Path(configured) if configured else DEFAULT_KHAZIX_SKILL_PATH
    if path.exists():
        return path.read_text(encoding="utf-8")[:24000], str(path)
    return "", str(path)


def _clip(text: str, limit: int) -> str:
    normalized = str(text or "").strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip() + "\n...(内容已截断)"


def _prepare_materials(packet: dict[str, Any], max_materials: int, max_chars: int) -> list[dict[str, Any]]:
    citations = list((packet.get("citation_list") or {}).get("citations") or [])
    materials: list[dict[str, Any]] = []
    for citation in citations[:max_materials]:
        text = str(
            citation.get("quote")
            or citation.get("text")
            or citation.get("body_text")
            or citation.get("usable_excerpt")
            or citation.get("normalized_claim")
            or citation.get("summary")
            or ""
        ).strip()
        materials.append(
            {
                "title": str(citation.get("title") or citation.get("source_title") or citation.get("source_id") or "").strip(),
                "source": str(citation.get("source") or citation.get("provider") or "").strip(),
                "url": str(citation.get("url") or "").strip(),
                "usage_scope": str(citation.get("usage_scope") or "").strip(),
                "text": _clip(text, max_chars),
            }
        )

    if materials:
        return materials

    creation_packet = dict(packet.get("creation_packet") or {})
    evidence_pack = dict(creation_packet.get("evidence_pack") or {})
    for item in list(evidence_pack.get("citations") or [])[:max_materials]:
        materials.append(
            {
                "title": str(item.get("title") or item.get("source_id") or "").strip(),
                "source": str(item.get("source") or "").strip(),
                "url": str(item.get("url") or "").strip(),
                "usage_scope": str(item.get("usage_scope") or "").strip(),
                "text": _clip(str(item.get("quote") or item.get("text") or item.get("summary") or "").strip(), max_chars),
            }
        )
    return materials


def _extract_title(markdown: str, fallback: str) -> str:
    for line in str(markdown or "").splitlines():
        text = line.strip()
        if text.startswith("#"):
            return text.lstrip("#").strip() or fallback
        if text:
            return text[:60]
    return fallback


def build_quality_report(markdown: str, *, enabled: bool = True) -> dict[str, Any]:
    if not enabled:
        return {"enabled": False, "summary": "未开启自动质检", "issues": [], "layers": {}}
    text = str(markdown or "")
    forbidden_hits = [word for word in FORBIDDEN_WORDS if word in text]
    punctuation_hits = [mark for mark in FORBIDDEN_PUNCTUATION if mark in text]
    paragraphs = [item.strip() for item in text.splitlines() if item.strip()]
    short_breaks = sum(1 for item in paragraphs if len(item) <= 12)
    has_emotion_mark = any(mark in text for mark in ["。。。", "？？？", "= ="])
    has_footer = "/ 作者：卡兹克" in text or "下次再见" in text
    issues: list[str] = []
    if forbidden_hits:
        issues.append("存在 Khazix 禁用词或套话：" + "、".join(forbidden_hits[:6]))
    if punctuation_hits:
        issues.append("存在禁用标点：" + "、".join(sorted(set(punctuation_hits))))
    if len(text) < 1200:
        issues.append("正文偏短，可能还不像一篇完整长文")
    if short_breaks < 2:
        issues.append("缺少短句断裂，节奏可能偏平")
    if not has_emotion_mark:
        issues.append("缺少明显口语情绪标记，可以补一点活人感")
    if not has_footer:
        issues.append("缺少 Khazix 固定尾部")

    return {
        "enabled": True,
        "generated_at": now_iso(),
        "summary": "通过基础质检" if not issues else f"发现 {len(issues)} 个需要注意的问题",
        "issues": issues[:5],
        "layers": {
            "L1 硬性规则": {
                "pass": not forbidden_hits and not punctuation_hits,
                "forbidden_words": forbidden_hits,
                "forbidden_punctuation": sorted(set(punctuation_hits)),
            },
            "L2 风格一致性": {
                "pass": short_breaks >= 2 and has_emotion_mark,
                "short_breaks": short_breaks,
                "has_emotion_mark": has_emotion_mark,
            },
            "L3 内容质量": {
                "pass": len(text) >= 1200,
                "char_count": len(text),
            },
            "L4 活人感": {
                "pass": has_emotion_mark and short_breaks >= 2,
                "has_footer": has_footer,
            },
        },
    }


def _build_messages(packet: dict[str, Any], writer_profile: dict[str, Any], skill_prompt: str, materials: list[dict[str, Any]]) -> list[dict[str, str]]:
    writer_brief = dict(packet.get("writer_ready_brief") or {})
    task = dict(packet.get("task") or {})
    outline = dict(packet.get("outline_packet") or {})
    writer_job = dict(packet.get("writer_job") or {})
    generation_options = dict(packet.get("article_generation_options") or {})
    target_word_count = str(generation_options.get("target_word_count") or writer_brief.get("target_word_count") or "").strip()
    selected_suggestions = list(generation_options.get("selected_revision_suggestions") or [])
    prompt_payload = {
        "topic": task.get("topic") or "",
        "platform": task.get("platform") or "",
        "goal": task.get("goal") or "",
        "audience": task.get("audience") or "",
        "writer_skill": writer_profile.get("skill") or writer_job.get("writer_skill") or "",
        "article_archetype": writer_job.get("article_archetype") or writer_brief.get("article_archetype") or "",
        "why_this_topic": writer_brief.get("why_this_topic") or "",
        "core_judgement": writer_brief.get("core_judgement") or outline.get("core_judgement") or "",
        "opening_hook": writer_brief.get("opening_hook") or "",
        "personal_observations": writer_brief.get("personal_observations") or "",
        "hkr_focus": writer_brief.get("hkr_focus") or [],
        "writer_voice_notes": writer_brief.get("writer_voice_notes") or writer_job.get("user_voice_notes") or [],
        "banned_patterns": writer_job.get("banned_patterns") or [],
        "target_word_count": target_word_count,
        "selected_revision_suggestions": selected_suggestions,
        "materials": materials,
    }
    system_prompt = (
        "你是创作台里的文章写作器。现在要根据用户选好的素材直接产出可编辑的公众号长文草稿。"
        "如果 writer_skill 是 khazix-writer，必须遵循下面的 Khazix Writer 规则。"
        "必须严格依据素材，不要编造第一手经历，不要伪造数据。"
        "只输出 Markdown 正文，不要输出解释、JSON 或过程说明。"
    )
    system_prompt += "\n\n硬性要求：文章结尾不要写署名、作者、联系方式、投稿邮箱、公众号引导、感谢阅读或固定尾巴。"
    if skill_prompt:
        system_prompt += "\n\n下面是 writer skill 规则：\n" + skill_prompt
    user_prompt = (
        "请根据下面的创作上下文，直接写出一篇完整文章草稿。\n"
        "要求：保留可编辑 Markdown；需要标题；尽量有 Khazix 的口语节奏；引用素材要自然融入正文；不要写成报告。\n\n"
        "\n如果提供了 target_word_count，请尽量贴近该字数，上下浮动不超过 15%。"
        "如果提供了 selected_revision_suggestions，请把这些调整要求应用到本次生成中。"
        "不要写署名和固定尾巴。\n\n"
        f"{json.dumps(prompt_payload, ensure_ascii=False)}"
    )
    return [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]


def _call_openai_compatible_llm(
    messages: list[dict[str, str]],
    settings: dict[str, Any],
    *,
    max_tokens: int | None = None,
) -> str:
    payload: dict[str, Any] = {
        "model": settings["model"],
        "temperature": 0.75,
        "messages": messages,
    }
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    request = Request(
        f"{settings['api_base']}/chat/completions",
        method="POST",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
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
        return "\n".join(str(item.get("text") or "") for item in content if isinstance(item, dict))
    return str(content or "").strip()


def test_writing_model_config(
    *,
    config_path: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = _resolve_settings(config_path=config_path, overrides=overrides)
    provider = str(settings.get("provider") or "").strip()
    if provider == "mock":
        return {
            "ok": True,
            "provider": provider,
            "model": str(settings.get("model") or "mock"),
            "message": "Mock writer is available. It is only for local testing.",
        }
    if provider != "openai_compatible":
        raise ValueError("写作模型未启用。请先选择 openai_compatible。")
    if not str(settings.get("model") or "").strip():
        raise ValueError("写作模型未配置。请填写模型名称。")
    if not str(settings.get("api_key") or "").strip():
        raise ValueError("API Key 未配置。请填写 API Key，或确认 API Key 环境变量已生效。")

    content = _call_openai_compatible_llm(
        [{"role": "user", "content": "Reply with OK."}],
        settings,
        max_tokens=8,
    )
    if not content:
        raise ValueError("模型连通了，但没有返回内容。")
    return {
        "ok": True,
        "provider": provider,
        "model": str(settings.get("model") or ""),
        "message": "模型验证通过，可以生成文章。",
        "sample": content[:120],
    }


def _mock_article(packet: dict[str, Any], materials: list[dict[str, Any]]) -> str:
    brief = dict(packet.get("writer_ready_brief") or {})
    task = dict(packet.get("task") or {})
    topic = str(task.get("topic") or "未命名选题").strip()
    judgement = str(brief.get("core_judgement") or task.get("angle") or topic).strip()
    opening = str(brief.get("opening_hook") or f"故事是这样的，最近我一直在看 {topic} 这件事。").strip()
    material_lines = []
    for item in materials[:5]:
        title = str(item.get("title") or "一条素材").strip()
        text = str(item.get("text") or "").strip()
        material_lines.append(f"我看到的一条材料是「{title}」，里面最值得抓住的是，{text[:180]}")
    material_block = "\n\n".join(material_lines) or "当前素材包里还没有足够可引用的正文。"
    return f"""# {topic}

{opening}

我先说我的判断。

{judgement}

这不是一个可以靠几句热闹话带过去的选题，因为它背后真正有意思的地方，是我们正在怎么重新理解工具、内容和人的关系。

{material_block}

说真的，这里面让我最有感觉的不是某一个单点功能，而是这些材料放在一起之后，突然能看到一条线。

它在提醒我们，内容创作不是把信息搬过来，也不是把观点包装得更像观点。

更重要的是，你要真的有一个被材料打动的瞬间。

如果没有这个瞬间，文章很容易就变成报告。

但如果有，这个选题就可以往下写。

以上，既然看到这里了，如果觉得不错，随手点个赞、在看、转发三连吧，如果想第一时间收到推送，也可以给我个星标⭐～
谢谢你看我的文章，我们，下次再见。
> / 作者：卡兹克
> / 投稿或爆料，请联系邮箱：wzglyay@virxact.com
"""


def _strip_signature_footer(markdown: str) -> str:
    lines = str(markdown or "").rstrip().splitlines()
    while lines and not lines[-1].strip():
        lines.pop()
    signature_patterns = [
        r"作者",
        r"署名",
        r"投稿",
        r"邮箱",
        r"联系",
        r"公众号",
        r"下次再见",
        r"谢谢你看",
        r"@\w+",
        r"[\w.+-]+@[\w.-]+\.\w+",
    ]
    while lines:
        tail = lines[-1].strip()
        if any(re.search(pattern, tail, flags=re.I) for pattern in signature_patterns):
            lines.pop()
            continue
        break
    return "\n".join(lines).rstrip() + "\n"


def _build_revision_suggestions(text: str, issues: list[str]) -> list[dict[str, str]]:
    suggestions: list[dict[str, str]] = []
    char_count = len(str(text or ""))
    if char_count < 1800:
        suggestions.append({
            "id": "expand_examples",
            "label": "补充案例和细节",
            "prompt": "在不增加空话的前提下，补充更多来自素材的案例、细节和对比，让文章更完整。",
        })
    if char_count > 5200:
        suggestions.append({
            "id": "tighten_structure",
            "label": "压缩结构",
            "prompt": "删掉重复表达和松散段落，让文章更紧凑，保留核心判断和关键素材。",
        })
    suggestions.extend([
        {
            "id": "stronger_opening",
            "label": "开头更抓人",
            "prompt": "重写开头 3-5 段，让第一屏更有判断、有画面感，不要用宏大背景铺垫。",
        },
        {
            "id": "more_khazix_voice",
            "label": "更像 Khazix",
            "prompt": "增强口语感、判断感和短句节奏，减少报告腔、总结腔和 AI 腔。",
        },
        {
            "id": "clearer_argument",
            "label": "主线更清楚",
            "prompt": "让文章主判断更早出现，并让每一节都服务同一个核心判断。",
        },
    ])
    if issues:
        suggestions.append({
            "id": "fix_quality_issues",
            "label": "按质检问题修",
            "prompt": "优先修复质检报告里指出的问题，但不要改变文章核心观点。",
        })
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in suggestions:
        if item["id"] in seen:
            continue
        seen.add(item["id"])
        deduped.append(item)
    return deduped[:5]


def build_quality_report(markdown: str, *, enabled: bool = True) -> dict[str, Any]:
    if not enabled:
        return {"enabled": False, "summary": "未开启自动质检", "issues": [], "layers": {}, "revision_suggestions": []}
    text = str(markdown or "")
    forbidden_hits = [word for word in FORBIDDEN_WORDS if word in text]
    punctuation_hits = [mark for mark in FORBIDDEN_PUNCTUATION if mark in text]
    paragraphs = [item.strip() for item in text.splitlines() if item.strip()]
    short_breaks = sum(1 for item in paragraphs if len(item) <= 12)
    has_emotion_mark = any(mark in text for mark in ["。", "？", "！", "= ="])
    tail = "\n".join(paragraphs[-8:])
    has_signature_footer = bool(re.search(r"(作者|署名|投稿|邮箱|联系|公众号|下次再见|谢谢你看|[\w.+-]+@[\w.-]+\.\w+)", tail, re.I))
    issues: list[str] = []
    if forbidden_hits:
        issues.append("存在 Khazix 禁用词或套话：" + "、".join(forbidden_hits[:6]))
    if punctuation_hits:
        issues.append("存在禁用标点：" + "、".join(sorted(set(punctuation_hits))))
    if len(text) < 1200:
        issues.append("正文偏短，可能还不像一篇完整长文")
    if short_breaks < 2:
        issues.append("缺少短句断裂，节奏可能偏平")
    if not has_emotion_mark:
        issues.append("缺少明显口语情绪，可以补一点活人感")
    if has_signature_footer:
        issues.append("文章尾部疑似包含署名、联系方式或固定尾巴，请删掉")
    return {
        "enabled": True,
        "generated_at": now_iso(),
        "summary": "通过基础质检" if not issues else f"发现 {len(issues)} 个需要注意的问题",
        "issues": issues[:5],
        "revision_suggestions": _build_revision_suggestions(text, issues),
        "layers": {
            "L1 硬性规则": {
                "pass": not forbidden_hits and not punctuation_hits and not has_signature_footer,
                "forbidden_words": forbidden_hits,
                "forbidden_punctuation": sorted(set(punctuation_hits)),
                "has_signature_footer": has_signature_footer,
            },
            "L2 风格一致性": {
                "pass": short_breaks >= 2 and has_emotion_mark,
                "short_breaks": short_breaks,
                "has_emotion_mark": has_emotion_mark,
            },
            "L3 内容质量": {
                "pass": len(text) >= 1200,
                "char_count": len(text),
            },
            "L4 活人感": {
                "pass": has_emotion_mark and short_breaks >= 2,
                "has_signature_footer": has_signature_footer,
            },
        },
    }


def generate_article_from_packet(
    *,
    packet: dict[str, Any],
    writer_profile: dict[str, Any],
    config_path: str | None = None,
) -> dict[str, Any]:
    settings = _resolve_settings(config_path=config_path)
    materials = _prepare_materials(packet, int(settings["max_materials"]), int(settings["max_chars_per_material"]))
    skill_prompt, skill_prompt_path = _load_skill_prompt(writer_profile)

    if settings["provider"] == "mock":
        markdown = _mock_article(packet, materials)
        source = "mock"
    elif settings["provider"] == "openai_compatible" and settings["model"] and settings["api_key"]:
        markdown = _call_openai_compatible_llm(
            _build_messages(packet, writer_profile, skill_prompt, materials),
            settings,
        )
        source = "llm"
    else:
        raise ValueError(
            "写作模型未配置。请设置 CREATE_STUDIO_WRITING_PROVIDER=openai_compatible、"
            "CREATE_STUDIO_WRITING_MODEL 和对应 API Key 后再生成文章。"
        )

    markdown = _strip_signature_footer(markdown)
    if not str(markdown or "").strip():
        raise ValueError("写作模型没有返回正文")

    return {
        "ok": True,
        "article_markdown": markdown.strip() + "\n",
        "title": _extract_title(markdown, str((packet.get("task") or {}).get("topic") or "未命名文章")),
        "quality_report": build_quality_report(markdown, enabled=bool(settings["quality_check_enabled"])),
        "generation_source": source,
        "model": str(settings.get("model") or "").strip(),
        "provider": str(settings.get("provider") or "").strip(),
        "materials_used": len(materials),
        "skill_prompt_path": skill_prompt_path,
    }
