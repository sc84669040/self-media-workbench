#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


SUPPORTED_PLATFORMS = {"youtube", "bilibili", "douyin"}
ANALYZER_VERSION = "video-transcript-analysis-v2"

ZH_STOPWORDS = {
    "我们", "你们", "他们", "这个", "那个", "这里", "那里", "因为", "所以", "然后", "就是", "一个", "一种", "已经",
    "可以", "如果", "还是", "没有", "不是", "其实", "自己", "时候", "现在", "非常", "比较", "进行", "通过",
    "对于", "关于", "以及", "但是", "而且", "的话", "这种", "那个", "这个", "很多", "一些", "一下", "什么",
    "怎么", "为什么", "是否", "需要", "觉得", "真的", "可能", "目前", "今天", "刚才", "就是", "知道", "看到",
    "一个", "一下", "大家", "东西", "内容", "视频", "作者", "标题", "作品", "我们会", "他们会",
}

EN_STOPWORDS = {
    "the", "and", "for", "that", "with", "this", "from", "have", "will", "your", "about", "there", "their", "what",
    "when", "where", "which", "into", "than", "then", "just", "also", "because", "while", "they", "them", "been",
    "were", "was", "are", "you", "our", "not", "but", "can", "could", "would", "should", "how", "why", "all", "any",
    "more", "most", "much", "many", "some", "such", "only", "very", "really", "over", "under", "after", "before",
    "today", "here", "there", "video", "title", "author", "content", "this", "that", "these", "those",
    "able", "unlocks", "using", "straight", "founder", "ceo", "would", "could", "should", "were", "been", "into",
    "what", "work", "right", "actually", "ideal", "early", "wanted", "guiding",
}

IMPACT_WORDS = {
    "发布", "推出", "上线", "更新", "增长", "下降", "突破", "替代", "重构", "自动化", "效率", "成本", "风险", "机会",
    "产品", "模型", "训练", "推理", "智能体", "agent", "benchmark", "inference", "deploy", "release", "launch", "api",
}


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _safe_list(items: list[str] | None, fallback: str) -> list[str]:
    values = [str(x).strip() for x in (items or []) if str(x).strip()]
    return values or [fallback]


def _dedupe_keep_order(items: list[str], limit: int | None = None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _collapse_ws(item)
        key = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff]+", "", text).lower()
        if not text or not key or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if limit and len(out) >= limit:
            break
    return out


def _slugify(value: str, fallback: str = "video-item") -> str:
    text = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff]+", "-", str(value or "").strip().lower()).strip("-")
    return text or fallback


def _platform_label(platform: str) -> str:
    mapping = {
        "youtube": "YouTube",
        "bilibili": "B站",
        "douyin": "抖音",
    }
    return mapping.get(str(platform or "").strip().lower(), "视频")


def _extract_youtube_id(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        host = (parsed.netloc or "").lower()
        if "youtu.be" in host:
            return parsed.path.strip("/")
        query_id = (parse_qs(parsed.query).get("v") or [""])[0].strip()
        if query_id:
            return query_id
        parts = [part for part in parsed.path.split("/") if part]
        if "shorts" in parts:
            idx = parts.index("shorts")
            if idx + 1 < len(parts):
                return parts[idx + 1]
    except Exception:
        return ""
    return ""


def _extract_douyin_id(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        parts = [part for part in parsed.path.split("/") if part]
        for idx, part in enumerate(parts):
            if part in {"video", "note"} and idx + 1 < len(parts):
                return parts[idx + 1]
        for part in reversed(parts):
            if re.fullmatch(r"\d{8,30}", part):
                return part
    except Exception:
        return ""
    return ""


def _extract_bilibili_id(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        path = parsed.path or ""
        match = re.search(r"/(BV[0-9A-Za-z]+)/?", path, re.I)
        if match:
            return match.group(1)
        match = re.search(r"/(av\d+)/?", path, re.I)
        if match:
            return match.group(1)
        for part in [p for p in path.split("/") if p]:
            if re.fullmatch(r"BV[0-9A-Za-z]+", part, re.I):
                return part
            if re.fullmatch(r"av\d+", part, re.I):
                return part
    except Exception:
        return ""
    return ""


def _infer_platform_from_url(url: str) -> str:
    raw = str(url or "").strip().lower()
    if not raw:
        return ""
    if "youtube.com/" in raw or "youtu.be/" in raw:
        return "youtube"
    if any(x in raw for x in ["bilibili.com/", "b23.tv/"]):
        return "bilibili"
    if any(x in raw for x in ["v.douyin.com/", "douyin.com/", "iesdouyin.com/"]):
        return "douyin"
    return ""


def _extract_platform_id(platform: str, url: str) -> str:
    if platform == "youtube":
        return _extract_youtube_id(url)
    if platform == "bilibili":
        return _extract_bilibili_id(url)
    if platform == "douyin":
        return _extract_douyin_id(url)
    return ""


def _normalize_transcript(raw: str, title: str = "") -> str:
    lines = []
    in_transcript_block = False
    for line in str(raw or "").splitlines():
        s = _collapse_ws(line)
        if not s:
            continue
        if s in {"ASR转写：", "页面字幕：", "字幕：", "Transcript:", "Transcript："}:
            in_transcript_block = True
            continue
        if not in_transcript_block and s.startswith(("标题：", "作者：", "作品ID：", "作品页：", "视频直链：", "说明：")):
            continue
        if title and s == _collapse_ws(title):
            continue
        lines.append(s)
    text = "\n".join(lines).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"([。！？!?；;])\1+", r"\1", text)
    return text.strip()


def _split_sentences(text: str) -> list[str]:
    raw = str(text or "").replace("\r", "\n")
    raw = re.sub(r"\n{2,}", "。\n", raw)
    parts = re.split(r"(?<=[。！？!?；;])\s+|(?<=\.)\s+(?=[A-Z\"'])|\n+", raw)
    sentences: list[str] = []
    for part in parts:
        s = _collapse_ws(part)
        if not s:
            continue
        if len(s) > 180 and ("。" in s or "." in s):
            subs = re.split(r"(?<=[。！？!?])\s*|(?<=\.)\s+(?=[A-Z\"'])", s)
            sentences.extend([_collapse_ws(x) for x in subs if _collapse_ws(x)])
            continue
        if len(s) > 140 and ("，" in s or "," in s):
            subs = re.split(r"(?<=[，,])\s*", s)
            sentences.extend([_collapse_ws(x) for x in subs if _collapse_ws(x)])
        else:
            sentences.append(s)
    return _dedupe_keep_order(sentences)


def _extract_candidate_terms(text: str) -> list[str]:
    values: list[str] = []
    en_pattern = re.compile(r"[A-Za-z][A-Za-z0-9+_.-]{1,30}")
    zh_pattern = re.compile(r"[\u4e00-\u9fff]{2,12}")
    for match in en_pattern.findall(text or ""):
        term = _collapse_ws(match)
        low = term.lower()
        if low in EN_STOPWORDS or len(low) <= 2:
            continue
        values.append(term)
    for match in zh_pattern.findall(text or ""):
        term = _collapse_ws(match)
        if term in ZH_STOPWORDS or len(term) < 2:
            continue
        values.append(term)
    return values


def _top_keywords(title: str, text: str, limit: int = 8) -> list[str]:
    counter: Counter[str] = Counter()
    for term in _extract_candidate_terms(text):
        key = term.lower()
        score = 1.0
        if key in {w.lower() for w in IMPACT_WORDS}:
            score += 1.0
        if re.search(r"\d", term):
            score += 0.6
        if len(term) >= 8:
            score += 0.2
        counter[term] += score
    for term in _extract_candidate_terms(title):
        counter[term] += 2.6

    ranked = sorted(counter.items(), key=lambda x: (-x[1], -len(x[0]), x[0]))
    return _dedupe_keep_order([term for term, _ in ranked], limit=limit)


def _sentence_score(sentence: str, keyword_weights: dict[str, float], title_terms: list[str]) -> float:
    s = _collapse_ws(sentence)
    if not s:
        return 0.0
    score = 0.0
    low = s.lower()
    length = len(s)
    score += min(length / 60.0, 1.6)
    if re.search(r"\d", s):
        score += 1.2
    if re.search(r"\b(launch|release|benchmark|api|agent|model|revenue|growth|deploy|gpu)\b", low):
        score += 1.2
    if re.search(r"发布|推出|上线|更新|增长|下降|效率|成本|风险|机会|模型|产品|训练|推理|智能体|工作流|自动化", s):
        score += 1.2
    if any(term and term.lower() in low for term in title_terms):
        score += 1.5
    if re.match(r"^(and|but|so|or|because|then)\b", low):
        score -= 1.2
    for keyword, weight in keyword_weights.items():
        if keyword.lower() in low:
            score += weight
    if length < 12:
        score -= 1.4
    if length > 140:
        score -= 0.8
    if s.endswith(("吗", "呢", "?", "？")):
        score -= 0.4
    return round(score, 3)


def _pick_summary(title: str, sentences: list[str], keyword_weights: dict[str, float], platform: str) -> str:
    if not sentences:
        return f"这条{_platform_label(platform)}内容目前只有基础信息，暂时还没有足够 transcript 支撑可靠摘要。"
    title_terms = _extract_candidate_terms(title)[:6]
    ranked = sorted(sentences, key=lambda s: _sentence_score(s, keyword_weights, title_terms), reverse=True)
    best = ranked[0]
    best = re.sub(r"^[，,。；;:：\-\s]+", "", best).strip()
    if len(best) > 90:
        best = best[:90].rstrip("，,。；;:： ") + "。"
    if not best.endswith(("。", "！", "？", ".", "!", "?")):
        best += "。"
    return best


def _pick_core_points(title: str, sentences: list[str], keyword_weights: dict[str, float], limit: int = 4) -> list[str]:
    title_terms = _extract_candidate_terms(title)[:6]
    ranked = sorted(sentences, key=lambda s: _sentence_score(s, keyword_weights, title_terms), reverse=True)
    points: list[str] = []
    for sentence in ranked:
        s = _collapse_ws(sentence)
        if len(s) < 18:
            continue
        if re.match(r"^(and|but|so|or|because|then)\b", s.lower()):
            continue
        if len(s) > 110:
            s = s[:110].rstrip("，,。；;:： ") + "。"
        if not s.endswith(("。", "！", "？", ".", "!", "?")):
            s += "。"
        points.append(s)
        if len(points) >= limit:
            break
    if not points and title:
        points = [f"这条内容围绕「{title}」展开，但 transcript 信息仍偏少，建议人工补看原视频。"]
    return _dedupe_keep_order(points, limit=limit)


def _pick_facts(sentences: list[str], keyword_weights: dict[str, float], limit: int = 4) -> list[str]:
    facts: list[tuple[float, str]] = []
    for sentence in sentences:
        s = _collapse_ws(sentence)
        if len(s) < 18:
            continue
        if re.match(r"^(and|but|so|or|because|then)\b", s.lower()):
            continue
        bonus = 0.0
        if re.search(r"\d", s):
            bonus += 2.0
        if re.search(r"%|亿美元|万人|万亿|kpi|token|latency|ms|秒|分钟|小时|天|月|年|版本|v\d|\b[A-Z]{2,}\b", s, re.I):
            bonus += 1.4
        if re.search(r"发布|推出|上线|支持|采用|达到|提升|降低|增长|下降|兼容|集成", s):
            bonus += 1.0
        score = _sentence_score(s, keyword_weights, []) + bonus
        facts.append((score, s))
    ranked = [text for _, text in sorted(facts, key=lambda x: x[0], reverse=True)]
    cleaned = []
    for item in ranked:
        text = item
        if len(text) > 95:
            text = text[:95].rstrip("，,。；;:： ") + "。"
        cleaned.append(text)
    return _dedupe_keep_order(cleaned, limit=limit) or ["当前 transcript 里可直接引用的硬信息不多，建议回看原视频核对细节。"]


def _pick_quotes(sentences: list[str], summary: str, limit: int = 4) -> list[str]:
    candidates: list[tuple[float, str]] = []
    for sentence in sentences:
        s = _collapse_ws(sentence).strip('"“”')
        if len(s) < 18 or len(s) > 90:
            continue
        if re.match(r"^(and|but|so|or|because|then|hi)\b", s.lower()):
            continue
        score = 0.0
        if re.search(r"不是|而是|关键|核心|真正|本质|不要|必须|终于|直接|一定|最重要", s):
            score += 2.0
        if re.search(r"\d", s):
            score += 0.6
        if len(s) <= 40:
            score += 0.8
        candidates.append((score, s))
    ranked = [text for _, text in sorted(candidates, key=lambda x: x[0], reverse=True)]
    out = _dedupe_keep_order(ranked, limit=limit)
    if not out and summary:
        out = [summary.rstrip("。")]
    return [f"「{item.rstrip('。')}」" for item in out[:limit]]


def _classify_content_type(title: str, text: str, platform: str) -> str:
    corpus = f"{title}\n{text}".lower()
    if re.search(r"采访|对谈|访谈|podcast|conversation|interview", corpus):
        return "访谈 / 对谈"
    if re.search(r"教程|实操|demo|演示|workflow|教程|how to|guide", corpus):
        return "教程 / 演示"
    if re.search(r"观点|判断|趋势|评论|看法|opinion|analysis", corpus):
        return "观点 / 评论"
    if re.search(r"发布|上线|更新|开源|release|launch|announce|introducing", corpus):
        return "资讯 / 发布"
    if platform == "douyin":
        return "短视频 / 口播 / 观点解读"
    if platform == "bilibili":
        return "中长视频 / 讲解 / 观察"
    return "视频 / 讲解 / 观察"


def _infer_topics_and_tags(platform: str, title: str, keywords: list[str]) -> tuple[list[str], list[str]]:
    topics: list[str] = []
    tags: list[str] = [platform, "video-transcript-analysis"]
    joined = " ".join([title, *keywords]).lower()

    mapping = [
        ("AI", ["ai", "大模型", "模型", "llm", "agent", "智能体", "openai", "claude", "gemini", "deepseek", "codex"]),
        ("智能体", ["agent", "智能体", "workflow", "自动化"]),
        ("模型能力", ["benchmark", "推理", "训练", "inference", "reasoning", "token"]),
        ("产品发布", ["发布", "上线", "release", "launch", "introducing", "更新"]),
        ("开发工具", ["api", "sdk", "cursor", "copilot", "mcp", "代码", "开发者", "codex", "runtime", "javascript", "c++", "rust"]),
        ("商业化", ["营收", "增长", "成本", "价格", "roi", "商业"]),
    ]
    for topic, hints in mapping:
        if any(h.lower() in joined for h in hints):
            topics.append(topic)
            tags.append(topic)
    tags.extend(keywords[:4])
    topics = _dedupe_keep_order(topics, limit=3) or [platform, "视频内容"]
    tags = _dedupe_keep_order(tags, limit=6)
    return topics, tags


def _build_angles(title: str, summary: str, keywords: list[str], content_type: str) -> list[str]:
    lead = keywords[0] if keywords else _collapse_ws(title)[:12] or "这条内容"
    angles = [
        f"从产品/行业角度写：{lead} 到底释放了什么新信号，为什么现在值得关注。",
        f"从实操角度写：这条{content_type}里哪些方法、流程或判断可以直接拿去复用。",
        f"从风险与边界角度写：{summary.rstrip('。')} 里哪些结论还需要二次核实。",
    ]
    return _dedupe_keep_order(angles, limit=3)


def _build_title_candidates(title: str, summary: str, keywords: list[str], platform: str) -> list[str]:
    lead = keywords[0] if keywords else (_collapse_ws(title)[:14] or "这条视频")
    if title:
        cleaned_title = re.sub(r"\b(what|how|why)\b", "", title, flags=re.I)
        cleaned_title = _collapse_ws(cleaned_title).strip("-:： ")
        if cleaned_title:
            lead = cleaned_title[:18]
    summary_short = summary.rstrip("。")[:24]
    base = [
        f"{lead} 这波到底在讲什么？我把重点拆开了",
        f"别只看标题，这条{_platform_label(platform)}内容真正重要的是：{summary_short}",
        f"关于 {lead}，最值得拿来写内容的 3 个点",
    ]
    if title and title not in base:
        base.insert(0, f"{title}：这条内容值不值得跟")
    return _dedupe_keep_order(base, limit=4)


def _build_extended_questions(summary: str, facts: list[str], keywords: list[str]) -> list[str]:
    lead = keywords[0] if keywords else "这条内容"
    out = [
        f"{lead} 这次提到的能力或判断，落到真实产品/业务里到底能省多少成本、带来多少效率提升？",
        f"除了视频里给出的说法，哪些关键前提、数据口径或案例还需要补证？",
    ]
    if facts:
        out[1] = f"视频里这些硬信息——{facts[0][:28]}——背后有没有更完整的数据、上下文或反例？"
    return _dedupe_keep_order(out, limit=2)


def _infer_confidence(transcript_text: str, sentences: list[str], facts: list[str]) -> str:
    length = len(_collapse_ws(transcript_text))
    if length >= 1800 and len(sentences) >= 10 and len(facts) >= 2:
        return "high"
    if length >= 500 and len(sentences) >= 4:
        return "medium"
    return "low"


def _infer_recommendation(title: str, summary: str, facts: list[str], keywords: list[str], confidence: str) -> tuple[str, str, str, float]:
    corpus = f"{title}\n{summary}\n{' '.join(facts)}\n{' '.join(keywords)}".lower()
    score = 5.2
    score += min(len(facts), 4) * 0.6
    if confidence == "high":
        score += 1.0
    elif confidence == "medium":
        score += 0.4
    else:
        score -= 0.8
    if re.search(r"发布|上线|更新|开源|release|launch|announce|introducing", corpus):
        score += 1.1
    if re.search(r"agent|智能体|模型|api|benchmark|workflow|自动化|产品", corpus):
        score += 0.8
    if re.search(r"猜测|可能|也许|不确定", corpus):
        score -= 0.6
    score = round(max(1.0, min(score, 9.8)), 2)
    if score >= 7.8:
        return "strong", "pending", "priority-watch", score
    if score >= 5.8:
        return "watch", "candidate", "watchlist", score
    return "skip", "discard", "low-signal", score


def _infer_creation_suggestion(recommendation: str, content_type: str, summary: str) -> str:
    if recommendation == "strong":
        return f"优先进入 NightHawk 强提醒，适合先做一版快评/拆解稿；理由是这条 {content_type} 已经有比较清晰的核心结论：{summary.rstrip('。')}。"
    if recommendation == "watch":
        return f"先进入候选池，适合做素材卡积累或选题预研；如果后续有相关热点，再把这条 {content_type} 拿出来二次展开。"
    return "先归档观察，不建议立刻进入正式创作；当前信息密度或确定性还不够。"


def _infer_risk_notes(payload: dict[str, Any], transcript_kind: str, confidence: str, transcript_text: str) -> list[str]:
    notes: list[str] = []
    if transcript_kind and ("asr" in transcript_kind.lower() or "auto" in transcript_kind.lower()):
        notes.append("当前 transcript 来自 ASR / 自动字幕，专有名词、英文术语和断句可能有误。")
    if confidence == "low":
        notes.append("当前 transcript 较短或信息不完整，结论更适合做线索，不适合直接当定论。")
    if len(_collapse_ws(transcript_text)) > 12000:
        notes.append("原始 transcript 较长，自动提炼可能遗漏上下文，正式写作前建议回看视频关键片段。")
    existing = [str(x).strip() for x in (payload.get("risk_notes") or []) if str(x).strip()]
    notes.extend(existing)
    return _dedupe_keep_order(notes, limit=4) or ["自动提炼已尽量压缩重点，但正式使用前仍建议人工复核上下文。"]


def _maybe_enrich(payload: dict[str, Any], platform: str, title: str, transcript_text: str) -> dict[str, Any]:
    sentences = _split_sentences(transcript_text)
    keywords = _top_keywords(title, transcript_text, limit=8)
    keyword_weights = {kw: 0.8 + math.log(idx + 2, 2) for idx, kw in enumerate(reversed(keywords))}

    summary = str(payload.get("one_sentence_summary") or payload.get("summary") or "").strip()
    if not summary:
        summary = _pick_summary(title, sentences, keyword_weights, platform)

    core_points = [str(x).strip() for x in (payload.get("core_points") or []) if str(x).strip()]
    if not core_points:
        core_points = _pick_core_points(title, sentences, keyword_weights, limit=4)

    facts = [str(x).strip() for x in (payload.get("facts_and_data") or []) if str(x).strip()]
    if not facts:
        facts = _pick_facts(sentences, keyword_weights, limit=4)

    quotes = [str(x).strip() for x in (payload.get("golden_quotes") or []) if str(x).strip()]
    if not quotes:
        quotes = _pick_quotes(sentences, summary, limit=4)

    content_type = str(payload.get("content_type") or "").strip() or _classify_content_type(title, transcript_text, platform)
    related_topics, tags = _infer_topics_and_tags(platform, title, keywords)

    existing_topics = [str(x).strip() for x in (payload.get("related_topics") or []) if str(x).strip()]
    if existing_topics:
        related_topics = _dedupe_keep_order(existing_topics + related_topics, limit=3)

    existing_tags = [str(x).strip() for x in (payload.get("tags") or []) if str(x).strip()]
    if existing_tags:
        tags = _dedupe_keep_order(existing_tags + tags, limit=6)

    angles = [str(x).strip() for x in (payload.get("angles") or []) if str(x).strip()]
    if not angles:
        angles = _build_angles(title, summary, keywords, content_type)

    title_candidates = [str(x).strip() for x in (payload.get("title_candidates") or []) if str(x).strip()]
    if not title_candidates:
        title_candidates = _build_title_candidates(title, summary, keywords, platform)

    extended_questions = [str(x).strip() for x in (payload.get("extended_questions") or []) if str(x).strip()]
    if not extended_questions:
        extended_questions = _build_extended_questions(summary, facts, keywords)

    confidence = str(payload.get("confidence") or "").strip() or _infer_confidence(transcript_text, sentences, facts)
    recommendation = str(payload.get("recommendation") or "").strip().lower()
    nighthawk_action = str(payload.get("nighthawk_action") or "").strip().lower()
    route_bucket = str(payload.get("route_bucket") or "").strip()
    if not recommendation or not nighthawk_action or not route_bucket:
        recommendation, nighthawk_action, route_bucket, score = _infer_recommendation(title, summary, facts, keywords, confidence)
    else:
        score = payload.get("score")
        try:
            score = float(score)
        except Exception:
            _, _, _, score = _infer_recommendation(title, summary, facts, keywords, confidence)

    creation_suggestion = str(payload.get("creation_suggestion") or "").strip() or _infer_creation_suggestion(recommendation, content_type, summary)
    recommendation_reason = str(payload.get("recommendation_reason") or "").strip() or (
        f"摘要与事实密度较高，主题集中在 {related_topics[0]}，当前更适合进入 {recommendation} 档持续跟踪。"
        if recommendation != "skip"
        else "当前内容更适合做低优先级归档观察。"
    )
    transcript_kind = str(payload.get("transcript_kind") or payload.get("transcript_source") or "").strip()
    risk_notes = _infer_risk_notes(payload, transcript_kind, confidence, transcript_text)

    return {
        "summary": summary,
        "one_sentence_summary": summary,
        "core_points": _dedupe_keep_order(core_points, limit=4),
        "facts_and_data": _dedupe_keep_order(facts, limit=4),
        "golden_quotes": _dedupe_keep_order(quotes, limit=4),
        "content_type": content_type,
        "related_topics": related_topics,
        "tags": tags,
        "angles": _dedupe_keep_order(angles, limit=3),
        "title_candidates": _dedupe_keep_order(title_candidates, limit=4),
        "extended_questions": _dedupe_keep_order(extended_questions, limit=2),
        "confidence": confidence,
        "recommendation": recommendation,
        "nighthawk_action": nighthawk_action,
        "route_bucket": route_bucket,
        "score": score,
        "creation_suggestion": creation_suggestion,
        "recommendation_reason": recommendation_reason,
        "risk_notes": risk_notes,
    }


def build_structured_payload(payload: dict[str, Any]) -> dict[str, Any]:
    source_url = str(payload.get("url") or payload.get("source_url") or "").strip()
    platform = str(payload.get("platform") or payload.get("channel") or _infer_platform_from_url(source_url) or "").strip().lower()
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"unsupported platform for transcript analysis: {platform}")

    title = str(payload.get("title") or "未命名视频").strip()
    source_name = str(payload.get("source_name") or payload.get("channel_name") or payload.get("author") or "").strip()
    published_at = str(payload.get("published_at") or "").strip()
    raw_transcript = str(payload.get("transcript_text") or payload.get("content_markdown") or "").strip()
    transcript_text = _normalize_transcript(raw_transcript, title=title)
    transcript_language = str(payload.get("transcript_language") or "").strip()
    transcript_kind = str(payload.get("transcript_kind") or payload.get("transcript_source") or "").strip()
    platform_id = str(payload.get("video_id") or payload.get("platform_id") or "").strip() or _extract_platform_id(platform, source_url)
    analyzed_at = datetime.now().isoformat(timespec="seconds")

    enriched = _maybe_enrich(payload, platform, title, transcript_text) if transcript_text else {}
    confidence = str(payload.get("confidence") or enriched.get("confidence") or ("medium" if transcript_text else "low")).strip()
    recommendation = str(payload.get("recommendation") or enriched.get("recommendation") or ("watch" if transcript_text else "skip")).strip()
    nighthawk_action = str(payload.get("nighthawk_action") or enriched.get("nighthawk_action") or ("candidate" if recommendation != "skip" else "discard")).strip()
    route_bucket = str(payload.get("route_bucket") or enriched.get("route_bucket") or f"{platform}-{recommendation}").strip()
    review_needed = bool(payload.get("review_needed", confidence != "high" or "asr" in transcript_kind.lower()))
    status = str(payload.get("status") or ("ready" if transcript_text else "skipped")).strip()
    slug = str(payload.get("slug") or _slugify(f"{platform}-{source_name}-{title}", fallback=f"{platform}-item"))

    raw_excerpt = transcript_text[:1200].strip()
    if raw_excerpt:
        raw_excerpt = raw_excerpt.replace("\n", " ")

    if platform == "douyin":
        content_type_default = "短视频 / 口播 / 观点解读"
    elif platform == "bilibili":
        content_type_default = "中长视频 / 讲解 / 观察"
    else:
        content_type_default = "视频 / 讲解 / 观察"
    summary_default = "这条视频已抓到 transcript，但自动摘要仍未生成。"
    suggestion_default = "适合先进入素材池，待后续结合同主题内容再决定是否升级为正式选题。"

    return {
        "platform": platform,
        "slug": slug,
        "platform_id": platform_id,
        "title": title,
        "source_name": source_name,
        "source_url": source_url,
        "published_at": published_at,
        "analyzed_at": analyzed_at,
        "analyzer_version": ANALYZER_VERSION,
        "content_type": str(payload.get("content_type") or enriched.get("content_type") or content_type_default).strip(),
        "confidence": confidence,
        "status": status,
        "recommendation": recommendation,
        "recommendation_reason": str(payload.get("recommendation_reason") or enriched.get("recommendation_reason") or "待补充推荐理由。").strip(),
        "nighthawk_action": nighthawk_action,
        "review_needed": review_needed,
        "route_bucket": route_bucket,
        "score": enriched.get("score") if payload.get("score") is None else payload.get("score"),
        "transcript_language": transcript_language,
        "transcript_kind": transcript_kind,
        "related_topics": _safe_list((payload.get("related_topics") or enriched.get("related_topics")), platform),
        "summary": str(payload.get("one_sentence_summary") or payload.get("summary") or enriched.get("summary") or summary_default).strip(),
        "one_sentence_summary": str(payload.get("one_sentence_summary") or payload.get("summary") or enriched.get("summary") or summary_default).strip(),
        "core_points": _safe_list((payload.get("core_points") or enriched.get("core_points")), "待提炼核心要点。"),
        "facts_and_data": _safe_list((payload.get("facts_and_data") or enriched.get("facts_and_data")), "待提取关键事实 / 数据。"),
        "tags": _safe_list((payload.get("tags") or enriched.get("tags")), platform),
        "golden_quotes": _safe_list((payload.get("golden_quotes") or enriched.get("golden_quotes")), "待提炼可直接引用的原话 / 金句。"),
        "angles": _safe_list((payload.get("angles") or enriched.get("angles")), "待提炼可写角度。"),
        "title_candidates": _safe_list((payload.get("title_candidates") or enriched.get("title_candidates")), "待生成标题候选。"),
        "extended_questions": _safe_list((payload.get("extended_questions") or enriched.get("extended_questions")), "待补充可延展问题。"),
        "risk_notes": _safe_list((payload.get("risk_notes") or enriched.get("risk_notes")), "自动提炼已完成，但正式使用前仍建议人工复核上下文。"),
        "creation_suggestion": str(payload.get("creation_suggestion") or enriched.get("creation_suggestion") or suggestion_default).strip(),
        "transcript_excerpt": raw_excerpt,
    }


def build_analysis_card(payload: dict[str, Any]) -> str:
    structured = build_structured_payload(payload)
    platform = structured["platform"]

    lines: list[str] = [
        "---",
        f'title: "{structured["title"]}"',
        f'source_type: "{platform}"',
        f'source_name: "{structured["source_name"]}"',
        f'source_url: "{structured["source_url"]}"',
        f'published_at: "{structured["published_at"]}"',
        f'analyzed_at: "{structured["analyzed_at"]}"',
        f'analyzer_version: "{structured["analyzer_version"]}"',
        f'content_type: "{structured["content_type"]}"',
        f'confidence: "{structured["confidence"]}"',
        f'status: "{structured["status"]}"',
        f'recommendation: "{structured["recommendation"]}"',
        f'nighthawk_action: "{structured["nighthawk_action"]}"',
        f'review_needed: {str(structured["review_needed"]).lower()}',
        f'route_bucket: "{structured["route_bucket"]}"',
        "related_topics:",
        *[f'  - "{topic}"' for topic in structured["related_topics"]],
        "---",
        "",
        f"# 分析卡片：{structured['title']}",
        "",
        "## 0. 关联信息",
        f"- 平台：{platform}",
        f"- 来源账号：{structured['source_name'] or '-'}",
        f"- 原始链接：{structured['source_url'] or '-'}",
        f"- 发布时间：{structured['published_at'] or '-'}",
        f"- transcript 语言：{structured['transcript_language'] or '-'}",
        f"- transcript 来源：{structured['transcript_kind'] or '-'}",
        f"- 平台内容 ID：{structured['platform_id'] or '-'}",
        f"- 评分：{structured.get('score') if structured.get('score') is not None else '-'}",
        "",
        "## 1. 一句话摘要",
        structured["summary"],
        "",
        "## 2. 核心要点",
    ]
    lines.extend([f"- {item}" for item in structured["core_points"]])
    lines.extend(["", "## 3. 关键事实 / 数据"])
    lines.extend([f"- {item}" for item in structured["facts_and_data"]])
    lines.extend(["", "## 4. 标签"])
    lines.extend([f"- {item}" for item in structured["tags"]])
    lines.extend(["", "## 5. 可直接引用的原话 / 金句"])
    lines.extend([f"- {item}" for item in structured["golden_quotes"]])
    lines.extend(["", "## 6. 可写角度"])
    lines.extend([f"- {item}" for item in structured["angles"]])
    lines.extend(["", "## 7. 标题候选"])
    lines.extend([f"- {item}" for item in structured["title_candidates"]])
    lines.extend(["", "## 8. 可延展问题"])
    lines.extend([f"- {item}" for item in structured["extended_questions"]])
    lines.extend(["", "## 9. 风险提示"])
    lines.extend([f"- {item}" for item in structured["risk_notes"]])
    lines.extend([
        "",
        "## 10. 创作建议",
        f"- {structured['creation_suggestion']}",
        "",
        "## 11. 候选判断 / NightHawk 路由",
        f"- recommendation：{structured['recommendation']}",
        f"- recommendation_reason：{structured['recommendation_reason']}",
        f"- nighthawk_action：{structured['nighthawk_action']}",
        f"- review_needed：{structured['review_needed']}",
        f"- route_bucket：{structured['route_bucket']}",
    ])
    if structured["transcript_excerpt"]:
        lines.extend(["", "## 12. Transcript 摘录（原始材料预览）", structured["transcript_excerpt"]])
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a structured video transcript analysis card from JSON payload")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--output", help="Output markdown file path")
    parser.add_argument("--json-output", help="Optional normalized JSON output path")
    args = parser.parse_args()

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    card = build_analysis_card(payload)
    structured = build_structured_payload(payload)

    if args.output:
        Path(args.output).write_text(card, encoding="utf-8")
        print(args.output)
    else:
        print(card)

    if args.json_output:
        Path(args.json_output).write_text(json.dumps(structured, ensure_ascii=False, indent=2), encoding="utf-8")
        print(args.json_output)


if __name__ == "__main__":
    main()
