#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


def _safe_list(items: list[str] | None, fallback: str) -> list[str]:
    values = [str(x).strip() for x in (items or []) if str(x).strip()]
    return values or [fallback]


def _slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return text or "youtube-item"


def _extract_video_id(url: str) -> str:
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


def build_structured_payload(payload: dict[str, Any]) -> dict[str, Any]:
    title = str(payload.get("title") or "未命名视频").strip()
    channel_name = str(payload.get("channel_name") or payload.get("source_name") or "").strip()
    source_url = str(payload.get("url") or payload.get("source_url") or "").strip()
    published_at = str(payload.get("published_at") or "").strip()
    transcript_text = str(payload.get("transcript_text") or "").strip()
    transcript_language = str(payload.get("transcript_language") or "").strip()
    transcript_kind = str(payload.get("transcript_kind") or "").strip()
    video_id = str(payload.get("video_id") or "").strip() or _extract_video_id(source_url)
    analyzer_version = "youtube-analysis-card-v1-draft"
    analyzed_at = datetime.now().isoformat(timespec="seconds")

    confidence = str(payload.get("confidence") or ("medium" if transcript_text else "low")).strip()
    status = str(payload.get("status") or ("draft" if transcript_text else "skipped")).strip()
    content_type = str(payload.get("content_type") or "访谈 / 演示 / 教程待判断").strip()
    recommendation = str(payload.get("recommendation") or ("watch" if transcript_text else "skip")).strip()
    recommendation_reason = str(
        payload.get("recommendation_reason")
        or payload.get("creation_suggestion")
        or "待补充推荐理由。"
    ).strip()
    nighthawk_action = str(payload.get("nighthawk_action") or ("candidate" if recommendation != "skip" else "discard")).strip()
    review_needed = bool(payload.get("review_needed", True))
    related_topics = _safe_list(payload.get("related_topics"), "YouTube")
    route_bucket = str(payload.get("route_bucket") or f"youtube-{recommendation}").strip()
    slug = str(payload.get("slug") or _slugify(f"{channel_name}-{title}"))

    raw_excerpt = transcript_text[:1200].strip()
    if raw_excerpt:
        raw_excerpt = raw_excerpt.replace("\n", " ")

    summary = str(payload.get("one_sentence_summary") or "待根据 transcript 提炼一句话摘要。").strip()
    core_points = _safe_list(payload.get("core_points"), "待提炼核心要点。")
    facts = _safe_list(payload.get("facts_and_data"), "待提取关键事实 / 数据。")
    tags = _safe_list(payload.get("tags"), "youtube")
    quotes = _safe_list(payload.get("golden_quotes"), "待提炼可直接引用的原话 / 金句。")
    angles = _safe_list(payload.get("angles"), "待提炼可写角度。")
    title_candidates = _safe_list(payload.get("title_candidates"), "待生成标题候选。")
    questions = _safe_list(payload.get("extended_questions"), "待补充可延展问题。")
    risk_notes = _safe_list(payload.get("risk_notes"), "当前为草稿卡片，仍需人工复核 transcript 质量与上下文。")
    creation_suggestion = str(payload.get("creation_suggestion") or "适合先作为观察素材，待摘要与要点提炼完成后再决定是否进入正式创作。").strip()

    return {
        "platform": "youtube",
        "slug": slug,
        "video_id": video_id,
        "title": title,
        "source_name": channel_name,
        "source_url": source_url,
        "published_at": published_at,
        "analyzed_at": analyzed_at,
        "analyzer_version": analyzer_version,
        "content_type": content_type,
        "confidence": confidence,
        "status": status,
        "recommendation": recommendation,
        "recommendation_reason": recommendation_reason,
        "nighthawk_action": nighthawk_action,
        "review_needed": review_needed,
        "route_bucket": route_bucket,
        "transcript_language": transcript_language,
        "transcript_kind": transcript_kind,
        "related_topics": related_topics,
        "summary": summary,
        "core_points": core_points,
        "facts_and_data": facts,
        "tags": tags,
        "golden_quotes": quotes,
        "angles": angles,
        "title_candidates": title_candidates,
        "extended_questions": questions,
        "risk_notes": risk_notes,
        "creation_suggestion": creation_suggestion,
        "transcript_excerpt": raw_excerpt,
    }


def build_analysis_card(payload: dict[str, Any]) -> str:
    structured = build_structured_payload(payload)

    lines: list[str] = [
        "---",
        f'title: "{structured["title"]}"',
        'source_type: "youtube"',
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
        f"- 来源频道：{structured['source_name'] or '-'}",
        f"- 原始链接：{structured['source_url'] or '-'}",
        f"- 发布时间：{structured['published_at'] or '-'}",
        f"- 字幕语言：{structured['transcript_language'] or '-'}",
        f"- 字幕类型：{structured['transcript_kind'] or '-'}",
        f"- 视频 ID：{structured['video_id'] or '-'}",
        "",
        "## 1. 一句话摘要",
        structured["summary"],
        "",
        "## 2. 核心要点",
    ]
    lines.extend([f"- {item}" for item in structured["core_points"]])
    lines.extend([
        "",
        "## 3. 关键事实 / 数据",
    ])
    lines.extend([f"- {item}" for item in structured["facts_and_data"]])
    lines.extend([
        "",
        "## 4. 标签",
    ])
    lines.extend([f"- {item}" for item in structured["tags"]])
    lines.extend([
        "",
        "## 5. 可直接引用的原话 / 金句",
    ])
    lines.extend([f"- {item}" for item in structured["golden_quotes"]])
    lines.extend([
        "",
        "## 6. 可写角度",
    ])
    lines.extend([f"- {item}" for item in structured["angles"]])
    lines.extend([
        "",
        "## 7. 标题候选",
    ])
    lines.extend([f"- {item}" for item in structured["title_candidates"]])
    lines.extend([
        "",
        "## 8. 可延展问题",
    ])
    lines.extend([f"- {item}" for item in structured["extended_questions"]])
    lines.extend([
        "",
        "## 9. 风险提示",
    ])
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
        lines.extend([
            "",
            "## 12. Transcript 摘录（原始材料预览）",
            structured["transcript_excerpt"],
        ])

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a draft YouTube analysis card from JSON payload")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--output", help="Output markdown file path")
    parser.add_argument("--json-output", help="Optional normalized JSON output path for NightHawk integration")
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
