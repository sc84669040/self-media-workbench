#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = BASE_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from content_object_models import content_object_from_raw_item
from runtime_config import event_radar_db_path

DB_PATH = event_radar_db_path()
DEFAULT_WINDOW_DAYS = 7

AI_STRONG_KEYWORDS = [
    "openai",
    "anthropic",
    "claude",
    "chatgpt",
    "gpt",
    "codex",
    "sora",
    "gemini",
    "gemma",
    "deepseek",
    "kimi",
    "cursor",
    "copilot",
    "windsurf",
    "midjourney",
    "runway",
    "flux",
    "stable diffusion",
    "llm",
    "agent",
    "agents",
    "mcp",
    "rag",
    "embedding",
    "aigc",
    "agi",
    "具身智能",
    "机器人",
    "智能体",
    "大模型",
    "推理模型",
    "多模态",
    "生成式",
    "提示词",
    "工作流",
    "知识库",
    "向量",
    "算力",
    "gpu",
    "nvidia",
    "英伟达",
]

AI_WEAK_KEYWORDS = ["ai", "模型", "推理", "自动驾驶", "芯片"]
EXCLUDE_KEYWORDS = ["旅游攻略", "彩票", "电摩", "摩托车", "serverless"]


@dataclass(frozen=True)
class SubjectRule:
    key: str
    label: str
    priority: int
    patterns: tuple[str, ...]


@dataclass(frozen=True)
class FacetRule:
    key: str
    label: str
    priority: int
    patterns: tuple[str, ...]


SUBJECT_RULES: tuple[SubjectRule, ...] = (
    SubjectRule("claude_design", "Claude Design", 170, ("claude design", "claude 设计", "claude设计")),
    SubjectRule("claude_code", "Claude Code", 170, ("claude code", "claude.md", "agents.md")),
    SubjectRule("claude_opus", "Claude Opus", 166, ("claude opus",)),
    SubjectRule("gpt_image_2", "GPT Image 2", 168, ("gpt image 2", "gpt-image-2")),
    SubjectRule("codex", "Codex", 166, (" codex", "codex ", "openai codex")),
    SubjectRule("chatgpt_gpt5", "ChatGPT / GPT-5", 162, ("chatgpt", "gpt-5", "gpt 5", "gpt pro")),
    SubjectRule("sora", "Sora", 158, ("sora",)),
    SubjectRule("hermes_agent", "Hermes Agent", 170, ("hermes agent", "hermes-agent", " hermes ")),
    SubjectRule("mcp", "MCP", 168, (" mcp", "mcp ", "model context protocol")),
    SubjectRule("gemini", "Gemini", 162, ("gemini",)),
    SubjectRule("gemma", "Gemma", 160, ("gemma",)),
    SubjectRule("deepseek", "DeepSeek", 160, ("deepseek", "深度求索")),
    SubjectRule("cursor", "Cursor / Copilot", 158, ("cursor", "copilot", "windsurf")),
    SubjectRule("embodied_ai", "具身智能 / 机器人", 156, ("具身智能", "robot", "机器人", "humanoid", "人形机器人")),
    SubjectRule("compute", "算力与芯片", 154, ("nvidia", "英伟达", "gpu", "算力", "芯片", "tpu", "inference chip")),
    SubjectRule("anthropic", "Anthropic / Claude", 124, ("anthropic", "claude")),
    SubjectRule("openai", "OpenAI 生态", 122, ("openai", "gpt")),
    SubjectRule("google_ai", "Google AI", 120, ("google ai", "google deepmind", "deepmind", "谷歌ai", "谷歌 ai")),
    SubjectRule("agent", "Agent", 116, ("agent", "agents", "智能体", "工作流", "workflow", "a2a", "orchestration")),
)


FACET_RULES: tuple[FacetRule, ...] = (
    FacetRule("prompting", "使用方法与提示词", 64, ("提示词", "最佳实践", "玩法", "指南", "怎么用", "playbook", "学习模式", "心智模型", "使用方法", "教程")),
    FacetRule("showcase", "案例实测", 62, ("实测", "案例", "体验", "原型", "作品", "生成效果", "海报", "插画", "网页原型", "上手体验")),
    FacetRule("release", "发布升级", 60, ("发布", "上线", "推出", "preview", "release", "更新", "升级", "版本", "模型更新")),
    FacetRule("capital", "组织资本", 58, ("融资", "ipo", "估值", "收购", "并购", "leadership", "ceo", "管理层", "董事会")),
    FacetRule("business", "创业与产品化", 57, ("创业", "人才回流", "产品化", "商业模式", "ltv", "公司运转", "创始人", "基金押注", "回流")),
    FacetRule("ecosystem", "生态扩展", 56, ("社区", "生态", "plugin", "plugins", "skills", "skill", "扩展", "star", "开源项目", "集成")),
    FacetRule("security", "风险争议", 54, ("安全", "风险", "争议", "trap", "jailbreak", "closed source", "闭源", "黑箱", "攻击", "泄露")),
    FacetRule("infra", "工程接入与基础设施", 52, ("tpu", "gpu", "基础设施", "infra", "压缩", "推理", "device", "本地优先", "on-device", "部署")),
    FacetRule("pricing_api", "API 与计费入口", 51, ("api", "billing", "quota", "额度", "计费", "pricing", "prepaid", "token", "调用", "context window")),
    FacetRule("product", "产品入口与体验", 50, ("客户端", "桌面客户端", "app", "应用", "入口", "体验", "唤起", "mac", "windows", "android", "ios", "ai studio", "voice")),
    FacetRule("office", "办公入口与集成", 50, ("word", "docs", "doc", "office", "workspace", "desktop app", "for word")),
    FacetRule("research", "研究评测", 49, ("论文", "paper", "research", "deep research", "研究", "benchmark", "评测", "测评")),
    FacetRule("usecase", "行业落地与用例", 48, ("用例", "部署", "落地", "产业", "收益", "机器人", "spot", "boston dynamics", "企业实践")),
    FacetRule("automation", "软件操控与自动化", 48, ("blender", "渲染", "3d", "gui", "自动化", "computer use", "实验室", "一站解决", "软件操作")),
    FacetRule("competition", "赛事与样机进展", 47, ("半马", "夺冠", "比赛", "竞赛", "样机", "闪电")),
    FacetRule("general", "动态与讨论", 10, ()),
)


TOPIC_TEMPLATES: dict[tuple[str, str], str] = {
    ("claude_code", "prompting"): "Claude Code 使用方法",
    ("claude_code", "infra"): "Claude Code 工程接入",
    ("claude_code", "release"): "Claude Code 发布升级",
    ("claude_code", "ecosystem"): "Claude Code 生态扩展",
    ("claude_code", "product"): "Claude Code 产品体验",
    ("claude_code", "general"): "Claude Code 使用案例与讨论",
    ("claude_design", "showcase"): "Claude Design 案例实测",
    ("claude_design", "prompting"): "Claude Design 使用方法",
    ("claude_design", "product"): "Claude Design 产品体验",
    ("claude_design", "general"): "Claude Design 产品体验与讨论",
    ("claude_opus", "release"): "Claude Opus 更新与评测",
    ("claude_opus", "research"): "Claude Opus 评测对比",
    ("claude_opus", "general"): "Claude Opus 动态与讨论",
    ("anthropic", "capital"): "Anthropic 组织资本",
    ("anthropic", "prompting"): "Claude 通用使用与心智模型",
    ("anthropic", "security"): "Anthropic / Claude 风险争议",
    ("anthropic", "product"): "Claude 产品入口与体验",
    ("anthropic", "pricing_api"): "Claude API 与计费入口",
    ("anthropic", "office"): "Claude 办公入口与集成",
    ("anthropic", "release"): "Claude 能力更新",
    ("anthropic", "research"): "Claude 评测与研究",
    ("anthropic", "general"): "Anthropic / Claude 产品与组织动态",
    ("gpt_image_2", "showcase"): "GPT Image 2 案例与提示词",
    ("gpt_image_2", "prompting"): "GPT Image 2 提示词与使用方法",
    ("gpt_image_2", "product"): "GPT Image 2 产品体验",
    ("gpt_image_2", "release"): "GPT Image 2 能力更新",
    ("gpt_image_2", "general"): "GPT Image 2 动态与讨论",
    ("codex", "prompting"): "Codex 使用方法",
    ("codex", "infra"): "Codex 工程接入",
    ("codex", "ecosystem"): "Codex 社区扩展",
    ("codex", "product"): "Codex 产品体验",
    ("codex", "release"): "Codex 发布升级",
    ("codex", "pricing_api"): "Codex API 与计费入口",
    ("codex", "general"): "Codex 产品与使用讨论",
    ("chatgpt_gpt5", "release"): "ChatGPT / GPT-5 动态",
    ("chatgpt_gpt5", "product"): "ChatGPT / GPT-5 产品体验",
    ("chatgpt_gpt5", "pricing_api"): "ChatGPT / GPT-5 API 与计费入口",
    ("chatgpt_gpt5", "usecase"): "ChatGPT / GPT-5 行业落地",
    ("chatgpt_gpt5", "security"): "ChatGPT / GPT-5 风险争议",
    ("chatgpt_gpt5", "general"): "ChatGPT / GPT-5 动态与讨论",
    ("sora", "showcase"): "Sora 案例实测",
    ("sora", "product"): "Sora 产品体验",
    ("sora", "release"): "Sora 发布升级",
    ("sora", "general"): "Sora 动态与讨论",
    ("hermes_agent", "ecosystem"): "Hermes Agent 社区扩展",
    ("hermes_agent", "infra"): "Hermes Agent 接入与编排",
    ("hermes_agent", "usecase"): "Hermes Agent 用例与落地",
    ("hermes_agent", "general"): "Hermes Agent 动态与讨论",
    ("mcp", "ecosystem"): "MCP 接入与生态",
    ("mcp", "infra"): "MCP 协议与基建",
    ("mcp", "pricing_api"): "MCP 接口与接入成本",
    ("mcp", "general"): "MCP 动态与讨论",
    ("agent", "security"): "Agent 安全与评测",
    ("agent", "infra"): "Agent 基建与工作流",
    ("agent", "showcase"): "Agent 案例实测",
    ("agent", "business"): "Agent 创业与产品化",
    ("agent", "usecase"): "Agent 行业落地与用例",
    ("agent", "automation"): "Agent 软件操控与自动化",
    ("agent", "ecosystem"): "Agent 生态扩展",
    ("agent", "research"): "Agent 评测与研究",
    ("agent", "release"): "Agent 产品与能力更新",
    ("agent", "general"): "Agent 生态与产品讨论",
    ("gemini", "infra"): "Gemini 工程接入与基础设施",
    ("gemini", "security"): "Gemini 风险与安全讨论",
    ("gemini", "research"): "Gemini 评测与研究",
    ("gemini", "product"): "Gemini 产品入口与体验",
    ("gemini", "usecase"): "Gemini 行业落地与用例",
    ("gemini", "release"): "Gemini 发布升级",
    ("gemini", "pricing_api"): "Gemini API 与计费入口",
    ("gemini", "general"): "Gemini 产品与能力讨论",
    ("gemma", "release"): "Gemma 模型进展",
    ("gemma", "infra"): "Gemma 工程接入",
    ("gemma", "research"): "Gemma 评测与研究",
    ("gemma", "general"): "Gemma 动态与讨论",
    ("deepseek", "capital"): "DeepSeek 资本与组织变化",
    ("deepseek", "release"): "DeepSeek 模型动态",
    ("deepseek", "research"): "DeepSeek 评测与研究",
    ("deepseek", "product"): "DeepSeek 产品体验",
    ("deepseek", "general"): "DeepSeek 产品与模型讨论",
    ("cursor", "prompting"): "Cursor / Copilot 使用方法",
    ("cursor", "product"): "Cursor / Copilot 产品体验",
    ("cursor", "pricing_api"): "Cursor / Copilot 计费与入口",
    ("cursor", "release"): "Cursor / Copilot 发布升级",
    ("cursor", "general"): "Cursor / Copilot 动态与讨论",
    ("compute", "infra"): "算力与芯片基础设施",
    ("compute", "capital"): "算力与芯片资本动态",
    ("compute", "showcase"): "算力与芯片案例实测",
    ("compute", "general"): "算力与芯片产业动态",
    ("embodied_ai", "usecase"): "具身智能 / 机器人 用例与落地",
    ("embodied_ai", "business"): "具身智能 / 机器人 创业与资本",
    ("embodied_ai", "competition"): "具身智能 / 机器人 赛事与样机",
    ("embodied_ai", "release"): "具身智能 / 机器人 发布升级",
    ("embodied_ai", "research"): "具身智能 / 机器人 评测与研究",
    ("embodied_ai", "general"): "具身智能 / 机器人 产业动态",
    ("openai", "pricing_api"): "OpenAI API 与计费入口",
    ("openai", "product"): "OpenAI 产品入口与体验",
    ("openai", "release"): "OpenAI 产品与模型更新",
    ("openai", "ecosystem"): "OpenAI 生态扩展",
    ("openai", "general"): "OpenAI 产品与生态动态",
    ("google_ai", "infra"): "Google AI 基础设施",
    ("google_ai", "research"): "Google AI 研究与安全",
    ("google_ai", "product"): "Google AI 产品入口与体验",
    ("google_ai", "general"): "Google AI 产品与研究动态",
}


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def is_ai_relevant(title: str, body_text: str) -> bool:
    text = f"{title or ''}\n{body_text or ''}".lower()
    if any(keyword.lower() in text for keyword in EXCLUDE_KEYWORDS):
        return False
    strong_hits = sum(1 for keyword in AI_STRONG_KEYWORDS if keyword.lower() in text)
    weak_hits = sum(1 for keyword in AI_WEAK_KEYWORDS if keyword.lower() in text)
    return strong_hits >= 1 or weak_hits >= 2


def score_rule_matches(text: str, patterns: tuple[str, ...]) -> int:
    lowered = text.lower()
    return sum(1 for pattern in patterns if pattern.lower() in lowered)


def pick_subject(title: str, body_text: str, source_name: str) -> SubjectRule | None:
    title_text = normalize_text(title)
    summary_text = normalize_text(body_text)[:1200]
    source_text = normalize_text(source_name)
    scored: list[tuple[int, int, SubjectRule]] = []
    for index, rule in enumerate(SUBJECT_RULES):
        title_hits = score_rule_matches(title_text, rule.patterns)
        source_hits = score_rule_matches(source_text, rule.patterns)
        body_hits = score_rule_matches(summary_text, rule.patterns)
        if title_hits == 0 and source_hits == 0 and body_hits < 2:
            continue
        score = rule.priority + title_hits * 6 + source_hits * 4 + min(body_hits, 3)
        scored.append((score, -index, rule))
    if not scored:
        return None
    scored.sort(reverse=True)
    return scored[0][2]


def pick_facet(title: str, body_text: str, source_name: str) -> FacetRule | None:
    title_text = normalize_text(title)
    summary_text = normalize_text(body_text)[:1200]
    source_text = normalize_text(source_name)
    scored: list[tuple[int, int, FacetRule]] = []
    for index, rule in enumerate(FACET_RULES):
        if rule.key == "general":
            continue
        title_hits = score_rule_matches(title_text, rule.patterns)
        source_hits = score_rule_matches(source_text, rule.patterns)
        body_hits = score_rule_matches(summary_text, rule.patterns)
        if title_hits == 0 and source_hits == 0 and body_hits == 0:
            continue
        score = rule.priority + title_hits * 5 + source_hits * 3 + min(body_hits, 3)
        scored.append((score, -index, rule))
    if not scored:
        return None
    scored.sort(reverse=True)
    return scored[0][2]


def facet_by_key(key: str) -> FacetRule:
    for rule in FACET_RULES:
        if rule.key == key:
            return rule
    raise KeyError(key)


def has_any(text: str, *tokens: str) -> bool:
    lowered = text.lower()
    return any(token.lower() in lowered for token in tokens)


def infer_subject_specific_facet(subject: SubjectRule, title: str, body_text: str, source_name: str) -> FacetRule:
    text = f"{normalize_text(title)}\n{normalize_text(body_text)[:1600]}\n{normalize_text(source_name)}".lower()

    if subject.key == "claude_code":
        if has_any(text, "教程", "怎么用", "最佳实践", "claude.md", "agents.md", "rule file", "提示词", "seo"):
            return facet_by_key("prompting")
        if has_any(text, "plugin", "skill", "skills", "github", "star", "game studios", "open source", "开源"):
            return facet_by_key("ecosystem")
        if has_any(text, "computer use", "browser", "comment mode", "client", "产品", "体验"):
            return facet_by_key("product")
        if has_any(text, "multi-agent", "自治工程", "workflow", "issue", "接入", "编排", "cli", "sdk"):
            return facet_by_key("infra")
        if has_any(text, "发布", "更新", "opus 4.7", "上线", "新功能"):
            return facet_by_key("release")
        return facet_by_key("prompting")

    if subject.key == "claude_design":
        if has_any(text, "提示词", "系统提示词", "教程", "怎么用", "design system", "ui kit"):
            return facet_by_key("prompting")
        if has_any(text, "发布", "上线", "introducing", "anthropic labs"):
            return facet_by_key("release")
        if has_any(text, "prototype", "slides", "one-pager", "案例", "实测", "网页原型"):
            return facet_by_key("showcase")
        return facet_by_key("showcase")

    if subject.key == "claude_opus":
        if has_any(text, "api", "计费", "token", "定价", "pricing"):
            return facet_by_key("pricing_api")
        if has_any(text, "benchmark", "评测", "vision", "视觉", "分辨率", "研究"):
            return facet_by_key("research")
        if has_any(text, "发布", "更新", "上线", "4.7", "4.6", "新模型"):
            return facet_by_key("release")
        return facet_by_key("research")

    if subject.key == "gemini":
        if has_any(text, "api", "billing", "prepaid", "token", "vertex ai", "quota", "额度", "计费"):
            return facet_by_key("pricing_api")
        if has_any(text, "mac", "desktop", "客户端", "app", "ai studio", "voice", "android", "ios", "workspace", "chrome"):
            return facet_by_key("product")
        if has_any(text, "tpu", "on-device", "device", "infra", "推理", "部署", "本地"):
            return facet_by_key("infra")
        if has_any(text, "paper", "research", "论文", "benchmark", "deepmind", "safety", "安全"):
            return facet_by_key("research")
        if has_any(text, "机器人", "robotics", "spot", "企业", "落地", "用例"):
            return facet_by_key("usecase")
        if has_any(text, "发布", "上线", "更新", "版本", "2.5", "2.0", "pro"):
            return facet_by_key("release")
        return facet_by_key("product")

    if subject.key == "anthropic":
        if has_any(text, "提示词", "怎么用", "使用方法", "playbook", "心智模型", "best practice", "教程"):
            return facet_by_key("prompting")
        if has_any(text, "for word", "word", "docs", "office", "workspace", "desktop app"):
            return facet_by_key("office")
        if has_any(text, "api", "billing", "计费", "额度", "context window", "token"):
            return facet_by_key("pricing_api")
        if has_any(text, "ipo", "估值", "融资", "ceo", "leadership", "管理层", "董事会"):
            return facet_by_key("capital")
        if has_any(text, "闭源", "黑箱", "风险", "争议", "安全", "泄露", "mythos"):
            return facet_by_key("security")
        if has_any(text, "app", "客户端", "desktop", "voice", "for word", "集成", "产品"):
            return facet_by_key("product")
        if has_any(text, "发布", "更新", "opus", "sonnet", "新模型", "版本"):
            return facet_by_key("release")
        if has_any(text, "benchmark", "评测", "论文", "research"):
            return facet_by_key("research")
        return facet_by_key("product")

    if subject.key == "agent":
        if has_any(text, "mcp", "protocol", "a2a", "workflow", "编排", "基础设施", "infra", "orchestration"):
            return facet_by_key("infra")
        if has_any(text, "blender", "渲染", "3d", "gui", "软件", "实验室", "computer use", "一站解决"):
            return facet_by_key("automation")
        if has_any(text, "安全", "风险", "trap", "benchmark", "评测", "jailbreak", "evaluation"):
            return facet_by_key("security")
        if has_any(text, "skills", "plugin", "扩展", "star", "社区", "生态"):
            return facet_by_key("ecosystem")
        if has_any(text, "企业", "落地", "部署", "收益", "用例", "机器人", "生产力"):
            return facet_by_key("usecase")
        if has_any(text, "研究", "paper", "benchmark", "评测"):
            return facet_by_key("research")
        if has_any(text, "发布", "更新", "上线", "版本", "产品"):
            return facet_by_key("release")
        return facet_by_key("infra")

    if subject.key == "openai":
        if has_any(text, "api", "billing", "token", "计费", "额度", "开发者"):
            return facet_by_key("pricing_api")
        if has_any(text, "app", "客户端", "voice", "operator", "产品", "体验"):
            return facet_by_key("product")
        if has_any(text, "发布", "更新", "新模型", "gpt", "chatgpt", "能力"):
            return facet_by_key("release")
        if has_any(text, "生态", "plugin", "skills", "开放"):
            return facet_by_key("ecosystem")
        return facet_by_key("product")

    if subject.key == "google_ai":
        if has_any(text, "deepmind", "research", "论文", "safety", "benchmark"):
            return facet_by_key("research")
        if has_any(text, "tpu", "infra", "基础设施", "数据中心"):
            return facet_by_key("infra")
        return facet_by_key("product")

    if subject.key == "codex":
        if has_any(text, "怎么用", "提示词", "教程", "最佳实践", "工作流"):
            return facet_by_key("prompting")
        if has_any(text, "api", "接入", "cli", "sdk", "集成", "工程"):
            return facet_by_key("infra")
        if has_any(text, "社区", "star", "扩展", "plugin", "生态"):
            return facet_by_key("ecosystem")
        if has_any(text, "计费", "额度", "api", "token"):
            return facet_by_key("pricing_api")
        if has_any(text, "发布", "更新", "版本"):
            return facet_by_key("release")
        return facet_by_key("product")

    if subject.key == "cursor":
        if has_any(text, "怎么用", "提示词", "教程", "工作流"):
            return facet_by_key("prompting")
        if has_any(text, "计费", "pricing", "quota", "额度", "pro"):
            return facet_by_key("pricing_api")
        if has_any(text, "发布", "更新", "版本", "agent"):
            return facet_by_key("release")
        return facet_by_key("product")

    if subject.key == "deepseek":
        if has_any(text, "融资", "估值", "资本", "组织", "团队"):
            return facet_by_key("capital")
        if has_any(text, "发布", "更新", "模型", "版本"):
            return facet_by_key("release")
        if has_any(text, "评测", "benchmark", "研究", "论文"):
            return facet_by_key("research")
        return facet_by_key("product")

    if subject.key == "gemma":
        if has_any(text, "研究", "论文", "benchmark", "评测"):
            return facet_by_key("research")
        if has_any(text, "部署", "接入", "端侧", "本地"):
            return facet_by_key("infra")
        return facet_by_key("release")

    if subject.key == "compute":
        if has_any(text, "融资", "估值", "资本", "收购"):
            return facet_by_key("capital")
        if has_any(text, "实测", "对比", "性能", "能效"):
            return facet_by_key("showcase")
        return facet_by_key("infra")

    if subject.key == "embodied_ai":
        if has_any(text, "发布", "量产", "上新", "发布会"):
            return facet_by_key("release")
        if has_any(text, "评测", "研究", "benchmark"):
            return facet_by_key("research")
        return facet_by_key("usecase")

    if subject.key == "gpt_image_2":
        if has_any(text, "提示词", "怎么画", "教程", "风格"):
            return facet_by_key("prompting")
        if has_any(text, "发布", "更新", "能力", "新功能"):
            return facet_by_key("release")
        if has_any(text, "产品", "入口", "app"):
            return facet_by_key("product")
        return facet_by_key("showcase")

    if subject.key == "chatgpt_gpt5":
        if has_any(text, "api", "计费", "开发者", "token"):
            return facet_by_key("pricing_api")
        if has_any(text, "企业", "落地", "用例", "行业"):
            return facet_by_key("usecase")
        if has_any(text, "风险", "争议", "版权", "安全"):
            return facet_by_key("security")
        if has_any(text, "发布", "更新", "上线", "新模型"):
            return facet_by_key("release")
        return facet_by_key("product")

    if subject.key == "mcp":
        if has_any(text, "api", "接入成本", "调用", "部署"):
            return facet_by_key("pricing_api")
        if has_any(text, "协议", "server", "client", "infra", "编排"):
            return facet_by_key("infra")
        return facet_by_key("ecosystem")

    if subject.key == "hermes_agent":
        if has_any(text, "部署", "接入", "编排", "workflow"):
            return facet_by_key("infra")
        if has_any(text, "企业", "落地", "用例"):
            return facet_by_key("usecase")
        return facet_by_key("ecosystem")

    if subject.key == "sora":
        if has_any(text, "发布", "更新", "模型", "能力"):
            return facet_by_key("release")
        if has_any(text, "入口", "产品", "体验"):
            return facet_by_key("product")
        return facet_by_key("showcase")

    return facet_by_key("general")


def refine_subject_facet(subject: SubjectRule, facet: FacetRule, title: str, body_text: str, source_name: str) -> FacetRule:
    text = f"{normalize_text(title)}\n{normalize_text(body_text)[:1600]}\n{normalize_text(source_name)}".lower()

    if subject.key == "anthropic":
        if has_any(text, "for word", "word", "docs", "office", "workspace", "desktop app"):
            return facet_by_key("office")
        if has_any(text, "mythos", "黑箱", "闭源", "风险", "争议", "攻击", "泄露"):
            return facet_by_key("security")
        if has_any(text, "提示词", "心智模型", "怎么用", "最佳实践") and facet.key in {"product", "release", "general"}:
            return facet_by_key("prompting")

    if subject.key == "agent":
        if has_any(text, "blender", "渲染", "3d", "gui", "实验室", "computer use", "一站解决"):
            return facet_by_key("automation")
        if facet.key == "business":
            return facet_by_key("release")

    if subject.key == "embodied_ai":
        if facet.key in {"business", "competition"}:
            if has_any(text, "部署态", "落地", "量产", "企业部署"):
                return facet_by_key("usecase")
            return facet_by_key("release")

    return facet


def build_topic_identity(subject: SubjectRule, facet: FacetRule) -> tuple[str, str]:
    custom_title = TOPIC_TEMPLATES.get((subject.key, facet.key))
    if custom_title:
        return (f"{subject.key}:{facet.key}", custom_title)
    return (f"{subject.key}:{facet.key}", f"{subject.label} {facet.label}")


def build_fallback_theme(title: str, source_name: str) -> tuple[str, str] | None:
    normalized_title = normalize_text(title)
    english_match = re.search(r"\b([A-Z][A-Za-z0-9\-\+]{2,}(?:\s+[A-Z][A-Za-z0-9\-\+]{2,}){0,2})\b", normalized_title)
    if english_match:
        token = english_match.group(1).strip()
        lower = token.lower()
        if lower not in {"today", "china", "github", "youtube"} and len(token) <= 32:
            return (f"fallback:{lower}", token)
    chinese_match = re.search(r"([\u4e00-\u9fff]{3,10})", normalized_title)
    if chinese_match:
        token = chinese_match.group(1).strip()
        if token not in {"今日", "最新", "文章", "视频", "热点"}:
            return (f"fallback:{token}", token)
    source_name = normalize_text(source_name)
    if source_name:
        return (f"fallback:{source_name.lower()}", source_name)
    return None


def classify_raw_item(row: dict[str, Any]) -> dict[str, Any] | None:
    obj = content_object_from_raw_item(row)
    if not obj.body_ready:
        return None
    if not is_ai_relevant(obj.title, obj.body_text):
        return None

    subject = pick_subject(obj.title, obj.body_text, obj.source_name)
    if subject is None:
        fallback_theme = build_fallback_theme(obj.title, obj.source_name)
        if not fallback_theme:
            return None
        topic_key, topic_title = fallback_theme
        subject_key = "fallback"
        subject_label = topic_title
        facet_key = "general"
        facet_label = "动态与讨论"
    else:
        facet = pick_facet(obj.title, obj.body_text, obj.source_name)
        if facet is None:
            facet = infer_subject_specific_facet(subject, obj.title, obj.body_text, obj.source_name)
        else:
            facet = refine_subject_facet(subject, facet, obj.title, obj.body_text, obj.source_name)
        topic_key, topic_title = build_topic_identity(subject, facet)
        subject_key = subject.key
        subject_label = subject.label
        facet_key = facet.key
        facet_label = facet.label

    return {
        "raw_item_id": int(row.get("id") or 0),
        "topic_key": topic_key,
        "topic_title": topic_title,
        "subject_key": subject_key,
        "subject_label": subject_label,
        "facet_key": facet_key,
        "facet_label": facet_label,
        "title": str(obj.title or "").strip(),
        "summary": str(obj.summary or "").strip(),
        "body_text": str(obj.body_text or "").strip(),
        "platform": str(obj.platform or "").strip(),
        "source_name": str(obj.source_name or "").strip(),
        "published_at": str(obj.published_at or "").strip(),
        "canonical_url": str(obj.canonical_url or "").strip(),
    }


def fetch_candidate_rows(conn: sqlite3.Connection, window_days: int) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
          id,
          platform,
          source_handle,
          item_id,
          title,
          content,
          url,
          published_at,
          metrics_json,
          fetched_at,
          body_status
        FROM raw_items
        WHERE datetime(COALESCE(published_at, fetched_at)) >= datetime('now', ?)
        ORDER BY datetime(COALESCE(published_at, fetched_at)) DESC, id DESC
        """,
        (f"-{max(1, int(window_days))} days",),
    ).fetchall()
    return [dict(row) for row in rows]


def generate_preview(window_days: int = DEFAULT_WINDOW_DAYS) -> dict[str, Any]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = fetch_candidate_rows(conn, window_days)

    scanned_count = len(rows)
    eligible_articles = 0
    unmatched_articles = 0
    grouped_articles: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        classified = classify_raw_item(row)
        if not classified:
            obj = content_object_from_raw_item(row)
            if obj.body_ready and is_ai_relevant(obj.title, obj.body_text):
                unmatched_articles += 1
            continue

        eligible_articles += 1
        topic_key = str(classified["topic_key"])
        topic_title = str(classified["topic_title"])
        grouped_articles[topic_key].append(
            {
                "raw_item_id": classified["raw_item_id"],
                "title": classified["title"],
                "platform": classified["platform"],
                "published_at": classified["published_at"],
                "canonical_url": classified["canonical_url"],
                "source_name": classified["source_name"],
                "topic_title": topic_title,
            }
        )

    ranked_topics: list[dict[str, Any]] = []
    for topic_key, articles in grouped_articles.items():
        seen_ids: set[int] = set()
        unique_articles: list[dict[str, Any]] = []
        for article in articles:
            raw_item_id = int(article.get("raw_item_id") or 0)
            if raw_item_id and raw_item_id in seen_ids:
                continue
            if raw_item_id:
                seen_ids.add(raw_item_id)
            unique_articles.append(article)

        if len(unique_articles) < 2:
            continue

        topic_title = unique_articles[0].get("topic_title") or topic_key
        platform_counter = Counter((article.get("platform") or "unknown") for article in unique_articles)
        source_counter = Counter((article.get("source_name") or "unknown") for article in unique_articles)
        ranked_topics.append(
            {
                "topic_key": topic_key,
                "display_title": topic_title,
                "article_count": len(unique_articles),
                "platform_count": len(platform_counter),
                "source_count": len(source_counter),
                "sample_articles": unique_articles[:5],
            }
        )

    ranked_topics.sort(
        key=lambda item: (
            -int(item["article_count"]),
            -int(item["platform_count"]),
            -int(item["source_count"]),
            str(item["display_title"]).lower(),
        )
    )

    covered_articles = sum(int(item["article_count"]) for item in ranked_topics)
    return {
        "db_path": str(DB_PATH),
        "window_days": window_days,
        "raw_items_scanned": scanned_count,
        "eligible_body_ready_articles": eligible_articles,
        "topics_built": len(ranked_topics),
        "covered_articles": covered_articles,
        "unmatched_articles": unmatched_articles,
        "top_topics": ranked_topics[:60],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args()

    payload = generate_preview(window_days=max(1, int(args.window_days or DEFAULT_WINDOW_DAYS)))
    payload["top_topics"] = payload.get("top_topics", [])[: max(1, int(args.top or 10))]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
