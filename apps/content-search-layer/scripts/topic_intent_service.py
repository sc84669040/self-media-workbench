from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re


DEFAULT_FACETS = ["背景", "重要性", "核心观点", "案例", "对比"]

FACET_HINTS: list[tuple[tuple[str, ...], list[str]]] = [
    (("价值", "值钱", "意义", "值得"), ["重要性", "创新点", "成绩", "对比"]),
    (("作用", "角色", "定位"), ["角色", "作用机制", "重要性", "协同关系"]),
    (("趋势", "方向", "机会"), ["背景", "变化", "驱动因素", "机会"]),
    (("风险", "问题", "挑战"), ["风险", "原因", "影响", "应对"]),
    (("方法", "做法", "策略"), ["方法", "步骤", "关键动作", "常见误区"]),
    (("对比", "区别", "差异"), ["差异化", "优劣势", "适用场景", "取舍"]),
]

STOPWORDS = {
    "的",
    "为什么",
    "怎么",
    "如何",
    "是什么",
    "到底",
    "一个",
    "现在",
    "现有",
    "整个",
    "这个",
    "那个",
}

GENERIC_ENTITY_PHRASES = {
    "在现有内容生产链路中",
    "现有内容生产链路",
    "内容生产链路",
}


@dataclass
class TopicIntent:
    topic: str
    normalized_topic: str
    topic_facets: list[str] = field(default_factory=list)
    entities: list[str] = field(default_factory=list)
    expanded_queries: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _unique_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _normalize_topic(topic: str) -> str:
    text = re.sub(r"\s+", " ", str(topic or "")).strip()
    if not text:
        return ""
    text = text.replace("hermes", "Hermes").replace("openclaw", "OpenClaw")
    if " 的价值" in text:
        return text.replace(" 的价值", " 在现有内容生产链路中的价值")
    if text.endswith("的价值"):
        entity = text[:-3].strip()
        return f"{entity} 在现有内容生产链路中的价值"
    if "为什么" in text and "更" not in text:
        return text.replace("为什么", "为什么值得关注")
    return text


def _extract_entities(topic: str) -> list[str]:
    text = str(topic or "").strip()
    candidates: list[str] = []
    candidates.extend(re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,}", text))

    chinese_chunks = re.split(r"[，。,？?！!；;：:、/\\()\[\]\s]+", text)
    for chunk in chinese_chunks:
        normalized = chunk.strip()
        if not normalized or normalized in STOPWORDS:
            continue
        if normalized.endswith("的价值"):
            normalized = normalized[:-3].strip()
        elif "的" in normalized:
            normalized = normalized.split("的", 1)[0].strip()
        if normalized in GENERIC_ENTITY_PHRASES:
            continue
        if len(normalized) >= 2 and normalized not in STOPWORDS:
            candidates.append(normalized)

    return _unique_keep_order(candidates)[:5]


def _infer_facets(topic: str) -> list[str]:
    text = str(topic or "").strip()
    facets: list[str] = []
    for keywords, values in FACET_HINTS:
        if any(keyword in text for keyword in keywords):
            facets.extend(values)
    if not facets:
        facets.extend(DEFAULT_FACETS)
    return _unique_keep_order(facets)[:6]


def _build_expanded_queries(topic: str, normalized_topic: str, entities: list[str], facets: list[str]) -> list[str]:
    queries: list[str] = []
    original = str(topic or "").strip()
    if original:
        queries.append(original)
    if normalized_topic and normalized_topic != original:
        queries.append(normalized_topic)

    if entities:
        if len(entities) >= 2:
            queries.append(f"{entities[0]} 与 {entities[1]} 对比")
        for entity in entities[:2]:
            queries.append(entity)
            for facet in facets[:4]:
                queries.append(f"{entity} {facet}")

    if normalized_topic and facets:
        for facet in facets[:3]:
            queries.append(f"{normalized_topic} {facet}")

    return _unique_keep_order(queries)[:10]


def build_topic_intent(topic: str) -> TopicIntent:
    normalized_topic = _normalize_topic(topic)
    entities = _extract_entities(normalized_topic or topic)
    facets = _infer_facets(normalized_topic or topic)
    expanded_queries = _build_expanded_queries(topic, normalized_topic, entities, facets)
    return TopicIntent(
        topic=str(topic or "").strip(),
        normalized_topic=normalized_topic,
        topic_facets=facets,
        entities=entities,
        expanded_queries=expanded_queries,
    )
