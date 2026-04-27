from __future__ import annotations

from abc import ABC, abstractmethod
import re
from typing import Any


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _tokenize(value: Any) -> list[str]:
    text = _normalize_text(value)
    if not text:
        return []
    parts = re.findall(r"[a-z0-9_]{2,}|[\u4e00-\u9fff]{2,}", text)
    return [part for part in parts if part]


def _score_overlap(query_tokens: set[str], object_tokens: set[str]) -> float:
    if not query_tokens or not object_tokens:
        return 0.0
    overlap = len(query_tokens & object_tokens)
    return overlap / max(len(query_tokens), 1)


class EmbeddingProvider(ABC):
    provider_id = "disabled"

    def __init__(self, *, model_name: str = ""):
        self.model_name = str(model_name or "").strip()

    @property
    def enabled(self) -> bool:
        return False

    @abstractmethod
    def search(
        self,
        *,
        query: str,
        query_terms: list[str],
        topic_intent: dict[str, Any],
        objects: list[dict[str, Any]],
        top_k: int,
    ) -> dict[str, Any]:
        raise NotImplementedError


class DisabledEmbeddingProvider(EmbeddingProvider):
    provider_id = "disabled"

    def search(
        self,
        *,
        query: str,
        query_terms: list[str],
        topic_intent: dict[str, Any],
        objects: list[dict[str, Any]],
        top_k: int,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "provider": self.provider_id,
            "model": self.model_name,
            "enabled": False,
            "items": [],
            "message": "embedding semantic recall disabled",
        }


class MockEmbeddingProvider(EmbeddingProvider):
    provider_id = "mock"

    @property
    def enabled(self) -> bool:
        return True

    def _object_text(self, item: dict[str, Any]) -> str:
        related_topics = " ".join(str(part).strip() for part in list(item.get("related_topics") or []) if str(part).strip())
        tags = " ".join(str(part).strip() for part in list(item.get("tags") or []) if str(part).strip())
        return " ".join(
            [
                str(item.get("title") or "").strip(),
                str(item.get("summary") or "").strip(),
                str(item.get("body_text") or "").strip(),
                related_topics,
                tags,
            ]
        ).strip()

    def search(
        self,
        *,
        query: str,
        query_terms: list[str],
        topic_intent: dict[str, Any],
        objects: list[dict[str, Any]],
        top_k: int,
    ) -> dict[str, Any]:
        facet_terms = [str(item or "").strip() for item in list(topic_intent.get("topic_facets") or []) if str(item or "").strip()]
        entity_terms = [str(item or "").strip() for item in list(topic_intent.get("entities") or []) if str(item or "").strip()]
        expanded_terms = [str(item or "").strip() for item in list(topic_intent.get("expanded_queries") or []) if str(item or "").strip()]
        semantic_query = " ".join([query, *query_terms, *facet_terms, *entity_terms, *expanded_terms]).strip()
        query_tokens = set(_tokenize(semantic_query))
        phrase_terms = [item.lower() for item in [query, *query_terms, *entity_terms, *facet_terms] if str(item or "").strip()]

        scored_items: list[dict[str, Any]] = []
        for item in objects:
            object_text = self._object_text(item)
            object_text_lower = object_text.lower()
            object_tokens = set(_tokenize(object_text))
            token_overlap_score = _score_overlap(query_tokens, object_tokens)
            phrase_hits = sum(1 for term in phrase_terms if term and term in object_text_lower)
            phrase_score = phrase_hits / max(len(phrase_terms), 1) if phrase_terms else 0.0
            score = round(0.7 * token_overlap_score + 0.3 * phrase_score, 4)
            if score <= 0:
                continue
            scored_items.append(
                {
                    "object_uid": str(item.get("object_uid") or "").strip(),
                    "embedding_score": score,
                    "explain": {
                        "provider": self.provider_id,
                        "token_overlap_score": round(token_overlap_score, 4),
                        "phrase_score": round(phrase_score, 4),
                        "matched_phrases": [term for term in phrase_terms if term and term in object_text_lower][:8],
                    },
                }
            )

        scored_items.sort(key=lambda entry: (-float(entry.get("embedding_score") or 0.0), str(entry.get("object_uid") or "")))
        return {
            "ok": True,
            "provider": self.provider_id,
            "model": self.model_name or "mock-semantic-v1",
            "enabled": True,
            "items": scored_items[: max(1, int(top_k or 20))],
            "message": f"mock semantic recall returned {min(len(scored_items), max(1, int(top_k or 20)))} items",
        }


def load_embedding_provider(config: dict[str, Any]) -> EmbeddingProvider:
    semantic_search = dict(config.get("semantic_search") or {})
    provider_name = str(semantic_search.get("embedding_provider") or "disabled").strip().lower()
    model_name = str(semantic_search.get("embedding_model") or "").strip()
    enable_embedding = bool(semantic_search.get("enable_embedding"))

    if not enable_embedding or provider_name in {"", "disabled", "none", "off"}:
        return DisabledEmbeddingProvider(model_name=model_name)
    if provider_name == "mock":
        return MockEmbeddingProvider(model_name=model_name or "mock-semantic-v1")
    return DisabledEmbeddingProvider(model_name=model_name)
