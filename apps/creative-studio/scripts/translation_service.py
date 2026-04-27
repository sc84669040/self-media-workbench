from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from content_object_models import content_object_from_raw_item
from create_studio_config import load_create_studio_config


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_DB_PATH = BASE_DIR / "data" / "translation_cache.db"
DEFAULT_PROVIDER = "google_free"
DEFAULT_TARGET_LANG = "zh-CN"
GOOGLE_TRANSLATE_URL = "https://translate.googleapis.com/translate_a/single"

_CJK_RE = re.compile(r"[\u3400-\u9fff]")
_LATIN_RE = re.compile(r"[A-Za-z]")
_WHITESPACE_RE = re.compile(r"\s+")
_BAD_TRANSLATION_MARKERS = (
    "QUERY LENGTH LIMIT EXCEEDED",
    "MYMEMORY WARNING",
)


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha1_text(text: str) -> str:
    return hashlib.sha1(str(text or "").encode("utf-8")).hexdigest()


def _resolve_cache_db_path(config_path: str | Path | None = None) -> Path:
    env_path = str(os.getenv("CREATE_STUDIO_TRANSLATION_CACHE_DB_PATH") or "").strip()
    if env_path:
        return Path(env_path).expanduser()
    config = load_create_studio_config(config_path=config_path)
    configured = str(((config.get("translation") or {}).get("cache_db_path") or "")).strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_CACHE_DB_PATH


def _translation_enabled(config_path: str | Path | None = None) -> bool:
    env_value = os.getenv("CREATE_STUDIO_TRANSLATION_ENABLED")
    if env_value is not None:
        return str(env_value).strip().lower() not in {"0", "false", "no", "off"}
    config = load_create_studio_config(config_path=config_path)
    translation_config = dict(config.get("translation") or {})
    return bool(translation_config.get("enabled", True))


def _resolve_provider(config_path: str | Path | None = None) -> str:
    env_value = str(os.getenv("CREATE_STUDIO_TRANSLATION_PROVIDER") or "").strip()
    if env_value:
        return env_value
    config = load_create_studio_config(config_path=config_path)
    translation_config = dict(config.get("translation") or {})
    return str(translation_config.get("provider") or DEFAULT_PROVIDER).strip() or DEFAULT_PROVIDER


def _resolve_target_lang(config_path: str | Path | None = None) -> str:
    env_value = str(os.getenv("CREATE_STUDIO_TRANSLATION_TARGET_LANG") or "").strip()
    if env_value:
        return env_value
    config = load_create_studio_config(config_path=config_path)
    translation_config = dict(config.get("translation") or {})
    return str(translation_config.get("target_lang") or DEFAULT_TARGET_LANG).strip() or DEFAULT_TARGET_LANG


def _get_timeout_sec() -> float:
    raw = str(os.getenv("CREATE_STUDIO_TRANSLATION_TIMEOUT_SEC") or "").strip()
    if not raw:
        return 12.0
    try:
        return max(3.0, float(raw))
    except Exception:  # noqa: BLE001
        return 12.0


def _ensure_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_item_translations (
              raw_item_id INTEGER NOT NULL,
              field_name TEXT NOT NULL,
              source_hash TEXT NOT NULL,
              source_lang TEXT NOT NULL DEFAULT '',
              translated_text TEXT NOT NULL DEFAULT '',
              provider TEXT NOT NULL DEFAULT '',
              updated_at TEXT NOT NULL,
              PRIMARY KEY (raw_item_id, field_name)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_item_translations_updated_at
            ON raw_item_translations(updated_at DESC)
            """
        )
        conn.commit()


def _compact_text(text: Any) -> str:
    return _WHITESPACE_RE.sub(" ", str(text or "")).strip()


def _is_bad_translation(text: Any) -> bool:
    compact = str(text or "").strip().upper()
    return any(marker in compact for marker in _BAD_TRANSLATION_MARKERS)


def should_translate_text(text: Any) -> bool:
    compact = _compact_text(text)
    if len(compact) < 6:
        return False
    latin_count = len(_LATIN_RE.findall(compact))
    cjk_count = len(_CJK_RE.findall(compact))
    if latin_count < 4:
        return False
    if cjk_count and cjk_count >= latin_count:
        return False
    if latin_count >= max(8, int(len(compact) * 0.25)):
        return True
    return latin_count >= 20 and latin_count > cjk_count * 2


def _load_cached_translation(
    raw_item_id: int,
    field_name: str,
    source_hash: str,
    *,
    cache_db_path: Path,
) -> dict[str, Any] | None:
    _ensure_schema(cache_db_path)
    with sqlite3.connect(cache_db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT raw_item_id, field_name, source_hash, source_lang, translated_text, provider, updated_at
            FROM raw_item_translations
            WHERE raw_item_id = ? AND field_name = ? AND source_hash = ?
            LIMIT 1
            """,
            (int(raw_item_id), str(field_name), str(source_hash)),
        ).fetchone()
    return dict(row) if row else None


def _save_cached_translation(
    raw_item_id: int,
    field_name: str,
    source_hash: str,
    translated_text: str,
    *,
    cache_db_path: Path,
    provider: str,
    source_lang: str,
) -> None:
    _ensure_schema(cache_db_path)
    with sqlite3.connect(cache_db_path) as conn:
        conn.execute(
            """
            INSERT INTO raw_item_translations (
              raw_item_id,
              field_name,
              source_hash,
              source_lang,
              translated_text,
              provider,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(raw_item_id, field_name) DO UPDATE SET
              source_hash = excluded.source_hash,
              source_lang = excluded.source_lang,
              translated_text = excluded.translated_text,
              provider = excluded.provider,
              updated_at = excluded.updated_at
            """,
            (
                int(raw_item_id),
                str(field_name),
                str(source_hash),
                str(source_lang or ""),
                str(translated_text or ""),
                str(provider or ""),
                _utc_now(),
            ),
        )
        conn.commit()


def _chunk_text(text: str, max_chars: int = 420) -> list[str]:
    compact = str(text or "").strip()
    if not compact:
        return []
    paragraphs = [part.strip() for part in re.split(r"\n{2,}", compact) if part.strip()]
    if not paragraphs:
        paragraphs = [compact]
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(paragraph) <= max_chars:
            current = paragraph
            continue
        sentences = re.split(r"(?<=[.!?。！？])\s+", paragraph)
        sentence_buffer = ""
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            sentence_candidate = sentence if not sentence_buffer else f"{sentence_buffer} {sentence}"
            if len(sentence_candidate) <= max_chars:
                sentence_buffer = sentence_candidate
                continue
            if sentence_buffer:
                chunks.append(sentence_buffer)
            if len(sentence) <= max_chars:
                sentence_buffer = sentence
                continue
            for start in range(0, len(sentence), max_chars):
                chunks.append(sentence[start:start + max_chars])
            sentence_buffer = ""
        current = sentence_buffer
    if current:
        chunks.append(current)
    return chunks or [compact]


def _translate_via_mock(text: str, target_lang: str) -> tuple[str, str]:
    return f"【中文】{text}", "en"


def _translate_via_google(text: str, target_lang: str, timeout_sec: float) -> tuple[str, str]:
    params = urlencode(
        [
            ("client", "gtx"),
            ("sl", "auto"),
            ("tl", target_lang),
            ("dt", "t"),
            ("q", text),
        ]
    )
    request = Request(
        f"{GOOGLE_TRANSLATE_URL}?{params}",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=timeout_sec) as response:
        payload = json.loads(response.read().decode("utf-8"))
    translated = "".join(str(part[0] or "") for part in (payload[0] or []) if isinstance(part, list) and part)
    detected = str(payload[2] or "").strip() if len(payload) > 2 else ""
    return translated.strip(), detected


def _translate_via_mymemory(text: str, target_lang: str, timeout_sec: float) -> tuple[str, str]:
    params = urlencode(
        [
            ("q", text),
            ("langpair", f"en|{target_lang}"),
        ]
    )
    request = Request(
        f"https://api.mymemory.translated.net/get?{params}",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=timeout_sec) as response:
        payload = json.loads(response.read().decode("utf-8"))
    translated = str(((payload.get("responseData") or {}).get("translatedText") or "")).strip()
    return translated, "en"


def _translate_text(
    text: str,
    *,
    provider: str,
    target_lang: str,
    timeout_sec: float,
) -> tuple[str, str, str]:
    normalized_provider = str(provider or DEFAULT_PROVIDER).strip().lower()
    if normalized_provider == "disabled":
        return str(text or ""), "", "disabled"
    if normalized_provider == "mock":
        translated, detected = _translate_via_mock(text, target_lang)
        return translated, detected, "mock"

    chunks = _chunk_text(text)
    translated_chunks: list[str] = []
    detected_lang = ""
    used_provider = "google_free"
    for chunk in chunks:
        try:
            translated_chunk, detected_lang = _translate_via_google(chunk, target_lang, timeout_sec)
        except Exception:  # noqa: BLE001
            translated_parts: list[str] = []
            for sub_chunk in _chunk_text(chunk, max_chars=80):
                sub_translated, detected_lang = _translate_via_mymemory(sub_chunk, target_lang, timeout_sec)
                translated_parts.append(sub_translated or sub_chunk)
                time.sleep(0.05)
            translated_chunk = "".join(translated_parts).strip()
            used_provider = "mymemory_free"
        translated_chunks.append(translated_chunk or chunk)
        time.sleep(0.08)
    return "\n\n".join(part for part in translated_chunks if part).strip(), detected_lang, used_provider


def get_or_translate_field(
    raw_item_id: int | str,
    field_name: str,
    text: Any,
    *,
    config_path: str | Path | None = None,
    allow_live_translate: bool = True,
) -> dict[str, Any]:
    raw_item_id = int(raw_item_id or 0)
    original_text = str(text or "").strip()
    if raw_item_id <= 0 or not original_text:
        return {
            "text": original_text,
            "translated_text": "",
            "display_text": original_text,
            "translated": False,
            "provider": "",
            "source_lang": "",
            "cached": False,
        }

    if not _translation_enabled(config_path=config_path):
        return {
            "text": original_text,
            "translated_text": "",
            "display_text": original_text,
            "translated": False,
            "provider": "disabled",
            "source_lang": "",
            "cached": False,
        }

    if not should_translate_text(original_text):
        return {
            "text": original_text,
            "translated_text": "",
            "display_text": original_text,
            "translated": False,
            "provider": "",
            "source_lang": "",
            "cached": False,
        }

    cache_db_path = _resolve_cache_db_path(config_path=config_path)
    source_hash = _sha1_text(original_text)
    cached = _load_cached_translation(raw_item_id, field_name, source_hash, cache_db_path=cache_db_path)
    if cached and str(cached.get("translated_text") or "").strip():
        translated_text = str(cached.get("translated_text") or "").strip()
        if not _is_bad_translation(translated_text):
            return {
                "text": original_text,
                "translated_text": translated_text,
                "display_text": translated_text,
                "translated": True,
                "provider": str(cached.get("provider") or ""),
                "source_lang": str(cached.get("source_lang") or ""),
                "cached": True,
            }

    if not allow_live_translate:
        return {
            "text": original_text,
            "translated_text": "",
            "display_text": original_text,
            "translated": False,
            "provider": "",
            "source_lang": "",
            "cached": False,
        }

    provider = _resolve_provider(config_path=config_path)
    if provider == "disabled":
        return {
            "text": original_text,
            "translated_text": "",
            "display_text": original_text,
            "translated": False,
            "provider": "disabled",
            "source_lang": "",
            "cached": False,
        }

    try:
        translated_text, source_lang, used_provider = _translate_text(
            original_text,
            provider=provider,
            target_lang=_resolve_target_lang(config_path=config_path),
            timeout_sec=_get_timeout_sec(),
        )
    except Exception:  # noqa: BLE001
        return {
            "text": original_text,
            "translated_text": "",
            "display_text": original_text,
            "translated": False,
            "provider": provider,
            "source_lang": "",
            "cached": False,
        }
    translated_text = str(translated_text or "").strip()
    if translated_text and translated_text != original_text and not _is_bad_translation(translated_text):
        _save_cached_translation(
            raw_item_id,
            field_name,
            source_hash,
            translated_text,
            cache_db_path=cache_db_path,
            provider=used_provider,
            source_lang=source_lang,
        )
        return {
            "text": original_text,
            "translated_text": translated_text,
            "display_text": translated_text,
            "translated": True,
            "provider": used_provider,
            "source_lang": source_lang,
            "cached": False,
        }
    return {
        "text": original_text,
        "translated_text": "",
        "display_text": original_text,
        "translated": False,
        "provider": provider,
        "source_lang": source_lang,
        "cached": False,
    }


def enrich_raw_item_translations(
    item: dict[str, Any],
    *,
    include_body: bool = False,
    config_path: str | Path | None = None,
    allow_live_translate: bool = True,
) -> dict[str, Any]:
    enriched = dict(item or {})
    raw_item_id = int(enriched.get("raw_item_id") or enriched.get("id") or 0)
    title_info = get_or_translate_field(
        raw_item_id,
        "title",
        enriched.get("title") or "",
        config_path=config_path,
        allow_live_translate=allow_live_translate,
    )
    summary_source = str(enriched.get("summary") or "").strip()
    preview_source = str(enriched.get("content_preview") or enriched.get("content_excerpt") or "").strip()
    summary_info = get_or_translate_field(
        raw_item_id,
        "summary",
        summary_source or preview_source,
        config_path=config_path,
        allow_live_translate=allow_live_translate,
    )

    body_info = {
        "display_text": str(enriched.get("body_text") or "").strip(),
        "translated_text": "",
        "translated": False,
    }
    if include_body:
        body_info = get_or_translate_field(
            raw_item_id,
            "body_text",
            enriched.get("body_text") or "",
            config_path=config_path,
            allow_live_translate=allow_live_translate,
        )

    enriched["title_zh"] = title_info.get("translated_text") or ""
    enriched["summary_zh"] = summary_info.get("translated_text") or ""
    enriched["content_preview_zh"] = summary_info.get("translated_text") or ""
    enriched["body_text_zh"] = body_info.get("translated_text") or ""
    enriched["display_title"] = title_info.get("display_text") or str(enriched.get("title") or "")
    enriched["display_summary"] = summary_info.get("display_text") or summary_source or preview_source
    enriched["display_content_preview"] = summary_info.get("display_text") or preview_source or summary_source
    enriched["display_body_text"] = body_info.get("display_text") or str(enriched.get("body_text") or "") or summary_source
    enriched["translation_meta"] = {
        "title_translated": bool(title_info.get("translated")),
        "summary_translated": bool(summary_info.get("translated")),
        "body_translated": bool(body_info.get("translated")),
        "provider": str(title_info.get("provider") or summary_info.get("provider") or body_info.get("provider") or ""),
    }
    return enriched


def backfill_nighthawk_translations(
    *,
    nighthawk_db_path: str | Path,
    limit: int = 300,
    include_body: bool = False,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    db_path = Path(nighthawk_db_path).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(f"NightHawk DB not found: {db_path}")
    limit = max(1, int(limit or 300))
    processed = 0
    translated_fields = 0
    body_fields = 0
    with sqlite3.connect(db_path) as conn:
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
              fetched_at,
              metrics_json,
              body_status
            FROM raw_items
            ORDER BY COALESCE(published_at, fetched_at, '') DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    for row in rows:
        payload = dict(row)
        obj = content_object_from_raw_item(payload)
        raw_item_id = int(payload.get("id") or 0)
        title_result = get_or_translate_field(raw_item_id, "title", obj.title, config_path=config_path)
        summary_result = get_or_translate_field(raw_item_id, "summary", obj.summary or obj.body_text[:240], config_path=config_path)
        processed += 1
        translated_fields += int(bool(title_result.get("translated_text"))) + int(bool(summary_result.get("translated_text")))
        if include_body and obj.body_ready and obj.body_text:
            body_result = get_or_translate_field(raw_item_id, "body_text", obj.body_text, config_path=config_path)
            body_fields += int(bool(body_result.get("translated_text")))
    return {
        "ok": True,
        "db_path": str(db_path),
        "processed": processed,
        "translated_fields": translated_fields,
        "body_fields": body_fields,
        "include_body": bool(include_body),
    }
