from __future__ import annotations

import copy
import os
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
repo_scripts_text = str(REPO_SCRIPTS)
if repo_scripts_text in sys.path:
    sys.path.remove(repo_scripts_text)
sys.path.insert(0, repo_scripts_text)

from self_media_config import LOCAL_CONFIG_PATH, get_config, get_path, get_value  # noqa: E402


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = LOCAL_CONFIG_PATH


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_csv_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
            continue
        result[key] = copy.deepcopy(value)
    return result


def _default_config() -> dict[str, Any]:
    root_config = get_config()
    vault_roots = list(get_value(root_config, "creative_studio.knowledge_sources.vault_roots", []) or [])
    if not vault_roots:
        vault_roots = [str(get_path(root_config, "paths.sample_vault_path"))]

    analysis_globs = list(get_value(root_config, "creative_studio.knowledge_sources.analysis_card_globs", []) or [])
    source_globs = list(get_value(root_config, "creative_studio.knowledge_sources.source_note_globs", []) or [])
    semantic = dict(get_value(root_config, "creative_studio.semantic_search", {}) or {})
    event_clustering = dict(get_value(root_config, "creative_studio.event_clustering", {}) or {})
    autofill = dict(get_value(root_config, "creative_studio.autofill", {}) or {})
    writing = dict(get_value(root_config, "creative_studio.writing", {}) or {})
    ui = dict(get_value(root_config, "creative_studio.ui", {}) or {})
    nighthawk = dict(get_value(root_config, "creative_studio.nighthawk", {}) or {})

    return {
        "knowledge_sources": {
            "vault_roots": vault_roots,
            "analysis_card_globs": analysis_globs,
            "source_note_globs": source_globs,
        },
        "database_sources": {
            "nighthawk_db_path": str(get_path(root_config, "paths.event_radar_db_path")),
        },
        "creation_workspace": {
            "data_root": str(get_path(root_config, "paths.creation_data_root")),
        },
        "indexing": {
            "content_index_db_path": str(get_path(root_config, "paths.create_studio_db_path")),
            "chunk_size": int(get_value(root_config, "creative_studio.indexing.chunk_size", 1200) or 1200),
            "chunk_overlap": int(get_value(root_config, "creative_studio.indexing.chunk_overlap", 180) or 180),
        },
        "semantic_search": {
            "embedding_provider": str(semantic.get("embedding_provider") or "disabled"),
            "embedding_model": str(semantic.get("embedding_model") or ""),
            "enable_fts": bool(semantic.get("enable_fts", True)),
            "enable_embedding": bool(semantic.get("enable_embedding", False)),
            "rerank_top_k": int(semantic.get("rerank_top_k") or 40),
            "final_top_k": int(semantic.get("final_top_k") or 20),
        },
        "event_clustering": {
            "mode": str(event_clustering.get("mode") or "semi_auto"),
            "auto_run_enabled": bool(event_clustering.get("auto_run_enabled", False)),
            "auto_run_interval_minutes": int(event_clustering.get("auto_run_interval_minutes") or 20),
            "lookback_hours": int(event_clustering.get("lookback_hours") or 48),
            "min_cluster_size": int(event_clustering.get("min_cluster_size") or 2),
            "conservative_threshold": float(event_clustering.get("conservative_threshold") or 0.78),
            "auto_publish_confirmed_only": bool(event_clustering.get("auto_publish_confirmed_only", True)),
        },
        "nighthawk": {
            "enable_upstream_raw_sync": bool(nighthawk.get("enable_upstream_raw_sync", False)),
            "upstream_timeout_sec": float(nighthawk.get("upstream_timeout_sec") or 2),
            "raw_sync_ttl_sec": int(nighthawk.get("raw_sync_ttl_sec") or 90),
            "raw_sync_page_size": int(nighthawk.get("raw_sync_page_size") or 100),
            "raw_sync_max_pages": int(nighthawk.get("raw_sync_max_pages") or 8),
        },
        "autofill": {
            "provider": str(autofill.get("provider") or "mock"),
            "model": str(autofill.get("model") or ""),
            "api_base": str(autofill.get("api_base") or "https://api.openai.com/v1"),
            "api_key_env": str(autofill.get("api_key_env") or "CREATE_STUDIO_AUTOFILL_API_KEY"),
            "timeout_sec": float(autofill.get("timeout_sec") or 45),
            "max_materials": int(autofill.get("max_materials") or 18),
            "max_chars_per_material": int(autofill.get("max_chars_per_material") or 1800),
        },
        "writing": {
            "provider": str(writing.get("provider") or "disabled"),
            "model": str(writing.get("model") or ""),
            "api_base": str(writing.get("api_base") or "https://api.openai.com/v1"),
            "api_key_env": str(writing.get("api_key_env") or "CREATE_STUDIO_WRITING_API_KEY"),
            "api_key": "",
            "timeout_sec": float(writing.get("timeout_sec") or 120),
            "max_materials": int(writing.get("max_materials") or 18),
            "max_chars_per_material": int(writing.get("max_chars_per_material") or 2200),
            "quality_check_enabled": bool(writing.get("quality_check_enabled", True)),
        },
        "ui": {
            "enable_debug_panel": bool(ui.get("enable_debug_panel", True)),
            "show_query_explain": bool(ui.get("show_query_explain", True)),
            "show_cluster_explain": bool(ui.get("show_cluster_explain", True)),
        },
    }


def _apply_env_overrides(config: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    updated = copy.deepcopy(config)
    overridden: list[str] = []
    env_map = {
        "CREATE_STUDIO_VAULT_ROOTS": ("knowledge_sources", "vault_roots", "list"),
        "CREATE_STUDIO_ANALYSIS_CARD_GLOBS": ("knowledge_sources", "analysis_card_globs", "list"),
        "CREATE_STUDIO_SOURCE_NOTE_GLOBS": ("knowledge_sources", "source_note_globs", "list"),
        "EVENT_RADAR_DB_PATH": ("database_sources", "nighthawk_db_path", "str"),
        "CREATE_STUDIO_NIGHTHAWK_DB_PATH": ("database_sources", "nighthawk_db_path", "str"),
        "CONTENT_SEARCH_CREATION_DATA_ROOT": ("creation_workspace", "data_root", "str"),
        "CREATE_STUDIO_INDEX_DB_PATH": ("indexing", "content_index_db_path", "str"),
        "CREATE_STUDIO_CHUNK_SIZE": ("indexing", "chunk_size", "int"),
        "CREATE_STUDIO_CHUNK_OVERLAP": ("indexing", "chunk_overlap", "int"),
        "CREATE_STUDIO_EMBEDDING_PROVIDER": ("semantic_search", "embedding_provider", "str"),
        "CREATE_STUDIO_EMBEDDING_MODEL": ("semantic_search", "embedding_model", "str"),
        "CREATE_STUDIO_ENABLE_FTS": ("semantic_search", "enable_fts", "bool"),
        "CREATE_STUDIO_ENABLE_EMBEDDING": ("semantic_search", "enable_embedding", "bool"),
        "CREATE_STUDIO_RERANK_TOP_K": ("semantic_search", "rerank_top_k", "int"),
        "CREATE_STUDIO_FINAL_TOP_K": ("semantic_search", "final_top_k", "int"),
        "CREATE_STUDIO_EVENT_CLUSTER_MODE": ("event_clustering", "mode", "str"),
        "CREATE_STUDIO_EVENT_AUTO_RUN_ENABLED": ("event_clustering", "auto_run_enabled", "bool"),
        "CREATE_STUDIO_EVENT_AUTO_RUN_INTERVAL_MINUTES": ("event_clustering", "auto_run_interval_minutes", "int"),
        "CREATE_STUDIO_EVENT_LOOKBACK_HOURS": ("event_clustering", "lookback_hours", "int"),
        "CREATE_STUDIO_EVENT_MIN_CLUSTER_SIZE": ("event_clustering", "min_cluster_size", "int"),
        "CREATE_STUDIO_EVENT_CONSERVATIVE_THRESHOLD": ("event_clustering", "conservative_threshold", "float"),
        "CREATE_STUDIO_EVENT_AUTO_PUBLISH_CONFIRMED_ONLY": ("event_clustering", "auto_publish_confirmed_only", "bool"),
        "CREATE_STUDIO_ENABLE_UPSTREAM_RAW_SYNC": ("nighthawk", "enable_upstream_raw_sync", "bool"),
        "CREATE_STUDIO_NIGHTHAWK_UPSTREAM_TIMEOUT_SEC": ("nighthawk", "upstream_timeout_sec", "float"),
        "CREATE_STUDIO_RAW_SYNC_TTL_SEC": ("nighthawk", "raw_sync_ttl_sec", "int"),
        "CREATE_STUDIO_RAW_SYNC_PAGE_SIZE": ("nighthawk", "raw_sync_page_size", "int"),
        "CREATE_STUDIO_RAW_SYNC_MAX_PAGES": ("nighthawk", "raw_sync_max_pages", "int"),
        "CREATE_STUDIO_AUTOFILL_PROVIDER": ("autofill", "provider", "str"),
        "CREATE_STUDIO_AUTOFILL_MODEL": ("autofill", "model", "str"),
        "CREATE_STUDIO_AUTOFILL_API_BASE": ("autofill", "api_base", "str"),
        "CREATE_STUDIO_AUTOFILL_API_KEY_ENV": ("autofill", "api_key_env", "str"),
        "CREATE_STUDIO_AUTOFILL_TIMEOUT_SEC": ("autofill", "timeout_sec", "float"),
        "CREATE_STUDIO_AUTOFILL_MAX_MATERIALS": ("autofill", "max_materials", "int"),
        "CREATE_STUDIO_AUTOFILL_MAX_CHARS_PER_MATERIAL": ("autofill", "max_chars_per_material", "int"),
        "CREATE_STUDIO_WRITING_PROVIDER": ("writing", "provider", "str"),
        "CREATE_STUDIO_WRITING_MODEL": ("writing", "model", "str"),
        "CREATE_STUDIO_WRITING_API_BASE": ("writing", "api_base", "str"),
        "CREATE_STUDIO_WRITING_API_KEY_ENV": ("writing", "api_key_env", "str"),
        "CREATE_STUDIO_WRITING_API_KEY": ("writing", "api_key", "str"),
        "CREATE_STUDIO_WRITING_TIMEOUT_SEC": ("writing", "timeout_sec", "float"),
        "CREATE_STUDIO_WRITING_MAX_MATERIALS": ("writing", "max_materials", "int"),
        "CREATE_STUDIO_WRITING_MAX_CHARS_PER_MATERIAL": ("writing", "max_chars_per_material", "int"),
        "CREATE_STUDIO_WRITING_QUALITY_CHECK_ENABLED": ("writing", "quality_check_enabled", "bool"),
        "CREATE_STUDIO_ENABLE_DEBUG_PANEL": ("ui", "enable_debug_panel", "bool"),
        "CREATE_STUDIO_SHOW_QUERY_EXPLAIN": ("ui", "show_query_explain", "bool"),
        "CREATE_STUDIO_SHOW_CLUSTER_EXPLAIN": ("ui", "show_cluster_explain", "bool"),
    }
    for env_name, (section, key, value_type) in env_map.items():
        raw = os.getenv(env_name)
        if raw is None:
            continue
        if value_type == "list":
            value = _parse_csv_list(raw)
        elif value_type == "bool":
            value = _parse_bool(raw, default=bool(updated.get(section, {}).get(key)))
        elif value_type == "int":
            try:
                value = int(str(raw).strip())
            except Exception:
                continue
        elif value_type == "float":
            try:
                value = float(str(raw).strip())
            except Exception:
                continue
        else:
            value = str(raw).strip()
        updated.setdefault(section, {})
        updated[section][key] = value
        overridden.append(env_name)
    return updated, overridden


def load_create_studio_config(config_path: str | Path | None = None) -> dict[str, Any]:
    resolved_config_path = Path(
        str(config_path or os.getenv("CREATE_STUDIO_CONFIG_PATH") or DEFAULT_CONFIG_PATH)
    ).expanduser()
    config = _default_config()
    file_loaded = False
    if resolved_config_path.exists():
        try:
            payload = yaml.safe_load(resolved_config_path.read_text(encoding="utf-8")) or {}
            if isinstance(payload, dict):
                compatibility_payload = payload.get("creative_studio_runtime", payload)
                config = _deep_merge(config, compatibility_payload)
                file_loaded = True
        except Exception:
            pass
    config, env_overrides = _apply_env_overrides(config)
    config["_meta"] = {
        "config_path": str(resolved_config_path),
        "config_file_loaded": file_loaded,
        "env_overrides": env_overrides,
        "default_fetch_hub_settings_path": str(Path(__file__).resolve().parents[3] / "config" / "local" / "local.yaml"),
    }
    return config


def get_create_studio_config_summary(config: dict[str, Any]) -> dict[str, Any]:
    knowledge_sources = dict(config.get("knowledge_sources") or {})
    database_sources = dict(config.get("database_sources") or {})
    creation_workspace = dict(config.get("creation_workspace") or {})
    indexing = dict(config.get("indexing") or {})
    semantic_search = dict(config.get("semantic_search") or {})
    event_clustering = dict(config.get("event_clustering") or {})
    nighthawk = dict(config.get("nighthawk") or {})
    autofill = dict(config.get("autofill") or {})
    writing = dict(config.get("writing") or {})
    ui_config = dict(config.get("ui") or {})
    meta = dict(config.get("_meta") or {})
    return {
        "ok": True,
        "config_path": str(meta.get("config_path") or ""),
        "config_file_loaded": bool(meta.get("config_file_loaded")),
        "env_overrides": list(meta.get("env_overrides") or []),
        "knowledge_sources": {
            "vault_roots": knowledge_sources.get("vault_roots") or [],
            "analysis_card_globs": knowledge_sources.get("analysis_card_globs") or [],
            "source_note_globs": knowledge_sources.get("source_note_globs") or [],
        },
        "database_sources": {"nighthawk_db_path": str(database_sources.get("nighthawk_db_path") or "")},
        "creation_workspace": {"data_root": str(creation_workspace.get("data_root") or "")},
        "indexing": {
            "content_index_db_path": str(indexing.get("content_index_db_path") or ""),
            "chunk_size": int(indexing.get("chunk_size") or 0),
            "chunk_overlap": int(indexing.get("chunk_overlap") or 0),
        },
        "semantic_search": {
            "embedding_provider": str(semantic_search.get("embedding_provider") or ""),
            "embedding_model": str(semantic_search.get("embedding_model") or ""),
            "enable_fts": bool(semantic_search.get("enable_fts")),
            "enable_embedding": bool(semantic_search.get("enable_embedding")),
            "rerank_top_k": int(semantic_search.get("rerank_top_k") or 0),
            "final_top_k": int(semantic_search.get("final_top_k") or 0),
        },
        "event_clustering": {
            "mode": str(event_clustering.get("mode") or ""),
            "auto_run_enabled": bool(event_clustering.get("auto_run_enabled")),
            "auto_run_interval_minutes": int(event_clustering.get("auto_run_interval_minutes") or 0),
            "lookback_hours": int(event_clustering.get("lookback_hours") or 0),
            "min_cluster_size": int(event_clustering.get("min_cluster_size") or 0),
            "conservative_threshold": float(event_clustering.get("conservative_threshold") or 0),
            "auto_publish_confirmed_only": bool(event_clustering.get("auto_publish_confirmed_only")),
        },
        "nighthawk": {
            "enable_upstream_raw_sync": bool(nighthawk.get("enable_upstream_raw_sync")),
            "upstream_timeout_sec": float(nighthawk.get("upstream_timeout_sec") or 0),
            "raw_sync_ttl_sec": int(nighthawk.get("raw_sync_ttl_sec") or 0),
            "raw_sync_page_size": int(nighthawk.get("raw_sync_page_size") or 0),
            "raw_sync_max_pages": int(nighthawk.get("raw_sync_max_pages") or 0),
        },
        "autofill": {
            "provider": str(autofill.get("provider") or ""),
            "model": str(autofill.get("model") or ""),
            "api_base": str(autofill.get("api_base") or ""),
            "api_key_env": str(autofill.get("api_key_env") or ""),
            "timeout_sec": float(autofill.get("timeout_sec") or 0),
            "max_materials": int(autofill.get("max_materials") or 0),
            "max_chars_per_material": int(autofill.get("max_chars_per_material") or 0),
        },
        "writing": {
            "provider": str(writing.get("provider") or ""),
            "model": str(writing.get("model") or ""),
            "api_base": str(writing.get("api_base") or ""),
            "api_key_env": str(writing.get("api_key_env") or ""),
            "has_api_key": bool(str(writing.get("api_key") or os.getenv(str(writing.get("api_key_env") or "")) or "").strip()),
            "timeout_sec": float(writing.get("timeout_sec") or 0),
            "max_materials": int(writing.get("max_materials") or 0),
            "max_chars_per_material": int(writing.get("max_chars_per_material") or 0),
            "quality_check_enabled": bool(writing.get("quality_check_enabled")),
        },
        "ui": {
            "enable_debug_panel": bool(ui_config.get("enable_debug_panel")),
            "show_query_explain": bool(ui_config.get("show_query_explain")),
            "show_cluster_explain": bool(ui_config.get("show_cluster_explain")),
        },
    }
