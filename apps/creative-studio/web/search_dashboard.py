#!/usr/bin/env python3
"""
X 搜索模板面板（第一版）
- 首页：关键词 + 模式 + 条件，执行搜索
- 账号页：查看当前白名单及启用状态

仅依赖标准库 + PyYAML。
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import shlex
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from shutil import which
from typing import Any
from urllib.error import URLError
from urllib.parse import parse_qs, quote, quote_plus, urlencode, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

import yaml
try:
    from zoneinfo import ZoneInfo
except Exception:  # noqa: BLE001
    ZoneInfo = None

BASE_DIR = Path(__file__).resolve().parents[1]
BEIJING_TZ = ZoneInfo("Asia/Shanghai") if ZoneInfo is not None else timezone(timedelta(hours=8))
SCRIPTS_DIR = BASE_DIR / "scripts"
scripts_dir_text = str(SCRIPTS_DIR)
if scripts_dir_text in sys.path:
    sys.path.remove(scripts_dir_text)
sys.path.insert(0, scripts_dir_text)

from create_studio_config import get_create_studio_config_summary, load_create_studio_config
from create_search_resolver import search_creation_candidates
from create_studio_store import CreateStudioStore
from creation_service import CreationValidationError, CreationWorkspaceService
from article_writer_service import test_writing_model_config
from event_packet_service import create_event_packet, get_event_packet_detail, get_latest_event_packet_ref
from index_sync_service import run_create_studio_index_sync
from nighthawk_supply_service import (
    build_creation_packet_from_nighthawk_raw_items,
    get_event_detail,
    get_nighthawk_raw_item_detail,
    get_nighthawk_supply_profile,
    list_nighthawk_raw_items,
    sync_recent_raw_items_from_upstream,
)
from topic_packet_service import create_topic_packet, create_topic_packet_from_topic_detail, get_topic_packet_detail, run_topic_search
from translation_service import enrich_raw_item_translations
from writer_adapter_registry import get_writer_registry_summary
from creation_target_registry import get_creation_target_summary
from workspace_paths import MONITORING_ROOT, VENV_ROOT, resolve_skill_project_root
from runtime_config import config as root_config, sample_vault_path, service_base_url, service_host, service_port
from self_media_config import DEFAULT_CONFIG_PATH, ENV_PATH, LOCAL_CONFIG_PATH, REPO_ROOT, load_config, load_yaml

WHITELIST_PATH = BASE_DIR / "config" / "x-author-whitelist.yaml"
TEMPLATES_PATH = BASE_DIR / "config" / "search-templates.yaml"
FEED_SOURCES_PATH = BASE_DIR / "config" / "feed-sources.yaml"
WECHAT_SOURCES_PATH = MONITORING_ROOT / "a-stage-wechat-sources.json"
YOUTUBE_CHANNELS_PATH = MONITORING_ROOT / "a-stage-youtube-channels.json"
BILIBILI_SOURCES_PATH = MONITORING_ROOT / "a-stage-bilibili-sources.json"
DOUYIN_SOURCES_PATH = MONITORING_ROOT / "a-stage-douyin-sources.json"
HISTORY_PATH = BASE_DIR / "data" / "search-history.jsonl"
INDEX_HTML_PATH = Path(__file__).with_name("index.html")
PORTAL_HTML_PATH = Path(__file__).with_name("portal.html")
CONFIG_HTML_PATH = Path(__file__).with_name("config.html")
CREATE_HTML_PATH = Path(__file__).with_name("create.html")
WRITE_HTML_PATH = Path(__file__).with_name("write.html")
STUDIO_HTML_PATH = Path(__file__).with_name("studio.html")
TOPIC_HTML_PATH = Path(__file__).with_name("topic.html")
PACKET_HTML_PATH = Path(__file__).with_name("packet.html")
NIGHTHAWK_HTML_PATH = Path(__file__).with_name("nighthawk.html")
EVENT_HTML_PATH = Path(__file__).with_name("event.html")
EVENT_PACKET_HTML_PATH = Path(__file__).with_name("event_packet.html")
EVENT_RADAR_DB_PATH = Path(
    os.environ.get("EVENT_RADAR_DB_PATH")
    or str(((load_create_studio_config().get("database_sources") or {}).get("nighthawk_db_path") or BASE_DIR / "data" / "event_radar.db"))
).expanduser()
DEFAULT_TOPIC_UPSTREAM_BASE = service_base_url("content_search_layer", 8787)
TOPIC_UPSTREAM_BASE = str(os.environ.get("CREATE_STUDIO_TOPIC_UPSTREAM_BASE") or DEFAULT_TOPIC_UPSTREAM_BASE).rstrip("/")
TOPIC_UPSTREAM_TIMEOUT_SEC = 2.5
TOPIC_LOCAL_SYNC_MIN_INTERVAL_SEC = 300
TOPIC_AUTO_SYNC_THREAD_STARTED = False
TOPIC_AUTO_SYNC_THREAD_LOCK = threading.Lock()
TWITTER_CMD_TIMEOUT_SEC = int(os.environ.get("TWITTER_CMD_TIMEOUT_SEC", "45"))
CONTENT_FETCH_HUB_CLI_PATH = resolve_skill_project_root("content-fetch-hub") / "scripts" / "fetch_content_cli.py"
CONTENT_FETCH_HUB_SETTINGS_PATH = resolve_skill_project_root("content-fetch-hub") / "config" / "fetch-settings.yaml"
TOPIC_PIPELINE_DIR = resolve_skill_project_root("content-search-layer") / "scripts" / "topic_pipeline"
TOPIC_LOCAL_SYNC_RUNTIME = {
    "last_checked_at": 0.0,
    "last_run_at": 0.0,
    "last_result": {},
}


@dataclass
class AppConfig:
    host: str = service_host("creative_studio")
    port: int = service_port("creative_studio", 8791)


@lru_cache(maxsize=1)
def get_create_studio_config() -> dict[str, Any]:
    return load_create_studio_config()


def _clear_runtime_config_caches() -> None:
    for cached in (get_create_studio_config, get_create_studio_store, get_creation_workspace_service):
        try:
            cached.cache_clear()
        except Exception:  # noqa: BLE001
            pass


def _without_meta(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _without_meta(item) for key, item in value.items() if key != "_meta"}
    if isinstance(value, list):
        return [_without_meta(item) for item in value]
    return value


def _selected_config_path() -> Path:
    return Path(os.getenv("SELF_MEDIA_CONFIG_PATH") or LOCAL_CONFIG_PATH).expanduser()


def _load_env_file_values() -> dict[str, str]:
    if not ENV_PATH.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in ENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _write_env_values(updates: dict[str, Any]) -> list[str]:
    clean_updates = {
        str(key).strip(): str(value).strip()
        for key, value in (updates or {}).items()
        if str(key).strip() and str(value).strip()
    }
    if not clean_updates:
        return []

    values = _load_env_file_values()
    values.update(clean_updates)
    lines = ["# Local private environment values. This file is ignored by Git."]
    for key in sorted(values):
        safe = values[key].replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'{key}="{safe}"')
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return sorted(clean_updates)


def _known_env_names(config: dict[str, Any]) -> list[str]:
    names: set[str] = set()

    def add(value: Any) -> None:
        text = str(value or "").strip()
        if text:
            names.add(text)

    for value in dict(config.get("credentials") or {}).values():
        add(value)
    add(((config.get("creative_studio") or {}).get("writing") or {}).get("api_key_env"))
    add(((config.get("creative_studio") or {}).get("autofill") or {}).get("api_key_env"))
    add(((config.get("notifications") or {}).get("telegram") or {}).get("proxy_env"))
    return sorted(names)


def _service_urls(config: dict[str, Any]) -> dict[str, str]:
    urls: dict[str, str] = {}
    for name, fallback_port in {
        "creative_studio": 8791,
        "content_search_layer": 8787,
        "content_fetch_hub": 8788,
    }.items():
        service_cfg = dict(((config.get("services") or {}).get(name) or {}))
        host = str(service_cfg.get("host") or "127.0.0.1").strip() or "127.0.0.1"
        port = int(service_cfg.get("port") or fallback_port)
        urls[name] = f"http://{host}:{port}"
    urls["rsshub"] = str((((config.get("services") or {}).get("rsshub") or {}).get("base_url") or "")).strip()
    return urls


def _scheduler_summary(config: dict[str, Any]) -> dict[str, Any]:
    jobs = dict(((config.get("scheduler") or {}).get("jobs") or {}))
    enabled = [name for name, job in jobs.items() if bool((job or {}).get("enabled"))]
    return {"total": len(jobs), "enabled": enabled, "disabled_count": max(0, len(jobs) - len(enabled))}


def _tool_status(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    external = dict(config.get("external_tools") or {})
    candidates = {
        "yt-dlp": str(external.get("yt_dlp_bin") or "yt-dlp").strip(),
        "twitter-cli": str(external.get("twitter_bin") or "twitter").strip(),
        "ffmpeg": str(external.get("ffmpeg_bin") or "ffmpeg").strip(),
        "FunASR": str(external.get("funasr_transcribe_script") or "").strip(),
    }
    status: dict[str, dict[str, Any]] = {}
    for name, candidate in candidates.items():
        if not candidate:
            status[name] = {"configured": False, "available": False, "path": ""}
            continue
        path = Path(candidate).expanduser()
        available_path = str(path) if path.exists() else (which(candidate) or "")
        status[name] = {
            "configured": bool(candidate),
            "available": bool(available_path),
            "path": available_path or candidate,
        }
    return status


def _sync_source_files_from_config(config: dict[str, Any]) -> None:
    sources = dict(config.get("sources") or {})
    MONITORING_ROOT.mkdir(parents=True, exist_ok=True)
    payloads = {
        "a-stage-feed-sources.json": {"version": 1, "sources": sources.get("feeds") or []},
        "a-stage-wechat-sources.json": {"version": 1, "sources": sources.get("wechat_sources") or []},
        "a-stage-youtube-channels.json": {"version": 1, "channels": sources.get("youtube_channels") or []},
        "a-stage-bilibili-sources.json": {"version": 1, "sources": sources.get("bilibili_sources") or []},
        "a-stage-douyin-sources.json": {"version": 1, "sources": sources.get("douyin_sources") or []},
        "a-stage-feishu-sources.json": {"version": 1, "sources": sources.get("feishu_sources") or []},
        "a-stage-x-sources.json": {"version": 1, "accounts": sources.get("x_accounts") or []},
    }
    for name, payload in payloads.items():
        (MONITORING_ROOT / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _config_checks(config: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    paths = dict(config.get("paths") or {})
    for key in [
        "runtime_dir",
        "sample_vault_path",
        "event_radar_db_path",
        "event_radar_mirror_db_path",
        "create_studio_db_path",
        "creation_data_root",
        "fetch_output_dir",
    ]:
        raw = str(paths.get(key) or "").strip()
        target = Path(raw).expanduser() if raw else Path()
        checks.append(
            {
                "name": f"paths.{key}",
                "ok": bool(raw),
                "value": raw,
                "exists": target.exists() if raw else False,
                "message": "configured" if raw else "missing",
            }
        )

    for service_name, url in _service_urls(config).items():
        checks.append({"name": f"services.{service_name}", "ok": bool(url), "value": url, "message": "configured"})

    env_values = _load_env_file_values()
    for env_name in _known_env_names(config):
        checks.append(
            {
                "name": f"env.{env_name}",
                "ok": bool(os.getenv(env_name) or env_values.get(env_name)),
                "value": env_name,
                "message": "set" if os.getenv(env_name) or env_values.get(env_name) else "not set",
            }
        )
    return checks


def _config_payload() -> dict[str, Any]:
    config = load_config()
    selected_config_path = _selected_config_path()
    local_yaml = selected_config_path.read_text(encoding="utf-8") if selected_config_path.exists() else ""
    template_yaml = (REPO_ROOT / "config" / "examples" / "local.example.yaml").read_text(encoding="utf-8")
    env_values = _load_env_file_values()
    env_names = _known_env_names(config)
    return {
        "ok": True,
        "config": _without_meta(config),
        "meta": dict(config.get("_meta") or {}),
        "paths": {
            "repo_root": str(REPO_ROOT),
            "default_config_path": str(DEFAULT_CONFIG_PATH),
            "local_config_path": str(LOCAL_CONFIG_PATH),
            "selected_config_path": str(selected_config_path),
            "env_path": str(ENV_PATH),
        },
        "local_yaml": local_yaml,
        "template_yaml": template_yaml,
        "local_exists": selected_config_path.exists(),
        "env_file_exists": ENV_PATH.exists(),
        "env": [
            {"name": name, "set": bool(os.getenv(name) or env_values.get(name)), "from_shell": bool(os.getenv(name))}
            for name in env_names
        ],
        "service_urls": _service_urls(config),
        "scheduler": _scheduler_summary(config),
        "tools": _tool_status(config),
        "checks": _config_checks(config),
    }


def _write_local_config(config: dict[str, Any] | None = None, yaml_text: str = "", env_values: dict[str, Any] | None = None) -> dict[str, Any]:
    selected_config_path = _selected_config_path()
    selected_config_path.parent.mkdir(parents=True, exist_ok=True)
    if yaml_text.strip():
        parsed = yaml.safe_load(yaml_text) or {}
        if not isinstance(parsed, dict):
            raise ValueError("Imported YAML must contain a mapping at the top level.")
        serialized = yaml.safe_dump(parsed, allow_unicode=True, sort_keys=False)
    else:
        clean = _without_meta(config or {})
        if not isinstance(clean, dict):
            raise ValueError("Config payload must be an object.")
        serialized = yaml.safe_dump(clean, allow_unicode=True, sort_keys=False)

    tmp_path = selected_config_path.with_suffix(selected_config_path.suffix + ".tmp")
    tmp_path.write_text(serialized, encoding="utf-8")
    tmp_path.replace(selected_config_path)
    updated_env = _write_env_values(env_values or {})
    merged_config = load_config(selected_config_path)
    _sync_source_files_from_config(merged_config)
    _clear_runtime_config_caches()
    payload = _config_payload()
    payload["saved"] = True
    payload["updated_env"] = updated_env
    return payload


def _writing_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "provider": "str",
        "model": "str",
        "api_base": "str",
        "api_key_env": "str",
        "timeout_sec": "float",
        "max_materials": "int",
        "max_chars_per_material": "int",
        "quality_check_enabled": "bool",
    }
    cleaned: dict[str, Any] = {}
    for key, value_type in allowed.items():
        if key not in payload:
            continue
        raw = payload.get(key)
        if value_type == "bool":
            cleaned[key] = raw is True or str(raw).strip().lower() in {"1", "true", "yes", "on"}
        elif value_type == "int":
            try:
                cleaned[key] = int(str(raw).strip())
            except Exception:  # noqa: BLE001
                continue
        elif value_type == "float":
            try:
                cleaned[key] = float(str(raw).strip())
            except Exception:  # noqa: BLE001
                continue
        else:
            cleaned[key] = str(raw or "").strip()
    api_key = str(payload.get("api_key") or "").strip()
    if api_key:
        cleaned["api_key"] = api_key
    if payload.get("clear_api_key"):
        cleaned["api_key"] = ""
    if cleaned.get("api_base"):
        cleaned["api_base"] = str(cleaned["api_base"]).rstrip("/")
    return cleaned


def update_create_studio_writing_config(payload: dict[str, Any]) -> dict[str, Any]:
    config_path = Path(
        str(
            os.getenv("CREATE_STUDIO_CONFIG_PATH")
            or (get_create_studio_config().get("_meta") or {}).get("config_path")
            or BASE_DIR / "config" / "create-studio.yaml"
        )
    ).expanduser()
    current: dict[str, Any] = {}
    if config_path.exists():
        current = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        if not isinstance(current, dict):
            current = {}
    current_writing = dict(current.get("writing") or {})
    current_writing.update(_writing_config_payload(payload))
    current["writing"] = current_writing
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(current, allow_unicode=True, sort_keys=False), encoding="utf-8")
    get_create_studio_config.cache_clear()
    return get_create_studio_config_summary(get_create_studio_config())


@lru_cache(maxsize=1)
def get_creation_workspace_service() -> CreationWorkspaceService:
    config = get_create_studio_config()
    data_root = str(
        ((config.get("creation_workspace") or {}).get("data_root") or os.getenv("CONTENT_SEARCH_CREATION_DATA_ROOT") or "")
    ).strip()
    return CreationWorkspaceService(data_root=data_root or None)


@lru_cache(maxsize=1)
def get_create_studio_store() -> CreateStudioStore:
    config = get_create_studio_config()
    db_path = str(
        ((config.get("indexing") or {}).get("content_index_db_path") or os.getenv("CREATE_STUDIO_INDEX_DB_PATH") or "")
    ).strip()
    return CreateStudioStore(db_path or (BASE_DIR / "data" / "create_studio.db"))


def _resolve_fetch_vault_root(vault: str = "") -> Path:
    custom = str(vault or "").strip()
    if custom:
        return Path(custom).expanduser()

    if CONTENT_FETCH_HUB_SETTINGS_PATH.exists():
        try:
            settings = yaml.safe_load(CONTENT_FETCH_HUB_SETTINGS_PATH.read_text(encoding="utf-8")) or {}
            cfg_vault = str(settings.get("vault_path") or "").strip()
            if cfg_vault:
                return Path(cfg_vault).expanduser()
        except Exception:  # noqa: BLE001
            pass

    return sample_vault_path()


def resolve_fetch_output_dir(vault: str = "") -> Path:
    return _resolve_fetch_vault_root(vault) / "抓取内容"


def open_fetch_output_dir(vault: str = "") -> dict[str, Any]:
    target = resolve_fetch_output_dir(vault)
    target.mkdir(parents=True, exist_ok=True)

    launch_errors: list[str] = []

    def _launch(cmd: list[str], method: str, extra: dict[str, Any] | None = None) -> dict[str, Any] | None:
        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            payload = {"path": str(target), "method": method}
            if extra:
                payload.update(extra)
            return payload
        except Exception as exc:  # noqa: BLE001
            launch_errors.append(f"{method}: {exc}")
            return None

    win_path = ""
    try:
        win_path = subprocess.check_output(["wslpath", "-w", str(target)], text=True).strip()
    except Exception as exc:  # noqa: BLE001
        launch_errors.append(f"wslpath: {exc}")

    if win_path:
        launched = _launch(["explorer.exe", win_path], "explorer.exe", {"windows_path": win_path})
        if launched:
            return launched

        explorer_path = which("explorer.exe")
        if explorer_path:
            launched = _launch([explorer_path, win_path], "explorer.exe(path)", {"windows_path": win_path})
            if launched:
                return launched

        launched = _launch(["cmd.exe", "/C", "start", "", win_path], "cmd-start", {"windows_path": win_path})
        if launched:
            return launched

    if which("wslview"):
        launched = _launch(["wslview", str(target)], "wslview")
        if launched:
            return launched

    if which("xdg-open"):
        launched = _launch(["xdg-open", str(target)], "xdg-open")
        if launched:
            return launched

    err_text = " | ".join(launch_errors) if launch_errors else "no-launcher"
    raise RuntimeError(f"无法打开目录，请手动访问：{target}（{err_text}）")


FETCH_TASKS: dict[str, dict[str, Any]] = {}
FETCH_TASKS_LOCK = threading.Lock()
FETCH_TASK_TTL_SEC = 6 * 60 * 60
DB_OVERVIEW_CACHE: dict[str, Any] = {"expires_at": 0.0, "payload": None}
DB_OVERVIEW_CACHE_TTL_SEC = 20
TOPICS_CACHE: dict[str, Any] = {"expires_at": 0.0, "payload": {}}
TOPICS_CACHE_TTL_SEC = 20


def _http_json(url: str, timeout_sec: float = 20.0) -> dict[str, Any]:
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout_sec) as response:
        return json.loads(response.read().decode("utf-8"))


def _ensure_topic_api_cache_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS topic_api_cache_topics (
          topic_id INTEGER PRIMARY KEY,
          topic_key TEXT NOT NULL DEFAULT '',
          title TEXT NOT NULL DEFAULT '',
          summary TEXT NOT NULL DEFAULT '',
          topic_type TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT '',
          first_seen_at TEXT NOT NULL DEFAULT '',
          last_seen_at TEXT NOT NULL DEFAULT '',
          importance_score REAL NOT NULL DEFAULT 0,
          impact_score REAL NOT NULL DEFAULT 0,
          creation_potential_score REAL NOT NULL DEFAULT 0,
          overall_score REAL NOT NULL DEFAULT 0,
          evidence_event_count INTEGER NOT NULL DEFAULT 0,
          evidence_article_count INTEGER NOT NULL DEFAULT 0,
          evidence_source_count INTEGER NOT NULL DEFAULT 0,
          evidence_platform_count INTEGER NOT NULL DEFAULT 0,
          card_summary TEXT NOT NULL DEFAULT '',
          primary_platforms_json TEXT NOT NULL DEFAULT '[]',
          primary_entities_json TEXT NOT NULL DEFAULT '[]',
          risk_flags_json TEXT NOT NULL DEFAULT '[]',
          topic_json TEXT NOT NULL DEFAULT '{}',
          synced_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS topic_api_cache_details (
          topic_id INTEGER PRIMARY KEY,
          topic_json TEXT NOT NULL DEFAULT '{}',
          articles_json TEXT NOT NULL DEFAULT '[]',
          article_pagination_json TEXT NOT NULL DEFAULT '{}',
          events_json TEXT NOT NULL DEFAULT '[]',
          event_pagination_json TEXT NOT NULL DEFAULT '{}',
          synced_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.commit()


def _cache_upstream_topic_items(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> None:
    _ensure_topic_api_cache_tables(conn)
    synced_at = datetime.now().isoformat(timespec="seconds")
    for item in items:
        platforms = json.dumps(item.get("primary_platforms") or [], ensure_ascii=False)
        entities = json.dumps(item.get("primary_entities") or [], ensure_ascii=False)
        risk_flags = json.dumps(item.get("risk_flags") or [], ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO topic_api_cache_topics (
              topic_id,
              topic_key,
              title,
              summary,
              topic_type,
              status,
              first_seen_at,
              last_seen_at,
              importance_score,
              impact_score,
              creation_potential_score,
              overall_score,
              evidence_event_count,
              evidence_article_count,
              evidence_source_count,
              evidence_platform_count,
              card_summary,
              primary_platforms_json,
              primary_entities_json,
              risk_flags_json,
              topic_json,
              synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic_id) DO UPDATE SET
              topic_key = excluded.topic_key,
              title = excluded.title,
              summary = excluded.summary,
              topic_type = excluded.topic_type,
              status = excluded.status,
              first_seen_at = excluded.first_seen_at,
              last_seen_at = excluded.last_seen_at,
              importance_score = excluded.importance_score,
              impact_score = excluded.impact_score,
              creation_potential_score = excluded.creation_potential_score,
              overall_score = excluded.overall_score,
              evidence_event_count = excluded.evidence_event_count,
              evidence_article_count = excluded.evidence_article_count,
              evidence_source_count = excluded.evidence_source_count,
              evidence_platform_count = excluded.evidence_platform_count,
              card_summary = excluded.card_summary,
              primary_platforms_json = excluded.primary_platforms_json,
              primary_entities_json = excluded.primary_entities_json,
              risk_flags_json = excluded.risk_flags_json,
              topic_json = excluded.topic_json,
              synced_at = excluded.synced_at
            """,
            (
                int(item.get("id") or 0),
                str(item.get("topic_key") or ""),
                str(item.get("title") or ""),
                str(item.get("summary") or ""),
                str(item.get("topic_type") or ""),
                str(item.get("status") or ""),
                str(item.get("first_seen_at") or ""),
                str(item.get("last_seen_at") or ""),
                float(item.get("importance_score") or 0),
                float(item.get("impact_score") or 0),
                float(item.get("creation_potential_score") or 0),
                float(item.get("overall_score") or 0),
                int(item.get("evidence_event_count") or 0),
                int(item.get("evidence_article_count") or 0),
                int(item.get("evidence_source_count") or 0),
                int(item.get("evidence_platform_count") or 0),
                str(item.get("card_summary") or ""),
                platforms,
                entities,
                risk_flags,
                json.dumps(item, ensure_ascii=False),
                synced_at,
            ),
        )
    conn.commit()


def _cache_upstream_topic_detail(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
    topic = dict(payload.get("topic") or {})
    articles = list(payload.get("articles") or [])
    if not topic:
        return
    _cache_upstream_topic_items(conn, [topic])
    _ensure_topic_api_cache_tables(conn)
    conn.execute(
        """
        INSERT INTO topic_api_cache_details (
          topic_id,
          topic_json,
          articles_json,
          article_pagination_json,
          events_json,
          event_pagination_json,
          synced_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(topic_id) DO UPDATE SET
          topic_json = excluded.topic_json,
          articles_json = excluded.articles_json,
          article_pagination_json = excluded.article_pagination_json,
          events_json = excluded.events_json,
          event_pagination_json = excluded.event_pagination_json,
          synced_at = excluded.synced_at
        """,
        (
            int(topic.get("id") or 0),
            json.dumps(topic, ensure_ascii=False),
            json.dumps(articles, ensure_ascii=False),
            json.dumps(payload.get("article_pagination") or {}, ensure_ascii=False),
            json.dumps(payload.get("events") or [], ensure_ascii=False),
            json.dumps(payload.get("event_pagination") or {}, ensure_ascii=False),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()


def _topic_sort_clause_for_cache(sort_by: str) -> str:
    sort_key = str(sort_by or "overall").strip().lower()
    mapping = {
        "latest": "COALESCE(last_seen_at, '') DESC, topic_id DESC",
        "overall": "overall_score DESC, COALESCE(last_seen_at, '') DESC, topic_id DESC",
        "importance": "importance_score DESC, COALESCE(last_seen_at, '') DESC, topic_id DESC",
        "impact": "impact_score DESC, COALESCE(last_seen_at, '') DESC, topic_id DESC",
        "articles": "evidence_article_count DESC, COALESCE(last_seen_at, '') DESC, topic_id DESC",
    }
    return mapping.get(sort_key, mapping["overall"])


def _list_topics_from_api_cache(conn: sqlite3.Connection, sort_by: str, page: int, limit: int, keyword: str) -> dict[str, Any]:
    _ensure_topic_api_cache_tables(conn)
    page_size = max(1, min(int(limit or 10), 50))
    current_page = max(1, int(page or 1))
    keyword_text = str(keyword or "").strip()
    where_sql = " WHERE 1=1 "
    params: list[Any] = []
    if keyword_text:
        like = f"%{keyword_text}%"
        where_sql += " AND (LOWER(title) LIKE LOWER(?) OR LOWER(topic_key) LIKE LOWER(?))"
        params.extend([like, like])
    total = int(
        conn.execute(f"SELECT COUNT(*) FROM topic_api_cache_topics {where_sql}", params).fetchone()[0] or 0
    )
    total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
    if current_page > total_pages:
        current_page = total_pages
    offset = (current_page - 1) * page_size
    rows = conn.execute(
        f"""
        SELECT *
        FROM topic_api_cache_topics
        {where_sql}
        ORDER BY {_topic_sort_clause_for_cache(sort_by)}
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for field in ["primary_platforms_json", "primary_entities_json", "risk_flags_json", "topic_json"]:
            raw = item.pop(field, None)
            if field == "topic_json":
                continue
            try:
                item[field.replace("_json", "")] = json.loads(raw or "[]")
            except Exception:
                item[field.replace("_json", "")] = []
        item["id"] = int(item.pop("topic_id"))
        items.append(item)
    return {
        "items": items,
        "total": total,
        "page": current_page,
        "page_size": page_size,
        "total_pages": total_pages,
        "sort_by": sort_by,
        "keyword": keyword_text,
        "data_source": "windows_topic_api_cache",
    }


def _get_topic_detail_from_api_cache(conn: sqlite3.Connection, topic_id: int) -> dict[str, Any] | None:
    _ensure_topic_api_cache_tables(conn)
    row = conn.execute(
        """
        SELECT topic_json, articles_json, article_pagination_json, events_json, event_pagination_json
        FROM topic_api_cache_details
        WHERE topic_id = ?
        LIMIT 1
        """,
        (int(topic_id),),
    ).fetchone()
    if not row:
        return None
    try:
        topic = json.loads(row["topic_json"] or "{}")
        articles = json.loads(row["articles_json"] or "[]")
        article_pagination = json.loads(row["article_pagination_json"] or "{}")
        events = json.loads(row["events_json"] or "[]")
        event_pagination = json.loads(row["event_pagination_json"] or "{}")
    except Exception:
        return None
    return {
        "ok": True,
        "topic": topic,
        "articles": articles,
        "article_pagination": article_pagination,
        "events": events,
        "event_pagination": event_pagination,
        "data_source": "windows_topic_api_cache",
    }


def _list_topics_from_local_tables(conn: sqlite3.Connection, sort_by: str, page: int, limit: int, keyword: str) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    page_size = max(1, min(int(limit or 10), 50))
    current_page = max(1, int(page or 1))
    keyword_text = str(keyword or "").strip()
    where_sql = " WHERE 1=1 "
    where_sql += _topics_filter_sql(cur)
    params: list[Any] = []
    if keyword_text:
        like = f"%{keyword_text}%"
        where_sql += " AND (LOWER(t.title) LIKE LOWER(?) OR LOWER(t.topic_key) LIKE LOWER(?))"
        params.extend([like, like])

    latest_scores_sql = "SELECT MAX(id) AS latest_id FROM topic_scores GROUP BY topic_id"
    try:
        total = int(
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM topics t
                JOIN topic_scores ts ON ts.topic_id = t.id
                JOIN ({latest_scores_sql}) latest ON latest.latest_id = ts.id
                {where_sql}
                """,
                params,
            ).fetchone()[0]
            or 0
        )
    except sqlite3.OperationalError:
        return {
            "items": [],
            "total": 0,
            "page": 1,
            "page_size": page_size,
            "total_pages": 1,
            "sort_by": sort_by,
            "keyword": keyword_text,
            "data_source": "windows_local_topics",
        }

    total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
    if current_page > total_pages:
        current_page = total_pages
    offset = (current_page - 1) * page_size
    order_clause = _topic_sort_clause(sort_by)
    rows = cur.execute(
        f"""
        SELECT
            t.id,
            t.topic_key,
            t.title,
            t.summary,
            t.topic_type,
            t.status,
            t.first_seen_at,
            t.last_seen_at,
            t.primary_platforms_json,
            t.primary_entities_json,
            t.risk_flags_json,
            ts.importance_score,
            ts.impact_score,
            ts.creation_potential_score,
            ts.overall_score,
            ts.evidence_event_count,
            ts.evidence_article_count,
            ts.evidence_source_count,
            ts.evidence_platform_count,
            ts.card_summary
        FROM topics t
        JOIN topic_scores ts ON ts.topic_id = t.id
        JOIN ({latest_scores_sql}) latest ON latest.latest_id = ts.id
        {where_sql}
        ORDER BY {order_clause}
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for field in ["primary_platforms_json", "primary_entities_json", "risk_flags_json"]:
            try:
                item[field.replace("_json", "")] = json.loads(item.get(field) or "[]")
            except Exception:
                item[field.replace("_json", "")] = []
            item.pop(field, None)
        item["first_seen_at"] = _format_datetime_text(item.get("first_seen_at") or "")
        item["last_seen_at"] = _format_datetime_text(item.get("last_seen_at") or "")
        item["summary"] = _trim_text(item.get("summary") or "", 220)
        item["card_summary"] = _trim_text(item.get("card_summary") or "", 220)
        items.append(item)
    return {
        "items": items,
        "total": total,
        "page": current_page,
        "page_size": page_size,
        "total_pages": total_pages,
        "sort_by": sort_by,
        "keyword": keyword_text,
        "data_source": "windows_local_topics",
    }


def _refresh_topic_api_cache_snapshot(sort_by: str = "overall") -> None:
    page_size = 100
    first_payload = _http_json(
        f"{TOPIC_UPSTREAM_BASE}/api/topics?{urlencode({'sort': sort_by, 'page': 1, 'limit': page_size, 'keyword': ''})}",
        timeout_sec=TOPIC_UPSTREAM_TIMEOUT_SEC,
    )
    items = list(first_payload.get("items") or [])
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
        _cache_upstream_topic_items(conn, items)
    total_pages = max(1, int(first_payload.get("total_pages") or 1))
    for current_page in range(2, total_pages + 1):
        payload = _http_json(
            f"{TOPIC_UPSTREAM_BASE}/api/topics?{urlencode({'sort': sort_by, 'page': current_page, 'limit': page_size, 'keyword': ''})}",
            timeout_sec=TOPIC_UPSTREAM_TIMEOUT_SEC,
        )
        items = list(payload.get("items") or [])
        if not items:
            continue
        with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
            _cache_upstream_topic_items(conn, items)


@lru_cache(maxsize=1)
def _get_topic_sync_v2_module():
    if str(TOPIC_PIPELINE_DIR) not in sys.path:
        sys.path.insert(0, str(TOPIC_PIPELINE_DIR))
    return importlib.import_module("sync_topics_v2")


def _get_local_topic_runtime_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    raw_row = conn.execute("SELECT MAX(id) AS max_id, MAX(published_at) AS latest_at FROM raw_items").fetchone()
    active_v2_row = conn.execute(
        """
        SELECT COUNT(*) AS c, MAX(last_seen_at) AS latest
        FROM topics
        WHERE topic_type='article_theme_v2' AND COALESCE(is_active, 1)=1
        """
    ).fetchone()
    state_row = None
    has_state_table = bool(
        conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='topic_pipeline_state' LIMIT 1").fetchone()
    )
    if has_state_table:
        state_row = conn.execute(
            """
            SELECT last_raw_item_id, last_run_mode, last_run_at
            FROM topic_pipeline_state
            WHERE pipeline_name='topic_v2'
            """
        ).fetchone()
    raw_max_id = int((raw_row["max_id"] if raw_row else 0) or 0)
    state_last_raw_item_id = int((state_row["last_raw_item_id"] if state_row else 0) or 0)
    return {
        "raw_latest_published_at": str((raw_row["latest_at"] if raw_row else "") or "").strip(),
        "raw_max_id": raw_max_id,
        "active_topic_total": int((active_v2_row["c"] if active_v2_row else 0) or 0),
        "active_topic_latest_seen_at": str((active_v2_row["latest"] if active_v2_row else "") or "").strip(),
        "state_last_raw_item_id": state_last_raw_item_id,
        "state_last_run_mode": str((state_row["last_run_mode"] if state_row else "") or "").strip(),
        "state_last_run_at": str((state_row["last_run_at"] if state_row else "") or "").strip(),
        "pipeline_caught_up": bool(raw_max_id and state_last_raw_item_id >= raw_max_id),
    }


def _maybe_refresh_local_topics(*, force: bool = False, full_rebuild: bool = False) -> dict[str, Any]:
    now = time.time()
    runtime = TOPIC_LOCAL_SYNC_RUNTIME
    if not force and now - float(runtime.get("last_checked_at") or 0.0) < 30:
        return dict(runtime.get("last_result") or {})

    runtime["last_checked_at"] = now
    if not force and not _topic_auto_sync_enabled():
        EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
            stats = _get_local_topic_runtime_stats(conn)
        result = {
            "ok": True,
            "disabled": True,
            "reason": "topic_auto_sync_disabled",
            "stale": False,
            "raw_latest_published_at": str(stats.get("raw_latest_published_at") or "").strip(),
            "raw_max_id": int(stats.get("raw_max_id") or 0),
            "active_topic_latest_seen_at": str(stats.get("active_topic_latest_seen_at") or "").strip(),
            "active_topic_total": int(stats.get("active_topic_total") or 0),
            "state_last_raw_item_id": int(stats.get("state_last_raw_item_id") or 0),
            "state_last_run_at": str(stats.get("state_last_run_at") or "").strip(),
            "pipeline_caught_up": bool(stats.get("pipeline_caught_up")),
            "mirror_sync": {"ok": True, "skipped": True, "reason": "topic_auto_sync_disabled"},
            "sync_triggered": False,
        }
        runtime["last_result"] = dict(result)
        return dict(result)

    mirror_sync: dict[str, Any] = {}
    try:
        mirror_sync = sync_recent_raw_items_from_upstream(
            db_path=EVENT_RADAR_DB_PATH,
            config_path=BASE_DIR / "config" / "create-studio.yaml",
        )
    except Exception as exc:  # noqa: BLE001
        mirror_sync = {"ok": False, "error": str(exc)}

    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
        stats = _get_local_topic_runtime_stats(conn)

    raw_latest = str(stats.get("raw_latest_published_at") or "").strip()
    topic_latest = str(stats.get("active_topic_latest_seen_at") or "").strip()
    raw_max_id = int(stats.get("raw_max_id") or 0)
    state_last_raw_item_id = int(stats.get("state_last_raw_item_id") or 0)
    is_stale = bool(raw_max_id and state_last_raw_item_id < raw_max_id)
    result = {
        "ok": True,
        "stale": is_stale,
        "raw_latest_published_at": raw_latest,
        "raw_max_id": raw_max_id,
        "active_topic_latest_seen_at": topic_latest,
        "active_topic_total": int(stats.get("active_topic_total") or 0),
        "state_last_raw_item_id": state_last_raw_item_id,
        "state_last_run_at": str(stats.get("state_last_run_at") or "").strip(),
        "pipeline_caught_up": bool(stats.get("pipeline_caught_up")),
        "mirror_sync": mirror_sync,
        "sync_triggered": False,
    }
    runtime["last_checked_at"] = now

    if not force and not is_stale:
        runtime["last_result"] = dict(result)
        return dict(result)

    if not force and now - float(runtime.get("last_run_at") or 0.0) < TOPIC_LOCAL_SYNC_MIN_INTERVAL_SEC:
        runtime["last_result"] = dict(result)
        return dict(result)

    module = _get_topic_sync_v2_module()
    module.DB_PATH = EVENT_RADAR_DB_PATH
    with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
        module.ensure_topic_pipeline_schema(conn)
        state = module.load_state(conn)
        if full_rebuild or not int(state.get("last_raw_item_id") or 0):
            sync_result = module.run_full_rebuild(conn, window_days=7)
            run_mode = "full-rebuild"
        else:
            sync_result = module.run_incremental(conn, state=state, window_days=7)
            run_mode = "incremental"
        module.save_state(
            conn,
            last_raw_item_id=int(sync_result.get("last_raw_item_id") or 0),
            run_mode=run_mode,
            notes={"window_days": 7},
        )
        conn.commit()
        refreshed_stats = _get_local_topic_runtime_stats(conn)

    runtime["last_run_at"] = now
    result.update(
        {
            "sync_triggered": True,
            "sync_mode": run_mode,
            "sync_result": sync_result,
            "raw_latest_published_at": str(refreshed_stats.get("raw_latest_published_at") or "").strip(),
            "raw_max_id": int(refreshed_stats.get("raw_max_id") or 0),
            "active_topic_latest_seen_at": str(refreshed_stats.get("active_topic_latest_seen_at") or "").strip(),
            "active_topic_total": int(refreshed_stats.get("active_topic_total") or 0),
            "state_last_raw_item_id": int(refreshed_stats.get("state_last_raw_item_id") or 0),
            "state_last_run_at": str(refreshed_stats.get("state_last_run_at") or "").strip(),
            "pipeline_caught_up": bool(refreshed_stats.get("pipeline_caught_up")),
            "stale": bool(
                int(refreshed_stats.get("raw_max_id") or 0)
                and int(refreshed_stats.get("state_last_raw_item_id") or 0) < int(refreshed_stats.get("raw_max_id") or 0)
            ),
        }
    )
    runtime["last_result"] = dict(result)
    return dict(result)


def get_topic_sync_status() -> dict[str, Any]:
    local_result = _maybe_refresh_local_topics(force=False)
    upstream_total = 0
    upstream_latest = ""
    upstream_available = False
    upstream_error = ""
    try:
        upstream_payload = _http_json(
            f"{TOPIC_UPSTREAM_BASE}/api/topics?{urlencode({'sort': 'overall', 'page': 1, 'limit': 1, 'keyword': ''})}",
            timeout_sec=TOPIC_UPSTREAM_TIMEOUT_SEC,
        )
        upstream_total = int(upstream_payload.get("total") or 0)
        upstream_latest_payload = _http_json(
            f"{TOPIC_UPSTREAM_BASE}/api/topics?{urlencode({'sort': 'latest', 'page': 1, 'limit': 1, 'keyword': ''})}",
            timeout_sec=TOPIC_UPSTREAM_TIMEOUT_SEC,
        )
        upstream_latest_items = list(upstream_latest_payload.get("items") or [])
        upstream_latest = str(((upstream_latest_items[0] or {}).get("last_seen_at") if upstream_latest_items else "") or "").strip()
        upstream_available = True
    except Exception as exc:  # noqa: BLE001
        upstream_error = str(exc)

    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_topic_api_cache_tables(conn)
        cache_row = conn.execute(
            """
            SELECT COUNT(*) AS c, MAX(last_seen_at) AS latest, MAX(synced_at) AS synced_at
            FROM topic_api_cache_topics
            """
        ).fetchone()
        legacy_topics = bool(
            conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='topics' LIMIT 1").fetchone()
        )
        legacy_total = 0
        legacy_latest = ""
        if legacy_topics:
            legacy_row = conn.execute("SELECT COUNT(*) AS c, MAX(last_seen_at) AS latest FROM topics").fetchone()
            legacy_total = int(legacy_row["c"] or 0)
            legacy_latest = str(legacy_row["latest"] or "").strip()

    cache_total = int(cache_row["c"] or 0)
    cache_latest = str(cache_row["latest"] or "").strip()
    cache_synced_at = str(cache_row["synced_at"] or "").strip()
    in_sync = bool(
        upstream_available
        and upstream_total > 0
        and cache_total == upstream_total
        and cache_latest == upstream_latest
    )

    return {
        "ok": True,
        "upstream": {
            "base_url": TOPIC_UPSTREAM_BASE,
            "available": upstream_available,
            "total": upstream_total,
            "latest_seen_at": upstream_latest,
            "error": upstream_error,
        },
        "cache": {
            "db_path": str(EVENT_RADAR_DB_PATH),
            "total": cache_total,
            "latest_seen_at": cache_latest,
            "synced_at": cache_synced_at,
        },
        "legacy_topics": {
            "total": legacy_total,
            "latest_seen_at": legacy_latest,
        },
        "local_topics": {
            "total": int(local_result.get("active_topic_total") or 0),
            "latest_seen_at": str(local_result.get("active_topic_latest_seen_at") or "").strip(),
            "raw_latest_published_at": str(local_result.get("raw_latest_published_at") or "").strip(),
            "raw_max_id": int(local_result.get("raw_max_id") or 0),
            "processed_raw_item_id": int(local_result.get("state_last_raw_item_id") or 0),
            "pipeline_caught_up": bool(local_result.get("pipeline_caught_up")),
            "last_run_at": str(local_result.get("state_last_run_at") or "").strip(),
            "stale": bool(local_result.get("stale")),
            "sync_triggered": bool(local_result.get("sync_triggered")),
            "sync_mode": str(local_result.get("sync_mode") or "").strip(),
        },
        "in_sync": in_sync,
    }


def sync_topic_snapshot() -> dict[str, Any]:
    local_result = _maybe_refresh_local_topics(force=True, full_rebuild=True)
    try:
        _refresh_topic_api_cache_snapshot(sort_by="overall")
    except Exception:  # noqa: BLE001
        pass
    payload = get_topic_sync_status()
    payload["sync_triggered"] = True
    payload["local_sync"] = local_result
    return payload


def _cleanup_fetch_tasks() -> None:
    now = time.time()
    stale_ids: list[str] = []
    with FETCH_TASKS_LOCK:
        for task_id, task in FETCH_TASKS.items():
            finished_raw = task.get("finished_at") or ""
            finished_at = 0.0
            if isinstance(finished_raw, (int, float)):
                finished_at = float(finished_raw)
            elif finished_raw:
                try:
                    finished_at = datetime.fromisoformat(str(finished_raw)).timestamp()
                except Exception:
                    finished_at = 0.0
            if finished_at and now - finished_at > FETCH_TASK_TTL_SEC:
                stale_ids.append(task_id)
        for task_id in stale_ids:
            FETCH_TASKS.pop(task_id, None)


def _build_content_fetch_cmd(url: str = "", urls: list[str] | None = None, vault: str = "", retry_count: int = 1, analyze: bool = False) -> tuple[list[str], str | None, list[str]]:
    url_list = [str(x or "").strip() for x in (urls or []) if str(x or "").strip()]
    if str(url or "").strip():
        url_list.insert(0, str(url or "").strip())
    if not url_list:
        raise ValueError("url 不能为空")

    if not CONTENT_FETCH_HUB_CLI_PATH.exists():
        raise ValueError(f"content-fetch-hub CLI 不存在：{CONTENT_FETCH_HUB_CLI_PATH}")

    cmd = ["python3", str(CONTENT_FETCH_HUB_CLI_PATH), "--json"]
    temp_path = None
    if len(url_list) == 1:
        cmd.insert(2, url_list[0])
    else:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as f:
            f.write("\n".join(url_list) + "\n")
            temp_path = f.name
        cmd.extend(["--file", temp_path])
    if vault:
        cmd.extend(["--vault", str(vault)])
    if retry_count >= 0:
        cmd.extend(["--retry", str(retry_count)])
    if analyze:
        cmd.append("--analyze")
    return cmd, temp_path, url_list


def _parse_fetch_progress_line(line: str) -> dict[str, Any] | None:
    raw = str(line or "").strip()
    if not raw.startswith("[progress]"):
        return None
    payload_raw = raw[len("[progress]") :].strip()
    try:
        payload = json.loads(payload_raw)
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {"stage": "running", "message": payload_raw}
    return None


def _serialize_fetch_task(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "task_id": task.get("task_id"),
        "status": task.get("status"),
        "phase": task.get("phase"),
        "message": task.get("message"),
        "created_at": task.get("created_at"),
        "started_at": task.get("started_at"),
        "finished_at": task.get("finished_at"),
        "urls": task.get("urls") or [],
        "vault": task.get("vault") or "",
        "retry_count": task.get("retry_count"),
        "analyze": bool(task.get("analyze")),
        "save_to_db": bool(task.get("save_to_db")),
        "pid": task.get("pid"),
        "result": task.get("result"),
        "error": task.get("error") or "",
        "progress": task.get("progress") or {},
        "logs": (task.get("logs") or [])[-30:],
        "cancel_requested": bool(task.get("cancel_requested")),
      }


def _update_fetch_task(task_id: str, **changes: Any) -> None:
    with FETCH_TASKS_LOCK:
        task = FETCH_TASKS.get(task_id)
        if not task:
            return
        task.update(changes)


def _append_fetch_task_log(task_id: str, line: str) -> None:
    clean = str(line or "").rstrip()
    if not clean:
        return
    with FETCH_TASKS_LOCK:
        task = FETCH_TASKS.get(task_id)
        if not task:
            return
        logs = task.setdefault("logs", [])
        logs.append(clean)
        if len(logs) > 100:
            del logs[:-100]

        progress = _parse_fetch_progress_line(clean)
        if progress:
            task["phase"] = str(progress.get("stage") or task.get("phase") or "running")
            task["message"] = str(progress.get("message") or task.get("message") or "执行中")
            task["progress"] = progress


def _run_fetch_task(task_id: str) -> None:
    with FETCH_TASKS_LOCK:
        task = FETCH_TASKS.get(task_id)
        if not task:
            return
        cmd = list(task.get("cmd") or [])
        temp_path = task.get("temp_path")

    proc: subprocess.Popen[str] | None = None
    stderr_lines: list[str] = []
    stdout_text = ""

    try:
        _update_fetch_task(task_id, status="running", phase="queued", message="任务已启动，等待抓取进程")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        _update_fetch_task(task_id, pid=proc.pid, started_at=datetime.now().isoformat(), phase="starting", message="已启动抓取进程")

        def _read_stderr() -> None:
            assert proc is not None
            if proc.stderr is None:
                return
            for line in proc.stderr:
                stderr_lines.append(line)
                _append_fetch_task_log(task_id, line)

        stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
        stderr_thread.start()
        stdout_text = proc.stdout.read() if proc.stdout else ""
        return_code = proc.wait()
        stderr_thread.join(timeout=2)

        with FETCH_TASKS_LOCK:
            cancel_requested = bool((FETCH_TASKS.get(task_id) or {}).get("cancel_requested"))

        if cancel_requested:
            _update_fetch_task(task_id, status="cancelled", phase="cancelled", message="任务已取消", error="cancelled", finished_at=datetime.now().isoformat())
            return

        out = (stdout_text or "").strip()
        err = ("".join(stderr_lines) or "").strip()
        if return_code != 0:
            raise RuntimeError(err or out or f"content-fetch-hub failed: {return_code}")
        if not out:
            raise RuntimeError("content-fetch-hub 未返回结果")

        _update_fetch_task(task_id, phase="parsing-result", message="抓取完成，正在解析结果")
        data = json.loads(out)
        if not isinstance(data, dict):
            raise RuntimeError("content-fetch-hub 返回格式错误")

        with FETCH_TASKS_LOCK:
            save_to_db = bool((FETCH_TASKS.get(task_id) or {}).get("save_to_db"))
        if save_to_db:
            _update_fetch_task(task_id, phase="saving-db", message="抓取完成，正在写入数据库")
            data["db_summary"] = ingest_fetch_result_to_db(data)

        _update_fetch_task(task_id, status="completed", phase="completed", message="抓取完成", result=data, finished_at=datetime.now().isoformat(), error="")
    except Exception as exc:  # noqa: BLE001
        with FETCH_TASKS_LOCK:
            cancel_requested = bool((FETCH_TASKS.get(task_id) or {}).get("cancel_requested"))
        if cancel_requested:
            _update_fetch_task(task_id, status="cancelled", phase="cancelled", message="任务已取消", error="cancelled", finished_at=datetime.now().isoformat())
        else:
            _update_fetch_task(task_id, status="failed", phase="failed", message=str(exc), error=str(exc), finished_at=datetime.now().isoformat())
    finally:
        if temp_path:
            try:
                Path(str(temp_path)).unlink(missing_ok=True)
            except Exception:
                pass
        _cleanup_fetch_tasks()


def _normalize_fetch_platform(value: Any) -> str:
    raw = str(value or "").strip().lower()
    mapping = {
        "twitter": "x",
        "x": "x",
        "youtube": "youtube",
        "bilibili": "bilibili",
        "douyin": "douyin",
        "wechat": "wechat",
        "feishu": "feishu",
        "web": "web",
    }
    return mapping.get(raw, raw or "web")


def _guess_fetch_source_handle(item: dict[str, Any]) -> str:
    meta = item.get("meta") or {}
    candidates = [
        item.get("author"),
        meta.get("source_handle"),
        meta.get("author_handle"),
        meta.get("channel_name"),
        meta.get("source_name"),
        meta.get("uploader"),
        meta.get("account_name"),
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text[:255]
    url = str(item.get("url") or "").strip()
    if url:
        try:
            host = urlparse(url).netloc.lower().strip()
            if host:
                return host[:255]
        except Exception:
            pass
    return ""


def _guess_fetch_item_id(item: dict[str, Any], platform: str) -> str:
    meta = item.get("meta") or {}
    candidates = [
        item.get("item_id"),
        meta.get("video_id"),
        meta.get("post_id"),
        meta.get("tweet_id"),
        meta.get("note_id"),
        meta.get("doc_id"),
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text[:255]
    raw_key = f"{platform}|{str(item.get('url') or '').strip()}|{str(item.get('title') or '').strip()}"
    return hashlib.md5(raw_key.encode("utf-8")).hexdigest()  # noqa: S324


def upsert_manual_fetch_item(conn: sqlite3.Connection, item: dict[str, Any]) -> str:
    platform = _normalize_fetch_platform(item.get("channel") or item.get("platform"))
    source_handle = _guess_fetch_source_handle(item)
    item_id = _guess_fetch_item_id(item, platform)
    title = str(item.get("title") or "").strip()
    content = str(item.get("content_markdown") or item.get("content") or "").strip()
    url = str(item.get("url") or "").strip()
    published_at = _format_datetime_text(item.get("published_at") or "")
    meta = dict(item.get("meta") or {})
    metrics_payload = {
        **meta,
        "manual_fetch": True,
        "saved_path": str(item.get("saved_path") or ""),
        "analysis_path": str(item.get("analysis_path") or ""),
        "ingest_source": "content-search-dashboard-fetch",
    }

    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO raw_items(
          platform, source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            platform,
            source_handle,
            item_id,
            title,
            content,
            url,
            published_at,
            json.dumps(metrics_payload, ensure_ascii=False),
        ),
    )
    if cur.rowcount > 0:
        return "inserted"

    cur.execute(
        """
        UPDATE raw_items
        SET source_handle=?, title=?, content=?, url=?, published_at=?, metrics_json=?, fetched_at=datetime('now')
        WHERE platform=? AND item_id=?
        """,
        (
            source_handle,
            title,
            content,
            url,
            published_at,
            json.dumps(metrics_payload, ensure_ascii=False),
            platform,
            item_id,
        ),
    )
    if cur.rowcount > 0:
        return "refreshed"
    return "noop"


def ingest_fetch_result_to_db(result: dict[str, Any]) -> dict[str, Any]:
    ensure_event_raw_items_table()
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    summary = {"enabled": True, "inserted": 0, "refreshed": 0, "noop": 0, "failed": 0, "items": []}
    try:
        for item in (result.get("results") or []):
            if str(item.get("status") or "").strip().lower() == "failed":
                continue
            if not item.get("ok"):
                continue
            try:
                db_status = upsert_manual_fetch_item(conn, item)
                item["db_saved"] = db_status in {"inserted", "refreshed", "noop"}
                item["db_status"] = db_status
                item["db_error"] = ""
                summary[db_status] = int(summary.get(db_status) or 0) + 1
            except Exception as exc:  # noqa: BLE001
                item["db_saved"] = False
                item["db_status"] = "failed"
                item["db_error"] = str(exc)
                summary["failed"] += 1
            summary["items"].append({
                "url": str(item.get("url") or ""),
                "title": str(item.get("title") or ""),
                "db_status": str(item.get("db_status") or ""),
                "db_error": str(item.get("db_error") or ""),
            })
        conn.commit()
        return summary
    finally:
        conn.close()


def create_fetch_task(url: str = "", urls: list[str] | None = None, vault: str = "", retry_count: int = 1, analyze: bool = False, save_to_db: bool = False) -> dict[str, Any]:
    cmd, temp_path, url_list = _build_content_fetch_cmd(url=url, urls=urls, vault=vault, retry_count=retry_count, analyze=analyze)
    task_id = uuid.uuid4().hex[:12]
    task = {
        "task_id": task_id,
        "status": "queued",
        "phase": "queued",
        "message": "任务已创建",
        "created_at": datetime.now().isoformat(),
        "started_at": "",
        "finished_at": "",
        "urls": url_list,
        "vault": vault,
        "retry_count": retry_count,
        "analyze": analyze,
        "save_to_db": save_to_db,
        "cmd": cmd,
        "temp_path": temp_path,
        "pid": None,
        "logs": [],
        "progress": {},
        "result": None,
        "error": "",
        "cancel_requested": False,
    }
    with FETCH_TASKS_LOCK:
        FETCH_TASKS[task_id] = task
    thread = threading.Thread(target=_run_fetch_task, args=(task_id,), daemon=True)
    thread.start()
    return _serialize_fetch_task(task)


def get_fetch_task(task_id: str) -> dict[str, Any] | None:
    _cleanup_fetch_tasks()
    with FETCH_TASKS_LOCK:
        task = FETCH_TASKS.get(task_id)
        if not task:
            return None
        return _serialize_fetch_task(task)


def cancel_fetch_task(task_id: str) -> dict[str, Any] | None:
    with FETCH_TASKS_LOCK:
        task = FETCH_TASKS.get(task_id)
        if not task:
            return None
        task["cancel_requested"] = True
        pid = int(task.get("pid") or 0)
    if pid > 0:
        try:
            os.killpg(pid, signal.SIGTERM)
        except Exception:
            pass
    _update_fetch_task(task_id, status="cancelled", phase="cancelled", message="正在取消任务", finished_at=datetime.now().isoformat(), error="cancelled")
    return get_fetch_task(task_id)


def run_content_fetch_hub(url: str = "", urls: list[str] | None = None, vault: str = "", retry_count: int = 1, analyze: bool = False, save_to_db: bool = False) -> dict[str, Any]:
    cmd, temp_path, _ = _build_content_fetch_cmd(url=url, urls=urls, vault=vault, retry_count=retry_count, analyze=analyze)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    finally:
        if temp_path:
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass

    out = (proc.stdout or "").strip()
    if proc.returncode != 0:
        err = (proc.stderr or out or "content-fetch-hub failed").strip()
        raise RuntimeError(err)
    if not out:
        raise RuntimeError("content-fetch-hub 未返回结果")
    try:
        data = json.loads(out)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"content-fetch-hub 输出非 JSON：{exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError("content-fetch-hub 返回格式错误")
    if save_to_db:
        data["db_summary"] = ingest_fetch_result_to_db(data)
    return data


def load_agent_reach_twitter_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    if not env.get("TWITTER_PROXY"):
        proxy = (
            env.get("HTTPS_PROXY")
            or env.get("https_proxy")
            or env.get("HTTP_PROXY")
            or env.get("http_proxy")
        )
        if proxy:
            env["TWITTER_PROXY"] = proxy
            env.setdefault("HTTPS_PROXY", proxy)
            env.setdefault("HTTP_PROXY", proxy)
    cfg = root_config()
    credentials = dict(cfg.get("credentials") or {})
    auth_env = str(credentials.get("twitter_auth_token_env") or "TWITTER_AUTH_TOKEN").strip()
    ct0_env = str(credentials.get("twitter_ct0_env") or "TWITTER_CT0").strip()
    if auth_env and os.environ.get(auth_env) and not env.get("TWITTER_AUTH_TOKEN"):
        env["TWITTER_AUTH_TOKEN"] = str(os.environ.get(auth_env) or "")
    if ct0_env and os.environ.get(ct0_env) and not env.get("TWITTER_CT0"):
        env["TWITTER_CT0"] = str(os.environ.get(ct0_env) or "")
    return env


def load_whitelist() -> dict[str, Any]:
    if not WHITELIST_PATH.exists():
        return {"accounts": []}
    return yaml.safe_load(WHITELIST_PATH.read_text(encoding="utf-8")) or {"accounts": []}


def load_wechat_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    if not WECHAT_SOURCES_PATH.exists():
        return []
    try:
        data = json.loads(WECHAT_SOURCES_PATH.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return []

    out: list[dict[str, Any]] = []
    for source in (data.get("sources") or []):
        item = dict(source or {})
        item["id"] = str(item.get("id") or "").strip()
        item["name"] = str(item.get("name") or "").strip()
        item["enabled"] = bool(item.get("enabled", True))
        item["priority"] = int(item.get("priority") or 5)
        item["entry_url"] = str(item.get("entry_url") or "").strip()
        item["fetch_method"] = str(item.get("fetch_method") or "").strip()
        if item["id"] and item["name"]:
            out.append(item)

    if enabled_only:
        out = [s for s in out if s.get("enabled")]
    return out


def load_youtube_channels(enabled_only: bool = True) -> list[dict[str, Any]]:
    if not YOUTUBE_CHANNELS_PATH.exists():
        return []
    try:
        data = json.loads(YOUTUBE_CHANNELS_PATH.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return []

    out: list[dict[str, Any]] = []
    for source in (data.get("channels") or []):
        item = dict(source or {})
        item["id"] = str(item.get("id") or "").strip()
        item["name"] = str(item.get("name") or "").strip()
        item["enabled"] = bool(item.get("enabled", True))
        item["priority"] = int(item.get("priority") or 5)
        item["channel_id"] = str(item.get("channel_id") or "").strip()
        item["channel_url"] = str(item.get("channel_url") or "").strip()
        item["url"] = str(item.get("url") or item.get("channel_url") or "").strip()
        item["fetch_method"] = str(item.get("fetch_method") or "").strip()
        item["category"] = str(item.get("category") or "").strip()
        item["notes"] = str(item.get("notes") or "").strip()
        item["description"] = str(item.get("description") or item.get("notes") or "").strip()
        if item["id"] and item["name"]:
            out.append(item)

    if enabled_only:
        out = [s for s in out if s.get("enabled")]
    return out


def load_douyin_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    if not DOUYIN_SOURCES_PATH.exists():
        return []
    try:
        data = json.loads(DOUYIN_SOURCES_PATH.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return []

    out: list[dict[str, Any]] = []
    for source in (data.get("sources") or []):
        item = dict(source or {})
        item["id"] = str(item.get("id") or "").strip()
        item["name"] = str(item.get("name") or "").strip()
        item["enabled"] = bool(item.get("enabled", True))
        item["priority"] = int(item.get("priority") or 5)
        item["profile_url"] = str(item.get("profile_url") or item.get("entry_url") or "").strip()
        item["fetch_method"] = str(item.get("fetch_method") or "profile_playlist").strip() or "profile_playlist"
        item["category"] = str(item.get("category") or "").strip()
        item["notes"] = str(item.get("notes") or "").strip()
        item["url"] = item["profile_url"]
        if item["id"] and item["name"]:
            out.append(item)

    if enabled_only:
        out = [s for s in out if s.get("enabled")]
    return out


@lru_cache(maxsize=1)
def ensure_event_accounts_table() -> None:
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              platform TEXT NOT NULL,
              handle TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              priority INTEGER NOT NULL DEFAULT 5,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(platform, handle)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_platform_enabled_priority ON accounts(platform, enabled DESC, priority DESC, id ASC)")
        conn.commit()
    finally:
        conn.close()


def sync_whitelist_accounts_to_event_db(accounts: list[dict[str, Any]]) -> None:
    ensure_event_accounts_table()
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    try:
        cur = conn.cursor()
        handles: list[str] = []
        for account in accounts or []:
            handle = str(account.get("handle") or "").strip()
            if not handle:
                continue
            handles.append(handle)
            cur.execute(
                """
                INSERT INTO accounts(platform, handle, enabled, priority)
                VALUES('x', ?, ?, ?)
                ON CONFLICT(platform, handle)
                DO UPDATE SET enabled=excluded.enabled, priority=excluded.priority
                """,
                (handle, 1 if account.get("enabled", True) else 0, int(account.get("priority") or 5)),
            )

        if handles:
            placeholders = ",".join(["?"] * len(handles))
            cur.execute(
                f"DELETE FROM accounts WHERE platform='x' AND handle NOT IN ({placeholders})",
                handles,
            )
        else:
            cur.execute("DELETE FROM accounts WHERE platform='x'")

        conn.commit()
    finally:
        conn.close()


def save_whitelist(data: dict[str, Any]) -> None:
    data = dict(data or {})
    if "accounts" not in data:
        data["accounts"] = []
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d")
    text = yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
    WHITELIST_PATH.write_text(text, encoding="utf-8")

    try:
        sync_whitelist_accounts_to_event_db(data.get("accounts") or [])
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] sync accounts to sqlite failed: {exc}")


def normalize_handle(handle: str) -> str:
    value = (handle or "").strip().lstrip("@")
    if not value:
        raise ValueError("handle 不能为空")
    if not re.fullmatch(r"[A-Za-z0-9_]{1,32}", value):
        raise ValueError("handle 格式不合法，仅支持字母/数字/下划线")
    return value


def normalize_source_id(value: str) -> str:
    raw = str(value or "").strip().lower().replace("@", "")
    raw = re.sub(r"https?://", "", raw)
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = raw.strip("-")
    if not raw:
        raise ValueError("来源标识不能为空")
    return raw[:64]


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {"version": 1}
    except Exception:  # noqa: BLE001
        return {"version": 1}


def _save_json_file(path: Path, data: dict[str, Any]) -> None:
    doc = dict(data or {})
    doc["version"] = int(doc.get("version") or 1)
    doc["updated_at"] = datetime.now().strftime("%Y-%m-%d")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _source_store_meta(platform: str) -> tuple[Path, str]:
    p = str(platform or "").strip().lower()
    if p == "wechat":
        return WECHAT_SOURCES_PATH, "sources"
    if p == "youtube":
        return YOUTUBE_CHANNELS_PATH, "channels"
    if p == "douyin":
        return DOUYIN_SOURCES_PATH, "sources"
    raise ValueError(f"不支持的平台：{platform}")


def _set_source_enabled(platform: str, source_id: str, enabled: bool) -> dict[str, Any]:
    path, list_key = _source_store_meta(platform)
    target_id = normalize_source_id(source_id)
    doc = _read_json_file(path)
    rows = list(doc.get(list_key) or [])
    target = None
    for row in rows:
        rid = normalize_source_id(str((row or {}).get("id") or "")) if str((row or {}).get("id") or "").strip() else ""
        if rid == target_id:
            row["enabled"] = bool(enabled)
            target = row
            break
    if target is None:
        raise ValueError(f"账号不存在：{source_id}")
    doc[list_key] = rows
    _save_json_file(path, doc)
    target["platform"] = platform
    return target


def _delete_source(platform: str, source_id: str) -> None:
    path, list_key = _source_store_meta(platform)
    target_id = normalize_source_id(source_id)
    doc = _read_json_file(path)
    rows = list(doc.get(list_key) or [])
    new_rows: list[dict[str, Any]] = []
    removed = False
    for row in rows:
        rid_raw = str((row or {}).get("id") or "").strip()
        rid = normalize_source_id(rid_raw) if rid_raw else ""
        if rid and rid == target_id:
            removed = True
            continue
        new_rows.append(row)
    if not removed:
        raise ValueError(f"账号不存在：{source_id}")
    doc[list_key] = new_rows
    _save_json_file(path, doc)


def set_account_enabled(handle: str, enabled: bool, platform: str = "x") -> dict[str, Any]:
    p = str(platform or "x").strip().lower() or "x"
    if p != "x":
        return _set_source_enabled(p, handle, enabled)

    handle = normalize_handle(handle)
    data = load_whitelist()
    accounts = data.get("accounts", []) or []
    target = None
    for account in accounts:
        if str(account.get("handle") or "").strip().lower() == handle.lower():
            target = account
            break

    if target is None:
        raise ValueError(f"账号不存在：{handle}")

    target["enabled"] = bool(enabled)
    save_whitelist(data)
    return target


def fetch_account_profile(handle: str) -> dict[str, Any]:
    handle = normalize_handle(handle)
    payload = run_twitter_command(["twitter", "user", handle, "--json"])
    data = payload.get("data") or {}
    return {
        "handle": handle,
        "username": str(data.get("name") or "").strip(),
        "screen_name": str(data.get("screenName") or handle).strip(),
        "verified": bool(data.get("verified")),
        "profile_image_url": str(data.get("profileImageUrl") or "").strip(),
    }



def add_x_account(payload: dict[str, Any]) -> dict[str, Any]:
    handle = normalize_handle(str(payload.get("handle") or ""))
    data = load_whitelist()
    accounts = data.get("accounts", []) or []
    for account in accounts:
        if str(account.get("handle") or "").strip().lower() == handle.lower():
            raise ValueError(f"账号已存在：{handle}")

    profile: dict[str, Any] = {}
    try:
        profile = fetch_account_profile(handle)
    except Exception:  # noqa: BLE001
        profile = {}

    new_account = {
        "platform": "x",
        "handle": handle,
        "id": handle,
        "name": str(payload.get("username") or profile.get("username") or handle).strip() or handle,
        "username": str(payload.get("username") or profile.get("username") or "").strip(),
        "enabled": bool(payload.get("enabled", True)),
        "tier": str(payload.get("tier") or "seed").strip() or "seed",
        "lang": str(payload.get("lang") or "").strip(),
        "category": str(payload.get("category") or "").strip(),
        "priority": max(0, min(int(payload.get("priority") or 5), 10)),
        "source": str(payload.get("source") or "manual").strip() or "manual",
        "notes": str(payload.get("notes") or "").strip(),
    }
    if profile.get("verified"):
        new_account["verified"] = True
    if profile.get("profile_image_url"):
        new_account["profile_image_url"] = profile["profile_image_url"]
    accounts.append(new_account)
    data["accounts"] = accounts
    save_whitelist(data)
    return new_account


def _append_source_item(*, path: Path, list_key: str, item: dict[str, Any]) -> dict[str, Any]:
    doc = _read_json_file(path)
    rows = list(doc.get(list_key) or [])
    item_id = str(item.get("id") or "").strip().lower()
    for row in rows:
        rid = str((row or {}).get("id") or "").strip().lower()
        if rid and rid == item_id:
            raise ValueError(f"账号已存在：{item.get('id')}")
    rows.append(item)
    doc[list_key] = rows
    _save_json_file(path, doc)
    return item


def add_wechat_source(payload: dict[str, Any]) -> dict[str, Any]:
    entry_url = str(payload.get("entry_url") or payload.get("url") or "").strip()
    if not entry_url:
        raise ValueError("微信公众号来源需要 entry_url")
    raw_id = str(payload.get("id") or payload.get("handle") or payload.get("name") or entry_url)
    source_id = normalize_source_id(raw_id)
    item = {
        "platform": "wechat",
        "id": source_id,
        "name": str(payload.get("name") or source_id).strip() or source_id,
        "enabled": bool(payload.get("enabled", True)),
        "priority": max(0, min(int(payload.get("priority") or 5), 10)),
        "category": str(payload.get("category") or "").strip(),
        "entry_url": entry_url,
        "fetch_method": str(payload.get("fetch_method") or "official_feed").strip() or "official_feed",
        "notes": str(payload.get("notes") or "").strip(),
    }
    return _append_source_item(path=WECHAT_SOURCES_PATH, list_key="sources", item=item)


def add_youtube_channel(payload: dict[str, Any]) -> dict[str, Any]:
    channel_url = str(payload.get("channel_url") or payload.get("url") or "").strip()
    if not channel_url:
        raise ValueError("YouTube 频道需要 channel_url")
    raw_id = str(payload.get("id") or payload.get("handle") or payload.get("name") or channel_url)
    channel_id = normalize_source_id(raw_id)
    item = {
        "platform": "youtube",
        "id": channel_id,
        "name": str(payload.get("name") or channel_id).strip() or channel_id,
        "enabled": bool(payload.get("enabled", True)),
        "priority": max(0, min(int(payload.get("priority") or 5), 10)),
        "category": str(payload.get("category") or "").strip(),
        "channel_url": channel_url,
        "fetch_method": str(payload.get("fetch_method") or "channel_videos").strip() or "channel_videos",
        "notes": str(payload.get("notes") or "").strip(),
    }
    return _append_source_item(path=YOUTUBE_CHANNELS_PATH, list_key="channels", item=item)


def add_douyin_source(payload: dict[str, Any]) -> dict[str, Any]:
    profile_url = str(payload.get("profile_url") or payload.get("url") or "").strip()
    if not profile_url:
        raise ValueError("抖音来源需要 profile_url")
    raw_id = str(payload.get("id") or payload.get("handle") or payload.get("name") or profile_url)
    source_id = normalize_source_id(raw_id)
    item = {
        "platform": "douyin",
        "id": source_id,
        "name": str(payload.get("name") or source_id).strip() or source_id,
        "enabled": bool(payload.get("enabled", True)),
        "priority": max(0, min(int(payload.get("priority") or 5), 10)),
        "category": str(payload.get("category") or "").strip(),
        "profile_url": profile_url,
        "fetch_method": str(payload.get("fetch_method") or "profile_playlist").strip() or "profile_playlist",
        "notes": str(payload.get("notes") or "").strip(),
    }
    return _append_source_item(path=DOUYIN_SOURCES_PATH, list_key="sources", item=item)


def add_account(payload: dict[str, Any]) -> dict[str, Any]:
    platform = str(payload.get("platform") or "x").strip().lower() or "x"
    if platform == "x":
        return add_x_account(payload)
    if platform == "wechat":
        return add_wechat_source(payload)
    if platform == "youtube":
        return add_youtube_channel(payload)
    if platform == "douyin":
        return add_douyin_source(payload)
    raise ValueError(f"不支持手动添加的平台：{platform}")


def delete_account(handle: str, platform: str = "x") -> None:
    p = str(platform or "x").strip().lower() or "x"
    if p != "x":
        _delete_source(p, handle)
        return

    handle = normalize_handle(handle)
    data = load_whitelist()
    accounts = data.get("accounts", []) or []
    new_accounts = [a for a in accounts if str(a.get("handle") or "").strip().lower() != handle.lower()]
    if len(new_accounts) == len(accounts):
        raise ValueError(f"账号不存在：{handle}")
    data["accounts"] = new_accounts
    save_whitelist(data)


def _is_transient_x_test_error(message: str) -> bool:
    msg = str(message or "").strip().lower()
    if not msg:
        return False
    transient_tokens = [
        "dependency: unspecified",
        "http 0",
        "timeout",
        "timed out",
        "tempor",
        "connection reset",
        "connection aborted",
        "network",
        "upstream",
        "service unavailable",
        "too many requests",
    ]
    return any(token in msg for token in transient_tokens)


def _friendly_x_test_error(message: str, retry_count: int) -> str:
    raw = str(message or "").strip() or "未知错误"
    msg = raw.lower()

    if "dependency: unspecified" in msg or "http 0" in msg:
        return f"X 接口临时波动（上游依赖异常），已自动重试 {retry_count} 次仍失败。建议 30~60 秒后再试。"
    if "too many requests" in msg or "429" in msg:
        return f"请求过于频繁，已自动重试 {retry_count} 次仍被限流。建议稍后重试。"
    if "auth" in msg or "401" in msg or "unauthorized" in msg:
        return "X 认证信息可能失效（token/ct0）。请检查登录态后再试。"
    if "timeout" in msg or "timed out" in msg:
        return f"请求超时，已自动重试 {retry_count} 次仍失败。请检查网络或代理后再试。"
    return f"测试失败：{raw}"


def _test_x_account(handle: str) -> dict[str, Any]:
    handle = normalize_handle(handle)
    profile = {}
    try:
        profile = fetch_account_profile(handle)
    except Exception:  # noqa: BLE001
        profile = {}

    cmd = ["twitter", "user-posts", handle, "-n", "1", "--json"]
    max_retries = 3
    last_error = ""
    payload: dict[str, Any] = {}

    for attempt in range(1, max_retries + 1):
        try:
            payload = run_twitter_command(cmd)
            last_error = ""
            break
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc).strip()
            transient = _is_transient_x_test_error(last_error)
            if attempt < max_retries and transient:
                time.sleep(0.8 * attempt)
                continue
            raise ValueError(_friendly_x_test_error(last_error, max_retries)) from exc

    items = payload.get("data", []) or []
    latest = items[0] if items else {}
    return {
        "handle": handle,
        "ok": True,
        "username": str(profile.get("username") or "").strip(),
        "post_count": len(items),
        "latest": {
            "id": latest.get("id") or "",
            "text": (latest.get("text") or "")[:120],
            "created_at": _format_datetime_text(latest.get("createdAtISO") or latest.get("createdAtLocal") or latest.get("createdAt") or ""),
        },
    }


def _load_wechat_collector() -> Any:
    scripts_dir = BASE_DIR / "scripts"
    scripts_dir_text = str(scripts_dir)
    if scripts_dir_text in sys.path:
        sys.path.remove(scripts_dir_text)
    sys.path.insert(0, scripts_dir_text)
    from wechat_collector import WeChatCollector  # type: ignore

    return WeChatCollector


def _load_youtube_fetcher() -> Any:
    scripts_dir = BASE_DIR / "scripts"
    scripts_dir_text = str(scripts_dir)
    if scripts_dir_text in sys.path:
        sys.path.remove(scripts_dir_text)
    sys.path.insert(0, scripts_dir_text)
    from youtube_collector import fetch_recent_videos  # type: ignore

    return fetch_recent_videos


def _test_wechat_source(source_id: str) -> dict[str, Any]:
    source_key = normalize_source_id(source_id)
    if not source_key:
        raise ValueError("微信公众号来源 id 不能为空")

    sources = load_wechat_sources(enabled_only=False)
    source = next((s for s in sources if normalize_source_id(s.get("id")) == source_key), None)
    if not source:
        raise ValueError(f"公众号来源不存在：{source_id}")

    try:
        collector_cls = _load_wechat_collector()
        collector = collector_cls()
        items = collector.collect_for_handle(str(source.get("id") or source_key), limit=3)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"公众号抓取失败：{exc}") from exc

    if not items:
        raise ValueError("公众号可访问，但暂未抓到文章（可能是入口无内容或被限流）")

    latest = items[0]
    latest_item_id = str(latest.get("item_id") or "").strip()
    latest_title = str(latest.get("title") or "").strip()
    latest_published = _format_datetime_text(latest.get("published_at") or "")

    in_db = False
    if latest_item_id:
        ensure_event_raw_items_table()
        conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
        try:
            row = conn.execute(
                "SELECT 1 FROM raw_items WHERE platform='wechat' AND item_id=? LIMIT 1",
                (latest_item_id,),
            ).fetchone()
            in_db = bool(row)
        finally:
            conn.close()

    increment_msg = "检测到新文章，增量入库链路正常。" if not in_db else "最新文章已在库中，增量巡检链路正常。"

    return {
        "ok": True,
        "platform": "wechat",
        "id": str(source.get("id") or "").strip(),
        "name": str(source.get("name") or "").strip(),
        "entry_url": str(source.get("entry_url") or "").strip(),
        "post_count": len(items),
        "increment": {
            "has_new": not in_db,
            "message": increment_msg,
        },
        "latest": {
            "id": latest_item_id,
            "text": latest_title[:120],
            "created_at": latest_published,
            "url": str(latest.get("url") or "").strip(),
        },
    }


def _test_youtube_source(source_id: str) -> dict[str, Any]:
    source_key = normalize_source_id(source_id)
    if not source_key:
        raise ValueError("YouTube 来源 id 不能为空")

    channels = load_youtube_channels(enabled_only=False)
    channel = next((c for c in channels if normalize_source_id(c.get("id")) == source_key), None)
    if not channel:
        raise ValueError(f"YouTube 来源不存在：{source_id}")

    try:
        fetch_recent_videos = _load_youtube_fetcher()
        videos = fetch_recent_videos(channel, limit=1, detail_lookup=False, timeout_sec=30)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"YouTube 抓取失败：{exc}") from exc

    if not videos:
        raise ValueError("YouTube 频道可访问，但暂未抓到视频")

    latest = videos[0] or {}
    latest_item_id = str(latest.get("video_id") or "").strip()
    latest_title = str(latest.get("title") or "").strip()
    latest_published = _format_datetime_text(latest.get("published_at") or "")

    in_db = False
    if latest_item_id:
        ensure_event_raw_items_table()
        conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
        try:
            row = conn.execute(
                "SELECT 1 FROM raw_items WHERE platform='youtube' AND item_id=? LIMIT 1",
                (latest_item_id,),
            ).fetchone()
            in_db = bool(row)
        finally:
            conn.close()

    increment_msg = "检测到新视频，增量入库链路正常。" if not in_db else "最新视频已在库中，增量巡检链路正常。"

    return {
        "ok": True,
        "platform": "youtube",
        "id": str(channel.get("id") or "").strip(),
        "name": str(channel.get("name") or "").strip(),
        "channel_url": str(channel.get("channel_url") or "").strip(),
        "post_count": len(videos),
        "increment": {
            "has_new": not in_db,
            "message": increment_msg,
        },
        "latest": {
            "id": latest_item_id,
            "text": latest_title[:120],
            "created_at": latest_published,
            "url": str(latest.get("url") or "").strip(),
        },
    }


def test_account(handle: str, platform: str = "x") -> dict[str, Any]:
    p = str(platform or "x").strip().lower() or "x"
    target = str(handle or "").strip()
    if p == "x":
        return _test_x_account(target)
    if p == "wechat":
        return _test_wechat_source(target)
    if p == "youtube":
        return _test_youtube_source(target)
    raise ValueError(f"当前测试仅支持 x/wechat/youtube：{p}")


def get_accounts(enabled_only: bool = False) -> list[dict[str, Any]]:
    data = load_whitelist()
    accounts = data.get("accounts", []) or []
    if enabled_only:
        accounts = [a for a in accounts if a.get("enabled")]
    return accounts


def load_templates_doc() -> dict[str, Any]:
    if not TEMPLATES_PATH.exists():
        return {"version": 1, "templates": []}
    return yaml.safe_load(TEMPLATES_PATH.read_text(encoding="utf-8")) or {"version": 1, "templates": []}


def load_templates(include_disabled: bool = True) -> list[dict[str, Any]]:
    data = load_templates_doc()
    templates = data.get("templates", []) or []
    if include_disabled:
        return templates
    return [t for t in templates if t.get("enabled", True)]


def save_templates_doc(data: dict[str, Any]) -> None:
    data = dict(data or {})
    if "templates" not in data:
        data["templates"] = []
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d")
    text = yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
    TEMPLATES_PATH.write_text(text, encoding="utf-8")


def upsert_template(payload: dict[str, Any]) -> dict[str, Any]:
    tpl = dict(payload or {})
    template_id = str(tpl.get("id") or "").strip()
    if not template_id:
        raise ValueError("模板 id 不能为空")

    provider = str(tpl.get("provider") or "x").strip().lower() or "x"
    if provider not in {"x", "wechat", "youtube", "bilibili", "feed", "github"}:
        raise ValueError(f"不支持的 provider：{provider}")

    default_mode_map = {
        "x": "pool_latest",
        "wechat": "wechat_search",
        "youtube": "youtube_search",
        "bilibili": "bilibili_search",
        "feed": "feed_search",
        "github": "github_trending",
    }

    tpl["id"] = template_id
    tpl["provider"] = provider
    tpl["name"] = str(tpl.get("name") or template_id).strip()
    tpl["mode"] = str(tpl.get("mode") or default_mode_map.get(provider, "pool_latest")).strip()
    tpl["sort_by"] = str(tpl.get("sort_by") or "time").strip()
    tpl["query"] = str(tpl.get("query") or "").strip()
    tpl["target_handle"] = str(tpl.get("target_handle") or "").strip()
    tpl["search_type"] = str(tpl.get("search_type") or ("top" if tpl["mode"] == "pool_top" else "latest")).strip()
    tpl["since"] = str(tpl.get("since") or "").strip()
    tpl["until"] = str(tpl.get("until") or "").strip()
    tpl["lang"] = str(tpl.get("lang") or "").strip()
    tpl["limit_per_account"] = max(1, min(int(tpl.get("limit_per_account") or 3), 20))
    tpl["notes"] = str(tpl.get("notes") or "").strip()
    tpl["enabled"] = bool(tpl.get("enabled", True))

    data = load_templates_doc()
    templates = data.get("templates", []) or []
    replaced = False
    for idx, item in enumerate(templates):
        if str(item.get("id") or "").strip() == template_id:
            templates[idx] = tpl
            replaced = True
            break
    if not replaced:
        templates.append(tpl)

    data["templates"] = templates
    save_templates_doc(data)
    return tpl


def delete_template(template_id: str) -> None:
    template_id = (template_id or "").strip()
    if not template_id:
        raise ValueError("模板 id 不能为空")
    data = load_templates_doc()
    templates = data.get("templates", []) or []
    new_templates = [t for t in templates if str(t.get("id") or "").strip() != template_id]
    if len(new_templates) == len(templates):
        raise ValueError(f"模板不存在：{template_id}")
    data["templates"] = new_templates
    save_templates_doc(data)


def set_template_enabled(template_id: str, enabled: bool) -> dict[str, Any]:
    template_id = (template_id or "").strip()
    if not template_id:
        raise ValueError("模板 id 不能为空")
    data = load_templates_doc()
    templates = data.get("templates", []) or []
    target = None
    for tpl in templates:
        if str(tpl.get("id") or "").strip() == template_id:
            target = tpl
            break
    if target is None:
        raise ValueError(f"模板不存在：{template_id}")

    target["enabled"] = bool(enabled)
    data["templates"] = templates
    save_templates_doc(data)
    return target


def load_feed_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    default_sources = [
        {
            "id": "openai-blog",
            "name": "OpenAI Blog",
            "url": "https://openai.com/blog/rss.xml",
            "category": "official_blog",
            "enabled": True,
        },
        {
            "id": "google-deepmind-blog",
            "name": "Google DeepMind Blog",
            "url": "https://blog.google/technology/ai/rss/",
            "category": "official_blog",
            "enabled": True,
        },
        {
            "id": "venturebeat-ai",
            "name": "VentureBeat AI",
            "url": "https://venturebeat.com/category/ai/feed/",
            "category": "media",
            "enabled": True,
        },
        {
            "id": "techcrunch-ai",
            "name": "TechCrunch AI",
            "url": "https://techcrunch.com/category/artificial-intelligence/feed/",
            "category": "media",
            "enabled": True,
        },
    ]
    if not FEED_SOURCES_PATH.exists():
        return default_sources if not enabled_only else [s for s in default_sources if s.get("enabled", True)]

    doc = yaml.safe_load(FEED_SOURCES_PATH.read_text(encoding="utf-8")) or {}
    sources = doc.get("sources") or []
    normalized: list[dict[str, Any]] = []
    for item in sources:
        source = dict(item or {})
        source["id"] = str(source.get("id") or "").strip() or str(source.get("name") or "feed").strip().lower().replace(" ", "-")
        source["name"] = str(source.get("name") or source["id"]).strip()
        source["url"] = str(source.get("url") or "").strip()
        source["enabled"] = bool(source.get("enabled", True))
        source["category"] = str(source.get("category") or "").strip()
        if source["url"]:
            normalized.append(source)
    if enabled_only:
        normalized = [s for s in normalized if s.get("enabled", True)]
    return normalized


def _strip_html(text: str) -> str:
    value = re.sub(r"<[^>]+>", " ", text or "")
    value = unescape(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _trim_text(value: Any, limit: int = 1500) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: max(0, limit)] + "\n\n...(内容已截断，点击查看正文可看完整内容)"


def _format_datetime_text(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        if raw.isdigit():
            if len(raw) == 8:
                dt = datetime.strptime(raw, "%Y%m%d")
                return dt.strftime("%Y-%m-%d 00:00:00")
            if len(raw) >= 10:
                dt = datetime.fromtimestamp(int(raw[:10]), tz=timezone.utc).astimezone(BEIJING_TZ).replace(tzinfo=None)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
        if len(raw) == 10 and raw.count("-") == 2:
            dt = datetime.strptime(raw, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d 00:00:00")
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(BEIJING_TZ).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is not None:
            dt = dt.astimezone(BEIJING_TZ).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        return raw


def _parse_feed_dt(value: str) -> str:
    return _format_datetime_text(value)


def _find_child_text(node: ET.Element, names: list[str]) -> str:
    for name in names:
        # 支持带 namespace 的 tag
        child = node.find(name)
        if child is not None and (child.text or "").strip():
            return (child.text or "").strip()
        child = node.find(f"{{*}}{name}")
        if child is not None and (child.text or "").strip():
            return (child.text or "").strip()
    return ""


def fetch_feed_entries(source: dict[str, Any], limit_per_source: int) -> list[dict[str, Any]]:
    url = source.get("url") or ""
    req = Request(url, headers={"User-Agent": "content-search-layer/1.0"})
    with urlopen(req, timeout=15) as resp:  # noqa: S310
        data = resp.read()

    root = ET.fromstring(data)
    entries: list[dict[str, Any]] = []

    # RSS
    rss_items = root.findall("./channel/item")
    if not rss_items:
        rss_items = root.findall("./{*}channel/{*}item")
    for item in rss_items[:limit_per_source]:
        title = _find_child_text(item, ["title"])
        link = _find_child_text(item, ["link"])
        summary = _find_child_text(item, ["description", "summary"])
        guid = _find_child_text(item, ["guid"]) or link
        published_at = _parse_feed_dt(_find_child_text(item, ["pubDate", "published", "updated"]))
        entries.append(
            {
                "id": guid,
                "title": _strip_html(title),
                "url": link,
                "summary": _strip_html(summary),
                "published_at": published_at,
                "source": source,
            }
        )

    # Atom
    if not entries:
        atom_entries = root.findall("./{*}entry")
        for entry in atom_entries[:limit_per_source]:
            title = _find_child_text(entry, ["title"])
            summary = _find_child_text(entry, ["summary", "content"])
            link_node = entry.find("{*}link")
            link = ""
            if link_node is not None:
                link = str(link_node.attrib.get("href") or "").strip()
            entry_id = _find_child_text(entry, ["id"]) or link
            published_at = _parse_feed_dt(_find_child_text(entry, ["published", "updated"]))
            entries.append(
                {
                    "id": entry_id,
                    "title": _strip_html(title),
                    "url": link,
                    "summary": _strip_html(summary),
                    "published_at": published_at,
                    "source": source,
                }
            )

    return entries


def score_feed_candidate(entry: dict[str, Any], query: str) -> tuple[int, str, str]:
    haystack = f"{entry.get('title','')}\n{entry.get('summary','')}".lower()
    q_tokens = [t for t in re.split(r"\s+|\||OR|or", query) if t and len(t) > 1]
    match_count = sum(1 for t in q_tokens if t.lower() in haystack)

    domain = urlparse(entry.get("url") or "").netloc.lower()
    official_bonus = 2 if any(x in domain for x in ["openai.com", "google.com", "deepmind", "anthropic.com"]) else 0
    score = match_count + official_bonus

    if score >= 4:
        return score, "strong", "关键词命中高且来源信号强，建议优先阅读。"
    if score >= 2:
        return score, "watch", "有一定关键词命中，建议加入观察列表。"
    return score, "skip", "命中较弱，先作为背景信息。"


def _normalize_query_scope(value: Any) -> str:
    scope = str(value or "all").strip().lower()
    if scope not in {"all", "title", "content"}:
        scope = "all"
    return scope


def _query_scope_sql(scope: str) -> str:
    scope = _normalize_query_scope(scope)
    if scope == "title":
        return "lower(COALESCE(title,'')) LIKE ?"
    if scope == "content":
        return "lower(COALESCE(content,'')) LIKE ?"
    return "lower(COALESCE(title,'') || ' ' || COALESCE(content,'')) LIKE ?"


def _query_scope_match(title: str, content: str, query: str, scope: str) -> tuple[bool, bool]:
    q = str(query or "").strip().lower()
    title_text = str(title or "").lower()
    content_text = str(content or "").lower()
    scope = _normalize_query_scope(scope)
    title_hit = bool(q) and q in title_text
    content_hit = bool(q) and q in content_text
    if scope == "title":
        return title_hit, False
    if scope == "content":
        return False, content_hit
    return title_hit, content_hit


def search_feed_sources(params: dict[str, Any]) -> dict[str, Any]:
    query = (params.get("query") or "").strip()
    if not query:
        raise ValueError("query 不能为空")

    limit_per_source = int(params.get("limit_per_account") or 3)
    limit_per_source = max(1, min(limit_per_source, 20))
    sort_by = (params.get("sort_by") or "time").strip().lower()
    if sort_by not in {"time", "engagement", "hybrid"}:
        sort_by = "time"
    query_scope = _normalize_query_scope(params.get("query_scope") or "all")

    sources = load_feed_sources(enabled_only=True)
    if not sources:
        return {
            "ok": True,
            "query": query,
            "mode": "feed_search",
            "count": 0,
            "results": [],
            "grouped": {"strong": [], "watch": [], "skip": []},
            "counts": {"strong": 0, "watch": 0, "skip": 0},
            "searched_handles": [],
            "success_handles": [],
            "errors": [],
            "error_count": 0,
            "observability": {"searched_count": 0, "success_count": 0, "error_count": 0, "failed_handles": [], "success_handles": []},
            "filters": {"sort_by": sort_by},
        }

    all_items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    searched = [s.get("id") for s in sources]
    success: list[str] = []

    for source in sources:
        sid = source.get("id") or "feed"
        try:
            entries = fetch_feed_entries(source, limit_per_source=limit_per_source)
            for entry in entries:
                title_hit, content_hit = _query_scope_match(entry.get("title") or "", entry.get("summary") or "", query, query_scope)
                if not (title_hit or content_hit):
                    continue
                score, bucket, why_pick = score_feed_candidate(entry, query)
                if score <= 0:
                    continue
                all_items.append(
                    {
                        "id": entry.get("id") or entry.get("url"),
                        "url": entry.get("url") or "",
                        "title": entry.get("title") or "",
                        "channel": "feed",
                        "source": source.get("name") or sid,
                        "published_at": entry.get("published_at") or "",
                        "summary": (entry.get("summary") or "")[:220],
                        "why_pick": why_pick,
                        "recommend_full_fetch": "yes" if bucket == "strong" else ("maybe" if bucket == "watch" else "no"),
                        "bucket": bucket,
                        "created_at": entry.get("published_at") or "",
                        "metrics": {"likes": 0, "retweets": 0, "replies": 0, "quotes": 0, "views": 0, "engagement_score": score},
                        "lang": "",
                        "raw": entry,
                    }
                )
            success.append(sid)
        except (ET.ParseError, URLError, TimeoutError, OSError) as exc:
            errors.append({"handle": str(sid), "error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            errors.append({"handle": str(sid), "error": str(exc)})

    # Feed 无真实互动，engagement/hybrid 暂映射为 time
    dedup: dict[str, dict[str, Any]] = {}
    for item in all_items:
        key = item.get("url") or item.get("id") or f"{item.get('source')}::{item.get('title')}"
        if key not in dedup:
            dedup[key] = item
    results = list(dedup.values())
    results = sorted(results, key=lambda x: _parse_date_or_datetime(str(x.get("published_at") or "")) or datetime.min, reverse=True)

    grouped = {
        "strong": [r for r in results if r.get("bucket") == "strong"],
        "watch": [r for r in results if r.get("bucket") == "watch"],
        "skip": [r for r in results if r.get("bucket") == "skip"],
    }

    failed = [e.get("handle") for e in errors if e.get("handle")]
    return {
        "ok": True,
        "query": query,
        "mode": "feed_search",
        "count": len(results),
        "results": results,
        "grouped": grouped,
        "counts": {"strong": len(grouped["strong"]), "watch": len(grouped["watch"]), "skip": len(grouped["skip"])},
        "searched_handles": searched,
        "success_handles": success,
        "filters": {
            "lang": "",
            "since": "",
            "until": "",
            "query_scope": query_scope,
            "min_likes": 0,
            "min_retweets": 0,
            "sort_by": sort_by,
        },
        "errors": errors,
        "error_count": len(errors),
        "observability": {
            "error_count": len(errors),
            "success_count": len(success),
            "searched_count": len(searched),
            "failed_handles": failed,
            "success_handles": success,
        },
    }


def fetch_github_trending(language: str, limit: int) -> list[dict[str, Any]]:
    lang = (language or "").strip().lower()
    if lang:
        url = f"https://github.com/trending/{quote_plus(lang)}?since=daily"
    else:
        url = "https://github.com/trending?since=daily"

    req = Request(url, headers={"User-Agent": "content-search-layer/1.0"})
    with urlopen(req, timeout=15) as resp:  # noqa: S310
        html = resp.read().decode("utf-8", errors="ignore")

    items: list[dict[str, Any]] = []
    article_blocks = re.findall(r"<article[^>]*class=\"Box-row\"[^>]*>(.*?)</article>", html, flags=re.S)
    for block in article_blocks[: max(1, limit * 2)]:
        href_match = re.search(r"href=\"\s*(/[^\"#?]+/[^\"#?]+)\s*\"", block)
        if not href_match:
            continue
        repo_path = href_match.group(1).strip().strip("/")
        if "/" not in repo_path:
            continue

        desc_match = re.search(r"<p[^>]*>(.*?)</p>", block, flags=re.S)
        desc = _strip_html(desc_match.group(1)) if desc_match else ""
        stars_today_match = re.search(r"([\d,]+)\s+stars?\s+today", _strip_html(block), flags=re.I)
        stars_today = int((stars_today_match.group(1).replace(",", "") if stars_today_match else "0"))

        items.append(
            {
                "id": f"gh-trending:{repo_path}",
                "repo": repo_path,
                "title": repo_path,
                "url": f"https://github.com/{repo_path}",
                "summary": desc,
                "published_at": "",
                "source": "GitHub Trending",
                "metrics": {
                    "stars_today": stars_today,
                    "engagement_score": stars_today * 5,
                },
                "raw": {"repo_path": repo_path, "stars_today": stars_today},
            }
        )
        if len(items) >= limit:
            break
    return items


def fetch_github_repo_search(query: str, limit: int) -> list[dict[str, Any]]:
    q = quote_plus(query)
    url = f"https://api.github.com/search/repositories?q={q}&sort=stars&order=desc&per_page={max(1, min(limit, 20))}"
    req = Request(
        url,
        headers={
            "User-Agent": "content-search-layer/1.0",
            "Accept": "application/vnd.github+json",
        },
    )
    with urlopen(req, timeout=15) as resp:  # noqa: S310
        payload = json.loads(resp.read().decode("utf-8", errors="ignore"))

    items: list[dict[str, Any]] = []
    for repo in (payload.get("items") or [])[:limit]:
        full_name = str(repo.get("full_name") or "").strip()
        if not full_name:
            continue
        items.append(
            {
                "id": f"gh-search:{full_name}",
                "repo": full_name,
                "title": full_name,
                "url": str(repo.get("html_url") or f"https://github.com/{full_name}"),
                "summary": str(repo.get("description") or "").strip(),
                "published_at": str(repo.get("updated_at") or "").strip(),
                "source": "GitHub Search API",
                "metrics": {
                    "stars": int(repo.get("stargazers_count") or 0),
                    "forks": int(repo.get("forks_count") or 0),
                    "watchers": int(repo.get("watchers_count") or 0),
                    "engagement_score": int(repo.get("stargazers_count") or 0) + int(repo.get("forks_count") or 0) * 2,
                },
                "raw": repo,
            }
        )
    return items


def score_github_candidate(item: dict[str, Any], query: str) -> tuple[float, str, str]:
    haystack = f"{item.get('title','')}\n{item.get('summary','')}".lower()
    q_tokens = [t.strip().lower() for t in re.split(r"\s+|\||OR|or", query) if t and len(t.strip()) > 1]
    match_count = sum(1 for t in q_tokens if t in haystack)

    base_score = float((item.get("metrics") or {}).get("engagement_score") or 0.0)
    source = str(item.get("source") or "")
    trend_bonus = 20.0 if source == "GitHub Trending" else 0.0
    score = base_score + match_count * 20.0 + trend_bonus

    if score >= 120:
        return score, "strong", "趋势热度高且与关键词相关，建议优先跟进。"
    if score >= 40:
        return score, "watch", "有趋势信号或关键词命中，建议观察。"
    return score, "skip", "相关性或热度一般，先放观察池。"


def normalize_github_item(item: dict[str, Any], bucket: str, why_pick: str, score: float) -> dict[str, Any]:
    metrics = item.get("metrics") or {}
    return {
        "id": item.get("id") or item.get("url") or item.get("title"),
        "url": item.get("url") or "",
        "title": item.get("title") or "",
        "channel": "github",
        "source": item.get("source") or "GitHub",
        "published_at": item.get("published_at") or "",
        "summary": (item.get("summary") or "")[:220],
        "why_pick": why_pick,
        "recommend_full_fetch": "yes" if bucket == "strong" else ("maybe" if bucket == "watch" else "no"),
        "bucket": bucket,
        "created_at": item.get("published_at") or "",
        "metrics": {
            "likes": int(metrics.get("stars") or metrics.get("stars_today") or 0),
            "retweets": int(metrics.get("forks") or 0),
            "replies": 0,
            "quotes": 0,
            "views": int(metrics.get("watchers") or 0),
            "engagement_score": round(float(score), 2),
        },
        "lang": "",
        "raw": item.get("raw") or item,
    }


def search_github_trending(params: dict[str, Any]) -> dict[str, Any]:
    query = (params.get("query") or "").strip()
    if not query:
        raise ValueError("query 不能为空")

    limit = int(params.get("limit_per_account") or 3)
    limit = max(1, min(limit, 20))
    lang = (params.get("lang") or "").strip()
    sort_by = (params.get("sort_by") or "hybrid").strip().lower()
    if sort_by not in {"time", "engagement", "hybrid"}:
        sort_by = "hybrid"
    query_scope = _normalize_query_scope(params.get("query_scope") or "all")

    searched = ["github-trending", "github-search-api"]
    success: list[str] = []
    errors: list[dict[str, str]] = []
    candidates: list[dict[str, Any]] = []

    try:
        candidates.extend(fetch_github_trending(language=lang, limit=limit))
        success.append("github-trending")
    except Exception as exc:  # noqa: BLE001
        errors.append({"handle": "github-trending", "error": str(exc)})

    try:
        candidates.extend(fetch_github_repo_search(query=query, limit=limit))
        success.append("github-search-api")
    except Exception as exc:  # noqa: BLE001
        errors.append({"handle": "github-search-api", "error": str(exc)})

    normalized: list[dict[str, Any]] = []
    for item in candidates:
        title_hit, content_hit = _query_scope_match(item.get("title") or "", item.get("summary") or "", query, query_scope)
        if not (title_hit or content_hit):
            continue
        score, bucket, why_pick = score_github_candidate(item, query)
        if score <= 0:
            continue
        normalized.append(normalize_github_item(item, bucket, why_pick, score))

    dedup: dict[str, dict[str, Any]] = {}
    for item in normalized:
        key = item.get("url") or item.get("id") or item.get("title")
        if key and key not in dedup:
            dedup[str(key)] = item

    results = _sort_results(list(dedup.values()), sort_by)
    grouped = {
        "strong": [r for r in results if r.get("bucket") == "strong"],
        "watch": [r for r in results if r.get("bucket") == "watch"],
        "skip": [r for r in results if r.get("bucket") == "skip"],
    }

    failed = [e.get("handle") for e in errors if e.get("handle")]
    return {
        "ok": True,
        "query": query,
        "mode": "github_trending",
        "count": len(results),
        "results": results,
        "grouped": grouped,
        "counts": {"strong": len(grouped["strong"]), "watch": len(grouped["watch"]), "skip": len(grouped["skip"])},
        "searched_handles": searched,
        "success_handles": success,
        "filters": {
            "lang": lang,
            "since": "",
            "until": "",
            "query_scope": query_scope,
            "min_likes": 0,
            "min_retweets": 0,
            "sort_by": sort_by,
        },
        "errors": errors,
        "error_count": len(errors),
        "observability": {
            "error_count": len(errors),
            "success_count": len(success),
            "searched_count": len(searched),
            "failed_handles": failed,
            "success_handles": success,
        },
    }


def resolve_twitter_bin() -> str:
    cfg = root_config()
    env_bin = str(os.environ.get("TWITTER_BIN") or ((cfg.get("external_tools") or {}).get("twitter_bin")) or "").strip()
    if env_bin and Path(env_bin).exists():
        return env_bin

    current_python_bin = Path(sys.executable).resolve().parent / "Scripts" / "twitter.exe"
    if current_python_bin.exists():
        return str(current_python_bin)

    cmd = which("twitter")
    if cmd:
        return cmd

    raise RuntimeError("未找到 twitter 命令，请安装 twitter-cli 或设置 TWITTER_BIN")


def run_twitter_command(cmd: list[str]) -> dict[str, Any]:
    cmd = [resolve_twitter_bin(), *cmd[1:]]
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=load_agent_reach_twitter_env(),
            timeout=TWITTER_CMD_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"twitter-cli 请求超时（>{TWITTER_CMD_TIMEOUT_SEC}s）：{' '.join(cmd[:4])} ...") from exc

    if completed.returncode != 0:
        err_text = (completed.stderr or completed.stdout).strip()
        try:
            err_payload = json.loads(err_text or "{}")
            if isinstance(err_payload, dict):
                err_obj = err_payload.get("error") or {}
                err_message = str(err_obj.get("message") or "").strip()
                if err_message:
                    raise RuntimeError(err_message)
        except json.JSONDecodeError:
            pass
        raise RuntimeError(err_text)

    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"twitter-cli 输出不是合法 JSON：{exc}") from exc

    if not payload.get("ok", False):
        error = payload.get("error", {})
        raise RuntimeError(error.get("message") or "twitter-cli 执行失败")

    return payload


def _parse_date_or_datetime(value: str, end_of_day: bool = False) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        if len(value) == 10 and value.count("-") == 2:
            base = datetime.fromisoformat(value)
            if end_of_day:
                return base.replace(hour=23, minute=59, second=59)
            return base.replace(hour=0, minute=0, second=0)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:  # noqa: BLE001
        return None


def _hit_time_window(created_at: str, since: str, until: str) -> bool:
    if not since and not until:
        return True
    created_dt = _parse_date_or_datetime(created_at)
    if not created_dt:
        return True
    since_dt = _parse_date_or_datetime(since) if since else None
    until_dt = _parse_date_or_datetime(until, end_of_day=True) if until else None
    if since_dt and created_dt < since_dt:
        return False
    if until_dt and created_dt > until_dt:
        return False
    return True


def _apply_quality_filters(items: list[dict[str, Any]], *, lang: str, since: str, until: str, min_likes: int, min_retweets: int) -> list[dict[str, Any]]:
    lang = (lang or "").strip().lower()
    filtered: list[dict[str, Any]] = []
    for item in items:
        item_lang = str(item.get("lang") or "").strip().lower()
        likes = int((item.get("metrics") or {}).get("likes") or 0)
        retweets = int((item.get("metrics") or {}).get("retweets") or 0)
        created_at = str(item.get("created_at") or "")

        if lang and item_lang and item_lang != lang:
            continue
        if likes < min_likes:
            continue
        if retweets < min_retweets:
            continue
        if not _hit_time_window(created_at, since, until):
            continue
        filtered.append(item)
    return filtered


def _sort_results(results: list[dict[str, Any]], sort_by: str) -> list[dict[str, Any]]:
    sort_by = (sort_by or "time").strip().lower()
    now = datetime.now()

    def time_score(item: dict[str, Any]) -> float:
        v = str(item.get("created_at") or "")
        dt = _parse_date_or_datetime(v)
        return dt.timestamp() if dt else 0.0

    def engagement_score(item: dict[str, Any]) -> float:
        return float((item.get("metrics") or {}).get("engagement_score") or 0.0)

    def hybrid_score(item: dict[str, Any]) -> float:
        base = engagement_score(item)
        dt = _parse_date_or_datetime(str(item.get("created_at") or ""))
        if not dt:
            return base
        age_hours = max(0.0, (now - dt).total_seconds() / 3600.0)
        recency_bonus = max(0.0, 72.0 - age_hours) / 72.0 * 50.0
        return base + recency_bonus

    if sort_by == "engagement":
        return sorted(results, key=engagement_score, reverse=True)
    if sort_by == "hybrid":
        return sorted(results, key=hybrid_score, reverse=True)
    return sorted(results, key=time_score, reverse=True)


def _normalize_db_datetime_columns(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    updated = 0
    tasks = [
        ("raw_items", "id", ["published_at", "fetched_at"]),
        ("alert_queue", "id", ["published_at", "fetched_at", "notified_at", "created_at", "updated_at"]),
    ]

    for table, pk, cols in tasks:
        try:
            rows = cur.execute(f"SELECT {pk}, {', '.join(cols)} FROM {table}").fetchall()
        except sqlite3.OperationalError:
            continue
        for row in rows:
            row_id = row[0]
            changed_pairs: list[tuple[str, str]] = []
            for idx, col in enumerate(cols, start=1):
                old = str(row[idx] or "").strip()
                if not old:
                    continue
                new = _format_datetime_text(old)
                if new and new != old:
                    changed_pairs.append((col, new))
            if not changed_pairs:
                continue
            set_sql = ", ".join([f"{name}=?" for name, _ in changed_pairs])
            values = [value for _, value in changed_pairs]
            values.append(row_id)
            cur.execute(f"UPDATE {table} SET {set_sql} WHERE {pk}=?", values)
            updated += 1

    if updated > 0:
        conn.commit()
    return updated


@lru_cache(maxsize=1)
def ensure_event_raw_items_table() -> None:
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              platform TEXT NOT NULL,
              source_handle TEXT,
              item_id TEXT,
              title TEXT,
              content TEXT,
              url TEXT,
              published_at TEXT,
              metrics_json TEXT,
              fetched_at TEXT DEFAULT (datetime('now')),
              UNIQUE(platform, item_id)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_items_platform_published_fetched ON raw_items(platform, published_at DESC, fetched_at DESC, id DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_items_platform_handle_published_fetched ON raw_items(platform, source_handle, published_at DESC, fetched_at DESC, id DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_items_fetched_at ON raw_items(fetched_at DESC)")
        conn.commit()
        _normalize_db_datetime_columns(conn)
    finally:
        conn.close()


def _normalize_bucket_from_score(engagement_score: float) -> tuple[str, str, str]:
    if engagement_score >= 120:
        return "strong", "yes", "互动显著且与关键词相关，优先深挖。"
    if engagement_score >= 20:
        return "watch", "maybe", "有一定讨论热度，建议跟踪后再决定。"
    return "skip", "no", "互动较低，先作为背景信息观察。"


def search_wechat_local_cache(params: dict[str, Any]) -> dict[str, Any]:
    query = (params.get("query") or "").strip()
    if not query:
        raise ValueError("query 不能为空")

    since = (params.get("since") or "").strip()
    until = (params.get("until") or "").strip()
    sort_by = (params.get("sort_by") or "time").strip().lower()
    if sort_by not in {"time", "engagement", "hybrid"}:
        sort_by = "time"
    query_scope = _normalize_query_scope(params.get("query_scope") or "all")

    limit_per_account = int(params.get("limit_per_account") or 3)
    limit_per_account = max(1, min(limit_per_account, 20))

    enabled_sources = load_wechat_sources(enabled_only=True)
    searched_handles = [str(s.get("id") or "").strip() for s in enabled_sources if str(s.get("id") or "").strip()]
    source_name_map = {str(s.get("id") or "").strip(): str(s.get("name") or "").strip() for s in enabled_sources}

    if not searched_handles:
        return {
            "ok": True,
            "query": query,
            "mode": "wechat_search",
            "count": 0,
            "results": [],
            "grouped": {"strong": [], "watch": [], "skip": []},
            "counts": {"strong": 0, "watch": 0, "skip": 0},
            "searched_handles": [],
            "success_handles": [],
            "filters": {
                "lang": "",
                "since": since,
                "until": until,
                "min_likes": 0,
                "min_retweets": 0,
                "sort_by": sort_by,
            },
            "errors": [],
            "error_count": 0,
            "observability": {
                "error_count": 0,
                "success_count": 0,
                "searched_count": 0,
                "failed_handles": [],
                "success_handles": [],
            },
            "message": "当前没有 enabled=true 的微信公众号来源。",
        }

    ensure_event_raw_items_table()
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        placeholders = ",".join(["?"] * len(searched_handles))
        sql = [
            "SELECT source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at",
            "FROM raw_items",
            "WHERE platform='wechat'",
            f"AND {_query_scope_sql(query_scope)}",
            f"AND source_handle IN ({placeholders})",
            "ORDER BY COALESCE(published_at, fetched_at) DESC, fetched_at DESC",
            "LIMIT 2000",
        ]
        args: list[Any] = [f"%{query.lower()}%", *searched_handles]
        rows = cur.execute("\n".join(sql), args).fetchall()
    finally:
        conn.close()

    per_source_count: dict[str, int] = {}
    results: list[dict[str, Any]] = []

    q_lower = query.lower()
    for row in rows:
        source_handle = str(row["source_handle"] or "").strip()
        if not source_handle:
            continue

        current = per_source_count.get(source_handle, 0)
        if current >= limit_per_account:
            continue

        published_at = str(row["published_at"] or "")
        if not _hit_time_window(published_at, since, until):
            continue

        title = str(row["title"] or "").strip()
        content = str(row["content"] or "").strip()
        summary = (content or title).replace("\n", " ").strip()
        summary = summary[:160] + ("..." if len(summary) > 160 else "")

        metrics: dict[str, Any]
        try:
            metrics = json.loads(row["metrics_json"] or "{}") or {}
        except Exception:  # noqa: BLE001
            metrics = {}

        title_hit, content_hit = _query_scope_match(title, content, query, query_scope)
        body_ok = bool(metrics.get("body_fetch_ok"))
        source_kind = str(metrics.get("source_kind") or "")

        score = 0.0
        if title_hit:
            score += 6
        if content_hit:
            score += 3
        if body_ok:
            score += 2
        if source_kind in {"official_feed", "official_atom"}:
            score += 2
        if len(content) >= 800:
            score += 1

        if score >= 8:
            bucket = "strong"
            recommend_full_fetch = "yes"
            why_pick = "关键词命中强，且来源稳定，建议优先处理。"
        elif score >= 4:
            bucket = "watch"
            recommend_full_fetch = "maybe"
            why_pick = "关键词有命中，建议放入观察。"
        else:
            bucket = "skip"
            recommend_full_fetch = "no"
            why_pick = "命中较弱，先作为背景信息。"

        item_id = str(row["item_id"] or "").strip()
        url = str(row["url"] or "").strip()
        if not item_id:
            item_id = hashlib.md5(f"wechat|{source_handle}|{published_at}|{title}".encode("utf-8")).hexdigest()  # noqa: S324

        source_name = source_name_map.get(source_handle) or source_handle
        results.append(
            {
                "id": item_id,
                "url": url,
                "title": title or summary or f"微信内容：{source_name}",
                "channel": "wechat",
                "source": source_name,
                "published_at": published_at,
                "summary": summary,
                "why_pick": why_pick,
                "recommend_full_fetch": recommend_full_fetch,
                "bucket": bucket,
                "author": source_handle,
                "author_name": source_name,
                "matched_handle": source_handle,
                "created_at": published_at,
                "metrics": {
                    "likes": 0,
                    "retweets": 0,
                    "replies": 0,
                    "quotes": 0,
                    "views": 0,
                    "engagement_score": round(score, 2),
                },
                "text": content,
                "lang": "",
                "raw": {
                    "source_handle": source_handle,
                    "item_id": item_id,
                    "source_kind": source_kind,
                    "entry_url": str(metrics.get("entry_url") or ""),
                    "body_fetch_ok": body_ok,
                },
            }
        )
        per_source_count[source_handle] = current + 1

    dedup: dict[str, dict[str, Any]] = {}
    for item in results:
        key = item.get("id") or item.get("url") or f"{item.get('author')}::{item.get('created_at')}::{item.get('title')}"
        if key not in dedup:
            dedup[str(key)] = item

    results = _sort_results(list(dedup.values()), sort_by)

    grouped = {
        "strong": [r for r in results if r.get("bucket") == "strong"],
        "watch": [r for r in results if r.get("bucket") == "watch"],
        "skip": [r for r in results if r.get("bucket") == "skip"],
    }

    success_handles = sorted({str(r.get("matched_handle") or "").strip() for r in results if str(r.get("matched_handle") or "").strip()})
    return {
        "ok": True,
        "query": query,
        "mode": "wechat_search",
        "count": len(results),
        "results": results,
        "grouped": grouped,
        "counts": {
            "strong": len(grouped["strong"]),
            "watch": len(grouped["watch"]),
            "skip": len(grouped["skip"]),
        },
        "searched_handles": searched_handles,
        "success_handles": success_handles,
        "filters": {
            "lang": "",
            "since": since,
            "until": until,
            "query_scope": query_scope,
            "min_likes": 0,
            "min_retweets": 0,
            "sort_by": sort_by,
        },
        "errors": [],
        "error_count": 0,
        "observability": {
            "error_count": 0,
            "success_count": len(success_handles),
            "searched_count": len(searched_handles),
            "failed_handles": [h for h in searched_handles if h not in success_handles],
            "success_handles": success_handles,
        },
    }


def load_bilibili_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    if BILIBILI_SOURCES_PATH.exists():
        try:
            data = json.loads(BILIBILI_SOURCES_PATH.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            data = {}

        raw_sources = (data.get("sources") or data.get("channels") or data.get("accounts") or [])
        out: list[dict[str, Any]] = []
        for source in raw_sources:
            item = dict(source or {})
            item["id"] = str(item.get("id") or item.get("name") or item.get("uid") or "").strip()
            item["name"] = str(item.get("name") or item.get("id") or item.get("uid") or "").strip()
            item["enabled"] = bool(item.get("enabled", True))
            item["priority"] = int(item.get("priority") or 5)
            item["uid"] = str(item.get("uid") or "").strip()
            item["space_url"] = str(item.get("space_url") or item.get("url") or item.get("homepage") or "").strip()
            item["url"] = item["space_url"]
            item["fetch_method"] = str(item.get("fetch_method") or "").strip()
            item["category"] = str(item.get("category") or "").strip()
            item["notes"] = str(item.get("notes") or "").strip()
            item["description"] = str(item.get("description") or item.get("notes") or "").strip()
            if item["id"] and item["name"]:
                out.append(item)

        if enabled_only:
            out = [s for s in out if s.get("enabled")]
        if out:
            return out

    ensure_event_accounts_table()
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        out: list[dict[str, Any]] = []
        try:
            account_rows = cur.execute(
                """
                SELECT handle, enabled, priority
                FROM accounts
                WHERE platform='bilibili'
                ORDER BY enabled DESC, priority DESC, id ASC
                """
            ).fetchall()
        except sqlite3.OperationalError:
            account_rows = []

        for row in account_rows:
            handle = str(row["handle"] or "").strip()
            if not handle:
                continue
            enabled = bool(row["enabled"])
            if enabled_only and not enabled:
                continue
            out.append(
                {
                    "id": handle,
                    "name": handle,
                    "enabled": enabled,
                    "priority": int(row["priority"] or 5),
                    "uid": "",
                    "space_url": "",
                    "url": "",
                    "fetch_method": "event-db-account",
                    "description": "来自 NightHawk accounts 表",
                }
            )

        if out:
            return out

        try:
            raw_rows = cur.execute(
                """
                SELECT source_handle, COUNT(*) AS item_count, MAX(COALESCE(published_at, fetched_at)) AS latest_at
                FROM raw_items
                WHERE platform='bilibili' AND TRIM(COALESCE(source_handle, '')) != ''
                GROUP BY source_handle
                ORDER BY latest_at DESC, item_count DESC, source_handle ASC
                """
            ).fetchall()
        except sqlite3.OperationalError:
            raw_rows = []

        for row in raw_rows:
            handle = str(row["source_handle"] or "").strip()
            if not handle:
                continue
            out.append(
                {
                    "id": handle,
                    "name": handle,
                    "enabled": True,
                    "priority": 5,
                    "uid": "",
                    "space_url": "",
                    "url": "",
                    "fetch_method": "raw-items-fallback",
                    "description": "来自 raw_items 回退来源",
                    "item_count": int(row["item_count"] or 0),
                    "latest_at": str(row["latest_at"] or ""),
                }
            )
        return out
    finally:
        conn.close()


def _search_video_local_cache(
    *,
    params: dict[str, Any],
    platform: str,
    mode: str,
    empty_message: str,
    display_name: str,
) -> dict[str, Any]:
    query = (params.get("query") or "").strip()
    if not query:
        raise ValueError("query 不能为空")

    since = (params.get("since") or "").strip()
    until = (params.get("until") or "").strip()
    sort_by = (params.get("sort_by") or "time").strip().lower()
    if sort_by not in {"time", "engagement", "hybrid"}:
        sort_by = "time"
    query_scope = _normalize_query_scope(params.get("query_scope") or "all")

    limit_per_account = int(params.get("limit_per_account") or 3)
    limit_per_account = max(1, min(limit_per_account, 20))

    if platform == "youtube":
        enabled_sources = load_youtube_channels(enabled_only=True)
    elif platform == "bilibili":
        enabled_sources = load_bilibili_sources(enabled_only=True)
    else:
        raise ValueError(f"不支持的视频平台：{platform}")

    searched_handles = [str(s.get("id") or "").strip() for s in enabled_sources if str(s.get("id") or "").strip()]
    source_name_map = {str(s.get("id") or "").strip(): str(s.get("name") or s.get("id") or "").strip() for s in enabled_sources}

    searched_aliases: list[str] = []
    for s in enabled_sources:
        sid = str(s.get("id") or "").strip()
        sname = str(s.get("name") or "").strip()
        if sid:
            searched_aliases.append(sid)
        if sname:
            searched_aliases.append(sname)
    searched_aliases = sorted({v for v in searched_aliases if v})

    if not searched_aliases:
        return {
            "ok": True,
            "query": query,
            "mode": mode,
            "count": 0,
            "results": [],
            "grouped": {"strong": [], "watch": [], "skip": []},
            "counts": {"strong": 0, "watch": 0, "skip": 0},
            "searched_handles": [],
            "success_handles": [],
            "filters": {
                "lang": "",
                "since": since,
                "until": until,
                "query_scope": query_scope,
                "min_likes": 0,
                "min_retweets": 0,
                "sort_by": sort_by,
            },
            "errors": [],
            "error_count": 0,
            "observability": {
                "error_count": 0,
                "success_count": 0,
                "searched_count": 0,
                "failed_handles": [],
                "success_handles": [],
            },
            "message": empty_message,
        }

    ensure_event_raw_items_table()
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        sql = [
            "SELECT source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at",
            "FROM raw_items",
            f"WHERE platform='{platform}'",
            f"AND {_query_scope_sql(query_scope)}",
            f"AND lower(COALESCE(source_handle,'')) IN ({','.join(['lower(?)'] * len(searched_aliases))})",
            "ORDER BY COALESCE(published_at, fetched_at) DESC, fetched_at DESC",
            "LIMIT 3000",
        ]
        args: list[Any] = [f"%{query.lower()}%", *searched_aliases]
        rows = cur.execute("\n".join(sql), args).fetchall()
    finally:
        conn.close()

    alias_to_id: dict[str, str] = {}
    for s in enabled_sources:
        sid = str(s.get("id") or "").strip()
        sname = str(s.get("name") or "").strip()
        if sid:
            alias_to_id[sid.lower()] = sid
        if sname and sid:
            alias_to_id[sname.lower()] = sid

    per_source_count: dict[str, int] = {}
    results: list[dict[str, Any]] = []
    q_lower = query.lower()

    for row in rows:
        source_handle = str(row["source_handle"] or "").strip()
        if not source_handle:
            continue

        source_id = alias_to_id.get(source_handle.lower(), source_handle)
        current = per_source_count.get(source_id, 0)
        if current >= limit_per_account:
            continue

        published_at = str(row["published_at"] or "")
        if not _hit_time_window(published_at, since, until):
            continue

        title = str(row["title"] or "").strip()
        content = str(row["content"] or "").strip()
        summary = (content or title).replace("\n", " ").strip()
        summary = summary[:220] + ("..." if len(summary) > 220 else "")

        metrics: dict[str, Any]
        try:
            metrics = json.loads(row["metrics_json"] or "{}") or {}
        except Exception:  # noqa: BLE001
            metrics = {}

        recommendation = str(metrics.get("recommendation") or "").strip().lower()
        confidence = str(metrics.get("confidence") or "").strip().lower()
        nighthawk_action = str(metrics.get("nighthawk_action") or "").strip().lower()
        route_bucket = str(metrics.get("route_bucket") or "").strip()
        transcript_language = str(metrics.get("transcript_language") or "").strip()
        review_needed = bool(metrics.get("review_needed"))

        title_hit, content_hit = _query_scope_match(title, content, query, query_scope)

        score = 0.0
        if title_hit:
            score += 4
        if content_hit:
            score += 3

        if recommendation == "strong":
            score += 4
        elif recommendation == "watch":
            score += 2

        if confidence == "high":
            score += 2
        elif confidence == "medium":
            score += 1

        if review_needed:
            score -= 0.5

        if recommendation == "strong" or nighthawk_action == "pending":
            bucket = "strong"
            recommend_full_fetch = "yes"
            why_pick = f"{display_name} 分析卡判定为强推荐，建议优先处理。"
        elif recommendation == "watch" or nighthawk_action in {"candidate", "watch"}:
            bucket = "watch"
            recommend_full_fetch = "maybe"
            why_pick = f"{display_name} 分析卡判定为可观察，建议纳入候选池。"
        elif recommendation == "skip" or nighthawk_action == "skipped":
            bucket = "skip"
            recommend_full_fetch = "no"
            why_pick = f"{display_name} 分析卡判定为略过，暂不优先。"
        elif score >= 7:
            bucket = "strong"
            recommend_full_fetch = "yes"
            why_pick = "关键词命中强，且分析层信号较高。"
        elif score >= 4:
            bucket = "watch"
            recommend_full_fetch = "maybe"
            why_pick = "关键词有命中，可先进入观察。"
        else:
            bucket = "skip"
            recommend_full_fetch = "no"
            why_pick = "命中较弱，先作为背景信息。"

        item_id = str(row["item_id"] or "").strip()
        url = str(row["url"] or "").strip()
        if not item_id:
            item_id = hashlib.md5(f"{platform}|{source_handle}|{published_at}|{title}".encode("utf-8")).hexdigest()  # noqa: S324

        source_name = source_name_map.get(source_id) or source_handle
        results.append(
            {
                "id": item_id,
                "url": url,
                "title": title or summary or f"{display_name} 内容：{source_name}",
                "channel": platform,
                "source": source_name,
                "published_at": published_at,
                "summary": summary,
                "why_pick": why_pick,
                "recommend_full_fetch": recommend_full_fetch,
                "bucket": bucket,
                "author": source_id,
                "author_name": source_name,
                "matched_handle": source_id,
                "created_at": published_at,
                "metrics": {
                    "likes": 0,
                    "retweets": 0,
                    "replies": 0,
                    "quotes": 0,
                    "views": 0,
                    "engagement_score": round(score, 2),
                },
                "text": content,
                "lang": transcript_language,
                "raw": {
                    "source_handle": source_handle,
                    "item_id": item_id,
                    "recommendation": recommendation,
                    "confidence": confidence,
                    "nighthawk_action": nighthawk_action,
                    "route_bucket": route_bucket,
                    "transcript_language": transcript_language,
                    "review_needed": review_needed,
                },
            }
        )
        per_source_count[source_id] = current + 1

    dedup: dict[str, dict[str, Any]] = {}
    for item in results:
        key = item.get("id") or item.get("url") or f"{item.get('author')}::{item.get('created_at')}::{item.get('title')}"
        if key not in dedup:
            dedup[str(key)] = item

    results = _sort_results(list(dedup.values()), sort_by)

    grouped = {
        "strong": [r for r in results if r.get("bucket") == "strong"],
        "watch": [r for r in results if r.get("bucket") == "watch"],
        "skip": [r for r in results if r.get("bucket") == "skip"],
    }

    success_handles = sorted({str(r.get("matched_handle") or "").strip() for r in results if str(r.get("matched_handle") or "").strip()})
    return {
        "ok": True,
        "query": query,
        "mode": mode,
        "count": len(results),
        "results": results,
        "grouped": grouped,
        "counts": {
            "strong": len(grouped["strong"]),
            "watch": len(grouped["watch"]),
            "skip": len(grouped["skip"]),
        },
        "searched_handles": searched_handles,
        "success_handles": success_handles,
        "filters": {
            "lang": "",
            "since": since,
            "until": until,
            "query_scope": query_scope,
            "min_likes": 0,
            "min_retweets": 0,
            "sort_by": sort_by,
        },
        "errors": [],
        "error_count": 0,
        "observability": {
            "error_count": 0,
            "success_count": len(success_handles),
            "searched_count": len(searched_handles),
            "failed_handles": [h for h in searched_handles if h not in success_handles],
            "success_handles": success_handles,
        },
    }


def search_youtube_local_cache(params: dict[str, Any]) -> dict[str, Any]:
    return _search_video_local_cache(
        params=params,
        platform="youtube",
        mode="youtube_search",
        empty_message="当前没有 enabled=true 的 YouTube 频道来源。",
        display_name="YouTube",
    )


def search_bilibili_local_cache(params: dict[str, Any]) -> dict[str, Any]:
    return _search_video_local_cache(
        params=params,
        platform="bilibili",
        mode="bilibili_search",
        empty_message="当前没有可用的 B站 来源；若尚未建监控账号，会自动回退到 raw_items 中已有的 B站 source_handle。",
        display_name="B站",
    )


def search_x_local_cache(params: dict[str, Any]) -> dict[str, Any]:
    query = (params.get("query") or "").strip()
    if not query:
        raise ValueError("query 不能为空")

    mode = params.get("mode") or "pool_latest"
    since = (params.get("since") or "").strip()
    until = (params.get("until") or "").strip()
    lang = (params.get("lang") or "").strip().lower()
    min_likes = max(0, int(params.get("min_likes") or 0))
    min_retweets = max(0, int(params.get("min_retweets") or 0))
    query_scope = _normalize_query_scope(params.get("query_scope") or "all")
    sort_by = (params.get("sort_by") or "time").strip().lower()
    if sort_by not in {"time", "engagement", "hybrid"}:
        sort_by = "time"

    limit_per_account = int(params.get("limit_per_account") or 3)
    limit_per_account = max(1, min(limit_per_account, 20))

    enabled_accounts = get_accounts(enabled_only=True)
    monitored_handles = [str(a.get("handle") or "").strip() for a in enabled_accounts if str(a.get("handle") or "").strip()]

    if mode == "source_recent":
        target = (params.get("target_handle") or "").strip()
        if not target:
            raise ValueError("source_recent 模式需要 target_handle")
        searched_handles = [target]
    else:
        searched_handles = monitored_handles

    ensure_event_raw_items_table()
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        sql = [
            "SELECT source_handle, item_id, title, content, url, published_at, metrics_json, fetched_at",
            "FROM raw_items",
            "WHERE platform='x'",
            f"AND {_query_scope_sql(query_scope)}",
        ]
        args: list[Any] = [f"%{query.lower()}%"]

        if mode == "source_recent":
            sql.append("AND lower(COALESCE(source_handle,'')) = ?")
            args.append(searched_handles[0].lower())

        sql.append("ORDER BY COALESCE(published_at, fetched_at) DESC, fetched_at DESC")
        # 预留更大窗口，再在 Python 层按过滤条件二次筛选
        sql.append("LIMIT 1200")

        rows = cur.execute("\n".join(sql), args).fetchall()
    finally:
        conn.close()

    results: list[dict[str, Any]] = []
    for row in rows:
        source_handle = str(row["source_handle"] or "").strip()
        if mode != "source_recent" and searched_handles and source_handle and source_handle not in searched_handles:
            continue

        metrics: dict[str, Any]
        try:
            metrics = json.loads(row["metrics_json"] or "{}") or {}
        except Exception:  # noqa: BLE001
            metrics = {}

        likes = int(metrics.get("likes") or 0)
        retweets = int(metrics.get("retweets") or 0)
        replies = int(metrics.get("replies") or 0)
        quotes = int(metrics.get("quotes") or 0)
        views = int(metrics.get("views") or 0)

        if likes < min_likes or retweets < min_retweets:
            continue

        published_at = str(row["published_at"] or "")
        if not _hit_time_window(published_at, since, until):
            continue

        content = str(row["content"] or "")
        title = str(row["title"] or "")
        row_lang = str(metrics.get("lang") or "").strip().lower()
        if lang and row_lang and row_lang != lang:
            continue

        engagement_score = float(metrics.get("engagement_score") or (likes * 1.0 + retweets * 2.0 + quotes * 2.0 + replies * 0.8))
        bucket, recommend_full_fetch, why_pick = _normalize_bucket_from_score(engagement_score)

        item_id = str(row["item_id"] or "").strip()
        url = str(row["url"] or "").strip()
        if not item_id:
            item_id = hashlib.md5(f"{source_handle}|{published_at}|{content[:200]}".encode("utf-8")).hexdigest()  # noqa: S324

        summary = (content or title).replace("\n", " ").strip()
        summary = summary[:160] + ("..." if len(summary) > 160 else "")

        results.append(
            {
                "id": item_id,
                "url": url,
                "title": title or summary or f"X post by @{source_handle}",
                "channel": "x",
                "source": f"@{source_handle}" if source_handle else "x",
                "published_at": published_at,
                "summary": summary,
                "why_pick": why_pick,
                "recommend_full_fetch": recommend_full_fetch,
                "bucket": bucket,
                "author": source_handle,
                "author_name": "",
                "matched_handle": source_handle,
                "created_at": published_at,
                "metrics": {
                    "likes": likes,
                    "retweets": retweets,
                    "replies": replies,
                    "quotes": quotes,
                    "views": views,
                    "engagement_score": round(engagement_score, 2),
                },
                "text": content,
                "lang": row_lang,
                "raw": {
                    "source_handle": source_handle,
                    "item_id": item_id,
                },
            }
        )

    dedup: dict[str, dict[str, Any]] = {}
    for item in results:
        key = item.get("id") or item.get("url") or f"{item.get('author')}::{item.get('created_at')}::{item.get('text')[:80]}"
        if key not in dedup:
            dedup[str(key)] = item

    results = _sort_results(list(dedup.values()), sort_by)
    # 按账号数粗限，避免前端一次返回过大
    limit_total = max(60, len(searched_handles) * limit_per_account)
    results = results[:limit_total]

    grouped = {
        "strong": [r for r in results if r.get("bucket") == "strong"],
        "watch": [r for r in results if r.get("bucket") == "watch"],
        "skip": [r for r in results if r.get("bucket") == "skip"],
    }

    success_handles = sorted({str(r.get("matched_handle") or "").strip() for r in results if str(r.get("matched_handle") or "").strip()})

    response = {
        "ok": True,
        "query": query,
        "mode": mode,
        "count": len(results),
        "results": results,
        "grouped": grouped,
        "counts": {
            "strong": len(grouped["strong"]),
            "watch": len(grouped["watch"]),
            "skip": len(grouped["skip"]),
        },
        "searched_handles": searched_handles,
        "success_handles": success_handles,
        "filters": {
            "lang": lang,
            "since": since,
            "until": until,
            "query_scope": query_scope,
            "min_likes": min_likes,
            "min_retweets": min_retweets,
            "sort_by": sort_by,
        },
        "errors": [],
        "error_count": 0,
        "observability": {
            "error_count": 0,
            "success_count": len(success_handles),
            "searched_count": len(searched_handles),
            "failed_handles": [h for h in searched_handles if h not in success_handles],
            "success_handles": success_handles,
        },
    }

    if not results:
        response["message"] = "本地缓存暂无命中，请先运行采集任务后再搜索。"

    return response


def normalize_item(item: dict[str, Any], matched_handle: str) -> dict[str, Any]:
    author = item.get("author") or {}
    metrics = item.get("metrics") or {}
    screen_name = author.get("screenName") or author.get("username") or "unknown"
    tweet_id = item.get("id")
    url = f"https://x.com/{screen_name}/status/{tweet_id}" if tweet_id else ""
    text = (item.get("text") or "").replace("\n", " ").strip()

    likes = metrics.get("likes") or 0
    retweets = metrics.get("retweets") or 0
    replies = metrics.get("replies") or 0
    quotes = metrics.get("quotes") or 0
    views = metrics.get("views") or 0
    engagement_score = likes * 1.0 + retweets * 2.0 + quotes * 2.0 + replies * 0.8

    if engagement_score >= 120:
        bucket = "strong"
        recommend_full_fetch = "yes"
        why_pick = "互动显著且与关键词相关，优先深挖。"
    elif engagement_score >= 20:
        bucket = "watch"
        recommend_full_fetch = "maybe"
        why_pick = "有一定讨论热度，建议跟踪后再决定。"
    else:
        bucket = "skip"
        recommend_full_fetch = "no"
        why_pick = "互动较低，先作为背景信息观察。"

    summary = text[:160] + ("..." if len(text) > 160 else "")

    return {
        "id": tweet_id,
        "url": url,
        "title": summary or f"X post by @{screen_name}",
        "channel": "x",
        "source": f"@{screen_name}",
        "published_at": item.get("createdAtISO") or item.get("createdAtLocal") or item.get("createdAt") or "",
        "summary": summary,
        "why_pick": why_pick,
        "recommend_full_fetch": recommend_full_fetch,
        "bucket": bucket,
        "author": screen_name,
        "author_name": author.get("name") or "",
        "matched_handle": matched_handle,
        "created_at": item.get("createdAtISO") or item.get("createdAtLocal") or item.get("createdAt") or "",
        "metrics": {
            "likes": likes,
            "retweets": retweets,
            "replies": replies,
            "quotes": quotes,
            "views": views,
            "engagement_score": round(engagement_score, 2),
        },
        "text": text,
        "lang": (item.get("lang") or "").lower(),
        "raw": item,
    }


def search_x_monitored_accounts(params: dict[str, Any]) -> dict[str, Any]:
    query = (params.get("query") or "").strip()
    if not query:
        raise ValueError("query 不能为空")

    mode = params.get("mode") or "pool_latest"
    search_type = params.get("search_type") or "latest"
    since = (params.get("since") or "").strip()
    until = (params.get("until") or "").strip()
    lang = (params.get("lang") or "").strip()
    min_likes = int(params.get("min_likes") or 0)
    min_retweets = int(params.get("min_retweets") or 0)
    sort_by = (params.get("sort_by") or "time").strip().lower()
    if sort_by not in {"time", "engagement", "hybrid"}:
        sort_by = "time"
    min_likes = max(0, min_likes)
    min_retweets = max(0, min_retweets)
    limit_per_account = int(params.get("limit_per_account") or 3)
    limit_per_account = max(1, min(limit_per_account, 20))

    enabled_accounts = get_accounts(enabled_only=True)
    if not enabled_accounts:
        return {
            "ok": True,
            "query": query,
            "mode": mode,
            "count": 0,
            "results": [],
            "message": "当前没有 enabled=true 的监控账号。",
        }

    all_items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    if mode == "source_recent":
        target = (params.get("target_handle") or "").strip()
        if not target:
            raise ValueError("source_recent 模式需要 target_handle")

        cmd = ["twitter", "user-posts", target, "-n", str(limit_per_account), "--json"]
        try:
            payload = run_twitter_command(cmd)
            items = payload.get("data", []) or []
            for item in items:
                text = (item.get("text") or "").lower()
                if query.lower() in text:
                    all_items.append(normalize_item(item, target))
        except Exception as exc:  # noqa: BLE001
            errors.append({"handle": target, "error": str(exc)})
    else:
        def fetch_one_account(handle: str) -> tuple[str, list[dict[str, Any]], str | None]:
            cmd = [
                "twitter",
                "search",
                query,
                "--json",
                "-n",
                str(limit_per_account),
                "--type",
                search_type,
                "--from",
                handle,
            ]
            if since:
                cmd += ["--since", since]
            if until:
                cmd += ["--until", until]
            if lang:
                cmd += ["--lang", lang]

            try:
                payload = run_twitter_command(cmd)
                items = payload.get("data", []) or []
                normalized = [normalize_item(item, handle) for item in items]
                return handle, normalized, None
            except Exception as exc:  # noqa: BLE001
                return handle, [], str(exc)

        handles = [str(a.get("handle") or "").strip() for a in enabled_accounts]
        handles = [h for h in handles if h]
        max_workers = max(1, min(6, len(handles)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_one_account, h): h for h in handles}
            for future in as_completed(futures):
                handle, items, err = future.result()
                if err:
                    errors.append({"handle": handle, "error": err})
                    continue
                all_items.extend(items)

    all_items = _apply_quality_filters(
        all_items,
        lang=lang,
        since=since,
        until=until,
        min_likes=min_likes,
        min_retweets=min_retweets,
    )

    dedup: dict[str, dict[str, Any]] = {}
    for item in all_items:
        key = item.get("id") or f"{item.get('author')}::{item.get('created_at')}::{item.get('text')[:80]}"
        if key not in dedup:
            dedup[key] = item

    results = list(dedup.values())
    results = _sort_results(results, sort_by)

    grouped = {
        "strong": [r for r in results if r.get("bucket") == "strong"],
        "watch": [r for r in results if r.get("bucket") == "watch"],
        "skip": [r for r in results if r.get("bucket") == "skip"],
    }

    searched_handles = [a.get("handle") for a in enabled_accounts]
    error_handles = [e.get("handle") for e in errors if e.get("handle")]
    success_handles = [h for h in searched_handles if h not in error_handles]
    if mode == "source_recent":
        target = (params.get("target_handle") or "").strip()
        searched_handles = [target] if target else []
        error_handles = [e.get("handle") for e in errors if e.get("handle")]
        success_handles = [h for h in searched_handles if h not in error_handles]

    return {
        "ok": True,
        "query": query,
        "mode": mode,
        "count": len(results),
        "results": results,
        "grouped": grouped,
        "counts": {
            "strong": len(grouped["strong"]),
            "watch": len(grouped["watch"]),
            "skip": len(grouped["skip"]),
        },
        "searched_handles": searched_handles,
        "success_handles": success_handles,
        "filters": {
            "lang": lang,
            "since": since,
            "until": until,
            "min_likes": min_likes,
            "min_retweets": min_retweets,
            "sort_by": sort_by,
        },
        "errors": errors,
        "error_count": len(errors),
        "observability": {
            "error_count": len(errors),
            "success_count": len(success_handles),
            "searched_count": len(searched_handles),
            "failed_handles": error_handles,
            "success_handles": success_handles,
        },
    }


class SearchProvider(ABC):
    """统一搜索 provider 抽象：不同来源输出同一候选结构。"""

    @property
    @abstractmethod
    def provider_id(self) -> str:
        raise NotImplementedError

    @property
    def display_name(self) -> str:
        return self.provider_id

    @abstractmethod
    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class XSearchProvider(SearchProvider):
    @property
    def provider_id(self) -> str:
        return "x"

    @property
    def display_name(self) -> str:
        return "X / Twitter"

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        # 默认从本地缓存搜索；仅在显式传入 fetch_live=true 时走在线拉取。
        if bool(params.get("fetch_live")):
            result = search_x_monitored_accounts(params)
        else:
            result = search_x_local_cache(params)
        result["provider"] = self.provider_id
        return result


class FeedSearchProvider(SearchProvider):
    @property
    def provider_id(self) -> str:
        return "feed"

    @property
    def display_name(self) -> str:
        return "Feed / 官方博客"

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        result = search_feed_sources(params)
        result["provider"] = self.provider_id
        return result


class GitHubSearchProvider(SearchProvider):
    @property
    def provider_id(self) -> str:
        return "github"

    @property
    def display_name(self) -> str:
        return "GitHub Trending"

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        result = search_github_trending(params)
        result["provider"] = self.provider_id
        return result


class WeChatSearchProvider(SearchProvider):
    @property
    def provider_id(self) -> str:
        return "wechat"

    @property
    def display_name(self) -> str:
        return "微信公众号"

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        result = search_wechat_local_cache(params)
        result["provider"] = self.provider_id
        return result


class YouTubeSearchProvider(SearchProvider):
    @property
    def provider_id(self) -> str:
        return "youtube"

    @property
    def display_name(self) -> str:
        return "YouTube"

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        result = search_youtube_local_cache(params)
        result["provider"] = self.provider_id
        return result


class BilibiliSearchProvider(SearchProvider):
    @property
    def provider_id(self) -> str:
        return "bilibili"

    @property
    def display_name(self) -> str:
        return "B站"

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        result = search_bilibili_local_cache(params)
        result["provider"] = self.provider_id
        return result


def get_search_providers() -> dict[str, SearchProvider]:
    return {
        "x": XSearchProvider(),
        "feed": FeedSearchProvider(),
        "github": GitHubSearchProvider(),
        "wechat": WeChatSearchProvider(),
        "youtube": YouTubeSearchProvider(),
        "bilibili": BilibiliSearchProvider(),
    }


def _normalize_provider_ids(params: dict[str, Any], providers: dict[str, SearchProvider]) -> list[str]:
    raw = params.get("providers")
    provider_ids: list[str] = []
    if isinstance(raw, list):
        provider_ids = [str(x or "").strip().lower() for x in raw]
    elif isinstance(raw, str) and raw.strip():
        provider_ids = [p.strip().lower() for p in raw.split(",") if p.strip()]

    if not provider_ids:
        fallback = str(params.get("provider") or "x").strip().lower()
        provider_ids = [fallback] if fallback else ["x"]

    normalized: list[str] = []
    for provider_id in provider_ids:
        if provider_id not in providers:
            raise ValueError(f"不支持的 provider：{provider_id}")
        if provider_id not in normalized:
            normalized.append(provider_id)
    return normalized


def _default_mode_for_provider(provider_id: str) -> str:
    mapping = {
        "x": "pool_latest",
        "feed": "feed_search",
        "github": "github_trending",
        "wechat": "wechat_search",
        "youtube": "youtube_search",
        "bilibili": "bilibili_search",
    }
    return mapping.get(provider_id, "pool_latest")


def _merge_search_results(results: list[dict[str, Any]], provider_ids: list[str], params: dict[str, Any]) -> dict[str, Any]:
    sort_by = str(params.get("sort_by") or "time").strip().lower()
    if sort_by not in {"time", "engagement", "hybrid"}:
        sort_by = "time"

    merged_items: list[dict[str, Any]] = []
    searched_handles: list[str] = []
    success_handles: list[str] = []
    errors: list[dict[str, Any]] = []
    per_provider: dict[str, Any] = {}
    messages: list[str] = []

    for result in results:
        provider_id = str(result.get("provider") or "").strip().lower()
        if not provider_id:
            continue
        per_provider[provider_id] = {
            "count": int(result.get("count") or 0),
            "searched_handles": result.get("searched_handles") or [],
            "success_handles": result.get("success_handles") or [],
            "error_count": int(result.get("error_count") or 0),
        }
        searched_handles.extend([str(x) for x in (result.get("searched_handles") or []) if str(x or "").strip()])
        success_handles.extend([str(x) for x in (result.get("success_handles") or []) if str(x or "").strip()])
        for item in (result.get("results") or []):
            merged = dict(item or {})
            merged.setdefault("channel", provider_id)
            merged_items.append(merged)
        for err in (result.get("errors") or []):
            err_item = dict(err or {})
            err_item.setdefault("provider", provider_id)
            errors.append(err_item)
        message = str(result.get("message") or "").strip()
        if message:
            messages.append(f"{provider_id}: {message}")

    dedup: dict[str, dict[str, Any]] = {}
    for item in merged_items:
        key = item.get("url") or item.get("id") or f"{item.get('channel')}::{item.get('source')}::{item.get('title')}::{item.get('published_at') or item.get('created_at')}"
        if key and key not in dedup:
            dedup[str(key)] = item

    merged_results = _sort_results(list(dedup.values()), sort_by)
    grouped = {
        "strong": [r for r in merged_results if r.get("bucket") == "strong"],
        "watch": [r for r in merged_results if r.get("bucket") == "watch"],
        "skip": [r for r in merged_results if r.get("bucket") == "skip"],
    }
    searched_handles = sorted(dict.fromkeys(searched_handles))
    success_handles = sorted(dict.fromkeys(success_handles))

    return {
        "ok": True,
        "provider": provider_ids[0] if len(provider_ids) == 1 else "multi",
        "providers": provider_ids,
        "query": str(params.get("query") or ""),
        "mode": _default_mode_for_provider(provider_ids[0]) if len(provider_ids) == 1 else "multi_search",
        "count": len(merged_results),
        "results": merged_results,
        "grouped": grouped,
        "counts": {
            "strong": len(grouped["strong"]),
            "watch": len(grouped["watch"]),
            "skip": len(grouped["skip"]),
        },
        "searched_handles": searched_handles,
        "success_handles": success_handles,
        "filters": {
            "lang": str(params.get("lang") or ""),
            "since": str(params.get("since") or ""),
            "until": str(params.get("until") or ""),
            "query_scope": _normalize_query_scope(params.get("query_scope") or "all"),
            "min_likes": int(params.get("min_likes") or 0),
            "min_retweets": int(params.get("min_retweets") or 0),
            "sort_by": sort_by,
        },
        "errors": errors,
        "error_count": len(errors),
        "observability": {
            "error_count": len(errors),
            "success_count": len(success_handles),
            "searched_count": len(searched_handles),
            "failed_handles": [x for x in searched_handles if x not in success_handles],
            "success_handles": success_handles,
        },
        "per_provider": per_provider,
        "schema_version": "candidate-pack-v1",
        "message": "；".join(messages) if messages else "",
    }


def search_candidates(params: dict[str, Any]) -> dict[str, Any]:
    providers = get_search_providers()
    provider_ids = _normalize_provider_ids(params, providers)
    if len(provider_ids) == 1:
        provider = providers[provider_ids[0]]
        result = provider.search(params)
        result.setdefault("provider", provider.provider_id)
        result.setdefault("providers", [provider.provider_id])
        result.setdefault("schema_version", "candidate-pack-v1")
        return result

    child_results: list[dict[str, Any]] = []
    for provider_id in provider_ids:
        child_params = dict(params)
        child_params["provider"] = provider_id
        child_params["mode"] = _default_mode_for_provider(provider_id)
        child_results.append(providers[provider_id].search(child_params))
    return _merge_search_results(child_results, provider_ids, params)


def _history_candidate(item: dict[str, Any]) -> dict[str, Any]:
    metrics = item.get("metrics") or {}
    return {
        "id": item.get("id") or "",
        "title": item.get("title") or "",
        "url": item.get("url") or "",
        "channel": item.get("channel") or "",
        "source": item.get("source") or "",
        "published_at": item.get("published_at") or item.get("created_at") or "",
        "summary": item.get("summary") or "",
        "why_pick": item.get("why_pick") or "",
        "recommend_full_fetch": item.get("recommend_full_fetch") or "no",
        "bucket": item.get("bucket") or "",
        "matched_handle": item.get("matched_handle") or "",
        "created_at": item.get("created_at") or "",
        "lang": item.get("lang") or "",
        "metrics": {
            "likes": int(metrics.get("likes") or 0),
            "retweets": int(metrics.get("retweets") or 0),
            "replies": int(metrics.get("replies") or 0),
            "quotes": int(metrics.get("quotes") or 0),
            "views": int(metrics.get("views") or 0),
            "engagement_score": float(metrics.get("engagement_score") or 0),
        },
    }


def build_history_entry(payload: dict[str, Any], result: dict[str, Any], rerun_from: str = "") -> dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    history_id = f"h-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    results = [_history_candidate(item) for item in (result.get("results") or [])]
    grouped = {
        "strong": [r for r in results if r.get("bucket") == "strong"],
        "watch": [r for r in results if r.get("bucket") == "watch"],
        "skip": [r for r in results if r.get("bucket") == "skip"],
    }
    return {
        "id": history_id,
        "created_at": now,
        "rerun_from": rerun_from,
        "payload": {
            "provider": str(payload.get("provider") or "x"),
            "providers": [str(x).strip().lower() for x in (payload.get("providers") or []) if str(x).strip()],
            "query": str(payload.get("query") or ""),
            "mode": str(payload.get("mode") or ""),
            "target_handle": str(payload.get("target_handle") or ""),
            "limit_per_account": int(payload.get("limit_per_account") or 3),
            "since": str(payload.get("since") or ""),
            "until": str(payload.get("until") or ""),
            "lang": str(payload.get("lang") or ""),
            "query_scope": _normalize_query_scope(payload.get("query_scope") or "all"),
            "min_likes": int(payload.get("min_likes") or 0),
            "min_retweets": int(payload.get("min_retweets") or 0),
            "sort_by": str(payload.get("sort_by") or "time"),
            "search_type": str(payload.get("search_type") or "latest"),
        },
        "snapshot": {
            "ok": bool(result.get("ok", True)),
            "provider": result.get("provider") or "",
            "providers": result.get("providers") or [],
            "schema_version": result.get("schema_version") or "candidate-pack-v1",
            "query": result.get("query") or "",
            "mode": result.get("mode") or "",
            "count": len(results),
            "results": results,
            "grouped": grouped,
            "counts": {
                "strong": len(grouped["strong"]),
                "watch": len(grouped["watch"]),
                "skip": len(grouped["skip"]),
            },
            "searched_handles": result.get("searched_handles") or [],
            "success_handles": result.get("success_handles") or [],
            "filters": result.get("filters") or {},
            "errors": result.get("errors") or [],
            "error_count": int(result.get("error_count") or 0),
            "observability": result.get("observability") or {},
        },
    }


def append_search_history(payload: dict[str, Any], result: dict[str, Any], rerun_from: str = "") -> str:
    entry = build_history_entry(payload, result, rerun_from=rerun_from)
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return str(entry.get("id") or "")


def list_search_history(limit: int = 20) -> list[dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
    items: list[dict[str, Any]] = []
    for line in reversed(lines):
        raw = line.strip()
        if not raw:
            continue
        try:
            entry = json.loads(raw)
            snapshot = entry.get("snapshot") or {}
            payload = entry.get("payload") or {}
            items.append(
                {
                    "id": entry.get("id") or "",
                    "created_at": entry.get("created_at") or "",
                    "rerun_from": entry.get("rerun_from") or "",
                    "provider": snapshot.get("provider") or payload.get("provider") or "x",
                    "providers": snapshot.get("providers") or payload.get("providers") or [],
                    "query": snapshot.get("query") or payload.get("query") or "",
                    "mode": snapshot.get("mode") or payload.get("mode") or "",
                    "count": int(snapshot.get("count") or 0),
                    "counts": snapshot.get("counts") or {},
                    "error_count": int(snapshot.get("error_count") or 0),
                }
            )
        except Exception:  # noqa: BLE001
            continue
        if len(items) >= max(1, min(limit, 200)):
            break
    return items


def get_history_entry(history_id: str) -> dict[str, Any] | None:
    history_id = (history_id or "").strip()
    if not history_id or not HISTORY_PATH.exists():
        return None
    for line in reversed(HISTORY_PATH.read_text(encoding="utf-8").splitlines()):
        raw = line.strip()
        if not raw:
            continue
        try:
            entry = json.loads(raw)
            if str(entry.get("id") or "").strip() == history_id:
                return entry
        except Exception:  # noqa: BLE001
            continue
    return None


def get_db_overview(force_refresh: bool = False) -> dict[str, Any]:
    now = time.time()
    cached_payload = DB_OVERVIEW_CACHE.get("payload")
    if not force_refresh and cached_payload and float(DB_OVERVIEW_CACHE.get("expires_at") or 0.0) > now:
        return dict(cached_payload)

    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        try:
            account_row = cur.execute(
                """
                SELECT
                    COUNT(*) AS total_accounts,
                    SUM(CASE WHEN enabled=1 THEN 1 ELSE 0 END) AS enabled_accounts
                FROM accounts
                """
            ).fetchone()
        except sqlite3.OperationalError:
            payload = {"ok": True, "db_path": str(EVENT_RADAR_DB_PATH), "total_accounts": 0, "enabled_accounts": 0, "raw_items_count": 0, "recent_24h_count": 0, "latest_fetched_at": "", "latest_published_at": "", "platform_stats": []}
            DB_OVERVIEW_CACHE.update({"expires_at": now + DB_OVERVIEW_CACHE_TTL_SEC, "payload": dict(payload)})
            return payload

        try:
            raw_items_row = cur.execute(
                """
                SELECT
                    COUNT(*) AS raw_items_count,
                    SUM(CASE WHEN fetched_at >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS recent_24h_count,
                    MAX(fetched_at) AS latest_fetched_at,
                    MAX(published_at) AS latest_published_at
                FROM raw_items
                """
            ).fetchone()
            platform_stats = [
                dict(row)
                for row in cur.execute(
                    """
                    SELECT platform, COUNT(*) AS item_count, MAX(fetched_at) AS latest_fetched_at
                    FROM raw_items
                    GROUP BY platform
                    ORDER BY item_count DESC, platform ASC
                    """
                ).fetchall()
            ]
        except sqlite3.OperationalError:
            raw_items_row = {
                "raw_items_count": 0,
                "recent_24h_count": 0,
                "latest_fetched_at": "",
                "latest_published_at": "",
            }
            platform_stats = []

        for item in platform_stats:
            item["latest_fetched_at"] = _format_datetime_text(item.get("latest_fetched_at") or "")

        payload = {
            "ok": True,
            "db_path": str(EVENT_RADAR_DB_PATH),
            "total_accounts": int((account_row["total_accounts"] if account_row else 0) or 0),
            "enabled_accounts": int((account_row["enabled_accounts"] if account_row else 0) or 0),
            "raw_items_count": int((raw_items_row["raw_items_count"] if raw_items_row else 0) or 0),
            "recent_24h_count": int((raw_items_row["recent_24h_count"] if raw_items_row else 0) or 0),
            "latest_fetched_at": _format_datetime_text((raw_items_row["latest_fetched_at"] if raw_items_row else "") or ""),
            "latest_published_at": _format_datetime_text((raw_items_row["latest_published_at"] if raw_items_row else "") or ""),
            "platform_stats": platform_stats,
        }
        DB_OVERVIEW_CACHE.update({"expires_at": now + DB_OVERVIEW_CACHE_TTL_SEC, "payload": dict(payload)})
        return payload
    finally:
        conn.close()


def list_recent_raw_items(limit: int = 50, platform: str = "", handle: str = "", page: int = 1) -> dict[str, Any]:
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        where_sql = " WHERE 1=1 "
        params: list[Any] = []
        if platform:
            where_sql += " AND platform=?"
            params.append(platform)
        if handle:
            where_sql += " AND LOWER(source_handle)=LOWER(?)"
            params.append(handle)

        page_size = max(1, min(limit, 100))
        page = max(1, int(page or 1))
        try:
            total = int(cur.execute(f"SELECT COUNT(*) FROM raw_items{where_sql}", params).fetchone()[0] or 0)
        except sqlite3.OperationalError:
            return {"items": [], "total": 0, "page": 1, "page_size": max(1, min(limit, 100)), "total_pages": 1}
        total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * page_size

        sql = f"""
            SELECT id, platform, source_handle, item_id, title, content, url, published_at, fetched_at, metrics_json
            FROM raw_items
            {where_sql}
            ORDER BY COALESCE(published_at, '') DESC, fetched_at DESC, id DESC
            LIMIT ? OFFSET ?
        """
        rows = cur.execute(sql, [*params, page_size, offset]).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                item["metrics"] = json.loads(item.get("metrics_json") or "{}")
            except Exception:  # noqa: BLE001
                item["metrics"] = {}
            item["published_at"] = _format_datetime_text(item.get("published_at") or "")
            item["fetched_at"] = _format_datetime_text(item.get("fetched_at") or "")
            item["content"] = _trim_text(item.get("content") or "")
            item.pop("metrics_json", None)
            items.append(item)
        return {"items": items, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}
    finally:
        conn.close()


def list_account_latest_items(platform: str = "x", page: int = 1, limit: int = 20) -> dict[str, Any]:
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        try:
            accounts = cur.execute(
            """
            SELECT handle, enabled, priority
            FROM accounts
            WHERE platform=?
            ORDER BY enabled DESC, priority DESC, id ASC
            """,
            (platform,),
        ).fetchall()
        except sqlite3.OperationalError:
            return {"items": [], "total": 0, "page": 1, "page_size": max(1, min(int(limit or 20), 100)), "total_pages": 1, "platform": platform}

        wechat_name_map: dict[str, str] = {}
        youtube_name_map: dict[str, str] = {}
        bilibili_name_map: dict[str, str] = {}
        if platform == "wechat":
            wechat_name_map = {str(s.get("id") or "").strip(): str(s.get("name") or "").strip() for s in load_wechat_sources(enabled_only=False)}
        if platform == "youtube":
            youtube_name_map = {str(s.get("id") or "").strip(): str(s.get("name") or "").strip() for s in load_youtube_channels(enabled_only=False)}
        if platform == "bilibili":
            bilibili_name_map = {str(s.get("id") or "").strip(): str(s.get("name") or "").strip() for s in load_bilibili_sources(enabled_only=False)}

        page_size = max(1, min(int(limit or 20), 100))
        total = len(accounts)
        total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
        page = max(1, int(page or 1))
        if page > total_pages:
            page = total_pages
        start_idx = (page - 1) * page_size
        page_accounts = accounts[start_idx:start_idx + page_size]

        items: list[dict[str, Any]] = []
        for account in page_accounts:
            handle = str(account["handle"] or "")
            handle_aliases = [handle]
            if platform == "wechat":
                source_name = wechat_name_map.get(handle)
                if source_name:
                    handle_aliases.append(source_name)
            if platform == "youtube":
                source_name = youtube_name_map.get(handle)
                if source_name:
                    handle_aliases.append(source_name)
            if platform == "bilibili":
                source_name = bilibili_name_map.get(handle)
                if source_name:
                    handle_aliases.append(source_name)

            latest = None
            for alias in handle_aliases:
                row = cur.execute(
                    """
                    SELECT platform, source_handle, item_id, title, content, url, published_at, fetched_at, metrics_json
                    FROM raw_items
                    WHERE platform=? AND source_handle=?
                    ORDER BY COALESCE(published_at, '') DESC, fetched_at DESC, id DESC
                    LIMIT 1
                    """,
                    (platform, alias),
                ).fetchone()
                if row is None:
                    continue
                if latest is None:
                    latest = row
                    continue
                cand_pub = str(row["published_at"] or "")
                best_pub = str(latest["published_at"] or "")
                cand_fetch = str(row["fetched_at"] or "")
                best_fetch = str(latest["fetched_at"] or "")
                if (cand_pub, cand_fetch) > (best_pub, best_fetch):
                    latest = row

            row = {
                "handle": handle,
                "platform": platform,
                "display_name": (
                    wechat_name_map.get(handle)
                    if platform == "wechat"
                    else (
                        youtube_name_map.get(handle)
                        if platform == "youtube"
                        else (bilibili_name_map.get(handle) if platform == "bilibili" else handle)
                    )
                ),
                "enabled": bool(account["enabled"]),
                "priority": int(account["priority"] or 0),
                "has_item": latest is not None,
            }
            if latest is not None:
                latest_item = dict(latest)
                try:
                    latest_item["metrics"] = json.loads(latest_item.get("metrics_json") or "{}")
                except Exception:  # noqa: BLE001
                    latest_item["metrics"] = {}
                latest_item["published_at"] = _format_datetime_text(latest_item.get("published_at") or "")
                latest_item["fetched_at"] = _format_datetime_text(latest_item.get("fetched_at") or "")
                latest_item["content"] = _trim_text(latest_item.get("content") or "")
                latest_item.pop("metrics_json", None)
                row["latest_item"] = latest_item
            items.append(row)
        return {"items": items, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages, "platform": platform}
    finally:
        conn.close()


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict[str, Any]):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, content: str):
        body = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_download(self, filename: str, content: str, content_type: str = "text/markdown; charset=utf-8"):
        body = content.encode("utf-8")
        safe_name = quote(filename or "article.md")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{safe_name}")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.end_headers()

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path in ["/", "/portal", "/portal.html"]:
            content = PORTAL_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/search", "/intelligence", "/index.html"]:
            content = INDEX_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/config", "/config.html"]:
            content = CONFIG_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/create", "/create/studio", "/create/studio.html"]:
            content = STUDIO_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/create/workspace", "/create/workspace.html", "/create.html"]:
            content = CREATE_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/create/write", "/create/write.html"]:
            content = WRITE_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/create/topic", "/create/topic.html"]:
            content = TOPIC_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/create/nighthawk", "/create/nighthawk.html"]:
            content = NIGHTHAWK_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        event_page_match = re.fullmatch(r"/create/events/(\d+)", path)
        if event_page_match:
            content = EVENT_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        event_packet_page_match = re.fullmatch(r"/create/event-packets/([A-Za-z0-9_-]+)", path)
        if event_packet_page_match:
            content = EVENT_PACKET_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        packet_page_match = re.fullmatch(r"/create/packets/([A-Za-z0-9_-]+)", path)
        if packet_page_match:
            content = PACKET_HTML_PATH.read_text(encoding="utf-8")
            self._send_html(content)
            return

        if path in ["/api/config/summary", "/api/config/local"]:
            self._send_json(200, _config_payload())
            return

        if path == "/api/config/template":
            template = (REPO_ROOT / "config" / "examples" / "local.example.yaml").read_text(encoding="utf-8")
            self._send_download("local.example.yaml", template, content_type="text/yaml; charset=utf-8")
            return

        if path == "/api/config/export":
            selected_config_path = _selected_config_path()
            content = selected_config_path.read_text(encoding="utf-8") if selected_config_path.exists() else ""
            if not content:
                content = (REPO_ROOT / "config" / "examples" / "local.example.yaml").read_text(encoding="utf-8")
            self._send_download(selected_config_path.name or "local.yaml", content, content_type="text/yaml; charset=utf-8")
            return

        if path == "/api/create/config":
            config_summary = get_create_studio_config_summary(get_create_studio_config())
            self._send_json(200, config_summary)
            return

        if path == "/api/create/writers":
            writer_summary = get_writer_registry_summary()
            self._send_json(200, writer_summary)
            return

        if path == "/api/create/targets":
            target_summary = get_creation_target_summary()
            self._send_json(200, target_summary)
            return

        if path == "/api/create/nighthawk/profile":
            profile_payload = get_nighthawk_supply_profile()
            self._send_json(200, profile_payload)
            return

        if path == "/api/create/nighthawk/items":
            limit_raw = (query.get("limit") or ["20"])[0]
            page_raw = (query.get("page") or ["1"])[0]
            platform = str((query.get("platform") or [""])[0] or "").strip()
            keyword = str((query.get("keyword") or [""])[0] or "").strip()
            body_ready_only_raw = str((query.get("body_ready_only") or [""])[0] or "").strip().lower()
            sort_by = str((query.get("sort_by") or ["time"])[0] or "time").strip().lower() or "time"
            try:
                limit = int(limit_raw)
            except Exception:  # noqa: BLE001
                limit = 20
            try:
                page = int(page_raw)
            except Exception:  # noqa: BLE001
                page = 1
            payload = list_nighthawk_raw_items(
                limit=limit,
                page=page,
                platform=platform,
                keyword=keyword,
                body_ready_only=body_ready_only_raw in {"1", "true", "yes", "on"},
                sort_by=sort_by,
            )
            self._send_json(200, payload)
            return

        nighthawk_item_match = re.fullmatch(r"/api/create/nighthawk/items/(\d+)", path)
        if nighthawk_item_match:
            payload = get_nighthawk_raw_item_detail(int(nighthawk_item_match.group(1)))
            self._send_json(200, payload)
            return

        event_detail_match = re.fullmatch(r"/api/events/(\d+)", path)
        if event_detail_match:
            payload = get_event_detail(int(event_detail_match.group(1)))
            self._send_json(200, payload)
            return

        if path == "/api/create/index/status":
            status_payload = get_create_studio_store().get_status()
            self._send_json(200, status_payload)
            return

        topic_packet_match = re.fullmatch(r"/api/create/topic-packets/([A-Za-z0-9_-]+)", path)
        if topic_packet_match:
            packet_payload = get_topic_packet_detail(topic_packet_match.group(1), config=get_create_studio_config())
            self._send_json(200, packet_payload)
            return

        event_packet_match = re.fullmatch(r"/api/create/event-packets/([A-Za-z0-9_-]+)", path)
        if event_packet_match:
            packet_payload = get_event_packet_detail(event_packet_match.group(1), config=get_create_studio_config())
            self._send_json(200, packet_payload)
            return

        event_packet_ref_match = re.fullmatch(r"/api/create/events/(\d+)/packet", path)
        if event_packet_ref_match:
            packet_payload = get_latest_event_packet_ref(int(event_packet_ref_match.group(1)), config=get_create_studio_config())
            self._send_json(200, packet_payload)
            return

        creation_service = get_creation_workspace_service()

        if path == "/api/create/tasks":
            limit_raw = (query.get("limit") or ["20"])[0]
            try:
                limit = int(limit_raw)
            except Exception:  # noqa: BLE001
                limit = 20
            items = creation_service.list_tasks(limit=limit)
            self._send_json(200, {"ok": True, "items": items, "count": len(items)})
            return

        creation_bundle_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)", path)
        if creation_bundle_match:
            task_id = creation_bundle_match.group(1)
            bundle = creation_service.get_task_bundle(task_id)
            self._send_json(200, bundle)
            return

        creation_retrieval_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/retrieval", path)
        if creation_retrieval_match:
            task_id = creation_retrieval_match.group(1)
            retrieval_payload = creation_service.get_retrieval_view(task_id)
            self._send_json(200, retrieval_payload)
            return

        creation_citations_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/citations", path)
        if creation_citations_match:
            task_id = creation_citations_match.group(1)
            citation_list = creation_service.get_citation_list(task_id)
            if not citation_list:
                self._send_json(404, {"ok": False, "message": "引用清单不存在"})
                return
            self._send_json(200, {"ok": True, "citation_list": citation_list})
            return

        creation_outline_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/outline", path)
        if creation_outline_match:
            task_id = creation_outline_match.group(1)
            outline_packet = creation_service.get_outline_packet(task_id)
            if not outline_packet:
                self._send_json(404, {"ok": False, "message": "框架包不存在"})
                return
            self._send_json(200, {"ok": True, "outline_packet": outline_packet})
            return

        creation_writer_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/writer-job", path)
        if creation_writer_match:
            task_id = creation_writer_match.group(1)
            writer_job = creation_service.get_latest_writer_job(task_id)
            if not writer_job:
                self._send_json(404, {"ok": False, "message": "WriterJob 不存在"})
                return
            self._send_json(200, {"ok": True, "writer_job": writer_job})
            return

        creation_packet_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/writer-packet", path)
        if creation_packet_match:
            task_id = creation_packet_match.group(1)
            writer_job = creation_service.get_latest_writer_job(task_id)
            if not writer_job:
                self._send_json(404, {"ok": False, "message": "WriterJob 不存在"})
                return
            packet = creation_service.get_writer_packet(task_id)
            self._send_json(200, {"ok": True, "packet": packet, "packet_path": writer_job.get("packet_path") or ""})
            return

        article_draft_match = re.fullmatch(r"/api/create/drafts/([A-Za-z0-9_-]+)", path)
        if article_draft_match:
            draft_payload = creation_service.get_article_draft(article_draft_match.group(1))
            self._send_json(200, draft_payload)
            return

        article_download_match = re.fullmatch(r"/api/create/drafts/([A-Za-z0-9_-]+)/download", path)
        if article_download_match:
            draft_payload = creation_service.get_article_draft(article_download_match.group(1))
            draft = dict(draft_payload.get("article_draft") or {})
            title = re.sub(r"[\\/:*?\"<>|]+", "-", str(draft.get("title") or draft.get("id") or "article").strip())
            self._send_download(f"{title}.md", str(draft_payload.get("article_markdown") or ""))
            return

        if path == "/api/accounts":
            accounts = get_accounts(enabled_only=False)
            enabled_accounts = [a for a in accounts if a.get("enabled")]
            self._send_json(
                200,
                {
                    "ok": True,
                    "accounts": accounts,
                    "enabled_count": len(enabled_accounts),
                    "total_count": len(accounts),
                },
            )
            return

        if path == "/api/wechat/sources":
            sources = load_wechat_sources(enabled_only=False)
            enabled_sources = [s for s in sources if s.get("enabled")]
            self._send_json(
                200,
                {
                    "ok": True,
                    "sources": sources,
                    "enabled_count": len(enabled_sources),
                    "total_count": len(sources),
                },
            )
            return

        if path == "/api/youtube/channels":
            channels = load_youtube_channels(enabled_only=False)
            enabled_channels = [c for c in channels if c.get("enabled")]
            self._send_json(
                200,
                {
                    "ok": True,
                    "channels": channels,
                    "enabled_count": len(enabled_channels),
                    "total_count": len(channels),
                },
            )
            return

        if path == "/api/bilibili/sources":
            sources = load_bilibili_sources(enabled_only=False)
            enabled_sources = [s for s in sources if s.get("enabled")]
            self._send_json(
                200,
                {
                    "ok": True,
                    "sources": sources,
                    "enabled_count": len(enabled_sources),
                    "total_count": len(sources),
                },
            )
            return

        if path == "/api/douyin/sources":
            sources = load_douyin_sources(enabled_only=False)
            enabled_sources = [s for s in sources if s.get("enabled")]
            self._send_json(
                200,
                {
                    "ok": True,
                    "sources": sources,
                    "enabled_count": len(enabled_sources),
                    "total_count": len(sources),
                },
            )
            return

        if path == "/api/feed/sources":
            sources = load_feed_sources(enabled_only=False)
            enabled_sources = [s for s in sources if s.get("enabled", True)]
            self._send_json(
                200,
                {
                    "ok": True,
                    "sources": sources,
                    "enabled_count": len(enabled_sources),
                    "total_count": len(sources),
                },
            )
            return

        if path == "/api/templates":
            all_templates = load_templates(include_disabled=True)
            active_templates = load_templates(include_disabled=False)
            self._send_json(
                200,
                {
                    "ok": True,
                    "templates": all_templates,
                    "active_templates": active_templates,
                    "count": len(all_templates),
                    "active_count": len(active_templates),
                },
            )
            return

        if path == "/api/providers":
            providers = get_search_providers()
            self._send_json(
                200,
                {
                    "ok": True,
                    "providers": [
                        {
                            "id": p.provider_id,
                            "name": p.display_name,
                        }
                        for p in providers.values()
                    ],
                    "default": "x",
                },
            )
            return

        if path == "/api/history":
            limit_raw = (query.get("limit") or ["20"])[0]
            try:
                limit = int(limit_raw)
            except Exception:  # noqa: BLE001
                limit = 20
            items = list_search_history(limit=limit)
            self._send_json(200, {"ok": True, "items": items, "count": len(items)})
            return

        if path == "/api/db/overview":
            refresh_raw = str((query.get("refresh") or [""])[0] or "").strip().lower()
            force_refresh = refresh_raw in {"1", "true", "yes", "y", "refresh"}
            self._send_json(200, get_db_overview(force_refresh=force_refresh))
            return

        if path == "/api/db/recent-items":
            limit_raw = (query.get("limit") or ["20"])[0]
            page_raw = (query.get("page") or ["1"])[0]
            platform = str((query.get("platform") or [""])[0] or "").strip()
            handle = str((query.get("handle") or [""])[0] or "").strip()
            try:
                limit = int(limit_raw)
            except Exception:  # noqa: BLE001
                limit = 20
            try:
                page = int(page_raw)
            except Exception:  # noqa: BLE001
                page = 1
            payload = list_recent_raw_items(limit=limit, platform=platform, handle=handle, page=page)
            self._send_json(200, {"ok": True, **payload, "count": len(payload.get("items") or [])})
            return

        if path == "/api/db/account-latest":
            platform = str((query.get("platform") or ["x"])[0] or "x").strip() or "x"
            limit_raw = (query.get("limit") or ["20"])[0]
            page_raw = (query.get("page") or ["1"])[0]
            try:
                limit = int(limit_raw)
            except Exception:  # noqa: BLE001
                limit = 20
            try:
                page = int(page_raw)
            except Exception:  # noqa: BLE001
                page = 1
            payload = list_account_latest_items(platform=platform, page=page, limit=limit)
            self._send_json(200, {"ok": True, **payload, "count": len(payload.get("items") or [])})
            return

        if path == "/api/topics":
            sort_by = str((query.get("sort") or ["overall"])[0] or "overall").strip() or "overall"
            keyword = str((query.get("keyword") or [""])[0] or "").strip()
            limit_raw = (query.get("limit") or ["10"])[0]
            page_raw = (query.get("page") or ["1"])[0]
            try:
                limit = int(limit_raw)
            except Exception:  # noqa: BLE001
                limit = 10
            try:
                page = int(page_raw)
            except Exception:  # noqa: BLE001
                page = 1
            payload = list_topics(sort_by=sort_by, page=page, limit=limit, keyword=keyword)
            self._send_json(200, {"ok": True, **payload, "count": len(payload.get("items") or [])})
            return

        if path == "/api/topics/sync-status":
            payload = get_topic_sync_status()
            self._send_json(200, payload)
            return

        topic_detail_match = re.fullmatch(r"/api/topics/(\d+)", path)
        if topic_detail_match:
            event_page_raw = (query.get("event_page") or ["1"])[0]
            event_limit_raw = (query.get("event_limit") or ["10"])[0]
            article_page_raw = (query.get("article_page") or ["1"])[0]
            article_limit_raw = (query.get("article_limit") or ["8"])[0]
            try:
                event_page = int(event_page_raw)
            except Exception:  # noqa: BLE001
                event_page = 1
            try:
                event_limit = int(event_limit_raw)
            except Exception:  # noqa: BLE001
                event_limit = 10
            try:
                article_page = int(article_page_raw)
            except Exception:  # noqa: BLE001
                article_page = 1
            try:
                article_limit = int(article_limit_raw)
            except Exception:  # noqa: BLE001
                article_limit = 8
            payload = get_topic_detail(
                int(topic_detail_match.group(1)),
                event_page=event_page,
                event_limit=event_limit,
                article_page=article_page,
                article_limit=article_limit,
            )
            code = 200 if payload.get("ok") else 404
            self._send_json(code, payload)
            return

        m = re.fullmatch(r"/api/fetch/tasks/([A-Za-z0-9_-]+)", path)
        if m:
            task = get_fetch_task(m.group(1))
            if not task:
                self._send_json(404, {"ok": False, "message": "任务不存在"})
                return
            self._send_json(200, task)
            return

        self._send_json(404, {"ok": False, "message": "Not Found"})

    def do_POST(self):  # noqa: N802
        fetch_aliases = {"/api/fetch", "/api/fetch/tasks"}
        creation_update_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)", self.path)
        creation_delete_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/delete", self.path)
        creation_retrieval_run_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/retrieval/run", self.path)
        creation_retrieval_keep_match = re.fullmatch(
            r"/api/create/tasks/([A-Za-z0-9_-]+)/retrieval/([A-Za-z0-9_-]+)/keep",
            self.path,
        )
        creation_retrieval_exclude_match = re.fullmatch(
            r"/api/create/tasks/([A-Za-z0-9_-]+)/retrieval/([A-Za-z0-9_-]+)/exclude",
            self.path,
        )
        creation_retrieval_fetch_body_match = re.fullmatch(
            r"/api/create/tasks/([A-Za-z0-9_-]+)/retrieval/([A-Za-z0-9_-]+)/([^/]+)/fetch-body",
            self.path,
        )
        creation_retrieval_classify_match = re.fullmatch(
            r"/api/create/tasks/([A-Za-z0-9_-]+)/retrieval/([A-Za-z0-9_-]+)/classify",
            self.path,
        )
        creation_citations_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/citations", self.path)
        creation_citations_generate_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/citations/generate", self.path)
        creation_citation_fetch_body_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/citations/fetch-primary-body", self.path)
        creation_citation_update_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/citations/([A-Za-z0-9_-]+)", self.path)
        creation_outline_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/outline", self.path)
        creation_outline_generate_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/outline/generate", self.path)
        creation_bootstrap_packet_task_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/bootstrap-packet-flow", self.path)
        creation_autofill_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/autofill", self.path)
        creation_autofill_task_id = ""
        if not creation_autofill_match and self.path.startswith("/api/create/tasks/") and self.path.endswith("/autofill"):
            candidate_task_id = self.path[len("/api/create/tasks/"):-len("/autofill")]
            candidate_task_id = candidate_task_id.strip("/")
            if re.fullmatch(r"[A-Za-z0-9_-]+", candidate_task_id or ""):
                creation_autofill_task_id = candidate_task_id
        creation_writer_generate_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/writer-job/generate", self.path)
        creation_writer_update_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/writer-job", self.path)
        creation_article_generate_match = re.fullmatch(r"/api/create/tasks/([A-Za-z0-9_-]+)/article/generate", self.path)
        creation_article_update_match = re.fullmatch(r"/api/create/drafts/([A-Za-z0-9_-]+)", self.path)
        creation_packet_to_task_match = re.fullmatch(r"/api/create/packets/([A-Za-z0-9_-]+)/to-task", self.path)
        topic_packet_create_match = re.fullmatch(r"/api/topics/(\d+)/packet", self.path)
        topic_to_task_match = re.fullmatch(r"/api/topics/(\d+)/to-task", self.path)
        cancel_match = re.fullmatch(r"/api/fetch/tasks/([A-Za-z0-9_-]+)/cancel", self.path)
        if self.path not in {
            "/api/search",
            "/api/accounts/toggle",
            "/api/accounts/add",
            "/api/accounts/delete",
            "/api/accounts/test",
            "/api/templates/upsert",
            "/api/templates/delete",
            "/api/templates/toggle",
            "/api/history/rerun",
            "/api/fetch/open-dir",
            "/api/config/local",
            "/api/config/import",
            "/api/config/check",
            "/api/create/tasks",
            "/api/create/writing-config",
            "/api/create/writing-config/test",
            "/api/create/index/bootstrap",
            "/api/create/index/sync",
            "/api/topics/sync",
            "/api/create/topic-search",
            "/api/create/topic-packets",
            "/api/create/event-packets",
            "/api/create/nighthawk/creation-packet",
            "/api/create/nighthawk/to-task",
            *fetch_aliases,
        } and not any(
            [
                cancel_match,
                creation_update_match,
                creation_delete_match,
                creation_retrieval_run_match,
                creation_retrieval_keep_match,
                creation_retrieval_exclude_match,
                creation_retrieval_fetch_body_match,
                creation_retrieval_classify_match,
                creation_citations_match,
                creation_citations_generate_match,
                creation_citation_fetch_body_match,
                creation_citation_update_match,
                creation_outline_match,
                creation_outline_generate_match,
                creation_bootstrap_packet_task_match,
                creation_autofill_match,
                creation_autofill_task_id,
                creation_writer_generate_match,
                creation_writer_update_match,
                creation_article_generate_match,
                creation_article_update_match,
                creation_packet_to_task_match,
                topic_packet_create_match,
                topic_to_task_match,
            ]
        ):
            self._send_json(404, {"ok": False, "message": "Not Found"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001
            self._send_json(400, {"ok": False, "message": "JSON 解析失败"})
            return

        try:
            if self.path == "/api/config/local":
                config_payload = payload.get("config")
                yaml_text = str(payload.get("yaml") or "")
                env_values = dict(payload.get("env_values") or {})
                result = _write_local_config(
                    config=config_payload if isinstance(config_payload, dict) else None,
                    yaml_text=yaml_text,
                    env_values=env_values,
                )
                self._send_json(200, result)
                return

            if self.path == "/api/config/import":
                yaml_text = str(payload.get("yaml") or "")
                result = _write_local_config(yaml_text=yaml_text, env_values=dict(payload.get("env_values") or {}))
                self._send_json(200, result)
                return

            if self.path == "/api/config/check":
                self._send_json(200, _config_payload())
                return

            creation_service = get_creation_workspace_service()

            if self.path == "/api/search":
                result = search_candidates(payload)
                try:
                    history_id = append_search_history(payload, result)
                    result["history_id"] = history_id
                except Exception:  # noqa: BLE001
                    pass
                self._send_json(200, result)
                return

            if self.path == "/api/create/tasks":
                bundle = creation_service.create_task(payload)
                self._send_json(200, bundle)
                return

            if self.path == "/api/create/writing-config":
                config_payload = update_create_studio_writing_config(payload)
                self._send_json(200, config_payload)
                return

            if self.path == "/api/create/writing-config/test":
                overrides = _writing_config_payload(payload)
                result = test_writing_model_config(
                    config_path=str((get_create_studio_config().get("_meta") or {}).get("config_path") or ""),
                    overrides=overrides,
                )
                self._send_json(200, result)
                return

            if self.path == "/api/create/index/bootstrap":
                status_payload = get_create_studio_store().initialize()
                status_payload["message"] = "创作台独立索引库已初始化"
                self._send_json(200, status_payload)
                return

            if self.path == "/api/create/index/sync":
                sync_payload = run_create_studio_index_sync(
                    config_path=(
                        str(payload.get("config_path") or "").strip()
                        or os.getenv("CREATE_STUDIO_CONFIG_PATH")
                        or (get_create_studio_config().get("_meta") or {}).get("config_path")
                        or None
                    ),
                    full=bool(payload.get("full")),
                    limit_per_phase=int(payload.get("limit_per_phase") or 0),
                )
                self._send_json(200, sync_payload)
                return

            if self.path == "/api/create/nighthawk/creation-packet":
                creation_packet_payload = build_creation_packet_from_nighthawk_raw_items(
                    list(payload.get("raw_item_ids") or []),
                    payload=payload,
                )
                self._send_json(200, creation_packet_payload)
                return

            if self.path == "/api/create/nighthawk/to-task":
                creation_packet_payload = build_creation_packet_from_nighthawk_raw_items(
                    list(payload.get("raw_item_ids") or []),
                    payload=payload,
                )
                task_payload = creation_service.create_task_from_nighthawk_sources(
                    creation_packet_payload.get("creation_packet") or {},
                    list(creation_packet_payload.get("selected_items") or []),
                    payload,
                )
                self._send_json(200, task_payload)
                return

            if self.path == "/api/create/topic-search":
                topic_search_payload = run_topic_search(
                    payload,
                    config=get_create_studio_config(),
                    external_search_callable=search_candidates,
                )
                self._send_json(200, topic_search_payload)
                return

            if self.path == "/api/create/topic-packets":
                topic_packet_payload = create_topic_packet(
                    payload,
                    config=get_create_studio_config(),
                    external_search_callable=search_candidates,
                )
                self._send_json(200, topic_packet_payload)
                return

            if topic_packet_create_match:
                topic_id = int(topic_packet_create_match.group(1))
                topic_detail_payload = get_topic_detail(
                    topic_id,
                    article_page=1,
                    article_limit=max(8, min(int(payload.get("article_limit") or 20), 50)),
                )
                topic_packet_payload = create_topic_packet_from_topic_detail(
                    topic_detail_payload,
                    config=get_create_studio_config(),
                    packet_id=str(payload.get("packet_id") or "").strip(),
                )
                self._send_json(200, topic_packet_payload)
                return

            if self.path == "/api/topics/sync":
                sync_payload = sync_topic_snapshot()
                self._send_json(200, sync_payload)
                return

            if self.path == "/api/create/event-packets":
                event_packet_payload = create_event_packet(
                    payload,
                    config=get_create_studio_config(),
                )
                self._send_json(200, event_packet_payload)
                return

            if creation_packet_to_task_match:
                packet_id = creation_packet_to_task_match.group(1)
                packet_payload: dict[str, Any]
                packet: dict[str, Any]
                try:
                    packet_payload = get_topic_packet_detail(packet_id, config=get_create_studio_config())
                    packet = dict(packet_payload.get("topic_packet") or {})
                except Exception:  # noqa: BLE001
                    packet_payload = get_event_packet_detail(packet_id, config=get_create_studio_config())
                    packet = dict(packet_payload.get("event_packet") or {})
                task_payload = creation_service.create_task_from_packet(packet, payload)
                self._send_json(200, task_payload)
                return

            if topic_to_task_match:
                topic_id = int(topic_to_task_match.group(1))
                topic_detail_payload = get_topic_detail(
                    topic_id,
                    article_page=1,
                    article_limit=max(8, min(int(payload.get("article_limit") or 20), 50)),
                )
                topic_packet_payload = create_topic_packet_from_topic_detail(
                    topic_detail_payload,
                    config=get_create_studio_config(),
                    packet_id=str(payload.get("packet_id") or "").strip(),
                )
                task_payload = creation_service.create_task_from_packet(
                    dict(topic_packet_payload.get("topic_packet") or {}),
                    payload,
                )
                task_payload["topic_packet"] = dict(topic_packet_payload.get("topic_packet") or {})
                self._send_json(200, task_payload)
                return

            if creation_bootstrap_packet_task_match:
                task_id = creation_bootstrap_packet_task_match.group(1)
                bootstrap_payload = creation_service.bootstrap_packet_task(task_id, payload)
                self._send_json(200, bootstrap_payload)
                return

            if creation_autofill_match:
                task_id = creation_autofill_match.group(1)
                autofill_payload = creation_service.autofill_task_target(task_id, payload)
                self._send_json(200, autofill_payload)
                return

            if creation_autofill_task_id:
                autofill_payload = creation_service.autofill_task_target(creation_autofill_task_id, payload)
                self._send_json(200, autofill_payload)
                return

            if creation_update_match:
                task_id = creation_update_match.group(1)
                task = creation_service.update_task(task_id, payload)
                self._send_json(200, {"ok": True, "task": task})
                return

            if creation_delete_match:
                task_id = creation_delete_match.group(1)
                deleted = creation_service.delete_task(task_id)
                self._send_json(200, deleted)
                return

            if creation_retrieval_run_match:
                task_id = creation_retrieval_run_match.group(1)
                retrieval_payload = creation_service.run_retrieval(
                    task_id,
                    payload,
                    lambda params: search_creation_candidates(
                        params,
                        external_search_callable=search_candidates,
                        config=get_create_studio_config(),
                    ),
                )
                self._send_json(200, retrieval_payload)
                return

            if creation_retrieval_keep_match:
                task_id, batch_id = creation_retrieval_keep_match.groups()
                retrieval_payload = creation_service.mark_retrieval_keep(task_id, batch_id, payload)
                self._send_json(200, retrieval_payload)
                return

            if creation_retrieval_exclude_match:
                task_id, batch_id = creation_retrieval_exclude_match.groups()
                retrieval_payload = creation_service.mark_retrieval_exclude(task_id, batch_id, payload)
                self._send_json(200, retrieval_payload)
                return

            if creation_retrieval_fetch_body_match:
                task_id, batch_id, source_id = creation_retrieval_fetch_body_match.groups()
                candidate = creation_service.get_retrieval_result_fetch_candidate(task_id, batch_id, source_id)
                retry_count_raw = payload.get("retry_count", 1)
                try:
                    retry_count = int(retry_count_raw)
                except Exception:  # noqa: BLE001
                    retry_count = 1
                analyze_raw = payload.get("analyze", False)
                analyze = analyze_raw is True or str(analyze_raw).strip().lower() in {"1", "true", "yes", "on"}
                save_to_db_raw = payload.get("save_to_db", False)
                save_to_db = save_to_db_raw is True or str(save_to_db_raw).strip().lower() in {"1", "true", "yes", "on"}
                fetch_task = create_fetch_task(
                    url=candidate.get("url") or "",
                    retry_count=retry_count,
                    analyze=analyze,
                    save_to_db=save_to_db,
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "task_id": task_id,
                        "batch_id": batch_id,
                        "source_id": candidate.get("source_id") or "",
                        "title": candidate.get("title") or "",
                        "url": candidate.get("url") or "",
                        "reason": candidate.get("reason") or "",
                        "fetch_task": fetch_task,
                        "gates": candidate.get("gates") or creation_service._build_gate_report(task_id),
                    },
                )
                return

            if creation_retrieval_classify_match:
                task_id, batch_id = creation_retrieval_classify_match.groups()
                retrieval_payload = creation_service.classify_retrieval_result(task_id, batch_id, payload)
                self._send_json(200, retrieval_payload)
                return

            if creation_citations_match:
                task_id = creation_citations_match.group(1)
                citation_list = creation_service.save_citation_list(task_id, payload)
                self._send_json(200, {"ok": True, "citation_list": citation_list})
                return

            if creation_citations_generate_match:
                task_id = creation_citations_generate_match.group(1)
                citation_payload = creation_service.generate_citation_list(task_id)
                self._send_json(200, citation_payload)
                return

            if creation_citation_fetch_body_match:
                task_id = creation_citation_fetch_body_match.group(1)
                candidate = creation_service.get_primary_source_fetch_candidate(task_id)
                retry_count_raw = payload.get("retry_count", 1)
                try:
                    retry_count = int(retry_count_raw)
                except Exception:  # noqa: BLE001
                    retry_count = 1
                analyze_raw = payload.get("analyze", False)
                analyze = analyze_raw is True or str(analyze_raw).strip().lower() in {"1", "true", "yes", "on"}
                save_to_db_raw = payload.get("save_to_db", False)
                save_to_db = save_to_db_raw is True or str(save_to_db_raw).strip().lower() in {"1", "true", "yes", "on"}
                fetch_task = create_fetch_task(
                    url=candidate.get("url") or "",
                    retry_count=retry_count,
                    analyze=analyze,
                    save_to_db=save_to_db,
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "task_id": task_id,
                        "source_id": candidate.get("source_id") or "",
                        "title": candidate.get("title") or "",
                        "url": candidate.get("url") or "",
                        "reason": candidate.get("reason") or "",
                        "fetch_task": fetch_task,
                        "gates": candidate.get("gates") or creation_service._build_gate_report(task_id),
                    },
                )
                return

            if creation_citation_update_match:
                task_id, citation_id = creation_citation_update_match.groups()
                citation_payload = creation_service.update_citation(task_id, citation_id, payload)
                self._send_json(200, citation_payload)
                return

            if creation_outline_match:
                task_id = creation_outline_match.group(1)
                outline_packet = creation_service.save_outline_packet(task_id, payload)
                self._send_json(200, {"ok": True, "outline_packet": outline_packet})
                return

            if creation_outline_generate_match:
                task_id = creation_outline_generate_match.group(1)
                outline_payload = creation_service.generate_outline_packet(task_id, payload)
                self._send_json(200, outline_payload)
                return

            if creation_bootstrap_packet_task_match:
                task_id = creation_bootstrap_packet_task_match.group(1)
                bootstrap_payload = creation_service.bootstrap_packet_task(task_id, payload)
                self._send_json(200, bootstrap_payload)
                return

            if creation_writer_generate_match:
                task_id = creation_writer_generate_match.group(1)
                writer_job = creation_service.generate_writer_job(task_id, payload)
                self._send_json(200, {"ok": True, "writer_job": writer_job})
                return

            if creation_writer_update_match:
                task_id = creation_writer_update_match.group(1)
                writer_job = creation_service.update_writer_job_status(task_id, payload)
                self._send_json(200, {"ok": True, "writer_job": writer_job})
                return

            if creation_article_generate_match:
                task_id = creation_article_generate_match.group(1)
                draft_payload = creation_service.generate_article_draft(task_id, payload)
                self._send_json(200, draft_payload)
                return

            if creation_article_update_match:
                draft_id = creation_article_update_match.group(1)
                draft_payload = creation_service.save_article_draft(draft_id, payload)
                self._send_json(200, draft_payload)
                return

            if self.path == "/api/accounts/toggle":
                platform = str(payload.get("platform") or "x").strip().lower() or "x"
                handle = str(payload.get("handle") or payload.get("id") or "").strip()
                enabled = bool(payload.get("enabled"))
                account = set_account_enabled(handle, enabled, platform=platform)
                self._send_json(200, {"ok": True, "account": account})
                return

            if self.path == "/api/accounts/add":
                account = add_account(payload.get("account") or {})
                self._send_json(200, {"ok": True, "account": account})
                return

            if self.path == "/api/accounts/delete":
                platform = str(payload.get("platform") or "x").strip().lower() or "x"
                handle = str(payload.get("handle") or payload.get("id") or "").strip()
                delete_account(handle, platform=platform)
                self._send_json(200, {"ok": True, "handle": handle, "platform": platform})
                return

            if self.path == "/api/accounts/test":
                platform = str(payload.get("platform") or "x").strip().lower() or "x"
                handle = str(payload.get("handle") or payload.get("id") or "").strip()
                result = test_account(handle, platform=platform)
                self._send_json(200, result)
                return

            if self.path == "/api/templates/upsert":
                template = upsert_template(payload.get("template") or {})
                self._send_json(200, {"ok": True, "template": template})
                return

            if self.path == "/api/templates/delete":
                template_id = str(payload.get("id") or "").strip()
                delete_template(template_id)
                self._send_json(200, {"ok": True, "id": template_id})
                return

            if self.path == "/api/history/rerun":
                history_id = str(payload.get("id") or "").strip()
                if not history_id:
                    raise ValueError("历史记录 id 不能为空")
                entry = get_history_entry(history_id)
                if not entry:
                    raise ValueError(f"历史记录不存在：{history_id}")
                rerun_payload = dict(entry.get("payload") or {})
                result = search_candidates(rerun_payload)
                try:
                    new_id = append_search_history(rerun_payload, result, rerun_from=history_id)
                    result["history_id"] = new_id
                    result["rerun_from"] = history_id
                except Exception:  # noqa: BLE001
                    result["rerun_from"] = history_id
                self._send_json(200, result)
                return

            if self.path in fetch_aliases:
                target_url = str(payload.get("url") or "").strip()
                urls_payload = payload.get("urls") or []
                urls: list[str] = []
                if isinstance(urls_payload, list):
                    urls = [str(x or "").strip() for x in urls_payload if str(x or "").strip()]
                urls_text = str(payload.get("urls_text") or "").strip()
                if urls_text:
                    urls.extend([line.strip() for line in urls_text.splitlines() if line.strip() and not line.strip().startswith("#")])
                if target_url:
                    urls.insert(0, target_url)
                vault = str(payload.get("vault") or "").strip()
                retry_count = int(payload.get("retry_count", 1))
                analyze_raw = payload.get("analyze", False)
                analyze = analyze_raw is True or str(analyze_raw).strip().lower() in {"1", "true", "yes", "on"}
                save_to_db_raw = payload.get("save_to_db", False)
                save_to_db = save_to_db_raw is True or str(save_to_db_raw).strip().lower() in {"1", "true", "yes", "on"}
                if self.path == "/api/fetch/tasks":
                    task = create_fetch_task(urls=urls, vault=vault, retry_count=retry_count, analyze=analyze, save_to_db=save_to_db)
                    self._send_json(200, task)
                    return
                result = run_content_fetch_hub(urls=urls, vault=vault, retry_count=retry_count, analyze=analyze, save_to_db=save_to_db)
                self._send_json(200, result)
                return

            if cancel_match:
                task = cancel_fetch_task(cancel_match.group(1))
                if not task:
                    self._send_json(404, {"ok": False, "message": "任务不存在"})
                    return
                self._send_json(200, task)
                return

            if self.path == "/api/fetch/open-dir":
                vault = str(payload.get("vault") or "").strip()
                result = open_fetch_output_dir(vault)
                self._send_json(200, {"ok": True, **result})
                return

            template_id = str(payload.get("id") or "").strip()
            enabled = bool(payload.get("enabled"))
            template = set_template_enabled(template_id, enabled)
            self._send_json(200, {"ok": True, "template": template})
        except Exception as exc:  # noqa: BLE001
            self._send_json(400, {"ok": False, "message": str(exc)})


def _topic_sort_clause(sort_by: str) -> str:
    mapping = {
        "overall": "ts.overall_score DESC, ts.importance_score DESC, COALESCE(t.last_seen_at, ts.generated_at, t.updated_at) DESC, t.id DESC",
        "importance": "ts.importance_score DESC, ts.overall_score DESC, COALESCE(t.last_seen_at, ts.generated_at, t.updated_at) DESC, t.id DESC",
        "impact": "ts.impact_score DESC, ts.overall_score DESC, COALESCE(t.last_seen_at, ts.generated_at, t.updated_at) DESC, t.id DESC",
        "articles": "ts.evidence_article_count DESC, ts.evidence_event_count DESC, ts.overall_score DESC, t.id DESC",
        "events": "ts.evidence_event_count DESC, ts.evidence_article_count DESC, ts.overall_score DESC, t.id DESC",
        "latest": "COALESCE(t.last_seen_at, ts.generated_at, t.updated_at) DESC, ts.overall_score DESC, t.id DESC",
    }
    return mapping.get(str(sort_by or "overall").strip().lower(), mapping["overall"])


def _topics_filter_sql(cur: sqlite3.Cursor) -> str:
    try:
        has_v2 = bool(
            cur.execute(
                "SELECT 1 FROM topics WHERE topic_type='article_theme_v2' AND COALESCE(is_active, 1)=1 LIMIT 1"
            ).fetchone()
        )
    except sqlite3.OperationalError:
        has_v2 = False
    if has_v2:
        return " AND COALESCE(t.is_active, 1)=1 AND t.topic_type='article_theme_v2' "
    return " AND COALESCE(t.is_active, 1)=1 "


def list_topics(sort_by: str = "overall", page: int = 1, limit: int = 10, keyword: str = "") -> dict[str, Any]:
    normalized_page = max(1, int(page or 1))
    normalized_limit = max(1, min(int(limit or 10), 50))
    normalized_keyword = str(keyword or "").strip()
    normalized_sort = str(sort_by or "overall")
    _maybe_refresh_local_topics(force=False)
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
        local_payload = _list_topics_from_local_tables(conn, sort_by=normalized_sort, page=normalized_page, limit=normalized_limit, keyword=normalized_keyword)
    if local_payload.get("total"):
        return local_payload

    upstream_params = urlencode(
        {
            "sort": normalized_sort,
            "page": str(normalized_page),
            "limit": str(normalized_limit),
            "keyword": normalized_keyword,
        }
    )
    try:
        payload = _http_json(f"{TOPIC_UPSTREAM_BASE}/api/topics?{upstream_params}", timeout_sec=TOPIC_UPSTREAM_TIMEOUT_SEC)
        items = list(payload.get("items") or [])
        EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
            _cache_upstream_topic_items(conn, items)
        return {
            "items": items,
            "total": int(payload.get("total") or 0),
            "page": int(payload.get("page") or normalized_page),
            "page_size": int(payload.get("page_size") or normalized_limit),
            "total_pages": int(payload.get("total_pages") or 1),
            "sort_by": str(payload.get("sort_by") or normalized_sort),
            "keyword": str(payload.get("keyword") or normalized_keyword).strip(),
            "data_source": "nighthawk_upstream_api",
        }
    except Exception:  # noqa: BLE001
        EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cached = _list_topics_from_api_cache(conn, sort_by=normalized_sort, page=normalized_page, limit=normalized_limit, keyword=normalized_keyword)
        if cached.get("total"):
            return cached
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
        return _list_topics_from_local_tables(conn, sort_by=normalized_sort, page=normalized_page, limit=normalized_limit, keyword=normalized_keyword)


def get_topic_detail(
    topic_id: int,
    event_page: int = 1,
    event_limit: int = 10,
    article_page: int = 1,
    article_limit: int = 8,
) -> dict[str, Any]:
    _maybe_refresh_local_topics(force=False)
    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        latest_scores_sql = "SELECT MAX(id) AS latest_id FROM topic_scores GROUP BY topic_id"
        try:
            topic_row = cur.execute(
                f"""
                SELECT
                    t.id,
                    t.topic_key,
                    t.title,
                    t.summary,
                    t.topic_type,
                    t.status,
                    t.time_window_hours,
                    t.first_seen_at,
                    t.last_seen_at,
                    t.primary_platforms_json,
                    t.primary_entities_json,
                    t.risk_flags_json,
                    ts.importance_score,
                    ts.impact_score,
                    ts.creation_potential_score,
                    ts.overall_score,
                    ts.evidence_event_count,
                    ts.evidence_article_count,
                    ts.evidence_source_count,
                    ts.evidence_platform_count,
                    ts.card_summary,
                    ts.recommended_angles_json,
                    ts.recommended_formats_json,
                    ts.emotion_points_json,
                    ts.debate_points_json
                FROM topics t
                JOIN topic_scores ts ON ts.topic_id = t.id
                JOIN ({latest_scores_sql}) latest ON latest.latest_id = ts.id
                WHERE t.id=?
                """,
                (int(topic_id),),
            ).fetchone()
        except sqlite3.OperationalError:
            topic_row = None
        if topic_row:
            topic = dict(topic_row)
            for field in [
                "primary_platforms_json",
                "primary_entities_json",
                "risk_flags_json",
                "recommended_angles_json",
                "recommended_formats_json",
                "emotion_points_json",
                "debate_points_json",
            ]:
                try:
                    topic[field.replace("_json", "")] = json.loads(topic.get(field) or "[]")
                except Exception:
                    topic[field.replace("_json", "")] = []
                topic.pop(field, None)
            topic["first_seen_at"] = _format_datetime_text(topic.get("first_seen_at") or "")
            topic["last_seen_at"] = _format_datetime_text(topic.get("last_seen_at") or "")
            topic["summary"] = _trim_text(topic.get("summary") or "", 400)
            topic["card_summary"] = _trim_text(topic.get("card_summary") or "", 400)

            has_topic_articles = bool(
                cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='topic_articles'").fetchone()
            )
            if str(topic.get("topic_type") or "").strip() == "article_theme_v2" and has_topic_articles:
                article_page_size = max(1, min(int(article_limit or 8), 30))
                article_page = max(1, int(article_page or 1))
                article_total = int(
                    cur.execute("SELECT COUNT(*) FROM topic_articles WHERE topic_id=?", (int(topic_id),)).fetchone()[0] or 0
                )
                article_total_pages = max(1, (article_total + article_page_size - 1) // article_page_size) if article_total else 1
                if article_page > article_total_pages:
                    article_page = article_total_pages
                article_offset = (article_page - 1) * article_page_size
                article_rows = cur.execute(
                    """
                    SELECT
                        ta.raw_item_id AS id,
                        COALESCE(ta.article_title, ri.title) AS title,
                        COALESCE(ta.platform, ri.platform) AS platform,
                        COALESCE(ta.canonical_url, ri.url) AS url,
                        COALESCE(ta.published_at, ri.published_at, ri.fetched_at) AS published_at,
                        ri.fetched_at,
                        ri.body_status,
                        ri.content
                    FROM topic_articles ta
                    LEFT JOIN raw_items ri ON ri.id = ta.raw_item_id
                    WHERE ta.topic_id=?
                    ORDER BY COALESCE(ta.published_at, ri.published_at, ri.fetched_at) DESC, ta.raw_item_id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (int(topic_id), article_page_size, article_offset),
                ).fetchall()
                articles = []
                for row in article_rows:
                    item = dict(row)
                    item["published_at"] = _format_datetime_text(item.get("published_at") or "")
                    item["fetched_at"] = _format_datetime_text(item.get("fetched_at") or "")
                    item["title"] = _trim_text(item.get("title") or "", 160)
                    item["body_status"] = str(item.get("body_status") or "").strip() or "unknown"
                    item["body_text"] = str(item.get("content") or "").strip()
                    item["content_excerpt"] = _trim_text(item.get("body_text") or "", 220)
                    item.pop("content", None)
                    articles.append(enrich_raw_item_translations(item, include_body=False, allow_live_translate=False))

                return {
                    "ok": True,
                    "topic": topic,
                    "events": [],
                    "event_pagination": {
                        "total": 0,
                        "page": 1,
                        "page_size": max(1, min(int(event_limit or 10), 50)),
                        "total_pages": 1,
                    },
                    "articles": articles,
                    "article_pagination": {
                        "total": article_total,
                        "page": article_page,
                        "page_size": article_page_size,
                        "total_pages": article_total_pages,
                    },
                    "data_source": "windows_local_topics",
                }
    finally:
        conn.close()

    upstream_params = urlencode(
        {
            "event_page": str(max(1, int(event_page or 1))),
            "event_limit": str(max(1, min(int(event_limit or 10), 50))),
            "article_page": str(max(1, int(article_page or 1))),
            "article_limit": str(max(1, min(int(article_limit or 8), 30))),
        }
    )
    try:
        payload = _http_json(f"{TOPIC_UPSTREAM_BASE}/api/topics/{int(topic_id)}?{upstream_params}", timeout_sec=TOPIC_UPSTREAM_TIMEOUT_SEC)
        if payload.get("ok"):
            EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
                _cache_upstream_topic_detail(conn, payload)
            payload["data_source"] = "nighthawk_upstream_api"
            return payload
    except Exception:  # noqa: BLE001
        EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(EVENT_RADAR_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cached = _get_topic_detail_from_api_cache(conn, int(topic_id))
        if cached:
            return cached

    EVENT_RADAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(EVENT_RADAR_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        latest_scores_sql = "SELECT MAX(id) AS latest_id FROM topic_scores GROUP BY topic_id"
        try:
            topic_row = cur.execute(
                f"""
                SELECT
                    t.id,
                    t.topic_key,
                    t.title,
                    t.summary,
                    t.topic_type,
                    t.status,
                    t.time_window_hours,
                    t.first_seen_at,
                    t.last_seen_at,
                    t.primary_platforms_json,
                    t.primary_entities_json,
                    t.risk_flags_json,
                    ts.importance_score,
                    ts.impact_score,
                    ts.creation_potential_score,
                    ts.overall_score,
                    ts.evidence_event_count,
                    ts.evidence_article_count,
                    ts.evidence_source_count,
                    ts.evidence_platform_count,
                    ts.card_summary,
                    ts.recommended_angles_json,
                    ts.recommended_formats_json,
                    ts.emotion_points_json,
                    ts.debate_points_json
                FROM topics t
                JOIN topic_scores ts ON ts.topic_id = t.id
                JOIN ({latest_scores_sql}) latest ON latest.latest_id = ts.id
                WHERE t.id=?
                """,
                (int(topic_id),),
            ).fetchone()
        except sqlite3.OperationalError:
            return {"ok": False, "message": "topics/topic_scores 表不存在"}
        if not topic_row:
            return {"ok": False, "message": "主题不存在"}

        topic = dict(topic_row)
        for field in [
            "primary_platforms_json",
            "primary_entities_json",
            "risk_flags_json",
            "recommended_angles_json",
            "recommended_formats_json",
            "emotion_points_json",
            "debate_points_json",
        ]:
            try:
                topic[field.replace("_json", "")] = json.loads(topic.get(field) or "[]")
            except Exception:
                topic[field.replace("_json", "")] = []
            topic.pop(field, None)
        topic["first_seen_at"] = _format_datetime_text(topic.get("first_seen_at") or "")
        topic["last_seen_at"] = _format_datetime_text(topic.get("last_seen_at") or "")
        topic["summary"] = _trim_text(topic.get("summary") or "", 400)
        topic["card_summary"] = _trim_text(topic.get("card_summary") or "", 400)

        has_topic_articles = False
        try:
            has_topic_articles = bool(
                cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='topic_articles'").fetchone()
            )
        except sqlite3.OperationalError:
            has_topic_articles = False

        if str(topic.get("topic_type") or "").strip() == "article_theme_v2" and has_topic_articles:
            article_page_size = max(1, min(int(article_limit or 8), 30))
            article_page = max(1, int(article_page or 1))
            article_total = int(
                cur.execute("SELECT COUNT(*) FROM topic_articles WHERE topic_id=?", (int(topic_id),)).fetchone()[0] or 0
            )
            article_total_pages = max(1, (article_total + article_page_size - 1) // article_page_size) if article_total else 1
            if article_page > article_total_pages:
                article_page = article_total_pages
            article_offset = (article_page - 1) * article_page_size
            article_rows = cur.execute(
                """
                SELECT
                    ta.raw_item_id AS id,
                    COALESCE(ta.article_title, ri.title) AS title,
                    COALESCE(ta.platform, ri.platform) AS platform,
                    COALESCE(ta.canonical_url, ri.url) AS url,
                    COALESCE(ta.published_at, ri.published_at, ri.fetched_at) AS published_at,
                    ri.fetched_at,
                    ri.body_status,
                    ri.content
                FROM topic_articles ta
                LEFT JOIN raw_items ri ON ri.id = ta.raw_item_id
                WHERE ta.topic_id=?
                ORDER BY COALESCE(ta.published_at, ri.published_at, ri.fetched_at) DESC, ta.raw_item_id DESC
                LIMIT ? OFFSET ?
                """,
                (int(topic_id), article_page_size, article_offset),
            ).fetchall()
            articles = []
            for row in article_rows:
                item = dict(row)
                item["published_at"] = _format_datetime_text(item.get("published_at") or "")
                item["fetched_at"] = _format_datetime_text(item.get("fetched_at") or "")
                item["title"] = _trim_text(item.get("title") or "", 160)
                item["body_status"] = str(item.get("body_status") or "").strip() or "unknown"
                item["body_text"] = str(item.get("content") or "").strip()
                item["content_excerpt"] = _trim_text(item.get("body_text") or "", 220)
                item.pop("content", None)
                articles.append(enrich_raw_item_translations(item, include_body=False, allow_live_translate=False))

            return {
                "ok": True,
                "topic": topic,
                "events": [],
                "event_pagination": {
                    "total": 0,
                    "page": 1,
                    "page_size": max(1, min(int(event_limit or 10), 50)),
                    "total_pages": 1,
                },
                "articles": articles,
                "article_pagination": {
                    "total": article_total,
                    "page": article_page,
                    "page_size": article_page_size,
                    "total_pages": article_total_pages,
                },
            }

        page_size = max(1, min(int(event_limit or 10), 50))
        page = max(1, int(event_page or 1))
        total = int(cur.execute("SELECT COUNT(*) FROM topic_events WHERE topic_id=?", (int(topic_id),)).fetchone()[0] or 0)
        total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * page_size
        event_rows = cur.execute(
            """
            SELECT
                ec.id,
                ec.event_type,
                ec.title,
                ec.summary,
                ec.subject,
                ec.object,
                ec.first_seen_at,
                ec.last_seen_at,
                ec.confidence,
                COUNT(ee.raw_item_id) AS article_count
            FROM topic_events te
            JOIN event_candidates ec ON ec.id = te.event_id
            LEFT JOIN event_evidence ee ON ee.event_id = ec.id
            WHERE te.topic_id=?
            GROUP BY ec.id
            ORDER BY COALESCE(ec.last_seen_at, ec.first_seen_at, ec.created_at) DESC, ec.id DESC
            LIMIT ? OFFSET ?
            """,
            (int(topic_id), page_size, offset),
        ).fetchall()
        events = []
        for row in event_rows:
            item = dict(row)
            item["first_seen_at"] = _format_datetime_text(item.get("first_seen_at") or "")
            item["last_seen_at"] = _format_datetime_text(item.get("last_seen_at") or "")
            item["summary"] = _trim_text(item.get("summary") or "", 280)
            events.append(item)
        article_page_size = max(1, min(int(article_limit or 8), 30))
        article_page = max(1, int(article_page or 1))
        article_total = int(
            cur.execute(
                """
                SELECT COUNT(DISTINCT ri.id)
                FROM topic_events te
                JOIN event_evidence ee ON ee.event_id = te.event_id
                JOIN raw_items ri ON ri.id = ee.raw_item_id
                WHERE te.topic_id=?
                """,
                (int(topic_id),),
            ).fetchone()[0]
            or 0
        )
        article_total_pages = max(1, (article_total + article_page_size - 1) // article_page_size) if article_total else 1
        if article_page > article_total_pages:
            article_page = article_total_pages
        article_offset = (article_page - 1) * article_page_size
        article_rows = cur.execute(
            """
            SELECT
                ri.id,
                ri.title,
                ri.platform,
                ri.url,
                ri.published_at,
                ri.fetched_at,
                ri.body_status,
                ri.content
            FROM topic_events te
            JOIN event_evidence ee ON ee.event_id = te.event_id
            JOIN raw_items ri ON ri.id = ee.raw_item_id
            WHERE te.topic_id=?
            GROUP BY ri.id
            ORDER BY COALESCE(ri.published_at, ri.fetched_at) DESC, ri.id DESC
            LIMIT ? OFFSET ?
            """,
            (int(topic_id), article_page_size, article_offset),
        ).fetchall()
        articles = []
        for row in article_rows:
            item = dict(row)
            item["published_at"] = _format_datetime_text(item.get("published_at") or "")
            item["fetched_at"] = _format_datetime_text(item.get("fetched_at") or "")
            item["title"] = _trim_text(item.get("title") or "", 160)
            item["body_status"] = str(item.get("body_status") or "").strip() or "unknown"
            item["body_text"] = str(item.get("content") or "").strip()
            item["content_excerpt"] = _trim_text(item.get("body_text") or "", 220)
            item.pop("content", None)
            articles.append(enrich_raw_item_translations(item, include_body=False, allow_live_translate=False))

        return {
            "ok": True,
            "topic": topic,
            "events": events,
            "event_pagination": {
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
            },
            "articles": articles,
            "article_pagination": {
                "total": article_total,
                "page": article_page,
                "page_size": article_page_size,
                "total_pages": article_total_pages,
            },
        }
    finally:
        conn.close()


def _topic_auto_sync_interval_sec() -> int:
    cfg = get_create_studio_config()
    event_cfg = dict(cfg.get("event_clustering") or {})
    try:
        minutes = int(event_cfg.get("auto_run_interval_minutes") or 20)
    except Exception:  # noqa: BLE001
        minutes = 20
    return max(300, minutes * 60)


def _topic_auto_sync_enabled() -> bool:
    cfg = get_create_studio_config()
    event_cfg = dict(cfg.get("event_clustering") or {})
    raw = event_cfg.get("auto_run_enabled", True)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _topic_auto_sync_loop() -> None:
    while True:
        if not _topic_auto_sync_enabled():
            time.sleep(60)
            continue
        try:
            result = _maybe_refresh_local_topics(force=True, full_rebuild=False)
            print(
                "[topic-auto-sync] "
                f"caught_up={bool(result.get('pipeline_caught_up'))} "
                f"processed={int(result.get('state_last_raw_item_id') or 0)} "
                f"raw_max={int(result.get('raw_max_id') or 0)} "
                f"mode={str(result.get('sync_mode') or '').strip() or 'check'}"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[topic-auto-sync] failed: {exc}")
        time.sleep(_topic_auto_sync_interval_sec())


def _start_topic_auto_sync_thread() -> None:
    global TOPIC_AUTO_SYNC_THREAD_STARTED
    with TOPIC_AUTO_SYNC_THREAD_LOCK:
        if TOPIC_AUTO_SYNC_THREAD_STARTED:
            return
        TOPIC_AUTO_SYNC_THREAD_STARTED = True
    thread = threading.Thread(target=_topic_auto_sync_loop, name="topic-auto-sync", daemon=True)
    thread.start()


def run_server(cfg: AppConfig):
    try:
        sync_whitelist_accounts_to_event_db(get_accounts(enabled_only=False))
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] init sqlite account sync failed: {exc}")

    _start_topic_auto_sync_thread()

    server = ThreadingHTTPServer((cfg.host, cfg.port), Handler)
    print(f"[content-search-layer] dashboard running at http://{cfg.host}:{cfg.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=service_host("creative_studio"))
    parser.add_argument("--port", type=int, default=service_port("creative_studio", 8791))
    args = parser.parse_args()
    run_server(AppConfig(host=args.host, port=args.port))


if __name__ == "__main__":
    main()
