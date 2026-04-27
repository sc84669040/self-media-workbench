from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml


def repo_root(start: Path | None = None) -> Path:
    env_home = os.getenv("SELF_MEDIA_HOME")
    if env_home:
        return Path(env_home).expanduser().resolve()

    current = Path(start or __file__).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "config" / "default.yaml").exists():
            return candidate
    return Path.cwd().resolve()


REPO_ROOT = repo_root()
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "default.yaml"
LOCAL_CONFIG_PATH = REPO_ROOT / "config" / "local" / "local.yaml"
ENV_PATH = REPO_ROOT / ".env"


def load_dotenv(path: Path = ENV_PATH) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _get_raw(config: dict[str, Any], dotted: str, default: Any = None) -> Any:
    cur: Any = config
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _expand_value(value: Any, variables: dict[str, str]) -> Any:
    if isinstance(value, str):
        out = value
        for key, replacement in variables.items():
            out = out.replace("${" + key + "}", replacement)
        return out
    if isinstance(value, list):
        return [_expand_value(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: _expand_value(item, variables) for key, item in value.items()}
    return value


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    load_dotenv()
    root = REPO_ROOT
    config = load_yaml(DEFAULT_CONFIG_PATH)
    selected = Path(config_path or os.getenv("SELF_MEDIA_CONFIG_PATH") or LOCAL_CONFIG_PATH).expanduser()
    if selected.exists():
        config = deep_merge(config, load_yaml(selected))

    runtime_raw = str(_get_raw(config, "paths.runtime_dir", str(root / "runtime")))
    variables = {
        "repo_root": str(root),
        "config_dir": str(root / "config"),
        "runtime_dir": runtime_raw.replace("${repo_root}", str(root)),
    }
    config = _expand_value(config, variables)

    config.setdefault("_meta", {})
    config["_meta"].update(
        {
            "repo_root": str(root),
            "default_config_path": str(DEFAULT_CONFIG_PATH),
            "selected_config_path": str(selected),
            "selected_config_loaded": selected.exists(),
        }
    )
    return config


def get_config() -> dict[str, Any]:
    return load_config()


def get_value(config: dict[str, Any], dotted: str, default: Any = None) -> Any:
    return _get_raw(config, dotted, default)


def get_path(config: dict[str, Any], dotted: str, default: str = "") -> Path:
    raw = str(get_value(config, dotted, default)).strip()
    if not raw:
        raw = default
    return Path(raw).expanduser()


def service(config: dict[str, Any], name: str) -> dict[str, Any]:
    return dict(get_value(config, f"services.{name}", {}) or {})


def env_or_config(config: dict[str, Any], env_name_path: str, value_path: str = "", default: str = "") -> str:
    env_name = str(get_value(config, env_name_path, "") or "").strip()
    if env_name and os.getenv(env_name):
        return str(os.getenv(env_name) or "").strip()
    if value_path:
        return str(get_value(config, value_path, default) or "").strip()
    return default
