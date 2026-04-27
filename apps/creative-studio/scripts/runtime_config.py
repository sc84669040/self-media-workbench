from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


def _repo_root(start: Path | None = None) -> Path:
    current = Path(start or __file__).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "config" / "default.yaml").exists():
            return candidate
    return Path.cwd().resolve()


REPO_ROOT = _repo_root()
ROOT_SCRIPTS_DIR = REPO_ROOT / "scripts"
root_scripts_text = str(ROOT_SCRIPTS_DIR)
if root_scripts_text in sys.path:
    sys.path.remove(root_scripts_text)
sys.path.insert(0, root_scripts_text)

from self_media_config import get_config, get_path, service  # noqa: E402


def config() -> dict[str, Any]:
    return get_config()


def event_radar_db_path() -> Path:
    override = str(os.getenv("EVENT_RADAR_DB_PATH") or "").strip()
    return Path(override).expanduser() if override else get_path(config(), "paths.event_radar_db_path")


def event_radar_mirror_db_path() -> Path:
    override = str(os.getenv("EVENT_RADAR_MIRROR_DB_PATH") or "").strip()
    return Path(override).expanduser() if override else get_path(config(), "paths.event_radar_mirror_db_path")


def sample_vault_path() -> Path:
    override = str(os.getenv("SELF_MEDIA_VAULT_PATH") or "").strip()
    return Path(override).expanduser() if override else get_path(config(), "paths.sample_vault_path")


def service_host(name: str, default: str = "127.0.0.1") -> str:
    return str(service(config(), name).get("host") or default)


def service_port(name: str, default: int) -> int:
    try:
        return int(service(config(), name).get("port") or default)
    except Exception:
        return default


def service_base_url(name: str, default_port: int) -> str:
    return f"http://{service_host(name)}:{service_port(name, default_port)}"
