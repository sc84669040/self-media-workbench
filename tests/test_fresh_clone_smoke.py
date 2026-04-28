from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

from self_media_config import REPO_ROOT, get_config, get_path, service


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )


def test_default_config_is_local_first() -> None:
    config = get_config()
    assert get_path(config, "paths.runtime_dir") == REPO_ROOT / "runtime"
    assert get_path(config, "paths.event_radar_db_path") == REPO_ROOT / "runtime" / "event_radar.db"
    assert int(service(config, "creative_studio").get("port")) == 8791
    assert int(service(config, "content_search_layer").get("port")) == 8787


def test_runtime_initialization_creates_sample_chain() -> None:
    completed = run_script("scripts/init_runtime.py", "--profile", "sample")
    assert completed.returncode == 0, completed.stderr or completed.stdout

    config = get_config()
    event_db = get_path(config, "paths.event_radar_db_path")
    assert event_db.exists()
    with sqlite3.connect(event_db) as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM raw_items WHERE platform='sample'").fetchone()[0]
        topic_count = conn.execute("SELECT COUNT(*) FROM topic_api_cache_topics").fetchone()[0]
    assert raw_count >= 1
    assert topic_count >= 1
    assert get_path(config, "paths.sample_vault_path").joinpath("analysis-cards", "sample-card.md").exists()


def test_runtime_initialization_keeps_public_source_catalogs() -> None:
    source_files = [
        REPO_ROOT / "config" / "sources" / "a-stage-wechat-sources.json",
        REPO_ROOT / "config" / "sources" / "a-stage-youtube-channels.json",
        REPO_ROOT / "config" / "sources" / "a-stage-x-sources.json",
    ]
    before = {path: path.read_text(encoding="utf-8") for path in source_files}

    completed = run_script("scripts/init_runtime.py", "--profile", "sample")

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert {path: path.read_text(encoding="utf-8") for path in source_files} == before


def test_secret_scan_passes() -> None:
    completed = run_script("scripts/scan_secrets.py", "--verbose")
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_scheduler_reads_central_config_once() -> None:
    completed = run_script("scripts/scheduler.py", "--once")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "[scheduler] running index_sync" in completed.stdout


def test_creative_studio_config_uses_root_runtime() -> None:
    scripts_dir = REPO_ROOT / "apps" / "creative-studio" / "scripts"
    sys.path.insert(0, str(scripts_dir))
    try:
        from create_studio_config import load_create_studio_config

        config = load_create_studio_config()
    finally:
        try:
            sys.path.remove(str(scripts_dir))
        except ValueError:
            pass

    assert Path(config["database_sources"]["nighthawk_db_path"]) == REPO_ROOT / "runtime" / "event_radar.db"
    assert Path(config["indexing"]["content_index_db_path"]) == REPO_ROOT / "runtime" / "create_studio.db"
