from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable

from runtime_config import event_radar_mirror_db_path

DEFAULT_MIRROR_DB_PATH = event_radar_mirror_db_path()
MIRROR_ENABLED = os.environ.get("EVENT_RADAR_MIRROR_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
MIRROR_TIMEOUT_SEC = max(1, int(os.environ.get("EVENT_RADAR_MIRROR_TIMEOUT_SEC", "30")))
MIRROR_RETRIES = max(1, int(os.environ.get("EVENT_RADAR_MIRROR_RETRIES", "3")))
MIRROR_RETRY_SLEEP_SEC = max(0.2, float(os.environ.get("EVENT_RADAR_MIRROR_RETRY_SLEEP_SEC", "1.5")))


def _source_change_marker(db_path: Path) -> tuple[int, int]:
    latest_mtime_ns = 0
    wal_size = 0
    for suffix in ("", "-wal", "-shm"):
        candidate = db_path if not suffix else Path(f"{db_path}{suffix}")
        if not candidate.exists():
            continue
        try:
            stat = candidate.stat()
        except OSError:
            continue
        latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)
        if suffix == "-wal":
            wal_size = max(wal_size, stat.st_size)
    return latest_mtime_ns, wal_size


def _remove_sidecars(db_path: Path) -> None:
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            sidecar.unlink(missing_ok=True)


def mirror_event_radar_db(
    source_db_path: str | Path,
    *,
    mirror_db_path: str | Path | None = None,
    force: bool = False,
    logger: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    source_path = Path(source_db_path).expanduser()
    target_path = Path(mirror_db_path).expanduser() if mirror_db_path else DEFAULT_MIRROR_DB_PATH
    summary: dict[str, Any] = {
        "ok": False,
        "enabled": MIRROR_ENABLED,
        "source_db_path": str(source_path),
        "mirror_db_path": str(target_path),
        "mirrored": False,
        "skipped": False,
    }
    if not MIRROR_ENABLED:
        summary.update({"ok": True, "skipped": True, "reason": "disabled"})
        return summary
    if not source_path.exists():
        summary["reason"] = "source_missing"
        return summary

    target_path.parent.mkdir(parents=True, exist_ok=True)
    if not force and target_path.exists():
        try:
            source_marker = _source_change_marker(source_path)
            target_marker = _source_change_marker(target_path)
            target_stat = target_path.stat()
            if target_stat.st_size > 0 and target_marker >= source_marker:
                summary.update({"ok": True, "skipped": True, "reason": "up_to_date"})
                return summary
        except OSError:
            pass

    tmp_path = target_path.with_name(f"{target_path.name}.tmp-{os.getpid()}")
    source_uri = f"file:{source_path.as_posix()}?mode=ro"
    last_error: Exception | None = None

    for attempt in range(1, MIRROR_RETRIES + 1):
        try:
            tmp_path.unlink(missing_ok=True)
            source_conn = sqlite3.connect(source_uri, uri=True, timeout=MIRROR_TIMEOUT_SEC)
            target_conn = sqlite3.connect(tmp_path, timeout=MIRROR_TIMEOUT_SEC)
            try:
                source_conn.execute(f"PRAGMA busy_timeout = {MIRROR_TIMEOUT_SEC * 1000}")
                target_conn.execute(f"PRAGMA busy_timeout = {MIRROR_TIMEOUT_SEC * 1000}")
                source_conn.backup(target_conn)
                target_conn.commit()
            finally:
                target_conn.close()
                source_conn.close()
            for _ in range(3):
                try:
                    os.replace(tmp_path, target_path)
                    break
                except PermissionError:
                    time.sleep(0.2)
            else:
                os.replace(tmp_path, target_path)
            _remove_sidecars(target_path)
            summary.update({"ok": True, "mirrored": True, "attempt": attempt})
            if logger:
                logger(f"[mirror] synced -> {target_path}")
            return summary
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if logger:
                logger(f"[mirror] attempt={attempt} failed: {exc}")
            time.sleep(MIRROR_RETRY_SLEEP_SEC)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except PermissionError:
                pass

    summary["reason"] = str(last_error or "mirror_failed")
    return summary
