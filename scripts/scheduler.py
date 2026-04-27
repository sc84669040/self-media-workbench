from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass

from self_media_config import REPO_ROOT, get_config, get_path, get_value


@dataclass
class JobState:
    name: str
    command: str
    interval_sec: int
    lookback_hours: int = 0
    concurrency: int = 1
    retry_count: int = 0
    next_run: float = 0.0


def load_jobs() -> list[JobState]:
    config = get_config()
    jobs = get_value(config, "scheduler.jobs", {}) or {}
    states: list[JobState] = []
    for name, raw in jobs.items():
        if not isinstance(raw, dict) or not raw.get("enabled"):
            continue
        command = str(raw.get("command") or "").strip()
        if not command:
            continue
        interval_minutes = max(1, int(raw.get("interval_minutes") or 60))
        states.append(
            JobState(
                name=name,
                command=command,
                interval_sec=interval_minutes * 60,
                lookback_hours=int(raw.get("lookback_hours") or 0),
                concurrency=max(1, int(raw.get("concurrency") or 1)),
                retry_count=max(0, int(raw.get("retry_count") or 0)),
            )
        )
    return states


def run_job(job: JobState) -> None:
    config = get_config()
    env = os.environ.copy()
    env.setdefault("SELF_MEDIA_HOME", str(REPO_ROOT))
    env.setdefault("EVENT_RADAR_DB_PATH", str(get_path(config, "paths.event_radar_db_path")))
    env.setdefault("CREATE_STUDIO_INDEX_DB_PATH", str(get_path(config, "paths.create_studio_db_path")))
    env.setdefault("CONTENT_SEARCH_CREATION_DATA_ROOT", str(get_path(config, "paths.creation_data_root")))
    if job.lookback_hours:
        env["SELF_MEDIA_JOB_LOOKBACK_HOURS"] = str(job.lookback_hours)
    env["SELF_MEDIA_JOB_CONCURRENCY"] = str(job.concurrency)
    cmd = shlex.split(job.command, posix=False)
    if cmd and cmd[0].lower() == "python":
        cmd[0] = sys.executable
    for attempt in range(job.retry_count + 1):
        print(f"[scheduler] running {job.name} attempt={attempt + 1}: {' '.join(cmd)}")
        completed = subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=False)
        print(f"[scheduler] {job.name} exited with {completed.returncode}")
        if completed.returncode == 0:
            return
        if attempt < job.retry_count:
            time.sleep(min(30, 2 ** attempt))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run enabled jobs once and exit.")
    args = parser.parse_args()

    jobs = load_jobs()
    if not jobs:
        print("No scheduler jobs enabled. Edit config/local/local.yaml to enable jobs.")
        return 0

    for job in jobs:
        job.next_run = time.time()

    while True:
        now = time.time()
        for job in jobs:
            if now >= job.next_run:
                run_job(job)
                job.next_run = time.time() + job.interval_sec
        if args.once:
            return 0
        time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main())
