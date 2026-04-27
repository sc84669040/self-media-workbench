from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from self_media_config import REPO_ROOT, get_config, get_path, service


def start_process(name: str, command: list[str], env: dict[str, str]) -> subprocess.Popen:
    print(f"starting {name}: {' '.join(command)}")
    return subprocess.Popen(command, cwd=REPO_ROOT, env=env)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-fetch-hub", action="store_true", help="Also start the content-fetch-hub API.")
    parser.add_argument("--creative-only", action="store_true", help="Start only Creative Studio.")
    parser.add_argument("--no-search-layer", action="store_true", help="Do not start the content-search-layer dashboard.")
    args = parser.parse_args()

    config = get_config()
    env = os.environ.copy()
    env.setdefault("SELF_MEDIA_HOME", str(REPO_ROOT))
    env.setdefault("SELF_MEDIA_CONFIG_PATH", str(Path(config["_meta"]["selected_config_path"])))
    env.setdefault("EVENT_RADAR_DB_PATH", str(get_path(config, "paths.event_radar_db_path")))
    env.setdefault("CREATE_STUDIO_INDEX_DB_PATH", str(get_path(config, "paths.create_studio_db_path")))
    env.setdefault("CONTENT_SEARCH_CREATION_DATA_ROOT", str(get_path(config, "paths.creation_data_root")))

    processes: list[subprocess.Popen] = []
    try:
        if args.with_fetch_hub and not args.creative_only:
            fetch = service(config, "content_fetch_hub")
            processes.append(
                start_process(
                    "content-fetch-hub",
                    [
                        sys.executable,
                        "apps/content-fetch-hub/scripts/fetch_web_server.py",
                        "--host",
                        str(fetch.get("host", "127.0.0.1")),
                        "--port",
                        str(fetch.get("port", 8788)),
                    ],
                    env,
                )
            )

        if not args.creative_only and not args.no_search_layer:
            search = service(config, "content_search_layer")
            processes.append(
                start_process(
                    "content-search-layer",
                    [
                        sys.executable,
                        "apps/content-search-layer/web/search_dashboard.py",
                        "--host",
                        str(search.get("host", "127.0.0.1")),
                        "--port",
                        str(search.get("port", 8787)),
                    ],
                    env,
                )
            )

        creative = service(config, "creative_studio")
        processes.append(
            start_process(
                "creative-studio",
                [
                    sys.executable,
                    "apps/creative-studio/web/search_dashboard.py",
                    "--host",
                    str(creative.get("host", "127.0.0.1")),
                    "--port",
                    str(creative.get("port", 8791)),
                ],
                env,
            )
        )
        print("Press Ctrl+C to stop local services.")
        return processes[-1].wait()
    except KeyboardInterrupt:
        print("stopping services...")
        return 0
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
