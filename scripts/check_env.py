from __future__ import annotations

import importlib.util
import platform
import shutil
import sys

from self_media_config import get_config, get_path, get_value, service


def module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def main() -> int:
    config = get_config()
    ok = True

    print("self-media environment check")
    print(f"- Python: {platform.python_version()} ({sys.executable})")
    if sys.version_info < (3, 11):
        print("  ERROR: Python 3.11+ is required.")
        ok = False

    for module in ["yaml", "requests", "bs4"]:
        present = module_available(module)
        print(f"- Python module {module}: {'ok' if present else 'missing'}")
        ok = ok and present

    loaded = bool(get_value(config, "_meta.selected_config_loaded"))
    print(f"- Config: {get_value(config, '_meta.selected_config_path')} ({'loaded' if loaded else 'default-only'})")
    print(f"- Runtime: {get_path(config, 'paths.runtime_dir')}")
    print(f"- Event DB: {get_path(config, 'paths.event_radar_db_path')}")
    print(f"- Create DB: {get_path(config, 'paths.create_studio_db_path')}")

    for svc_name in ["creative_studio", "content_fetch_hub", "content_search_layer"]:
        svc = service(config, svc_name)
        print(f"- Service {svc_name}: {svc.get('host', '127.0.0.1')}:{svc.get('port', '')}")

    optional_tools = {
        "yt-dlp": get_value(config, "external_tools.yt_dlp_bin", "") or "yt-dlp",
        "twitter": get_value(config, "external_tools.twitter_bin", "") or "twitter",
        "ffmpeg": get_value(config, "external_tools.ffmpeg_bin", "") or "ffmpeg",
    }
    for label, command in optional_tools.items():
        available = bool(shutil.which(str(command)))
        print(f"- Optional tool {label}: {'ok' if available else 'not configured'}")

    print("Result:", "ok" if ok else "failed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
