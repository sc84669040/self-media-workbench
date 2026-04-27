#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from shutil import which
from typing import Any

import yaml

from runtime_config import config as root_config
from workspace_paths import MONITORING_ROOT, VENV_ROOT, resolve_skill_project_root

BASE_DIR = Path(__file__).resolve().parents[1]
YOUTUBE_CHANNELS_PATH = MONITORING_ROOT / "a-stage-youtube-channels.json"
YT_DLP_BIN = "yt-dlp"
YT_DLP_TIMEOUT_SEC = int(os.environ.get("YT_DLP_TIMEOUT_SEC", "35"))
YT_DLP_CHANNEL_TIMEOUT_SEC = int(os.environ.get("YT_DLP_CHANNEL_TIMEOUT_SEC", "60"))
YT_DLP_DETAIL_TIMEOUT_SEC = int(os.environ.get("YT_DLP_DETAIL_TIMEOUT_SEC", "45"))
YT_DLP_TRANSCRIPT_TIMEOUT_SEC = int(os.environ.get("YT_DLP_TRANSCRIPT_TIMEOUT_SEC", "90"))
CONTENT_FETCH_HUB_SETTINGS_PATH = resolve_skill_project_root("content-fetch-hub") / "config" / "fetch-settings.yaml"


def _load_youtube_cookies_path() -> str:
    cfg = root_config()
    credentials = dict(cfg.get("credentials") or {})
    fetch_cfg = dict(cfg.get("content_fetch_hub") or {})
    env_name = str(credentials.get("youtube_cookies_path_env") or "YT_DLP_COOKIES_PATH").strip()
    candidates = [os.environ.get(env_name), os.environ.get("YT_DLP_COOKIES_PATH"), fetch_cfg.get("youtube_cookies_path")]
    try:
        if CONTENT_FETCH_HUB_SETTINGS_PATH.exists():
            settings = yaml.safe_load(CONTENT_FETCH_HUB_SETTINGS_PATH.read_text(encoding="utf-8")) or {}
            youtube_cfg = settings.get("youtube") or {}
            candidates.extend([
                youtube_cfg.get("cookies_path"),
                settings.get("youtube_cookies_path"),
            ])
    except Exception:
        pass
    for value in candidates:
        path = str(value or "").strip()
        if path and Path(path).exists():
            return path
    return ""


@lru_cache(maxsize=1)
def _load_wsl_proxy_env() -> dict[str, str]:
    def _normalize_proxy(proxy: str) -> dict[str, str]:
        value = str(proxy or "").strip()
        if not value or "$" in value:
            return {}
        return {
            "HTTPS_PROXY": value,
            "https_proxy": value,
            "HTTP_PROXY": value,
            "http_proxy": value,
        }

    candidates = [
        os.environ.get("HTTPS_PROXY", "").strip(),
        os.environ.get("https_proxy", "").strip(),
        os.environ.get("HTTP_PROXY", "").strip(),
        os.environ.get("http_proxy", "").strip(),
    ]
    if any(candidates):
        env = {}
        for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
            value = os.environ.get(key, "").strip()
            if value and "$" not in value:
                env[key] = value
        if env:
            return env

    twitter_proxy = _normalize_proxy(str(os.environ.get("TWITTER_PROXY") or "").strip())
    if twitter_proxy:
        return twitter_proxy

    return {}


def _build_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update(_load_wsl_proxy_env())
    return env


def _resolve_yt_dlp_bin() -> str:
    cfg = root_config()
    env_bin = str(os.environ.get("YT_DLP_BIN") or ((cfg.get("external_tools") or {}).get("yt_dlp_bin")) or "").strip()
    if env_bin and Path(env_bin).exists():
        return env_bin

    cmd = which("yt-dlp")
    if cmd:
        return cmd

    raise RuntimeError("未找到 yt-dlp，可设置 YT_DLP_BIN 或安装到 ~/.local/bin/yt-dlp")


def load_youtube_channels(enabled_only: bool = True) -> list[dict[str, Any]]:
    if not YOUTUBE_CHANNELS_PATH.exists():
        return []
    data = json.loads(YOUTUBE_CHANNELS_PATH.read_text(encoding="utf-8")) or {}
    channels = data.get("channels") or []
    out: list[dict[str, Any]] = []
    for channel in channels:
        item = dict(channel or {})
        item["id"] = str(item.get("id") or "").strip()
        item["name"] = str(item.get("name") or "").strip()
        item["enabled"] = bool(item.get("enabled", True))
        item["priority"] = int(item.get("priority") or 5)
        item["category"] = str(item.get("category") or "").strip()
        item["channel_url"] = str(item.get("channel_url") or "").strip()
        item["fetch_method"] = str(item.get("fetch_method") or "channel_videos").strip() or "channel_videos"
        item["notes"] = str(item.get("notes") or "").strip()
        if item["id"] and item["name"] and item["channel_url"]:
            out.append(item)
    if enabled_only:
        out = [c for c in out if c.get("enabled")]
    return out


def _to_iso_from_yt_date(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    try:
        return datetime.strptime(value, "%Y%m%d").replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return value


def _channel_videos_url(channel_url: str) -> str:
    url = str(channel_url or "").strip().rstrip("/")
    if not url:
        return url
    if url.endswith("/videos"):
        return url
    return f"{url}/videos"


def _clean_vtt(content: str) -> str:
    lines = content.splitlines()
    text_lines: list[str] = []
    timestamp_pattern = re.compile(r"\d{2}:\d{2}:\d{2}\.\d{3}\s-->\s\d{2}:\d{2}:\d{2}\.\d{3}")
    for line in lines:
        line = line.strip()
        if not line or line == "WEBVTT" or line.isdigit():
            continue
        if timestamp_pattern.match(line):
            continue
        if line.startswith("NOTE") or line.startswith("STYLE"):
            continue
        line = re.sub(r"<[^>]+>", "", line)
        if text_lines and text_lines[-1] == line:
            continue
        text_lines.append(line)
    return "\n".join(text_lines).strip()


def _build_yt_dlp_base_cmd() -> list[str]:
    cmd = [_resolve_yt_dlp_bin(), "--remote-components", "ejs:github"]
    cookies_path = _load_youtube_cookies_path()
    if cookies_path:
        cmd.extend(["--cookies", cookies_path])
    return cmd


def _run_yt_dlp_json(url: str, flat_playlist: bool = False, playlist_end: int | None = None, timeout_sec: int | None = None) -> list[dict[str, Any]]:
    cmd = _build_yt_dlp_base_cmd()
    if flat_playlist:
        cmd.append("--flat-playlist")
    if playlist_end is not None:
        cmd.extend(["--playlist-end", str(max(1, playlist_end))])
    cmd.extend(["--dump-json", url])
    effective_timeout = int(timeout_sec or YT_DLP_TIMEOUT_SEC)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=effective_timeout, env=_build_subprocess_env())
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"yt-dlp 请求超时（>{effective_timeout}s）") from exc
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "yt-dlp failed").strip()
        raise RuntimeError(err)

    items: list[dict[str, Any]] = []
    for line in (proc.stdout or "").splitlines():
        raw = line.strip()
        if not raw:
            continue
        items.append(json.loads(raw))
    return items


def _build_video_row(channel: dict[str, Any], item: dict[str, Any], fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = fallback or {}
    video_id = str(item.get("id") or fallback.get("id") or "").strip()
    title = str(item.get("title") or fallback.get("title") or "").strip()
    webpage_url = str(item.get("webpage_url") or fallback.get("webpage_url") or "").strip()
    if not webpage_url and video_id:
        webpage_url = f"https://www.youtube.com/watch?v={video_id}"

    upload_date = str(item.get("upload_date") or fallback.get("upload_date") or "")
    published_at = _to_iso_from_yt_date(upload_date)
    if not published_at and item.get("timestamp"):
        try:
            published_at = datetime.fromtimestamp(int(item.get("timestamp")), tz=timezone.utc).isoformat()
        except Exception:
            published_at = ""

    duration_value = item.get("duration")
    if duration_value is None:
        duration_value = fallback.get("duration")
    view_count_value = item.get("view_count")
    if view_count_value is None:
        view_count_value = fallback.get("view_count")

    return {
        "channel_id": str(channel.get("id") or ""),
        "channel_name": str(channel.get("name") or ""),
        "video_id": video_id,
        "title": title,
        "published_at": published_at,
        "url": webpage_url,
        "duration_sec": int(duration_value or 0),
        "view_count": int(view_count_value or 0),
    }


def fetch_recent_videos(channel: dict[str, Any], limit: int = 3, detail_lookup: bool = True, timeout_sec: int | None = None) -> list[dict[str, Any]]:
    target_url = _channel_videos_url(str(channel.get("channel_url") or ""))
    if not target_url:
        return []

    playlist_items = _run_yt_dlp_json(target_url, flat_playlist=True, playlist_end=limit, timeout_sec=timeout_sec or YT_DLP_CHANNEL_TIMEOUT_SEC)

    videos: list[dict[str, Any]] = []
    for item in playlist_items:
        base_row = _build_video_row(channel, item)
        if base_row.get("published_at") or not detail_lookup:
            videos.append(base_row)
            continue

        video_url = base_row.get("url") or ""
        try:
            detail_items = _run_yt_dlp_json(video_url, timeout_sec=timeout_sec or YT_DLP_DETAIL_TIMEOUT_SEC)
            detail = detail_items[-1] if detail_items else {}
            videos.append(_build_video_row(channel, detail, fallback=item))
        except Exception:
            videos.append(base_row)
    return videos


def fetch_transcript(video_url: str, preferred_lang: str = "en") -> dict[str, Any]:
    video_url = str(video_url or "").strip()
    if not video_url:
        return {"available": False, "language": "", "kind": "", "text": "", "error": "video url empty"}

    with tempfile.TemporaryDirectory() as temp_dir:
        base_output = str(Path(temp_dir) / "subs")
        cmd = _build_yt_dlp_base_cmd() + [
            "--write-subs",
            "--write-auto-subs",
            "--skip-download",
            "--sub-lang",
            preferred_lang,
            "--output",
            base_output,
            video_url,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=YT_DLP_TRANSCRIPT_TIMEOUT_SEC, env=_build_subprocess_env())
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "yt-dlp subtitle fetch failed").strip()
            return {"available": False, "language": preferred_lang, "kind": "", "text": "", "error": err}

        temp_path = Path(temp_dir)
        vtt_files = sorted(temp_path.glob("*.vtt"))
        if not vtt_files:
            return {"available": False, "language": preferred_lang, "kind": "", "text": "", "error": "no subtitles found"}

        vtt_file = vtt_files[0]
        raw_text = vtt_file.read_text(encoding="utf-8", errors="ignore")
        cleaned = _clean_vtt(raw_text)
        suffixes = vtt_file.name.lower().split(".")
        lang = preferred_lang
        kind = "manual"
        if len(suffixes) >= 2:
            lang = suffixes[-2]
        if ".en-orig." in vtt_file.name.lower() or ".orig." in vtt_file.name.lower() or "auto" in vtt_file.name.lower():
            kind = "auto"
        return {
            "available": bool(cleaned),
            "language": lang,
            "kind": kind,
            "text": cleaned,
            "error": "",
        }


def validate_channels(limit_channels: int = 3, limit_videos: int = 3, with_transcript: bool = False) -> dict[str, Any]:
    channels = load_youtube_channels(enabled_only=True)
    selected = channels[: max(1, limit_channels)]
    results: list[dict[str, Any]] = []
    for channel in selected:
        row: dict[str, Any] = {
            "channel_id": channel.get("id") or "",
            "channel_name": channel.get("name") or "",
            "channel_url": channel.get("channel_url") or "",
            "ok": False,
            "videos": [],
        }
        try:
            videos = fetch_recent_videos(channel, limit=limit_videos)
            if with_transcript:
                for video in videos:
                    transcript = fetch_transcript(str(video.get("url") or ""))
                    video["transcript_available"] = bool(transcript.get("available"))
                    video["transcript_language"] = str(transcript.get("language") or "")
                    video["transcript_kind"] = str(transcript.get("kind") or "")
                    video["transcript_preview"] = str(transcript.get("text") or "")[:600]
                    video["transcript_error"] = str(transcript.get("error") or "")
            row["videos"] = videos
            row["ok"] = True
        except Exception as exc:  # noqa: BLE001
            row["error"] = str(exc)
        results.append(row)
    return {
        "ok": True,
        "checked_channels": len(selected),
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate YouTube channel metadata fetching")
    parser.add_argument("--channels", type=int, default=3, help="How many enabled channels to validate")
    parser.add_argument("--videos", type=int, default=3, help="How many recent videos per channel to fetch")
    parser.add_argument("--with-transcript", action="store_true", help="Also fetch transcript preview for each video")
    parser.add_argument("--json", action="store_true", help="Print JSON only")
    args = parser.parse_args()

    result = validate_channels(limit_channels=args.channels, limit_videos=args.videos, with_transcript=args.with_transcript)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print(f"checked_channels={result['checked_channels']}")
    for item in result["results"]:
        print(f"\n## {item['channel_name']} ({item['channel_id']})")
        if not item.get("ok"):
            print(f"ERROR: {item.get('error') or 'unknown error'}")
            continue
        videos = item.get("videos") or []
        if not videos:
            print("(no videos)")
            continue
        for idx, video in enumerate(videos, start=1):
            print(f"{idx}. {video.get('published_at') or '-'} | {video.get('title') or '(无标题)'}")
            print(f"   {video.get('url') or '-'}")
            if 'transcript_available' in video:
                print(
                    f"   transcript: available={video.get('transcript_available')} lang={video.get('transcript_language') or '-'} kind={video.get('transcript_kind') or '-'}"
                )
                preview = str(video.get('transcript_preview') or '').strip()
                if preview:
                    compact_preview = preview.replace('\n', ' ')[:200]
                    print(f"   preview: {compact_preview}")
                elif video.get('transcript_error'):
                    print(f"   transcript_error: {video.get('transcript_error')}")


if __name__ == "__main__":
    main()
