#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workspace_paths import MONITORING_ROOT

BASE_DIR = Path(__file__).resolve().parents[1]
DOUYIN_SOURCES_PATH = MONITORING_ROOT / "a-stage-douyin-sources.json"
YT_DLP_BIN = "yt-dlp"


def load_douyin_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    if not DOUYIN_SOURCES_PATH.exists():
        return []
    data = json.loads(DOUYIN_SOURCES_PATH.read_text(encoding="utf-8")) or {}
    sources = data.get("sources") or []
    out: list[dict[str, Any]] = []
    for source in sources:
        item = dict(source or {})
        item["id"] = str(item.get("id") or "").strip()
        item["name"] = str(item.get("name") or "").strip()
        item["enabled"] = bool(item.get("enabled", True))
        item["priority"] = int(item.get("priority") or 5)
        item["category"] = str(item.get("category") or "").strip()
        item["profile_url"] = str(item.get("profile_url") or item.get("entry_url") or item.get("url") or "").strip()
        item["fetch_method"] = str(item.get("fetch_method") or "profile_playlist").strip() or "profile_playlist"
        item["notes"] = str(item.get("notes") or "").strip()
        if item["id"] and item["name"] and item["profile_url"]:
            out.append(item)
    if enabled_only:
        out = [x for x in out if x.get("enabled")]
    return out


def _to_iso(raw: Any) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if value.isdigit():
        try:
            return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
        except Exception:
            return value
    try:
        return datetime.strptime(value, "%Y%m%d").replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return value


def _run_yt_dlp_json(url: str, flat_playlist: bool = False, playlist_end: int | None = None) -> list[dict[str, Any]]:
    cmd = [YT_DLP_BIN, "--ignore-config"]
    if flat_playlist:
        cmd.append("--flat-playlist")
    if playlist_end is not None:
        cmd.extend(["--playlist-end", str(max(1, playlist_end))])
    cmd.extend(["--dump-json", url])
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
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


def _build_video_row(source: dict[str, Any], item: dict[str, Any], fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = fallback or {}
    video_id = str(item.get("aweme_id") or item.get("id") or fallback.get("aweme_id") or fallback.get("id") or "").strip()
    title = str(item.get("title") or item.get("description") or item.get("desc") or fallback.get("title") or fallback.get("description") or fallback.get("desc") or "").strip()
    webpage_url = str(item.get("webpage_url") or item.get("original_url") or item.get("url") or fallback.get("webpage_url") or fallback.get("original_url") or fallback.get("url") or "").strip()
    if webpage_url and not webpage_url.startswith("http"):
        webpage_url = ""
    if not webpage_url and video_id:
        webpage_url = f"https://www.douyin.com/video/{video_id}"

    timestamp_value = item.get("timestamp")
    if timestamp_value is None:
        timestamp_value = item.get("create_time")
    if timestamp_value is None:
        timestamp_value = fallback.get("timestamp")
    if timestamp_value is None:
        timestamp_value = fallback.get("create_time")
    upload_date = item.get("upload_date") or fallback.get("upload_date") or ""
    published_at = _to_iso(timestamp_value or upload_date)

    duration_value = item.get("duration")
    if duration_value is None:
        duration_value = item.get("duration_sec")
    if duration_value is None:
        duration_value = fallback.get("duration")

    view_count_value = item.get("view_count")
    if view_count_value is None:
        stats = item.get("statistics") or {}
        view_count_value = stats.get("play_count")
    if view_count_value is None:
        fallback_stats = fallback.get("statistics") or {}
        view_count_value = fallback_stats.get("play_count")

    uploader = str(item.get("uploader") or item.get("channel") or item.get("creator") or fallback.get("uploader") or fallback.get("channel") or source.get("name") or "").strip()

    return {
        "source_id": str(source.get("id") or ""),
        "source_name": str(source.get("name") or ""),
        "video_id": video_id,
        "title": title,
        "published_at": published_at,
        "url": webpage_url,
        "duration_sec": int(duration_value or 0),
        "view_count": int(view_count_value or 0),
        "uploader": uploader,
    }


def fetch_recent_videos(source: dict[str, Any], limit: int = 3) -> list[dict[str, Any]]:
    target_url = str(source.get("profile_url") or "").strip()
    if not target_url:
        return []

    if str(source.get("fetch_method") or "").strip() == "single_video":
        detail_items = _run_yt_dlp_json(target_url)
        detail = detail_items[-1] if detail_items else {}
        row = _build_video_row(source, detail)
        return [row] if row.get("url") else []

    playlist_items = _run_yt_dlp_json(target_url, flat_playlist=True, playlist_end=limit)

    videos: list[dict[str, Any]] = []
    for item in playlist_items:
        base_row = _build_video_row(source, item)
        needs_detail = not base_row.get("published_at") or not base_row.get("title") or not base_row.get("url")
        if not needs_detail:
            videos.append(base_row)
            continue

        detail_url = base_row.get("url") or target_url
        try:
            detail_items = _run_yt_dlp_json(detail_url)
            detail = detail_items[-1] if detail_items else {}
            videos.append(_build_video_row(source, detail, fallback=item))
        except Exception:
            videos.append(base_row)
    return videos[: max(1, limit)]
