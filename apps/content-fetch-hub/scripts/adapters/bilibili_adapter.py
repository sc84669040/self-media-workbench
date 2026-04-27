#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from shutil import which

import yaml

from adapters.base import FetchAdapter, host_matches
from models import FetchResult

BASE_DIR = Path(__file__).resolve().parents[2]
REPO_SCRIPTS = Path(__file__).resolve().parents[4] / "scripts"
if REPO_SCRIPTS.exists():
    repo_scripts_text = str(REPO_SCRIPTS)
    if repo_scripts_text in sys.path:
        sys.path.remove(repo_scripts_text)
    sys.path.insert(0, repo_scripts_text)

try:
    from self_media_config import get_config, get_value  # type: ignore
except Exception:  # noqa: BLE001
    get_config = None  # type: ignore
    get_value = None  # type: ignore


class BilibiliAdapter(FetchAdapter):
    name = "bilibili"
    SETTINGS_PATH = BASE_DIR / "config" / "fetch-settings.yaml"

    def can_handle(self, url: str) -> bool:
        return host_matches(url, "bilibili.com", "b23.tv")

    def _format_publish_date(self, raw: str) -> str:
        s = str(raw or "").strip()
        if re.match(r"^\d{8}$", s):
            return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return s
        return s

    def _load_settings(self) -> dict:
        settings: dict = {}
        if get_config is not None and get_value is not None:
            try:
                root = get_config()
                settings.update(get_value(root, "content_fetch_hub", {}) or {})
                settings.setdefault("external_tools", get_value(root, "external_tools", {}) or {})
            except Exception:
                pass
        try:
            if self.SETTINGS_PATH.exists():
                payload = yaml.safe_load(self.SETTINGS_PATH.read_text(encoding="utf-8")) or {}
                if isinstance(payload, dict):
                    settings.update(payload.get("content_fetch_hub", payload))
        except Exception:
            pass
        return settings

    def _load_cookies_path(self) -> str:
        settings = self._load_settings()
        bilibili_cfg = settings.get("bilibili") or {}
        candidates = [
            bilibili_cfg.get("cookies_path"),
            settings.get("bilibili_cookies_path"),
            settings.get("cookies_path"),
        ]
        for value in candidates:
            path = str(value or "").strip()
            if path and Path(path).exists():
                return path
        return ""

    def _resolve_yt_dlp_binary(self) -> str:
        settings = self._load_settings()
        bilibili_cfg = settings.get("bilibili") or {}
        configured = [
            os.environ.get("YT_DLP_BIN"),
            bilibili_cfg.get("yt_dlp_path"),
            settings.get("yt_dlp_path"),
            (settings.get("external_tools") or {}).get("yt_dlp_bin"),
            os.environ.get("YT_DLP_PATH"),
        ]
        for value in configured:
            candidate = str(value or "").strip()
            if candidate and Path(candidate).exists():
                return candidate

        discovered = which("yt-dlp")
        if discovered:
            return discovered

        local_bin = Path.home() / ".local" / "bin" / "yt-dlp"
        if local_bin.exists():
            return str(local_bin)

        return "yt-dlp"

    def _subtitle_lang_key(self, path: Path) -> str:
        stem = path.stem
        if "." in stem:
            return stem.split(".", 1)[1].strip().lower()
        return stem.strip().lower()

    def _subtitle_priority(self, path: Path) -> tuple[int, str]:
        key = self._subtitle_lang_key(path)
        preferred_order = [
            "ai-zh",
            "zh-hans",
            "zh-cn",
            "zh",
            "zh-hant",
            "zh-tw",
            "zh-hk",
        ]
        if key in preferred_order:
            return (preferred_order.index(key), key)
        if key.startswith("ai-zh"):
            return (0, key)
        if key.startswith("zh"):
            return (10, key)
        if key.startswith("ai-"):
            return (50, key)
        return (100, key)

    def _pick_best_subtitle_file(self, candidates: list[Path]) -> Path | None:
        files = [path for path in candidates if path.suffix.lower() == ".srt"]
        if not files:
            return None
        return sorted(files, key=self._subtitle_priority)[0]

    def _clean_subtitle_text(self, raw: str) -> str:
        lines: list[str] = []
        timecode_re = re.compile(r"^\d{2}:\d{2}:\d{2}[,\.]\d{3}\s+-->\s+\d{2}:\d{2}:\d{2}[,\.]\d{3}$")
        for line in str(raw or "").splitlines():
            text = line.strip()
            if not text:
                continue
            if text.isdigit():
                continue
            if timecode_re.match(text):
                continue
            if lines and lines[-1] == text:
                continue
            lines.append(text)
        return "\n".join(lines).strip()

    def _fetch_metadata(self, target_url: str, cookies_path: str) -> tuple[dict, str]:
        cmd = [self._resolve_yt_dlp_binary(), "--ignore-config", "--dump-single-json"]
        if cookies_path:
            cmd.extend(["--cookies", cookies_path])
        cmd.append(target_url)
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        if proc.returncode != 0 or not (proc.stdout or "").strip():
            err = (proc.stderr or "").strip() or "bilibili-metadata-empty"
            raise RuntimeError(err)
        return json.loads(proc.stdout), "yt_dlp_metadata"

    def _fetch_subtitle_bundle(self, target_url: str, cookies_path: str) -> tuple[str, str, str, str]:
        with tempfile.TemporaryDirectory(prefix="bilibili-subs-") as tmpdir:
            tmp = Path(tmpdir)
            output_tpl = tmp / "%(id)s.%(ext)s"
            cmd = [
                self._resolve_yt_dlp_binary(),
                "--ignore-config",
                "--skip-download",
                "--write-subs",
                "--sub-langs",
                "all",
                "--convert-subs",
                "srt",
                "--output",
                str(output_tpl),
            ]
            if cookies_path:
                cmd.extend(["--cookies", cookies_path])
            cmd.append(target_url)
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            subtitle_files = sorted(tmp.glob("*.srt"))
            best_file = self._pick_best_subtitle_file(subtitle_files)
            if not best_file:
                err = (proc.stderr or proc.stdout or "").strip()
                return "", "", "", err
            text = self._clean_subtitle_text(best_file.read_text(encoding="utf-8", errors="ignore"))
            if not text:
                return "", best_file.name, self._subtitle_lang_key(best_file), "subtitle-empty"
            return text, best_file.name, self._subtitle_lang_key(best_file), ""

    def fetch(self, url: str) -> FetchResult:
        target_url = str(url or "").strip()
        if not target_url:
            return FetchResult(ok=False, channel=self.name, url=url, error="url-empty")

        cookies_path = self._load_cookies_path()
        try:
            obj, fetch_method = self._fetch_metadata(target_url, cookies_path)
        except Exception as exc:  # noqa: BLE001
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"bilibili-metadata-failed: {exc}")

        final_url = str(obj.get("webpage_url") or target_url).strip()
        title = str(obj.get("title") or "(无标题)").strip()[:140]
        author = str(obj.get("uploader") or obj.get("channel") or "").strip()
        video_id = str(obj.get("id") or "").strip()
        published_at = self._format_publish_date(str(obj.get("upload_date") or ""))
        cover_url = str(obj.get("thumbnail") or "").strip()
        video_url = str(obj.get("url") or "").strip()

        transcript_text, subtitle_file, transcript_language, subtitle_error = self._fetch_subtitle_bundle(final_url, cookies_path)
        transcript_available = bool(transcript_text)
        transcript_source = "yt_dlp_subtitles" if transcript_available else "yt_dlp_subtitles_missing"

        content_lines = [
            f"标题：{title}",
            f"作者：{author}",
            f"作品ID：{video_id}",
            f"作品页：{final_url}",
            f"视频直链：{video_url}",
        ]
        if transcript_available:
            content_lines.extend(["", "字幕转写：", transcript_text])
            fetch_method = "yt_dlp_subtitles"
        else:
            content_lines.append("说明：当前未拿到可用字幕，先保留元信息；ASR fallback 暂未启用。")
        content = "\n".join([line for line in content_lines if line is not None]).strip()

        return FetchResult(
            ok=True,
            channel=self.name,
            url=final_url,
            title=title,
            author=author,
            published_at=published_at,
            content_markdown=content,
            meta={
                "platform": "bilibili",
                "video_id": video_id,
                "cover_url": cover_url,
                "video_url": video_url,
                "fetch_method": fetch_method,
                "subtitle_file": subtitle_file,
                "transcript_available": transcript_available,
                "transcript_source": transcript_source,
                "transcript_language": transcript_language,
                "cookies_configured": bool(cookies_path),
                "subtitle_probe_error": subtitle_error,
            },
            error="",
        )
