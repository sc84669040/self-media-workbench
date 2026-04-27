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
from typing import Any

import yaml

from adapters.base import FetchAdapter, host_matches
from models import FetchResult


class YoutubeAdapter(FetchAdapter):
    name = "youtube"
    SETTINGS_PATH = Path(__file__).resolve().parents[2] / "config" / "fetch-settings.yaml"
    REPO_SCRIPTS = Path(__file__).resolve().parents[4] / "scripts"

    def _load_settings(self) -> dict[str, Any]:
        settings: dict[str, Any] = {}
        if self.REPO_SCRIPTS.exists():
            repo_scripts_text = str(self.REPO_SCRIPTS)
            if repo_scripts_text in sys.path:
                sys.path.remove(repo_scripts_text)
            sys.path.insert(0, repo_scripts_text)
            try:
                from self_media_config import get_config, get_value  # type: ignore

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
        youtube_cfg = settings.get("youtube") or {}
        candidates = [
            os.environ.get("YT_DLP_COOKIES_PATH"),
            youtube_cfg.get("cookies_path"),
            settings.get("youtube_cookies_path"),
        ]
        for value in candidates:
            path = str(value or "").strip()
            if path and Path(path).exists():
                return path
        return ""

    def _emit_progress(self, stage: str, message: str, **extra: Any) -> None:
        payload = {"stage": stage, "message": message}
        if extra:
            payload.update(extra)
        print(f"[progress] {json.dumps(payload, ensure_ascii=False)}", file=sys.stderr, flush=True)

    def _load_wsl_proxy(self) -> str:
        candidates = [
            os.environ.get("TWITTER_PROXY", "").strip(),
            os.environ.get("HTTPS_PROXY", "").strip(),
            os.environ.get("https_proxy", "").strip(),
            os.environ.get("HTTP_PROXY", "").strip(),
            os.environ.get("http_proxy", "").strip(),
        ]
        for value in candidates:
            if value:
                return value

        return ""

    def _build_subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()
        proxy = self._load_wsl_proxy()
        if proxy:
            env.setdefault("HTTPS_PROXY", proxy)
            env.setdefault("https_proxy", proxy)
            env.setdefault("HTTP_PROXY", proxy)
            env.setdefault("http_proxy", proxy)
        return env

    def _build_yt_dlp_base_cmd(self, yt_dlp_bin: str) -> list[str]:
        cmd = [yt_dlp_bin, "--remote-components", "ejs:github"]
        cookies_path = self._load_cookies_path()
        if cookies_path:
            cmd.extend(["--cookies", cookies_path])
        return cmd

    def _resolve_yt_dlp_bin(self) -> str:
        settings = self._load_settings()
        env_bin = str(os.environ.get("YT_DLP_BIN") or "").strip()
        if env_bin and Path(env_bin).exists():
            return env_bin
        configured = str((settings.get("external_tools") or {}).get("yt_dlp_bin") or "").strip()
        if configured and Path(configured).exists():
            return configured

        cmd = which("yt-dlp")
        if cmd:
            return cmd
        return ""

    def _extract_video_id(self, target_url: str) -> str:
        s = str(target_url or "").strip()
        m = re.search(r"(?:youtu\.be/|youtube\.com/watch\?v=|youtube\.com/shorts/|youtube\.com/live/)([A-Za-z0-9_-]{6,})", s)
        return m.group(1) if m else ""

    def _extract_lang_from_vtt_name(self, filename: str) -> str:
        parts = str(filename or "").split(".")
        if len(parts) < 2:
            return ""
        cand = parts[-2]
        if re.match(r"^[a-z]{2,3}(?:-[A-Za-z0-9]+)?$", cand):
            return cand
        return ""

    def _clean_vtt(self, content: str) -> str:
        lines = content.splitlines()
        text_lines: list[str] = []
        ts = re.compile(r"\d{2}:\d{2}:\d{2}\.\d{3}\s-->\s\d{2}:\d{2}:\d{2}\.\d{3}")
        for line in lines:
            s = line.strip()
            if not s or s == "WEBVTT" or s.isdigit() or ts.match(s):
                continue
            s = re.sub(r"<[^>]+>", "", s)
            if text_lines and text_lines[-1] == s:
                continue
            text_lines.append(s)
        return "\n".join(text_lines).strip()

    def _timeout_from_env(self, env_name: str, default_sec: int) -> int | None:
        raw = str(os.environ.get(env_name) or "").strip()
        if not raw:
            return default_sec
        try:
            val = int(raw)
            if val <= 0:
                return None
            return val
        except Exception:
            return default_sec

    def _list_subtitle_langs(self, yt_dlp_bin: str, target_url: str) -> list[str]:
        timeout_sec = self._timeout_from_env("YT_DLP_LIST_SUBS_TIMEOUT", 90)
        env = self._build_subprocess_env()
        try:
            cmd = self._build_yt_dlp_base_cmd(yt_dlp_bin) + ["--skip-download", "--list-subs", target_url]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout_sec, env=env)
            text = "\n".join([proc.stdout or "", proc.stderr or ""])
            langs: list[str] = []
            for line in text.splitlines():
                m = re.match(r"^\s*([a-z]{2,3}(?:-[A-Za-z0-9]+)?)\s+", line)
                if not m:
                    continue
                lang = str(m.group(1) or "").strip()
                if lang and lang not in langs:
                    langs.append(lang)
            return langs
        except Exception:
            return []

    def _fetch_metadata(self, yt_dlp_bin: str, target_url: str) -> dict[str, Any]:
        self._emit_progress("metadata", "正在读取视频信息")
        timeout_sec = self._timeout_from_env("YT_DLP_METADATA_TIMEOUT", 180)
        env = self._build_subprocess_env()
        try:
            cmd = self._build_yt_dlp_base_cmd(yt_dlp_bin) + ["--dump-single-json", target_url]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout_sec, env=env)
            if proc.returncode == 0 and (proc.stdout or "").strip():
                data = json.loads(proc.stdout)
                self._emit_progress(
                    "metadata",
                    "视频信息读取完成",
                    title=str(data.get("title") or "")[:80],
                    duration_sec=int(data.get("duration") or 0),
                    subtitle_keys=sorted((data.get("subtitles") or {}).keys()),
                    auto_caption_keys=sorted((data.get("automatic_captions") or {}).keys()),
                )
                return data
        except subprocess.TimeoutExpired:
            self._emit_progress("metadata", "视频信息读取超时，改用字幕列表兜底")
        except Exception as exc:
            self._emit_progress("metadata", f"视频信息读取失败：{exc}")

        listed_langs = self._list_subtitle_langs(yt_dlp_bin, target_url)
        if listed_langs:
            self._emit_progress("metadata", "已通过 --list-subs 获取字幕语言", subtitle_keys=listed_langs)
            return {
                "subtitles": {lang: [{}] for lang in listed_langs},
                "automatic_captions": {},
            }
        return {}

    def _subtitle_lang_plans(self, duration_sec: int, metadata: dict[str, Any], target_url: str = "") -> list[str]:
        subtitles = metadata.get("subtitles") or {}
        auto_captions = metadata.get("automatic_captions") or {}
        available: set[str] = set()
        if isinstance(subtitles, dict):
            available.update(str(k or "").strip() for k in subtitles.keys())
        if isinstance(auto_captions, dict):
            available.update(str(k or "").strip() for k in auto_captions.keys())
        available = {x for x in available if x}

        title = str(metadata.get("title") or "")
        looks_chinese = bool(re.search(r"[\u4e00-\u9fff]", title + " " + str(target_url or "")))
        preferred = ["zh", "zh-Hans", "zh-Hant", "en"] if looks_chinese else ["en", "zh", "zh-Hans", "zh-Hant"]
        if duration_sec >= 60 * 60:
            preferred = [x for x in preferred if x != "zh-Hant"]

        exact = [lang for lang in preferred if lang in available]
        fuzzy: list[str] = []
        for lang in preferred:
            if lang in exact:
                continue
            prefix = lang.split("-", 1)[0]
            match = next((x for x in available if x == prefix or x.startswith(prefix + "-")), "")
            if match and match not in exact and match not in fuzzy:
                fuzzy.append(match)

        plans = exact + fuzzy
        if plans:
            return plans
        return preferred

    def _fetch_subtitle_once(self, yt_dlp_bin: str, target_url: str, lang_spec: str) -> tuple[str, str, str, str]:
        with tempfile.TemporaryDirectory() as tmp:
            out_tpl = str(Path(tmp) / "subs")
            cmd = self._build_yt_dlp_base_cmd(yt_dlp_bin) + [
                "--write-subs",
                "--write-auto-subs",
                "--skip-download",
                "--sub-lang",
                lang_spec,
                "--output",
                out_tpl,
                target_url,
            ]
            timeout_sec = self._timeout_from_env("YT_DLP_SUBTITLE_TIMEOUT", 240)
            env = self._build_subprocess_env()
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout_sec, env=env)
            except subprocess.TimeoutExpired:
                return "", "", "", "subtitle-fetch-timeout"
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "yt-dlp failed").strip()
                return "", "", "", err

            vtts = sorted(Path(tmp).glob("*.vtt"))
            if not vtts:
                return "", "", "", "subtitle-not-found"

            picked = vtts[0]
            raw = picked.read_text(encoding="utf-8", errors="ignore")
            transcript = self._clean_vtt(raw)
            transcript_language = self._extract_lang_from_vtt_name(picked.name)
            return transcript, transcript_language, picked.name, ""

    def can_handle(self, url: str) -> bool:
        return host_matches(url, "youtube.com", "youtu.be")

    def fetch(self, url: str) -> FetchResult:
        target_url = str(url or "").strip()
        if not target_url:
            return FetchResult(ok=False, channel=self.name, url=url, error="url-empty")

        yt_dlp_bin = self._resolve_yt_dlp_bin()
        if not yt_dlp_bin:
            return FetchResult(ok=False, channel=self.name, url=target_url, error="youtube-yt-dlp-missing")

        video_id = self._extract_video_id(target_url)
        metadata = self._fetch_metadata(yt_dlp_bin, target_url)
        title = str(metadata.get("title") or "")
        author = str(metadata.get("uploader") or metadata.get("channel") or "")
        video_id = str(metadata.get("id") or video_id or "").strip()
        upload_date = str(metadata.get("upload_date") or "")
        duration_sec = int(metadata.get("duration") or 0)
        published_at = ""
        if re.match(r"^\d{8}$", upload_date):
            published_at = f"{upload_date[0:4]}-{upload_date[4:6]}-{upload_date[6:8]} 00:00:00"

        lang_plans = self._subtitle_lang_plans(duration_sec, metadata, target_url)
        self._emit_progress("subtitle-plan", "已生成字幕抓取计划", attempted_langs=lang_plans)
        errors: list[str] = []
        for lang_spec in lang_plans:
            try:
                self._emit_progress("subtitle-fetch", f"正在尝试字幕语言：{lang_spec}", lang=lang_spec)
                transcript, transcript_language, subtitle_file, subtitle_error = self._fetch_subtitle_once(yt_dlp_bin, target_url, lang_spec)
                if transcript:
                    self._emit_progress("subtitle-fetch", f"字幕抓取成功：{transcript_language or lang_spec}", lang=transcript_language or lang_spec, subtitle_file=subtitle_file)
                    return FetchResult(
                        ok=True,
                        channel=self.name,
                        url=target_url,
                        title=(title or "(无标题)")[:140],
                        author=author,
                        published_at=published_at,
                        content_markdown=transcript[:40000],
                        meta={
                            "fetch_method": "yt_dlp_subtitles",
                            "subtitle_file": subtitle_file,
                            "transcript_available": True,
                            "transcript_source": "yt_dlp_subtitles",
                            "transcript_language": transcript_language or lang_spec,
                            "video_id": video_id,
                            "duration_sec": duration_sec,
                            "subtitle_strategy": "metadata-guided-sequential-single-lang",
                            "subtitle_attempted_langs": lang_plans,
                            "subtitle_selected_lang": lang_spec,
                            "subtitle_metadata_keys": sorted((metadata.get("subtitles") or {}).keys()),
                            "auto_caption_metadata_keys": sorted((metadata.get("automatic_captions") or {}).keys()),
                        },
                        error="",
                    )
                errors.append(f"{lang_spec}: {subtitle_error or 'empty-transcript'}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{lang_spec}: {exc}")

        error_text = " | ".join(errors) if errors else "youtube-subtitle-not-found"
        self._emit_progress("failed", f"字幕抓取失败：{error_text}")
        return FetchResult(
            ok=False,
            channel=self.name,
            url=target_url,
            title=(title or "")[:140],
            author=author,
            published_at=published_at,
            meta={
                "video_id": video_id,
                "duration_sec": duration_sec,
                "subtitle_strategy": "metadata-guided-sequential-single-lang",
                "subtitle_attempted_langs": lang_plans,
                "subtitle_metadata_keys": sorted((metadata.get("subtitles") or {}).keys()),
                "auto_caption_metadata_keys": sorted((metadata.get("automatic_captions") or {}).keys()),
            },
            error=f"youtube-subtitle-fetch-failed: {error_text}",
        )
