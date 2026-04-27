#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import requests

from adapters.base import FetchAdapter, host_matches
from models import FetchResult

_MOBILE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/121.0.2277.107 Version/17.0 Mobile/15E148 Safari/604.1"
}


class DouyinAdapter(FetchAdapter):
    name = "douyin"

    def _emit_progress(self, stage: str, message: str, **extra) -> None:
        payload = {"stage": stage, "message": message}
        if extra:
            payload.update(extra)
        print(f"[progress] {json.dumps(payload, ensure_ascii=False)}", file=sys.stderr, flush=True)

    def can_handle(self, url: str) -> bool:
        return host_matches(url, "v.douyin.com", "douyin.com", "iesdouyin.com")

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.trust_env = False
        return s

    def _collapse_ws(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def _extract_video_id_from_url(self, url: str) -> str:
        path = (urlparse(str(url or "")).path or "").strip("/")
        parts = [p for p in path.split("/") if p]
        for idx, part in enumerate(parts):
            if part in {"video", "note"} and idx + 1 < len(parts):
                return parts[idx + 1]
        for part in reversed(parts):
            if re.fullmatch(r"\d{8,30}", part):
                return part
        return ""

    def _resolve_share(self, share_url: str) -> tuple[str, str]:
        self._emit_progress("resolve-share", "正在解析抖音分享链接")
        s = self._session()
        r = s.get(share_url, headers=_MOBILE_HEADERS, timeout=20, allow_redirects=True)
        r.raise_for_status()
        final_url = str(r.url or share_url)
        video_id = self._extract_video_id_from_url(final_url)
        self._emit_progress("resolve-share", "分享链接解析完成", final_url=final_url, video_id=video_id)
        return final_url, video_id

    def _extract_router_data(self, html: str) -> dict:
        text = str(html or "")
        patterns = [
            r"window\._ROUTER_DATA\s*=\s*(.*?)</script>",
            r'<script id="RENDER_DATA" type="application/json">(.*?)</script>',
        ]
        for pat in patterns:
            m = re.search(pat, text, re.DOTALL)
            if not m:
                continue
            raw = m.group(1).strip()
            if "RENDER_DATA" in pat:
                try:
                    from urllib.parse import unquote

                    raw = unquote(raw)
                except Exception:
                    pass
            try:
                return json.loads(raw)
            except Exception:
                continue
        return {}

    def _find_first_item(self, router_data: dict) -> tuple[dict, str]:
        loader = router_data.get("loaderData") or {}
        for key in ["video_(id)/page", "note_(id)/page"]:
            block = loader.get(key) or {}
            info = block.get("videoInfoRes") or {}
            items = info.get("item_list") or []
            if items:
                return items[0], key

        def walk(obj):
            if isinstance(obj, dict):
                if isinstance(obj.get("item_list"), list) and obj.get("item_list"):
                    return obj["item_list"][0]
                for v in obj.values():
                    res = walk(v)
                    if res:
                        return res
            elif isinstance(obj, list):
                for v in obj:
                    res = walk(v)
                    if res:
                        return res
            return None

        item = walk(router_data) or {}
        return item, ""

    def _probe_page_transcript(self, item: dict) -> tuple[str, dict]:
        candidates: list[tuple[str, object]] = [
            ("video_text", item.get("video_text")),
            ("label_top_text", item.get("label_top_text")),
            ("chapter_list", item.get("chapter_list")),
            ("subtitle_list", item.get("subtitle_list")),
            ("captions", item.get("captions")),
            ("caption_info", item.get("caption_info")),
        ]

        def collapse_lines(lines: list[str]) -> str:
            return "\n".join([self._collapse_ws(x) for x in lines if self._collapse_ws(x)]).strip()

        for source, raw in candidates:
            if isinstance(raw, str) and self._collapse_ws(raw):
                text = self._collapse_ws(raw)
                return text, {"transcript_available": True, "transcript_source": source}

            if isinstance(raw, list) and raw:
                lines: list[str] = []
                for entry in raw:
                    if isinstance(entry, str):
                        lines.append(entry)
                        continue
                    if isinstance(entry, dict):
                        for key in ["text", "content", "caption", "subtitle"]:
                            val = entry.get(key)
                            if isinstance(val, str) and self._collapse_ws(val):
                                lines.append(val)
                text = collapse_lines(lines)
                if text:
                    return text, {"transcript_available": True, "transcript_source": source}

        return "", {"transcript_available": False, "transcript_source": "page_probe_none"}

    def _funasr_script_path(self) -> Path:
        configured = os.environ.get("FUNASR_TRANSCRIBE_SCRIPT", "").strip()
        return Path(configured) if configured else Path("__funasr_transcribe_script_not_configured__")

    def _funasr_env_path(self) -> Path:
        configured = os.environ.get("FUNASR_ENV_PATH", "").strip()
        return Path(configured) if configured else Path("__funasr_env_not_configured__")

    def _detect_asr_readiness(self) -> dict:
        funasr_script = self._funasr_script_path()
        funasr_env = self._funasr_env_path()
        return {
            "ffmpeg_available": bool(shutil.which("ffmpeg")),
            "ffprobe_available": bool(shutil.which("ffprobe")),
            "whisper_available": bool(shutil.which("whisper")),
            "faster_whisper_available": bool(shutil.which("faster-whisper")),
            "funasr_script_available": funasr_script.exists(),
            "funasr_env_available": funasr_env.exists(),
        }

    def _safe_stem(self, text: str, fallback: str = "douyin") -> str:
        cleaned = re.sub(r'[\\/:*?"<>|\r\n]+', "-", str(text or ""))
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-")
        return (cleaned[:80] if cleaned else fallback) or fallback

    def _download_file(self, url: str, output_path: Path) -> None:
        self._emit_progress("asr-download", "正在下载抖音源视频用于转写", temp_video_path=str(output_path))
        s = self._session()
        with s.get(url, headers=_MOBILE_HEADERS, timeout=120, stream=True) as resp:
            resp.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
        self._emit_progress("asr-download", "抖音源视频下载完成", temp_video_path=str(output_path))

    def _run_cmd(self, args: list[str], timeout: int = 3600) -> subprocess.CompletedProcess:
        return subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout)

    def _extract_audio(self, video_path: Path, audio_path: Path) -> None:
        self._emit_progress("asr-audio", "正在提取音频", temp_video_path=str(video_path), temp_audio_path=str(audio_path))
        self._run_cmd(
            [
                shutil.which("ffmpeg") or "ffmpeg",
                "-y",
                "-i",
                str(video_path),
                "-vn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "pcm_s16le",
                str(audio_path),
            ],
            timeout=3600,
        )
        self._emit_progress("asr-audio", "音频提取完成", temp_audio_path=str(audio_path))

    def _run_funasr(self, audio_path: Path) -> str:
        script_path = self._funasr_script_path()
        if not script_path.exists():
            raise FileNotFoundError(f"funasr-script-missing: {script_path}")

        self._emit_progress("asr-transcribe", "正在调用 FunASR 转写", script_path=str(script_path), temp_audio_path=str(audio_path))
        env = os.environ.copy()
        env.setdefault("HOME", str(Path.home()))
        subprocess.run(
            ["bash", str(script_path), str(audio_path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=7200,
            env=env,
        )
        transcript_path = audio_path.with_suffix(".txt")
        if not transcript_path.exists():
            raise FileNotFoundError(f"funasr-output-missing: {transcript_path}")
        self._emit_progress("asr-transcribe", "FunASR 转写完成", transcript_path=str(transcript_path))
        return transcript_path.read_text(encoding="utf-8").strip()

    def _transcribe_with_funasr(self, *, video_url: str, video_id: str, title: str) -> tuple[str, dict]:
        readiness = self._detect_asr_readiness()
        self._emit_progress("asr-prepare", "正在检查抖音 ASR 环境", asr_readiness=readiness)
        if not readiness.get("ffmpeg_available"):
            self._emit_progress("asr-skipped", "ASR 跳过：ffmpeg 不可用")
            return "", {"asr_attempted": False, "asr_error": "ffmpeg-unavailable"}
        if not readiness.get("funasr_script_available"):
            self._emit_progress("asr-skipped", "ASR 跳过：FunASR 脚本不存在")
            return "", {"asr_attempted": False, "asr_error": "funasr-script-unavailable"}
        if not readiness.get("funasr_env_available"):
            self._emit_progress("asr-skipped", "ASR 跳过：FunASR 环境不存在")
            return "", {"asr_attempted": False, "asr_error": "funasr-env-unavailable"}
        if not self._collapse_ws(video_url):
            self._emit_progress("asr-skipped", "ASR 跳过：视频直链为空")
            return "", {"asr_attempted": False, "asr_error": "video-url-empty"}

        stem = self._safe_stem(title, fallback=video_id or "douyin")
        with tempfile.TemporaryDirectory(prefix="douyin-asr-") as tmpdir:
            tmp = Path(tmpdir)
            video_path = tmp / f"{video_id or 'douyin'}_{stem}.mp4"
            audio_path = tmp / f"{video_id or 'douyin'}_{stem}_16k.wav"
            try:
                self._emit_progress("asr-prepare", "已创建临时目录用于抖音转写", temp_dir=str(tmp))
                self._download_file(video_url, video_path)
                self._extract_audio(video_path, audio_path)
                transcript = self._run_funasr(audio_path)
                transcript = transcript.strip()
                if transcript:
                    self._emit_progress("asr-success", "抖音 ASR 转写成功", transcript_length=len(transcript))
                    return transcript, {
                        "asr_attempted": True,
                        "asr_engine": "funasr",
                        "asr_audio_format": "wav-16k-mono",
                        "asr_error": "",
                    }
                return "", {
                    "asr_attempted": True,
                    "asr_engine": "funasr",
                    "asr_error": "funasr-empty-transcript",
                }
            except Exception as exc:  # noqa: BLE001
                self._emit_progress("asr-failed", f"抖音 ASR 转写失败：{exc}")
                return "", {
                    "asr_attempted": True,
                    "asr_engine": "funasr",
                    "asr_error": str(exc),
                }

    def fetch(self, url: str) -> FetchResult:
        target_url = str(url or "").strip()
        if not target_url:
            return FetchResult(ok=False, channel=self.name, url=url, error="url-empty")

        try:
            final_url, video_id = self._resolve_share(target_url)
        except Exception as exc:  # noqa: BLE001
            self._emit_progress("failed", f"抖音分享链接解析失败：{exc}")
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"douyin-resolve-failed: {exc}")

        share_page = final_url
        if video_id:
            share_page = f"https://www.iesdouyin.com/share/video/{video_id}"

        try:
            self._emit_progress("fetch-page", "正在读取抖音页面信息", share_page=share_page)
            s = self._session()
            r = s.get(share_page, headers=_MOBILE_HEADERS, timeout=20)
            r.raise_for_status()
            router_data = self._extract_router_data(r.text)
            item, matched_key = self._find_first_item(router_data)
            if not item:
                self._emit_progress("failed", "抖音页面数据为空")
                return FetchResult(ok=False, channel=self.name, url=final_url, error="douyin-router-data-empty")

            video = item.get("video") or {}
            play_addr = video.get("play_addr") or {}
            url_list = play_addr.get("url_list") or []
            video_url = str(url_list[0] if url_list else "").strip()
            if video_url:
                video_url = video_url.replace("playwm", "play")

            title = self._collapse_ws(item.get("desc") or item.get("title") or "") or "(无标题)"
            author_obj = item.get("author") or {}
            author = self._collapse_ws(
                author_obj.get("nickname")
                or author_obj.get("unique_id")
                or author_obj.get("short_id")
                or author_obj.get("uid")
                or ""
            )
            cover_obj = video.get("cover") or video.get("origin_cover") or {}
            cover_list = cover_obj.get("url_list") or []
            cover_url = str(cover_list[0] if cover_list else "").strip()
            final_video_id = str(item.get("aweme_id") or item.get("awemeId") or video_id or "").strip()
            create_time = item.get("create_time") or item.get("createTime") or ""
            published_at = str(create_time)
            if str(create_time).isdigit():
                try:
                    from datetime import datetime, timezone

                    published_at = datetime.fromtimestamp(int(create_time)).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    published_at = str(create_time)

            self._emit_progress("fetch-page", "抖音页面信息读取完成", video_id=final_video_id, router_key=matched_key)
            self._emit_progress("page-transcript", "正在探测页面字幕/文案")
            transcript_text, transcript_meta = self._probe_page_transcript(item)
            asr_readiness = self._detect_asr_readiness()
            asr_meta: dict = {"asr_attempted": False, "asr_error": "", "asr_engine": ""}
            transcript_label = "页面字幕"
            transcript_language = ""

            if transcript_text:
                self._emit_progress("page-transcript", "页面字幕探测成功", transcript_source=transcript_meta.get("transcript_source") or "")

            if not transcript_text:
                self._emit_progress("page-transcript", "页面字幕未命中，准备走 ASR 转写")
                transcript_text, asr_meta = self._transcribe_with_funasr(
                    video_url=video_url,
                    video_id=final_video_id or video_id,
                    title=title,
                )
                if transcript_text:
                    transcript_meta = {
                        "transcript_available": True,
                        "transcript_source": "funasr_asr",
                    }
                    transcript_label = "ASR转写"
                    transcript_language = "zh"

            content_lines = [
                f"标题：{title}",
                f"作者：{author}" if author else "",
                f"作品ID：{final_video_id}" if final_video_id else "",
                f"作品页：{final_url}",
                f"视频直链：{video_url}" if video_url else "",
            ]
            if transcript_text:
                content_lines.extend(["", f"{transcript_label}：", transcript_text])
            else:
                reason = asr_meta.get("asr_error") or "page_probe_none"
                content_lines.append(
                    f"说明：D2 页面字幕探测未命中，且 ASR fallback 未成功；当前仅拿到元信息与真实视频地址。失败原因：{reason}"
                )
            content = "\n".join([x for x in content_lines if x is not None]).strip()

            self._emit_progress("completed", "抖音正文抓取完成", transcript_source=transcript_meta.get("transcript_source") or "", transcript_available=bool(transcript_text))
            return FetchResult(
                ok=True,
                channel=self.name,
                url=final_url,
                title=title[:140],
                content_markdown=content,
                author=author,
                published_at=published_at,
                meta={
                    "platform": "douyin",
                    "video_id": final_video_id,
                    "cover_url": cover_url,
                    "video_url": video_url,
                    "fetch_method": "router_data",
                    "router_key": matched_key,
                    "transcript_available": bool(transcript_text),
                    "transcript_source": transcript_meta.get("transcript_source") or "page_probe_none",
                    "transcript_language": transcript_language,
                    "share_page": share_page,
                    "asr_readiness": asr_readiness,
                    "asr_attempted": bool(asr_meta.get("asr_attempted")),
                    "asr_engine": asr_meta.get("asr_engine") or "",
                    "asr_error": asr_meta.get("asr_error") or "",
                },
                error="",
            )
        except Exception as exc:  # noqa: BLE001
            self._emit_progress("failed", f"抖音抓取失败：{exc}")
            return FetchResult(ok=False, channel=self.name, url=final_url, error=f"douyin-fetch-failed: {exc}")
