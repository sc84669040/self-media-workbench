from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import adapters.bilibili_adapter as bilibili_module  # noqa: E402
from adapters.bilibili_adapter import BilibiliAdapter  # noqa: E402


class DummyCompletedProcess:
    def __init__(self, *, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_can_handle_bilibili_and_b23_urls():
    adapter = BilibiliAdapter()

    assert adapter.can_handle("https://www.bilibili.com/video/BV1ZuQvB9EVr/") is True
    assert adapter.can_handle("https://b23.tv/abcd123") is True
    assert adapter.can_handle("https://m.bilibili.com/video/BV1ZuQvB9EVr") is True
    assert adapter.can_handle("https://www.youtube.com/watch?v=abc") is False


def test_pick_best_subtitle_file_prefers_ai_zh(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    adapter = BilibiliAdapter()
    candidates = [
        tmp_path / "BV1ZuQvB9EVr.en.srt",
        tmp_path / "BV1ZuQvB9EVr.ai-en.srt",
        tmp_path / "BV1ZuQvB9EVr.zh-Hant.srt",
        tmp_path / "BV1ZuQvB9EVr.ai-zh.srt",
    ]

    chosen = adapter._pick_best_subtitle_file(candidates)

    assert chosen is not None
    assert chosen.name == "BV1ZuQvB9EVr.ai-zh.srt"


def test_resolve_yt_dlp_binary_falls_back_to_user_local_bin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    adapter = BilibiliAdapter()
    local_bin = tmp_path / ".local" / "bin"
    local_bin.mkdir(parents=True)
    yt_dlp_bin = local_bin / "yt-dlp"
    yt_dlp_bin.write_text("#!/bin/sh\n", encoding="utf-8")

    monkeypatch.setattr(bilibili_module, "which", lambda _: None)
    monkeypatch.setattr(bilibili_module.Path, "home", lambda: tmp_path)

    assert adapter._resolve_yt_dlp_binary() == str(yt_dlp_bin)


def test_fetch_returns_metadata_from_yt_dlp_dump_json_when_no_subtitles(monkeypatch: pytest.MonkeyPatch):
    adapter = BilibiliAdapter()
    payload = {
        "id": "BV1ZuQvB9EVr",
        "title": "B站测试视频",
        "uploader": "测试UP主",
        "upload_date": "20260416",
        "thumbnail": "https://i0.hdslb.com/test.jpg",
        "webpage_url": "https://www.bilibili.com/video/BV1ZuQvB9EVr/",
        "url": "https://upos-sz-mirror.example.com/test.mp4",
    }

    expected_yt_dlp = adapter._resolve_yt_dlp_binary()

    def fake_run(cmd, capture_output, text, timeout, check):
        if "--dump-single-json" in cmd:
            assert cmd[:3] == [expected_yt_dlp, "--ignore-config", "--dump-single-json"]
            return DummyCompletedProcess(stdout=json.dumps(payload))
        if "--write-subs" in cmd:
            return DummyCompletedProcess(returncode=0)
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = adapter.fetch("https://b23.tv/demo")

    assert result.ok is True
    assert result.channel == "bilibili"
    assert result.url == "https://www.bilibili.com/video/BV1ZuQvB9EVr/"
    assert result.title == "B站测试视频"
    assert result.author == "测试UP主"
    assert result.published_at == "2026-04-16"
    assert "标题：B站测试视频" in result.content_markdown
    assert "说明：当前未拿到可用字幕" in result.content_markdown
    assert result.meta["platform"] == "bilibili"
    assert result.meta["video_id"] == "BV1ZuQvB9EVr"
    assert result.meta["cover_url"] == "https://i0.hdslb.com/test.jpg"
    assert result.meta["video_url"] == "https://upos-sz-mirror.example.com/test.mp4"
    assert result.meta["fetch_method"] == "yt_dlp_metadata"
    assert result.meta["transcript_available"] is False
    assert result.meta["transcript_source"] == "yt_dlp_subtitles_missing"
    assert result.meta["transcript_language"] == ""


def test_fetch_returns_transcript_and_uses_cookie_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    cookie_path = tmp_path / "bilibili-cookies.txt"
    cookie_path.write_text("# test cookie fixture\n", encoding="utf-8")
    settings_path = tmp_path / "fetch-settings.yaml"
    settings_path.write_text(
        yaml.safe_dump({"bilibili": {"cookies_path": str(cookie_path)}}, allow_unicode=True),
        encoding="utf-8",
    )
    monkeypatch.setattr(BilibiliAdapter, "SETTINGS_PATH", settings_path)

    adapter = BilibiliAdapter()
    payload = {
        "id": "BV1ZuQvB9EVr",
        "title": "B站字幕测试视频",
        "uploader": "测试UP主",
        "upload_date": "20260416",
        "thumbnail": "https://i0.hdslb.com/test.jpg",
        "webpage_url": "https://www.bilibili.com/video/BV1ZuQvB9EVr/",
        "url": "https://upos-sz-mirror.example.com/test.mp4",
    }

    def fake_run(cmd, capture_output, text, timeout, check):
        if "--dump-single-json" in cmd:
            return DummyCompletedProcess(stdout=json.dumps(payload))
        if "--write-subs" in cmd:
            assert "--cookies" in cmd
            assert str(cookie_path) in cmd
            output_tpl = Path(cmd[cmd.index("--output") + 1])
            output_dir = output_tpl.parent
            (output_dir / "BV1ZuQvB9EVr.ai-en.srt").write_text(
                "1\n00:00:00,000 --> 00:00:01,000\nhello\n",
                encoding="utf-8",
            )
            (output_dir / "BV1ZuQvB9EVr.ai-zh.srt").write_text(
                "1\n00:00:00,000 --> 00:00:01,000\n这一期我们来聊一聊web coding\n\n2\n00:00:01,000 --> 00:00:02,000\n现在已经迭代到了4.0版本\n",
                encoding="utf-8",
            )
            return DummyCompletedProcess(returncode=0)
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = adapter.fetch("https://b23.tv/demo")

    assert result.ok is True
    assert result.channel == "bilibili"
    assert result.url == "https://www.bilibili.com/video/BV1ZuQvB9EVr/"
    assert result.title == "B站字幕测试视频"
    assert "字幕转写：" in result.content_markdown
    assert "这一期我们来聊一聊web coding" in result.content_markdown
    assert "现在已经迭代到了4.0版本" in result.content_markdown
    assert result.meta["fetch_method"] == "yt_dlp_subtitles"
    assert result.meta["subtitle_file"] == "BV1ZuQvB9EVr.ai-zh.srt"
    assert result.meta["transcript_available"] is True
    assert result.meta["transcript_source"] == "yt_dlp_subtitles"
    assert result.meta["transcript_language"] == "ai-zh"
    assert result.meta["cookies_configured"] is True
