from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import fetch_content_cli  # noqa: E402


class _FakeResult:
    def __init__(self, ok: bool = True):
        self.ok = ok

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "channel": "web",
            "url": "https://example.com/article",
            "title": "Example Article",
            "content_markdown": "hello world body",
            "author": "Example",
            "published_at": "2026-04-19 12:00:00",
            "fetched_at": "2026-04-19T04:00:00+00:00",
            "images": [],
            "meta": {"fetch_method": "unit-test"},
            "error": "",
        }


class _FakeAdapter:
    def fetch(self, url: str) -> _FakeResult:
        return _FakeResult(ok=True)


class _FakeRoute:
    adapter = _FakeAdapter()


class _FakeRegistry:
    def resolve(self, url: str) -> _FakeRoute:
        return _FakeRoute()


def test_run_batch_fetch_only_does_not_write_obsidian(monkeypatch, tmp_path):
    calls: list[str] = []

    monkeypatch.setattr(fetch_content_cli, "build_default_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(fetch_content_cli, "write_result_to_obsidian", lambda result, vault_root: calls.append(str(vault_root)) or "should-not-happen")

    summary = fetch_content_cli.run_batch(
        ["https://example.com/article"],
        vault=str(tmp_path),
        retry_count=0,
        analyze=False,
        write_obsidian=False,
    )

    assert summary["ok"] is True
    assert summary["write_obsidian"] is False
    assert summary["success"] == 1
    assert calls == []
    assert summary["results"][0]["saved_path"] == ""
    assert summary["results"][0]["status"] == "success"


def test_run_batch_write_obsidian_calls_writer(monkeypatch, tmp_path):
    calls: list[str] = []

    monkeypatch.setattr(fetch_content_cli, "build_default_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(fetch_content_cli, "write_result_to_obsidian", lambda result, vault_root: calls.append(str(vault_root)) or str(Path(vault_root) / "抓取内容" / "x.md"))

    summary = fetch_content_cli.run_batch(
        ["https://example.com/article"],
        vault=str(tmp_path),
        retry_count=0,
        analyze=False,
        write_obsidian=True,
    )

    assert summary["ok"] is True
    assert summary["write_obsidian"] is True
    assert summary["success"] == 1
    assert calls == [str(tmp_path)]
    assert summary["results"][0]["saved_path"].replace("\\", "/").endswith("抓取内容/x.md")
