#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from shutil import which

import yaml
from datetime import datetime

from adapters.base import FetchAdapter, host_matches
from models import FetchResult


class XAdapter(FetchAdapter):
    name = "x"
    REPO_SCRIPTS = Path(__file__).resolve().parents[4] / "scripts"

    def _root_config(self) -> dict:
        if self.REPO_SCRIPTS.exists():
            repo_scripts_text = str(self.REPO_SCRIPTS)
            if repo_scripts_text in sys.path:
                sys.path.remove(repo_scripts_text)
            sys.path.insert(0, repo_scripts_text)
            try:
                from self_media_config import get_config  # type: ignore

                return get_config()
            except Exception:
                return {}
        return {}

    def _format_datetime_text(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            if raw.isdigit() and len(raw) >= 10:
                return datetime.fromtimestamp(int(raw[:10])).strftime("%Y-%m-%d %H:%M:%S")
            if len(raw) == 10 and raw.count("-") == 2:
                return datetime.strptime(raw, "%Y-%m-%d").strftime("%Y-%m-%d 00:00:00")
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.astimezone().replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return raw

    def can_handle(self, url: str) -> bool:
        return host_matches(url, "x.com", "twitter.com")

    def _collapse_ws(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def _load_proxy(self) -> str:
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

    def _load_auth_env(self) -> dict[str, str]:
        out: dict[str, str] = {}
        auth_token = str(os.environ.get("TWITTER_AUTH_TOKEN") or "").strip()
        ct0 = str(os.environ.get("TWITTER_CT0") or "").strip()
        if auth_token:
            out["TWITTER_AUTH_TOKEN"] = auth_token
        if ct0:
            out["TWITTER_CT0"] = ct0
        return out

    def _extract_x_identifiers(self, target_url: str) -> tuple[str, str]:
        m = re.search(r"(?:x|twitter)\.com/([^/]+)/status/(\d+)", target_url, re.I)
        if not m:
            return "", ""
        return m.group(1), m.group(2)

    def _resolve_twitter_bin(self) -> str:
        root = self._root_config()
        env_bin = os.environ.get("TWITTER_BIN", "").strip()
        if env_bin and Path(env_bin).exists():
            return env_bin
        configured = str(((root.get("external_tools") or {}).get("twitter_bin")) or "").strip()
        if configured and Path(configured).exists():
            return configured

        cmd = which("twitter")
        if cmd:
            return cmd
        return ""

    def _fetch_via_twitter_cli(self, target_url: str) -> FetchResult:
        screen_name, tweet_id = self._extract_x_identifiers(target_url)
        if not screen_name or not tweet_id:
            return FetchResult(ok=False, channel=self.name, url=target_url, error="x-parse-status-url-failed")

        twitter_bin = self._resolve_twitter_bin()
        if not twitter_bin:
            return FetchResult(ok=False, channel=self.name, url=target_url, error="x-twitter-cli-missing")

        env = os.environ.copy()
        env.update(self._load_auth_env())
        proxy = self._load_proxy()
        if proxy and not env.get("TWITTER_PROXY"):
            env["TWITTER_PROXY"] = proxy

        # 与 NightHawk 保持一致：优先 user-posts
        cmd = [twitter_bin, "user-posts", screen_name, "-n", "20", "--json"]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=45, env=env)
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "twitter-cli failed").strip()
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"x-twitter-cli-failed: {err}")

        try:
            payload = json.loads(proc.stdout or "{}")
        except Exception as exc:  # noqa: BLE001
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"x-twitter-cli-json-error: {exc}")

        if not payload.get("ok"):
            err_msg = str((payload.get("error") or {}).get("message") or "twitter-cli api failed")
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"x-twitter-cli-api-failed: {err_msg}")

        items = payload.get("data") or []
        if not isinstance(items, list):
            return FetchResult(ok=False, channel=self.name, url=target_url, error="x-twitter-cli-invalid-data")

        data = next((it for it in items if str(it.get("id") or "") == tweet_id), None)
        if data is None and items:
            data = items[0]
        if not data:
            return FetchResult(ok=False, channel=self.name, url=target_url, error="x-twitter-cli-empty-data")

        text = self._collapse_ws(str(data.get("text") or ""))
        author = data.get("author") or {}
        title = self._collapse_ws(str(author.get("screenName") or author.get("username") or screen_name))
        published_at = self._format_datetime_text(str(data.get("createdAtISO") or data.get("createdAtLocal") or data.get("createdAt") or ""))

        if not text:
            return FetchResult(ok=False, channel=self.name, url=target_url, error="x-twitter-cli-empty-text")

        content = f"标题：{title} 的帖子\n链接：{target_url}\n\n{text}"
        return FetchResult(
            ok=True,
            channel=self.name,
            url=target_url,
            title=(f"{title} 的帖子" if title else "X 帖子")[:140],
            content_markdown=content[:20000],
            published_at=published_at,
            meta={"fetch_method": "twitter_cli"},
        )

    def fetch(self, url: str) -> FetchResult:
        target_url = str(url or "").strip()
        if not target_url:
            return FetchResult(ok=False, channel=self.name, url=url, error="url-empty")

        return self._fetch_via_twitter_cli(target_url)
