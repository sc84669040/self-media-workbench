#!/usr/bin/env python3
from __future__ import annotations

import re

import requests

from adapters.base import FetchAdapter, host_matches
from models import FetchResult

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
}


class WechatAdapter(FetchAdapter):
    name = "wechat"

    def can_handle(self, url: str) -> bool:
        return host_matches(url, "mp.weixin.qq.com")

    def _collapse_ws(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def fetch(self, url: str) -> FetchResult:
        target_url = str(url or "").strip()
        if not target_url:
            return FetchResult(ok=False, channel=self.name, url=url, error="url-empty")
        try:
            from bs4 import BeautifulSoup

            resp = requests.get(target_url, headers=_HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            title = self._collapse_ws((soup.select_one("h1") or soup.title).get_text(" ", strip=True) if (soup.select_one("h1") or soup.title) else "")
            author = self._collapse_ws((soup.select_one("#js_name") or soup.select_one(".profile_nickname")).get_text(" ", strip=True) if (soup.select_one("#js_name") or soup.select_one(".profile_nickname")) else "")

            content_node = soup.select_one("#js_content") or soup.select_one(".rich_media_content") or soup.body
            if content_node is None:
                return FetchResult(ok=False, channel=self.name, url=target_url, error="wechat-empty-body")

            chunks: list[str] = []
            for el in content_node.find_all(["h1", "h2", "h3", "p", "li"]):
                txt = self._collapse_ws(el.get_text(" ", strip=True))
                if len(txt) >= 8:
                    chunks.append(txt)
            content = "\n\n".join(chunks).strip()[:25000]

            return FetchResult(
                ok=bool(content),
                channel=self.name,
                url=target_url,
                title=(title or "(无标题)")[:140],
                content_markdown=content,
                author=author,
                meta={"fetch_method": "wechat_html"},
                error="" if content else "wechat-content-empty",
            )
        except Exception as exc:  # noqa: BLE001
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"wechat-fetch-failed: {exc}")
