#!/usr/bin/env python3
from __future__ import annotations

import re

import requests

from adapters.base import FetchAdapter, get_url_host
from models import FetchResult

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
}


class WebAdapter(FetchAdapter):
    name = "web"

    def can_handle(self, url: str) -> bool:
        u = str(url or "").strip().lower()
        return (u.startswith("http://") or u.startswith("https://")) and bool(get_url_host(url))

    def _collapse_ws(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def fetch(self, url: str) -> FetchResult:
        target_url = str(url or "").strip()
        if not target_url:
            return FetchResult(ok=False, channel=self.name, url=url, error="url-empty")

        # 与 NightHawk 一致：仅 HTML 解析
        try:
            from bs4 import BeautifulSoup

            resp = requests.get(target_url, headers=_HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
                tag.decompose()

            title = self._collapse_ws(soup.title.get_text(" ", strip=True) if soup.title else "")
            node = soup.select_one("article") or soup.select_one("main") or soup.body
            if node is None:
                return FetchResult(ok=False, channel=self.name, url=target_url, error="web-empty-body")

            chunks: list[str] = []
            for el in node.find_all(["h1", "h2", "h3", "p", "li"]):
                txt = self._collapse_ws(el.get_text(" ", strip=True))
                if len(txt) >= 12:
                    chunks.append(txt)
            content = "\n\n".join(chunks).strip()[:25000]
            return FetchResult(
                ok=bool(content),
                channel=self.name,
                url=target_url,
                title=(title or "(无标题)")[:140],
                content_markdown=content,
                meta={"fetch_method": "html_parser"},
                error="" if content else "web-content-empty",
            )
        except Exception as exc:  # noqa: BLE001
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"web-fetch-failed: {exc}")
