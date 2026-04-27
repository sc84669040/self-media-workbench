#!/usr/bin/env python3
from __future__ import annotations

import json
import re

import requests

from models import FetchResult
from adapters.base import FetchAdapter, host_matches

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
}


class FeishuAdapter(FetchAdapter):
    name = "feishu"

    def can_handle(self, url: str) -> bool:
        return host_matches(url, "feishu.cn", "larksuite.com")

    def _collapse_ws(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def _extract_balanced_json(self, text: str, start_idx: int) -> str:
        if start_idx < 0 or start_idx >= len(text) or text[start_idx] != "{":
            return ""
        depth = 0
        in_str = False
        escape = False
        for idx in range(start_idx, len(text)):
            ch = text[idx]
            if in_str:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start_idx : idx + 1]
        return ""

    def _extract_server_data_title(self, html: str) -> str:
        marker = "window.SERVER_DATA = Object("
        pos = html.find(marker)
        if pos < 0:
            return ""
        start = html.find("{", pos + len(marker) - 1)
        raw = self._extract_balanced_json(html, start)
        if not raw:
            return ""
        try:
            data = json.loads(raw)
            meta = data.get("meta") or {}
            return self._collapse_ws(meta.get("title") or "")
        except Exception:
            return ""

    def _extract_from_client_vars(self, html: str) -> tuple[str, str]:
        marker = "window.DATA = Object.assign({}, window.DATA, { clientVars: Object("
        pos = html.find(marker)
        if pos < 0:
            return "", ""
        start = html.find("{", pos + len(marker) - 1)
        raw = self._extract_balanced_json(html, start)
        if not raw:
            return "", ""
        try:
            data = json.loads(raw)
        except Exception:
            return "", ""

        data_payload = data.get("data") or {}
        payload = data_payload.get("block_map") or {}
        block_sequence = data_payload.get("block_sequence") or []
        if not isinstance(payload, dict):
            return "", ""

        ordered_ids = [str(x or "").strip() for x in block_sequence if str(x or "").strip()]
        if not ordered_ids:
            ordered_ids = list(payload.keys())

        chunks: list[str] = []
        seen: set[str] = set()
        for block_id in ordered_ids:
            block = payload.get(block_id) or {}
            if not isinstance(block, dict):
                continue
            block_data = block.get("data") or {}
            block_type = str(block_data.get("type") or "")
            text_obj = (((block_data.get("text") or {}).get("initialAttributedTexts") or {}).get("text") or {})
            texts: list[str] = []
            if isinstance(text_obj, dict):
                for _, val in sorted(text_obj.items(), key=lambda kv: str(kv[0])):
                    txt = self._collapse_ws(val)
                    if txt:
                        texts.append(txt)
            line = self._collapse_ws(" ".join(texts))
            if not line:
                continue
            if block_type in {"bullet", "ordered", "checkBox"}:
                line = f"- {line}"
            if line not in seen:
                seen.add(line)
                chunks.append(line)
        content = "\n\n".join(chunks).strip()[:40000]
        title = chunks[0] if chunks else ""
        return title, content

    def fetch(self, url: str) -> FetchResult:
        target_url = str(url or "").strip()
        if not target_url:
            return FetchResult(ok=False, channel=self.name, url=url, error="url-empty")

        try:
            from bs4 import BeautifulSoup

            resp = requests.get(target_url, headers=_HEADERS, timeout=30)
            resp.raise_for_status()
            html = resp.text

            api_title = self._extract_server_data_title(html)
            client_title, client_content = self._extract_from_client_vars(html)
            if client_content:
                title = api_title or client_title or "(无标题)"
                return FetchResult(
                    ok=True,
                    channel=self.name,
                    url=target_url,
                    title=title[:140],
                    content_markdown=client_content,
                    meta={"fetch_method": "feishu_client_vars"},
                    error="",
                )

            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
                tag.decompose()

            title = api_title or self._collapse_ws(soup.title.get_text(" ", strip=True) if soup.title else "")
            node = (
                soup.select_one("article")
                or soup.select_one("main")
                or soup.select_one(".wiki-block")
                or soup.select_one(".docx-content")
                or soup.body
            )
            if node is None:
                return FetchResult(
                    ok=False,
                    channel=self.name,
                    url=target_url,
                    error="feishu-empty-body",
                )

            chunks: list[str] = []
            for el in node.find_all(["h1", "h2", "h3", "p", "li"]):
                txt = self._collapse_ws(el.get_text(" ", strip=True))
                if len(txt) >= 8:
                    chunks.append(txt)
            content = "\n\n".join(chunks).strip()[:20000]

            if not title:
                title = "(无标题)"
            return FetchResult(
                ok=bool(content),
                channel=self.name,
                url=target_url,
                title=title[:140],
                content_markdown=content,
                meta={"fetch_method": "html_parser"},
                error="" if content else "feishu-content-empty",
            )
        except Exception as exc:  # noqa: BLE001
            return FetchResult(ok=False, channel=self.name, url=target_url, error=f"feishu-fetch-failed: {exc}")
