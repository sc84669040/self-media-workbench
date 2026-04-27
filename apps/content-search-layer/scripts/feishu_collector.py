#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from workspace_paths import MONITORING_ROOT, resolve_skill_project_root

BASE_DIR = Path(__file__).resolve().parents[1]
FEISHU_SOURCES_PATH = MONITORING_ROOT / "a-stage-feishu-sources.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
}
REQUEST_TIMEOUT = 30
CONTENT_FETCH_HUB_CLI_PATH = resolve_skill_project_root("content-fetch-hub") / "scripts" / "fetch_content_cli.py"


def collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def fetch_via_content_fetch_hub(url: str) -> tuple[str, str, str]:
    target_url = str(url or "").strip()
    if not target_url:
        return "", "", "url-empty"
    if not CONTENT_FETCH_HUB_CLI_PATH.exists():
        return "", "", "hub-cli-missing"

    cmd = ["python3", str(CONTENT_FETCH_HUB_CLI_PATH), target_url, "--json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=240, check=False)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "hub-failed").strip()
        return "", "", f"hub-failed: {err}"

    try:
        data = json.loads(proc.stdout or "{}")
    except Exception as exc:  # noqa: BLE001
        return "", "", f"hub-json-error: {exc}"

    item = (data.get("results") or [{}])[0]
    if not item.get("ok"):
        return "", "", str(item.get("error") or "hub-empty-result")
    return (
        str(item.get("title") or "").strip(),
        str(item.get("content_markdown") or "").strip(),
        str((item.get("meta") or {}).get("fetch_method") or "content_fetch_hub").strip() or "content_fetch_hub",
    )


def load_feishu_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    if not FEISHU_SOURCES_PATH.exists():
        return []

    data = json.loads(FEISHU_SOURCES_PATH.read_text(encoding="utf-8")) or {}
    sources = data.get("sources") or []
    out: list[dict[str, Any]] = []
    for source in sources:
        item = dict(source or {})
        item["id"] = str(item.get("id") or "").strip()
        item["name"] = str(item.get("name") or "").strip()
        item["enabled"] = bool(item.get("enabled", True))
        item["priority"] = int(item.get("priority") or 5)
        item["category"] = str(item.get("category") or "").strip()
        item["entry_url"] = str(item.get("entry_url") or "").strip()
        item["fetch_method"] = str(item.get("fetch_method") or "jina_reader").strip() or "jina_reader"
        item["notes"] = str(item.get("notes") or "").strip()
        if item["id"] and item["name"] and item["entry_url"]:
            out.append(item)

    if enabled_only:
        out = [x for x in out if x.get("enabled")]
    return out


def _extract_title_and_content_from_jina(markdown_text: str, fallback_title: str = "") -> tuple[str, str]:
    lines = [line.rstrip() for line in str(markdown_text or "").splitlines()]
    title = ""
    content_lines: list[str] = []

    for line in lines:
        s = line.strip()
        if not s:
            continue
        if not title and s.startswith("# "):
            title = collapse_ws(s[2:])
            continue
        if s.startswith("!"):
            continue
        content_lines.append(line)

    if not title:
        for line in lines:
            s = line.strip()
            if s and not s.startswith("["):
                title = collapse_ws(s)[:120]
                break

    if not title:
        title = collapse_ws(fallback_title) or "(无标题)"

    content = "\n".join(content_lines).strip()
    content = re.sub(r"\n{3,}", "\n\n", content)
    return title[:140], content[:20000]


def fetch_feishu_doc(url: str) -> tuple[str, str, str]:
    target_url = str(url or "").strip()
    if not target_url:
        return "", "", "url-empty"

    # 方案0：优先统一走 content-fetch-hub，中台失败再回退老逻辑。
    title, content, method = fetch_via_content_fetch_hub(target_url)
    if content and len(collapse_ws(content)) >= 80:
        return title, content, method

    # 方案A：走 jina reader，优先拿可读正文。
    jina_url = f"https://r.jina.ai/{target_url}"
    try:
        resp = requests.get(jina_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        title, content = _extract_title_and_content_from_jina(resp.text, fallback_title=target_url)
        if content and len(collapse_ws(content)) >= 80:
            return title, content, "jina_reader"
    except Exception:
        pass

    # 方案B：直接抓网页，降级提取正文。
    try:
        resp = requests.get(target_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
            tag.decompose()

        title = collapse_ws((soup.title.get_text(" ", strip=True) if soup.title else ""))
        node = (
            soup.select_one("article")
            or soup.select_one("main")
            or soup.select_one(".wiki-block")
            or soup.select_one(".docx-content")
            or soup.body
        )
        if node is None:
            return title[:140], "", "html_parser"

        texts: list[str] = []
        for el in node.find_all(["h1", "h2", "h3", "p", "li"]):
            line = collapse_ws(el.get_text(" ", strip=True))
            if len(line) >= 8:
                texts.append(line)

        if not texts:
            body_text = collapse_ws(node.get_text(" ", strip=True))[:20000]
            return (title or "(无标题)")[:140], body_text, "html_parser"

        content = "\n\n".join(texts)[:20000]
        return (title or "(无标题)")[:140], content, "html_parser"
    except Exception as exc:  # noqa: BLE001
        return "", "", f"fetch-failed: {exc}"


class FeishuCollector:
    platform = "feishu"

    def __init__(self, logger=None) -> None:
        self.logger = logger or (lambda *_args, **_kwargs: None)
        self.sources = {item["id"]: item for item in load_feishu_sources(enabled_only=False)}

    def collect_for_handle(self, handle: str, limit: int) -> list[dict[str, Any]]:
        source = self.sources.get(str(handle or "").strip())
        if not source:
            self.logger(f"[feishu] source not found for handle={handle}")
            return []

        title, content, method = fetch_feishu_doc(source.get("entry_url") or "")
        body_ok = bool(collapse_ws(content))
        fetched_at = datetime.utcnow().isoformat() + "Z"

        # 飞书文档常是持续更新，item_id 用 URL + 内容指纹，变化时可形成新候选。
        digest_base = f"{source.get('entry_url') or ''}|{collapse_ws(content)[:2000]}"
        item_id = hashlib.sha1(digest_base.encode("utf-8")).hexdigest()

        metrics_payload = {
            "source_kind": "feishu_doc",
            "entry_url": source.get("entry_url") or "",
            "body_fetch_ok": body_ok,
            "fetch_method": method,
            "category": source.get("category") or "",
            "fetched_at": fetched_at,
        }

        out = [
            {
                "platform": "feishu",
                "source_handle": str(source.get("name") or source.get("id") or handle),
                "item_id": item_id,
                "title": (title or str(source.get("name") or "飞书文档"))[:140],
                "content": (content or title or "").strip(),
                "url": str(source.get("entry_url") or "").strip(),
                "published_at": "",
                "metrics_json": json.dumps(metrics_payload, ensure_ascii=False),
            }
        ]
        self.logger(f"[feishu] fetched source={source.get('name') or handle} body_ok={body_ok}")
        return out[: max(1, limit)]
