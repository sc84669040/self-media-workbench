#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests
import yaml
from bs4 import BeautifulSoup

from workspace_paths import MONITORING_ROOT, resolve_skill_project_root

BASE_DIR = Path(__file__).resolve().parents[1]
WECHAT_SOURCES_PATH = MONITORING_ROOT / "a-stage-wechat-sources.json"
LEGACY_SCRIPT_PATH = resolve_skill_project_root("content-to-obsidian") / "scripts" / "monitor_wechat_articles.py"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
}
REQUEST_TIMEOUT = 25
CONTENT_FETCH_HUB_CLI_PATH = resolve_skill_project_root("content-fetch-hub") / "scripts" / "fetch_content_cli.py"
COLLECTOR_FLAGS_PATH = BASE_DIR / "config" / "collector-flags.yaml"

try:
    BEIJING_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    BEIJING_TZ = timezone(timedelta(hours=8))


def load_wechat_sources() -> list[dict[str, Any]]:
    if not WECHAT_SOURCES_PATH.exists():
        return []
    data = json.loads(WECHAT_SOURCES_PATH.read_text(encoding="utf-8")) or {}
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
        item["fetch_method"] = str(item.get("fetch_method") or "wechat_page").strip() or "wechat_page"
        item["notes"] = str(item.get("notes") or "").strip()
        if item["id"] and item["name"]:
            out.append(item)
    return out


@lru_cache(maxsize=1)
def load_collector_flags() -> dict[str, Any]:
    default_flags: dict[str, Any] = {
        "wechat": {
            "metadata_only": True,
            "enable_body_hydration": False,
            "body_fetch_via_hub": True,
            "body_fetch_via_html_fallback": True,
        }
    }
    if not COLLECTOR_FLAGS_PATH.exists():
        return default_flags
    try:
        data = yaml.safe_load(COLLECTOR_FLAGS_PATH.read_text(encoding="utf-8")) or {}
        merged = dict(default_flags)
        merged_wechat = dict(default_flags.get("wechat") or {})
        merged_wechat.update(data.get("wechat") or {})
        merged["wechat"] = merged_wechat
        return merged
    except Exception:
        return default_flags


def load_legacy_monitor_module():
    spec = importlib.util.spec_from_file_location("monitor_wechat_articles", LEGACY_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载微信监控脚本：{LEGACY_SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def _fix_suspicious_future_iso(raw: str, dt_raw: datetime) -> datetime:
    dt_local = dt_raw.astimezone(BEIJING_TZ)
    now_local = datetime.now(tz=BEIJING_TZ)
    if dt_local <= now_local + timedelta(minutes=10):
        return dt_local
    raw_upper = (raw or "").upper()
    has_utc_marker = ("Z" in raw_upper) or ("GMT" in raw_upper) or ("+00:00" in raw_upper) or ("+0000" in raw_upper)
    if not has_utc_marker:
        return dt_local
    local_dt = dt_raw.replace(tzinfo=None).replace(tzinfo=BEIJING_TZ)
    if local_dt <= now_local + timedelta(minutes=10):
        return local_dt
    return dt_local


def to_iso_time(raw: str) -> str:
    value = collapse_ws(raw)
    if not value:
        return ""
    # RSS 常见格式
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BEIJING_TZ)
        dt = _fix_suspicious_future_iso(value, dt)
        return dt.isoformat()
    except Exception:
        pass
    # Atom / 常见日期格式
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=BEIJING_TZ)
            dt = _fix_suspicious_future_iso(value, dt)
            return dt.isoformat()
        except Exception:
            continue
    return value


def parse_feed_items_from_entry(source: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    entry_url = str(source.get("entry_url") or "").strip()
    if not entry_url:
        return []

    resp = requests.get(entry_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    raw_xml = resp.text

    source_name = str(source.get("name") or "")
    source_kind = str(source.get("fetch_method") or "official_feed")

    items: list[dict[str, Any]] = []

    try:
        root = ET.fromstring(raw_xml)
        # RSS
        if root.tag.lower().endswith("rss") or root.find(".//channel") is not None:
            for it in root.findall(".//item")[: max(1, limit * 3)]:
                title = collapse_ws(it.findtext("title") or "")
                link = collapse_ws(it.findtext("link") or "")
                published_at = to_iso_time(it.findtext("pubDate") or "")
                if title and link:
                    items.append(
                        {
                            "source": source_name,
                            "source_kind": source_kind,
                            "title": title,
                            "link": link,
                            "published_at": published_at,
                        }
                    )
        else:
            # Atom
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry in root.findall(".//atom:entry", ns)[: max(1, limit * 3)]:
                title = collapse_ws(entry.findtext("atom:title", default="", namespaces=ns))
                link = ""
                for link_tag in entry.findall("atom:link", ns):
                    href = collapse_ws(link_tag.get("href") or "")
                    rel = collapse_ws(link_tag.get("rel") or "")
                    if href and (not rel or rel == "alternate"):
                        link = href
                        break
                published_at = to_iso_time(
                    entry.findtext("atom:updated", default="", namespaces=ns)
                    or entry.findtext("atom:published", default="", namespaces=ns)
                )
                if title and link:
                    items.append(
                        {
                            "source": source_name,
                            "source_kind": source_kind,
                            "title": title,
                            "link": link,
                            "published_at": published_at,
                        }
                    )
    except Exception:
        # 容错：部分站点 feed XML 不规范，回退用 BeautifulSoup 的 xml 解析
        soup = BeautifulSoup(raw_xml, "xml")
        rss_items = soup.find_all("item")
        if rss_items:
            for it in rss_items[: max(1, limit * 3)]:
                title = collapse_ws((it.find("title").get_text(" ", strip=True) if it.find("title") else ""))
                link = collapse_ws((it.find("link").get_text(" ", strip=True) if it.find("link") else ""))
                published_at = to_iso_time((it.find("pubDate").get_text(" ", strip=True) if it.find("pubDate") else ""))
                if title and link:
                    items.append(
                        {
                            "source": source_name,
                            "source_kind": source_kind,
                            "title": title,
                            "link": link,
                            "published_at": published_at,
                        }
                    )
        else:
            atom_entries = soup.find_all("entry")
            for entry in atom_entries[: max(1, limit * 3)]:
                title = collapse_ws((entry.find("title").get_text(" ", strip=True) if entry.find("title") else ""))
                link = ""
                for link_tag in entry.find_all("link"):
                    href = collapse_ws(link_tag.get("href") or "")
                    rel = collapse_ws(link_tag.get("rel") or "")
                    if href and (not rel or rel == "alternate"):
                        link = href
                        break
                updated_node = entry.find("updated") or entry.find("published")
                published_at = to_iso_time(updated_node.get_text(" ", strip=True) if updated_node else "")
                if title and link:
                    items.append(
                        {
                            "source": source_name,
                            "source_kind": source_kind,
                            "title": title,
                            "link": link,
                            "published_at": published_at,
                        }
                    )

    items.sort(key=lambda x: str(x.get("published_at") or ""), reverse=True)
    return items[: max(1, limit)]


def fetch_article_body(url: str) -> tuple[str, str]:
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()

    title = collapse_ws((soup.title.get_text(" ", strip=True) if soup.title else ""))

    candidates = [
        soup.select_one("article"),
        soup.select_one("main"),
        soup.select_one("#js_content"),
        soup.select_one(".article-content"),
        soup.select_one(".content"),
        soup.select_one(".post-content"),
        soup.select_one(".rich_media_content"),
        soup.body,
    ]
    node = next((x for x in candidates if x is not None), None)
    if node is None:
        return title, ""

    texts: list[str] = []
    for el in node.find_all(["h1", "h2", "h3", "p", "li"]):
        line = collapse_ws(el.get_text(" ", strip=True))
        if len(line) >= 12:
            texts.append(line)
    if not texts:
        all_text = collapse_ws(node.get_text(" ", strip=True))
        return title, all_text[:12000]

    # 去掉相邻重复
    deduped: list[str] = []
    prev = ""
    for line in texts:
        if line != prev:
            deduped.append(line)
        prev = line
    content = "\n\n".join(deduped)[:12000]
    return title, content


class WeChatCollector:
    platform = "wechat"

    def __init__(self, logger=None) -> None:
        self.logger = logger or (lambda *_args, **_kwargs: None)
        self.sources = {item["id"]: item for item in load_wechat_sources()}
        self.legacy = load_legacy_monitor_module()
        flags = load_collector_flags().get("wechat") or {}
        self.metadata_only = bool(flags.get("metadata_only", True))
        self.enable_body_hydration = bool(flags.get("enable_body_hydration", False))
        self.body_fetch_via_hub = bool(flags.get("body_fetch_via_hub", True))
        self.body_fetch_via_html_fallback = bool(flags.get("body_fetch_via_html_fallback", True))
        self.logger(
            "[wechat] flags "
            f"metadata_only={self.metadata_only} "
            f"enable_body_hydration={self.enable_body_hydration} "
            f"hub={self.body_fetch_via_hub} html_fallback={self.body_fetch_via_html_fallback}"
        )

    def collect_for_handle(self, handle: str, limit: int) -> list[dict[str, Any]]:
        source = self.sources.get(str(handle).strip())
        if not source:
            self.logger(f"[wechat] source not found for handle={handle}")
            return []

        result = self.legacy.fetch_source_items(source["name"])
        if not result.get("ok"):
            # 兼容新增正式层来源：优先尝试按 entry_url 直接解析 feed/atom
            fetch_method = str(source.get("fetch_method") or "")
            if fetch_method in {"official_feed", "official_atom"}:
                try:
                    fallback_items = parse_feed_items_from_entry(source, limit=max(1, limit))
                    if fallback_items:
                        result = {"ok": True, "reason": "", "items": fallback_items}
                    else:
                        raise RuntimeError("fallback_feed_empty")
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(f"fallback_feed_failed: {exc}") from exc
            else:
                raise RuntimeError(result.get("reason") or f"fetch source failed: {source['name']}")

        items = result.get("items") or []
        out: list[dict[str, Any]] = []
        for article in items[: max(1, limit)]:
            url = str(article.get("link") or "").strip()
            title = str(article.get("title") or "").strip()
            published_at = str(article.get("published_at") or "").strip()
            item_id = hashlib.md5(f"wechat|{source['id']}|{url}".encode("utf-8")).hexdigest()  # noqa: S324
            body_title = title
            content = title
            body_ok = False
            body_error = ""
            body_fetch_method = ""
            body_fetch_skipped = self.metadata_only or not self.enable_body_hydration

            if url and not body_fetch_skipped:
                try:
                    extracted_title = ""
                    if self.body_fetch_via_hub:
                        self.logger(f"[wechat] body hydration via hub url={url}")
                        extracted_title, content, body_fetch_method = fetch_via_content_fetch_hub(url)
                    if not content and self.body_fetch_via_html_fallback:
                        self.logger(f"[wechat] body hydration via html fallback url={url}")
                        extracted_title, content = fetch_article_body(url)
                        body_fetch_method = body_fetch_method or "html_parser_fallback"
                    if extracted_title:
                        body_title = extracted_title[:200]
                    body_ok = bool(content)
                except Exception as exc:  # noqa: BLE001
                    body_error = str(exc)[:300]
                    content = title
            elif url and body_fetch_skipped:
                body_fetch_method = "metadata_only"

            metrics_payload = {
                "source_kind": article.get("source_kind") or source.get("fetch_method") or "wechat_page",
                "entry_url": source.get("entry_url") or "",
                "body_fetch_ok": body_ok,
                "body_fetch_error": body_error,
                "body_fetch_method": body_fetch_method,
                "body_fetch_skipped": body_fetch_skipped,
                "metadata_only": self.metadata_only,
                "enable_body_hydration": self.enable_body_hydration,
                "category": source.get("category") or "",
            }
            out.append(
                {
                    "platform": "wechat",
                    "source_handle": source["name"],
                    "item_id": item_id,
                    "title": body_title[:140],
                    "content": content or title,
                    "url": url,
                    "published_at": published_at,
                    "metrics_json": json.dumps(metrics_payload, ensure_ascii=False),
                }
            )
        self.logger(f"[wechat] fetched source={source['name']} count={len(out)}")
        return out
