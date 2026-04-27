#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
from datetime import datetime
from pathlib import Path

from models import FetchResult

CHANNEL_DIR_MAP = {
    "feishu": "飞书",
    "wechat": "微信公众号",
    "youtube": "YouTube",
    "bilibili": "B站",
    "douyin": "抖音",
    "x": "X",
    "web": "网页",
    "unsupported": "未支持渠道",
}


def _slugify(text: str) -> str:
    value = str(text or "").strip()
    value = re.sub(r"[\\/:*?\"<>|]", "-", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:90] or "untitled"


def write_result_to_obsidian(result: FetchResult, vault_root: str | Path) -> str:
    root = Path(vault_root)
    target_dir = root / "抓取内容" / CHANNEL_DIR_MAP.get(result.channel, result.channel)
    target_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{date_str}_{result.channel}_{_slugify(result.title)}.md"
    path = target_dir / file_name
    if path.exists():
        stem = path.stem
        suffix = path.suffix
        for i in range(2, 1000):
            cand = target_dir / f"{stem}-{i}{suffix}"
            if not cand.exists():
                path = cand
                break

    content_hash = hashlib.sha1((result.content_markdown or "").encode("utf-8")).hexdigest()
    lines = [
        "---",
        f"source_url: {result.url}",
        f"channel: {result.channel}",
        f"author: {result.author or ''}",
        f"published_at: {result.published_at or ''}",
        f"fetched_at: {result.fetched_at}",
        f"content_hash: {content_hash}",
        "---",
        "",
        f"# {result.title or '(无标题)'}",
        "",
        result.content_markdown or "",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)
