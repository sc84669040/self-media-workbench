#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any


@dataclass
class ImageAsset:
    url: str
    alt: str = ""


@dataclass
class FetchResult:
    ok: bool
    channel: str
    url: str
    title: str = ""
    content_markdown: str = ""
    author: str = ""
    published_at: str = ""
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    images: list[ImageAsset] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["images"] = [asdict(x) for x in self.images]
        return data
