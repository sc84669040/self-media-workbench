#!/usr/bin/env python3
from __future__ import annotations

from adapters.base import FetchAdapter
from models import FetchResult


class UnsupportedAdapter(FetchAdapter):
    name = "unsupported"

    def can_handle(self, url: str) -> bool:
        return True

    def fetch(self, url: str) -> FetchResult:
        return FetchResult(
            ok=False,
            channel=self.name,
            url=url,
            error="unsupported-channel-yet",
        )
