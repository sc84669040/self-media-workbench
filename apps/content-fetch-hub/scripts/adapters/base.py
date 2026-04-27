#!/usr/bin/env python3
from __future__ import annotations

from abc import ABC, abstractmethod
from urllib.parse import urlparse

from models import FetchResult


def get_url_host(url: str) -> str:
    try:
        return (urlparse(str(url or "").strip()).hostname or "").lower().strip(".")
    except Exception:
        return ""


def host_matches(url: str, *domains: str) -> bool:
    host = get_url_host(url)
    if not host:
        return False
    normalized = [str(domain or "").lower().strip(".") for domain in domains if str(domain or "").strip()]
    return any(host == domain or host.endswith(f".{domain}") for domain in normalized)


class FetchAdapter(ABC):
    name: str = "base"

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def fetch(self, url: str) -> FetchResult:
        raise NotImplementedError
