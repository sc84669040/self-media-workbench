#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass

from adapters.base import FetchAdapter
from adapters.bilibili_adapter import BilibiliAdapter
from adapters.douyin_adapter import DouyinAdapter
from adapters.feishu_adapter import FeishuAdapter
from adapters.wechat_adapter import WechatAdapter
from adapters.youtube_adapter import YoutubeAdapter
from adapters.x_adapter import XAdapter
from adapters.web_adapter import WebAdapter
from adapters.unsupported_adapter import UnsupportedAdapter


@dataclass
class RouteResult:
    channel: str
    adapter: FetchAdapter


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: list[FetchAdapter] = []

    def register(self, adapter: FetchAdapter) -> None:
        self._adapters.append(adapter)

    def resolve(self, url: str) -> RouteResult:
        for adapter in self._adapters:
            try:
                if adapter.can_handle(url):
                    return RouteResult(channel=adapter.name, adapter=adapter)
            except Exception:
                continue
        fallback = UnsupportedAdapter()
        return RouteResult(channel=fallback.name, adapter=fallback)


def build_default_registry() -> AdapterRegistry:
    reg = AdapterRegistry()
    # 扩展点：后续新增渠道仅需 register 新 adapter，不改核心路由流程。
    # 注意顺序：越具体的域名越靠前，web 通用适配器放靠后。
    reg.register(FeishuAdapter())
    reg.register(WechatAdapter())
    reg.register(YoutubeAdapter())
    reg.register(BilibiliAdapter())
    reg.register(DouyinAdapter())
    reg.register(XAdapter())
    reg.register(WebAdapter())
    return reg
