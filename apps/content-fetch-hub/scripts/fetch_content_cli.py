#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import yaml

from fetch_router import build_default_registry
from writer_obsidian import write_result_to_obsidian
from video_transcript_analysis import build_analysis_card, build_structured_payload

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
if str(REPO_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(REPO_SCRIPTS))

from self_media_config import LOCAL_CONFIG_PATH, get_config, get_path, get_value  # noqa: E402

SETTINGS_PATH = Path(os.getenv("FETCH_HUB_CONFIG_PATH") or LOCAL_CONFIG_PATH)
ANALYSIS_CHANNELS = {"youtube", "bilibili", "douyin"}
ANALYSIS_DIR_MAP = {
    "youtube": "YouTube",
    "bilibili": "B站",
    "douyin": "抖音",
}


def load_settings() -> dict:
    root_config = get_config()
    settings = dict(get_value(root_config, "content_fetch_hub", {}) or {})
    settings.setdefault("vault_path", str(get_path(root_config, "paths.sample_vault_path")))
    settings.setdefault("retry_count", 1)
    settings.setdefault("youtube_cookies_path", "")
    settings.setdefault("bilibili_cookies_path", "")

    if os.getenv("FETCH_HUB_CONFIG_PATH") and SETTINGS_PATH.exists():
        payload = yaml.safe_load(SETTINGS_PATH.read_text(encoding="utf-8")) or {}
        if isinstance(payload, dict):
            settings.update(payload.get("content_fetch_hub", payload))
    return settings


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="content-fetch-hub CLI")
    p.add_argument("url", nargs="?", default="", help="单条链接")
    p.add_argument("--file", default="", help="批量链接文件（每行一个链接，支持 # 注释）")
    p.add_argument("--vault", default="", help="Obsidian 根目录，默认读 config/fetch-settings.yaml")
    p.add_argument("--retry", type=int, default=-1, help="失败重试次数（不含首次，默认读配置，缺省 1）")
    p.add_argument("--json", action="store_true", help="仅输出 JSON")
    p.add_argument("--analyze", action="store_true", help="对支持 transcript 的视频内容生成分析卡（当前支持 youtube / bilibili / douyin）")
    p.add_argument("--write-obsidian", action="store_true", help="显式写入 Obsidian；默认仅抓取，不做知识库写盘")
    return p.parse_args()


def load_urls(args: argparse.Namespace) -> list[str]:
    urls: list[str] = []
    if args.url.strip():
        urls.append(args.url.strip())

    if args.file.strip():
        f = Path(args.file.strip())
        if not f.exists():
            raise FileNotFoundError(f"batch file not found: {f}")
        for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            urls.append(s)

    if not urls:
        raise ValueError("请提供 url 或 --file")
    return urls


def build_existing_index(vault_root: Path) -> dict[tuple[str, str], str]:
    idx: dict[tuple[str, str], str] = {}
    base = vault_root / "抓取内容"
    if not base.exists():
        return idx

    for md in base.rglob("*.md"):
        try:
            text = md.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        source_url = ""
        title = ""
        for line in text.splitlines()[:80]:
            if line.startswith("source_url:"):
                source_url = line.split(":", 1)[1].strip()
            elif line.startswith("# ") and not title:
                title = line[2:].strip()
            if source_url and title:
                break
        if source_url and title:
            idx[(source_url, title)] = str(md)
    return idx


def fetch_with_retry(url: str, retry_count: int) -> tuple[dict[str, Any], str]:
    registry = build_default_registry()
    route = registry.resolve(url)
    last_result = route.adapter.fetch(url)

    if last_result.ok:
        return last_result.to_dict(), ""

    last_err = last_result.error or "unknown-error"
    for _ in range(max(0, retry_count)):
        time.sleep(0.5)
        retry_result = route.adapter.fetch(url)
        if retry_result.ok:
            return retry_result.to_dict(), ""
        last_err = retry_result.error or last_err

    payload = last_result.to_dict()
    payload["error"] = last_err
    return payload, last_err


def _safe_slug(text: str, fallback: str = "analysis") -> str:
    s = re.sub(r'[\\/:*?"<>|\r\n]+', '-', str(text or ''))
    s = re.sub(r"\s+", " ", s).strip(" .-")
    return (s[:90] if s else fallback) or fallback


def maybe_build_analysis(payload: dict[str, Any], vault_root: Path) -> tuple[str, str]:
    channel = str(payload.get("channel") or "").strip().lower()
    if channel not in ANALYSIS_CHANNELS:
        return "", ""

    meta = payload.get("meta") or {}
    transcript_available = bool(meta.get("transcript_available"))
    if not transcript_available:
        return "", "transcript-unavailable"

    analysis_payload = {
        "platform": channel,
        "title": str(payload.get("title") or "").strip(),
        "author": str(payload.get("author") or "").strip(),
        "source_name": str(payload.get("author") or "").strip(),
        "url": str(payload.get("url") or "").strip(),
        "published_at": str(payload.get("published_at") or "").strip(),
        "transcript_text": str(payload.get("content_markdown") or "").strip(),
        "transcript_language": str(meta.get("transcript_language") or "").strip(),
        "transcript_source": str(meta.get("transcript_source") or "").strip(),
        "video_id": str(meta.get("video_id") or "").strip(),
        "route_bucket": f"{channel}-transcript-analysis",
        "tags": [channel, "transcript-analysis"],
        "related_topics": [channel, "video transcript", "analysis"],
        "risk_notes": [
            "当前分析卡为 transcript 驱动草稿，摘要/要点仍需继续提炼。",
            "ASR / 自动字幕场景可能存在错字、断句或专有名词识别偏差。",
        ],
    }
    structured = build_structured_payload(analysis_payload)
    card = build_analysis_card(analysis_payload)

    target_dir = vault_root / "抓取内容" / "分析卡片" / ANALYSIS_DIR_MAP.get(channel, channel)
    target_dir.mkdir(parents=True, exist_ok=True)
    date_str = time.strftime("%Y-%m-%d")
    file_stem = f"{date_str}_{channel}_analysis_{_safe_slug(structured.get('title') or structured.get('platform_id') or channel)}"
    md_path = target_dir / f"{file_stem}.analysis.md"
    json_path = target_dir / f"{file_stem}.analysis.json"
    if md_path.exists() or json_path.exists():
        for i in range(2, 1000):
            cand_md = target_dir / f"{file_stem}-{i}.analysis.md"
            cand_json = target_dir / f"{file_stem}-{i}.analysis.json"
            if not cand_md.exists() and not cand_json.exists():
                md_path = cand_md
                json_path = cand_json
                break

    md_path.write_text(card, encoding="utf-8")
    json_path.write_text(json.dumps(structured, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(md_path), ""


def run_batch(urls: list[str], vault: str, retry_count: int, analyze: bool, write_obsidian: bool = False) -> dict[str, Any]:
    vault_root = Path(vault)
    dedupe_idx = build_existing_index(vault_root) if write_obsidian else {}

    results: list[dict[str, Any]] = []
    success = failed = skipped = 0

    for u in urls:
        item: dict[str, Any] = {
            "url": u,
            "ok": False,
            "channel": "",
            "title": "",
            "saved_path": "",
            "analysis_path": "",
            "status": "failed",
            "error": "",
        }

        payload, err = fetch_with_retry(u, retry_count=retry_count)
        item.update(payload)

        if not payload.get("ok"):
            item["status"] = "failed"
            item["error"] = err or payload.get("error") or "fetch-failed"
            failed += 1
            results.append(item)
            continue

        key = (str(payload.get("url") or "").strip(), str(payload.get("title") or "").strip())
        if write_obsidian and key in dedupe_idx:
            existing_path = dedupe_idx.get(key, "")
            item["status"] = "skipped"
            item["saved_path"] = existing_path
            item["error"] = "已存在同链接同标题文件，未重复保存"
            item["duplicate_reason"] = "duplicate-by-source_url+title"
            if analyze:
                analysis_path, analysis_error = maybe_build_analysis(payload, vault_root)
                item["analysis_path"] = analysis_path
                if analysis_error and not item["error"]:
                    item["error"] = analysis_error
            skipped += 1
            results.append(item)
            continue

        from models import FetchResult, ImageAsset

        result_obj = FetchResult(
            ok=bool(payload.get("ok")),
            channel=str(payload.get("channel") or ""),
            url=str(payload.get("url") or ""),
            title=str(payload.get("title") or ""),
            content_markdown=str(payload.get("content_markdown") or ""),
            author=str(payload.get("author") or ""),
            published_at=str(payload.get("published_at") or ""),
            fetched_at=str(payload.get("fetched_at") or ""),
            images=[ImageAsset(**img) for img in (payload.get("images") or [])],
            meta=payload.get("meta") or {},
            error=str(payload.get("error") or ""),
        )
        saved_path = ""
        if write_obsidian:
            saved_path = write_result_to_obsidian(result_obj, vault_root)
            item["saved_path"] = saved_path
        if analyze:
            analysis_path, analysis_error = maybe_build_analysis(payload, vault_root)
            item["analysis_path"] = analysis_path
            if analysis_error:
                item["error"] = analysis_error
        item["status"] = "success"
        success += 1
        dedupe_idx[key] = saved_path
        results.append(item)

    return {
        "ok": True,
        "total": len(urls),
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "write_obsidian": bool(write_obsidian),
        "results": results,
    }


def main() -> None:
    args = parse_args()
    settings = load_settings()
    vault = (args.vault or settings.get("vault_path") or str(get_path(get_config(), "paths.sample_vault_path"))).strip()
    retry_count = args.retry if args.retry >= 0 else int(settings.get("retry_count", 1))

    urls = load_urls(args)
    summary = run_batch(urls, vault=vault, retry_count=retry_count, analyze=bool(args.analyze), write_obsidian=bool(args.write_obsidian))

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    print(
        "\n".join(
            [
                "✅ content-fetch-hub 执行完成",
                f"- 总数：{summary['total']}",
                f"- 成功：{summary['success']}",
                f"- 失败：{summary['failed']}",
                f"- 跳过：{summary['skipped']}",
                f"- 写入 Obsidian：{'是' if bool(args.write_obsidian) else '否'}",
            ]
        )
    )
    for item in summary["results"]:
        print(
            f"- [{item['status']}] {item.get('channel','-')} | {item.get('title') or item.get('url')}"
            + (f" | {item.get('saved_path')}" if item.get('saved_path') else "")
            + (f" | analysis={item.get('analysis_path')}" if item.get('analysis_path') else "")
            + (f" | err={item.get('error')}" if item.get('error') else "")
        )


if __name__ == "__main__":
    main()
