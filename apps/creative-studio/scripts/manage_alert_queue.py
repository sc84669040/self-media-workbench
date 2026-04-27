#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "event_radar.db"


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def parse_reasons(raw: str) -> list[str]:
    try:
        payload = json.loads(str(raw or "{}")) or {}
        return [str(x).strip() for x in (payload.get("reasons") or []) if str(x).strip()]
    except Exception:
        return []


def format_source_label(platform: str, source_handle: str) -> str:
    platform_norm = str(platform or "").strip().lower()
    handle = str(source_handle or "").strip()
    if not handle:
        return "(未知来源)"
    if platform_norm == "x":
        return f"@{handle}"
    return handle


def fetch_rows(conn: sqlite3.Connection, status: str, platform: str, limit: int) -> list[sqlite3.Row]:
    cur = conn.cursor()
    return cur.execute(
        """
        SELECT id, platform, item_id, source_handle, title, url, published_at,
               recommend_level, score, reason_json, notify_status, notify_error,
               notify_attempts, notified_at, created_at, updated_at
        FROM alert_queue
        WHERE notify_status=? AND platform=?
        ORDER BY score DESC, published_at DESC, id DESC
        LIMIT ?
        """,
        (status, platform, limit),
    ).fetchall()


def rows_to_items(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "id": int(row["id"]),
                "platform": str(row["platform"] or ""),
                "source": format_source_label(str(row["platform"] or ""), str(row["source_handle"] or "")),
                "source_handle": str(row["source_handle"] or ""),
                "item_id": str(row["item_id"] or ""),
                "title": str(row["title"] or ""),
                "url": str(row["url"] or ""),
                "published_at": str(row["published_at"] or ""),
                "recommend_level": str(row["recommend_level"] or ""),
                "score": float(row["score"] or 0),
                "notify_status": str(row["notify_status"] or ""),
                "notify_error": str(row["notify_error"] or ""),
                "notify_attempts": int(row["notify_attempts"] or 0),
                "reasons": parse_reasons(str(row["reason_json"] or "{}")),
            }
        )
    return items


def render_text(items: list[dict[str, Any]], status: str, platform: str) -> str:
    lines = [f"alert_queue {platform}/{status}: {len(items)} 条"]
    for idx, item in enumerate(items, start=1):
        reasons = "；".join(item["reasons"][:3]) if item["reasons"] else "-"
        lines.extend(
            [
                f"{idx}. [#{item['id']}] {item['title'] or '(无标题)'}",
                f"   来源：{item['source']} | 评分：{item['score']} | 等级：{item['recommend_level']} | 状态：{item['notify_status']}",
                f"   时间：{item['published_at'] or '-'}",
                f"   理由：{reasons}",
                f"   链接：{item['url'] or '-'}",
            ]
        )
    return "\n".join(lines)


def render_markdown(items: list[dict[str, Any]], status: str, platform: str) -> str:
    lines = [f"# alert_queue {platform}/{status}", ""]
    for item in items:
        reasons = "；".join(item["reasons"][:3]) if item["reasons"] else "-"
        lines.extend(
            [
                f"## [#{item['id']}] {item['title'] or '(无标题)'}",
                "",
                f"- 来源：{item['source']}",
                f"- 评分：{item['score']}",
                f"- 等级：{item['recommend_level']}",
                f"- 状态：{item['notify_status']}",
                f"- 发布时间：{item['published_at'] or '-'}",
                f"- 理由：{reasons}",
                f"- 链接：{item['url'] or '-'}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def list_command(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        rows = fetch_rows(conn, args.status, args.platform, args.limit)
        items = rows_to_items(rows)
    finally:
        conn.close()

    if args.format == "json":
        output = json.dumps(items, ensure_ascii=False, indent=2)
    elif args.format == "markdown":
        output = render_markdown(items, args.status, args.platform)
    else:
        output = render_text(items, args.status, args.platform)

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"OK: wrote {args.output}")
    else:
        print(output)
    return 0


def promote_command(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT id, notify_status, title FROM alert_queue WHERE id=? AND platform=? LIMIT 1",
            (args.id, args.platform),
        ).fetchone()
        if row is None:
            raise SystemExit(f"未找到记录：id={args.id} platform={args.platform}")
        if str(row["notify_status"] or "") != "candidate":
            raise SystemExit(f"当前状态不是 candidate，而是 {row['notify_status']}")
        cur.execute(
            """
            UPDATE alert_queue
            SET notify_status='pending',
                notify_error='manual_promote',
                updated_at=datetime('now')
            WHERE id=?
            """,
            (args.id,),
        )
        conn.commit()
        print(f"OK: promoted #{args.id} -> pending | {row['title'] or '(无标题)'}")
        return 0
    finally:
        conn.close()


def dismiss_command(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT id, notify_status, title FROM alert_queue WHERE id=? AND platform=? LIMIT 1",
            (args.id, args.platform),
        ).fetchone()
        if row is None:
            raise SystemExit(f"未找到记录：id={args.id} platform={args.platform}")
        if str(row["notify_status"] or "") != "candidate":
            raise SystemExit(f"当前状态不是 candidate，而是 {row['notify_status']}")
        cur.execute(
            """
            UPDATE alert_queue
            SET notify_status='skipped',
                notify_error='manual_dismiss',
                updated_at=datetime('now')
            WHERE id=?
            """,
            (args.id,),
        )
        conn.commit()
        print(f"OK: dismissed #{args.id} -> skipped | {row['title'] or '(无标题)'}")
        return 0
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NightHawk alert_queue 管理工具")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="查看指定状态队列")
    p_list.add_argument("--status", default="candidate", choices=["candidate", "pending", "sent", "failed", "skipped", "suppressed"])
    p_list.add_argument("--platform", default="wechat")
    p_list.add_argument("--limit", type=int, default=20)
    p_list.add_argument("--format", default="text", choices=["text", "json", "markdown"])
    p_list.add_argument("--output", default="")
    p_list.set_defaults(func=list_command)

    p_promote = sub.add_parser("promote", help="将 candidate 人工推进到 pending")
    p_promote.add_argument("--id", type=int, required=True)
    p_promote.add_argument("--platform", default="wechat")
    p_promote.set_defaults(func=promote_command)

    p_dismiss = sub.add_parser("dismiss", help="将 candidate 人工标记为跳过")
    p_dismiss.add_argument("--id", type=int, required=True)
    p_dismiss.add_argument("--platform", default="wechat")
    p_dismiss.set_defaults(func=dismiss_command)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
