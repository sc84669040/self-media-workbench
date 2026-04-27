#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

from collector_daemon import DB_PATH, configure_sqlite_connection, ensure_tables, now_ts
from wechat_collector import fetch_article_body, fetch_via_content_fetch_hub, load_collector_flags


BODY_FETCH_TIMEOUT_SEC = 45
BODY_TIMEOUT_BREAKER_THRESHOLD = 3
BODY_FAILURE_BREAKER_THRESHOLD = 3
BODY_MAX_TIMEOUTS_PER_RUN = 3
BODY_RETRY_BACKOFF_BASE_SEC = 300
BODY_RETRY_BACKOFF_MAX_SEC = 21600
BODY_DB_TIMEOUT_SEC = int(os.environ.get("EVENT_RADAR_DB_TIMEOUT_SEC", "60"))
BODY_DB_LOCK_RETRY_ATTEMPTS = int(os.environ.get("EVENT_RADAR_DB_LOCK_RETRY_ATTEMPTS", "6"))
BODY_DB_LOCK_RETRY_SLEEP_SEC = float(os.environ.get("EVENT_RADAR_DB_LOCK_RETRY_SLEEP_SEC", "1.5"))


def log(msg: str) -> None:
    print(f"[{now_ts()}] {msg}", flush=True)


def log_event(event: str, **fields: Any) -> None:
    payload = " ".join([f"{key}={fields[key]}" for key in fields if fields[key] is not None])
    log(f"[{event}] {payload}".rstrip())


class BodyFetchTimeoutError(TimeoutError):
    pass


@contextmanager
def hard_timeout(seconds: int):
    if seconds <= 0:
        yield
        return

    def _handle_timeout(_signum, _frame):
        raise BodyFetchTimeoutError(f"body-fetch-hard-timeout>{seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, float(seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NightHawk body hydration worker")
    parser.add_argument("--once", action="store_true", help="只执行一轮")
    parser.add_argument("--dry-run", action="store_true", help="只查看待处理任务，不写库")
    parser.add_argument("--limit", type=int, default=5, help="每轮最多处理多少条")
    parser.add_argument("--poll-sec", type=int, default=120, help="守护模式轮询间隔")
    parser.add_argument("--platforms", nargs="*", default=["wechat"], help="允许处理的平台")
    parser.add_argument("--force-id", action="append", type=int, default=[], help="强制将指定 raw_items.id 推入正文队列，可重复传入")
    parser.add_argument("--body-timeout-sec", type=int, default=BODY_FETCH_TIMEOUT_SEC, help="单条正文抓取硬超时")
    parser.add_argument("--breaker-timeouts", type=int, default=BODY_TIMEOUT_BREAKER_THRESHOLD, help="单来源连续 timeout 达到多少后本轮熔断")
    parser.add_argument("--breaker-failures", type=int, default=BODY_FAILURE_BREAKER_THRESHOLD, help="单来源连续失败达到多少后本轮熔断")
    parser.add_argument("--max-timeouts-per-run", type=int, default=BODY_MAX_TIMEOUTS_PER_RUN, help="单轮累计 timeout 上限，超过后提前停止")
    return parser.parse_args()


def parse_metrics(metrics_json: str) -> dict[str, Any]:
    try:
        data = json.loads(metrics_json or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def run_write_txn(conn: sqlite3.Connection, fn: Any, action: str) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, max(1, BODY_DB_LOCK_RETRY_ATTEMPTS) + 1):
        try:
            result = fn()
            conn.commit()
            return result
        except sqlite3.OperationalError as exc:
            conn.rollback()
            last_error = exc
            if "locked" not in str(exc).lower() or attempt >= max(1, BODY_DB_LOCK_RETRY_ATTEMPTS):
                raise
            sleep_sec = max(0.2, BODY_DB_LOCK_RETRY_SLEEP_SEC * attempt)
            log(f"[db-lock-retry] action={action} attempt={attempt}/{BODY_DB_LOCK_RETRY_ATTEMPTS} sleep_sec={round(sleep_sec, 2)} err={str(exc)[:180]}")
            time.sleep(sleep_sec)
    raise RuntimeError(f"db-write-retry-exhausted:{action}:{last_error}")


def get_body_queue_stats(conn: sqlite3.Connection, platforms: list[str] | None = None) -> dict[str, int]:
    cur = conn.cursor()
    target_platforms = [str(p).strip() for p in (platforms or []) if str(p).strip()]
    filters = ""
    params: list[Any] = []
    if target_platforms:
        placeholders = ",".join("?" for _ in target_platforms)
        filters = f" WHERE platform IN ({placeholders})"
        params.extend(target_platforms)

    stats = {
        "body_none": 0,
        "body_pending": 0,
        "body_in_progress": 0,
        "body_success": 0,
        "body_failed": 0,
        "body_timeout": 0,
        "body_skipped": 0,
        "body_retry_waiting": 0,
        "body_due_now": 0,
    }
    rows = cur.execute(f"SELECT body_status, COUNT(*) FROM raw_items{filters} GROUP BY body_status", tuple(params)).fetchall()
    for status, count in rows:
        key = f"body_{str(status or 'none').strip() or 'none'}"
        if key in stats:
            stats[key] = int(count or 0)

    retry_clauses = ["body_status='pending'", "body_next_retry_at IS NOT NULL", "body_next_retry_at != ''", "body_next_retry_at > datetime('now')"]
    due_clauses = ["body_status='pending'", "(body_next_retry_at IS NULL OR body_next_retry_at='' OR body_next_retry_at <= datetime('now'))"]
    retry_params = list(params)
    due_params = list(params)
    if filters:
        platform_clause = filters.replace(" WHERE ", "")
        retry_clauses.insert(0, platform_clause)
        due_clauses.insert(0, platform_clause)
    stats["body_retry_waiting"] = int(cur.execute(f"SELECT COUNT(*) FROM raw_items WHERE {' AND '.join(retry_clauses)}", tuple(retry_params)).fetchone()[0])
    stats["body_due_now"] = int(cur.execute(f"SELECT COUNT(*) FROM raw_items WHERE {' AND '.join(due_clauses)}", tuple(due_params)).fetchone()[0])
    return stats


def force_enqueue_ids(conn: sqlite3.Connection, ids: list[int]) -> int:
    if not ids:
        return 0

    def _op() -> int:
        cur = conn.cursor()
        changed = 0
        for row_id in ids:
            row = cur.execute("SELECT platform, metrics_json FROM raw_items WHERE id=?", (int(row_id),)).fetchone()
            if not row:
                continue
            platform = str(row[0] or "")
            if platform != "wechat":
                continue
            metrics = parse_metrics(str(row[1] or "{}"))
            metrics["body_candidate_reason"] = "manual-force"
            cur.execute(
                "UPDATE raw_items SET body_status='pending', body_priority=1000, body_next_retry_at=NULL, metrics_json=? WHERE id=?",
                (json.dumps(metrics, ensure_ascii=False), int(row_id)),
            )
            changed += cur.rowcount
        return changed

    changed = run_write_txn(conn, _op, "force-enqueue")
    return int(changed or 0)


def select_pending_rows(conn: sqlite3.Connection, limit: int, platforms: list[str]) -> list[sqlite3.Row]:
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in platforms)
    sql = f"""
        SELECT id, platform, source_handle, item_id, title, content, url, metrics_json, body_status, body_attempts, body_priority, body_next_retry_at
        FROM raw_items
        WHERE body_status='pending'
          AND platform IN ({placeholders})
          AND (body_next_retry_at IS NULL OR body_next_retry_at='' OR body_next_retry_at <= datetime('now'))
        ORDER BY body_priority DESC, fetched_at DESC, id DESC
        LIMIT ?
    """
    return cur.execute(sql, (*platforms, max(1, limit))).fetchall()


def recover_in_progress_rows(conn: sqlite3.Connection, platforms: list[str]) -> int:
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in platforms)
    exists_sql = f"SELECT 1 FROM raw_items WHERE body_status='in_progress' AND platform IN ({placeholders}) LIMIT 1"
    if cur.execute(exists_sql, tuple(platforms)).fetchone() is None:
        return 0

    def _op() -> int:
        inner = conn.cursor()
        sql = f"UPDATE raw_items SET body_status='pending' WHERE body_status='in_progress' AND platform IN ({placeholders})"
        return inner.execute(sql, tuple(platforms)).rowcount

    changed = run_write_txn(conn, _op, "recover-in-progress")
    return int(changed or 0)


def claim_rows(conn: sqlite3.Connection, rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    if not rows:
        return []

    def _op() -> list[sqlite3.Row]:
        cur = conn.cursor()
        claimed: list[sqlite3.Row] = []
        for row in rows:
            updated = cur.execute(
                "UPDATE raw_items SET body_status='in_progress', body_attempts=COALESCE(body_attempts, 0)+1 WHERE id=? AND body_status='pending'",
                (row["id"],),
            ).rowcount
            if updated > 0:
                claimed.append(row)
        return claimed

    return list(run_write_txn(conn, _op, "claim-rows") or [])


def hydrate_wechat(row: sqlite3.Row, timeout_sec: int) -> tuple[str, str, str, str]:
    flags = load_collector_flags().get("wechat") or {}
    use_hub = bool(flags.get("body_fetch_via_hub", True))
    use_html = bool(flags.get("body_fetch_via_html_fallback", True))
    url = str(row["url"] or "").strip()
    title = str(row["title"] or "").strip()

    final_title = title
    content = ""
    method = ""
    error = ""

    try:
        with hard_timeout(timeout_sec):
            extracted_title = ""
            prefer_html_first = "mp.weixin.qq.com/" not in url.lower()
            if url and use_html and prefer_html_first:
                extracted_title, content = fetch_article_body(url)
                method = "html_parser_fallback"
            if not content and url and use_hub:
                extracted_title, content, method = fetch_via_content_fetch_hub(url)
            if not content and url and use_html and not prefer_html_first:
                extracted_title, content = fetch_article_body(url)
                method = method or "html_parser_fallback"
        if extracted_title:
            final_title = extracted_title[:200]
        if content:
            return "success", final_title[:140], content, method or "body_worker"
        return "failed", final_title[:140], "", method or "empty-content"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)[:500]
        if "timed out" in error.lower() or "timeout" in error.lower():
            return "timeout", final_title[:140], "", error
        return "failed", final_title[:140], "", error


def update_success(conn: sqlite3.Connection, row_id: int, title: str, content: str, method: str) -> None:
    def _op() -> None:
        cur = conn.cursor()
        row = cur.execute("SELECT metrics_json FROM raw_items WHERE id=?", (row_id,)).fetchone()
        metrics = parse_metrics(str(row[0] or "{}") if row else "{}")
        metrics.update(
            {
                "body_fetch_ok": True,
                "body_fetch_error": "",
                "body_fetch_method": method,
                "body_fetch_skipped": False,
                "body_hydrated": True,
                "metadata_only": False,
            }
        )
        cur.execute(
            """
            UPDATE raw_items
            SET title=?, content=?, metrics_json=?, body_status='success', body_error='', body_fetched_at=datetime('now'), body_next_retry_at=NULL
            WHERE id=?
            """,
            (title, content, json.dumps(metrics, ensure_ascii=False), row_id),
        )

    run_write_txn(conn, _op, f"update-success:{row_id}")


def compute_retry_after(body_attempts: int, status: str) -> str | None:
    if status in {"success", "skipped"}:
        return None
    attempts = max(1, int(body_attempts or 1))
    multiplier = 2 ** max(0, attempts - 1)
    seconds = min(BODY_RETRY_BACKOFF_MAX_SEC, BODY_RETRY_BACKOFF_BASE_SEC * multiplier)
    if status == "timeout":
        seconds = min(BODY_RETRY_BACKOFF_MAX_SEC, max(seconds, BODY_RETRY_BACKOFF_BASE_SEC * 2))
    return (datetime.now() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")


def update_failure(conn: sqlite3.Connection, row_id: int, body_attempts: int, status: str, error: str) -> str | None:
    retry_after = compute_retry_after(body_attempts=body_attempts, status=status)

    def _op() -> str | None:
        cur = conn.cursor()
        row = cur.execute("SELECT metrics_json FROM raw_items WHERE id=?", (row_id,)).fetchone()
        metrics = parse_metrics(str(row[0] or "{}") if row else "{}")
        metrics.update(
            {
                "body_fetch_ok": False,
                "body_fetch_error": error,
                "body_fetch_method": metrics.get("body_fetch_method") or "body_worker",
                "body_hydrated": False,
                "body_retry_after": retry_after or "",
            }
        )
        cur.execute(
            "UPDATE raw_items SET body_status=?, body_error=?, body_next_retry_at=?, metrics_json=? WHERE id=?",
            (status, error[:500], retry_after, json.dumps(metrics, ensure_ascii=False), row_id),
        )
        return retry_after

    return run_write_txn(conn, _op, f"update-failure:{row_id}:{status}")


def run_once(limit: int, dry_run: bool, platforms: list[str], force_ids: list[int] | None = None, body_timeout_sec: int = BODY_FETCH_TIMEOUT_SEC, breaker_timeouts: int = BODY_TIMEOUT_BREAKER_THRESHOLD, breaker_failures: int = BODY_FAILURE_BREAKER_THRESHOLD, max_timeouts_per_run: int = BODY_MAX_TIMEOUTS_PER_RUN) -> dict[str, int | float | str]:
    started_at = time.time()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = configure_sqlite_connection(sqlite3.connect(DB_PATH, timeout=BODY_DB_TIMEOUT_SEC))
    conn.row_factory = sqlite3.Row
    try:
        ensure_tables(conn)
        forced = force_enqueue_ids(conn, force_ids or [])
        if forced > 0:
            log(f"[force] enqueued ids={','.join(str(x) for x in (force_ids or []))} changed={forced}")
        recovered = recover_in_progress_rows(conn, platforms)
        if recovered > 0:
            log(f"[recover] reset in_progress -> pending rows={recovered}")
        pending = select_pending_rows(conn, limit=max(1, limit), platforms=platforms)
        if dry_run:
            for row in pending:
                log(f"[dry-run] pending id={row['id']} platform={row['platform']} source={row['source_handle']} title={row['title']}")
            stats = get_body_queue_stats(conn, platforms=platforms)
            return {"service": "body_worker", "platforms": ",".join(platforms), "pending": len(pending), "claimed": 0, "success": 0, "failed": 0, "timeout": 0, "skipped": 0, "forced": forced, "duration_sec": round(time.time() - started_at, 2), **stats}

        claimed = claim_rows(conn, pending)
        success = failed = timeout = skipped = 0
        consecutive_timeouts_by_source: dict[str, int] = {}
        consecutive_failures_by_source: dict[str, int] = {}
        breaker_open_sources: set[str] = set()

        for row in claimed:
            platform = str(row["platform"] or "")
            source_handle = str(row["source_handle"] or "") or "unknown"
            row_id = int(row["id"])
            body_attempts = int(row["body_attempts"] or 0) + 1

            if timeout >= max(1, max_timeouts_per_run):
                retry_after = update_failure(conn, row_id, body_attempts, "pending", f"run-timeout-budget-exhausted>{max_timeouts_per_run}")
                skipped += 1
                log_event("budget-stop", id=row_id, source=source_handle, retry_after=retry_after)
                continue

            if source_handle in breaker_open_sources:
                retry_after = update_failure(conn, row_id, body_attempts, "pending", f"source-circuit-open:{source_handle}")
                skipped += 1
                log_event("circuit-skip", id=row_id, source=source_handle, retry_after=retry_after)
                continue

            if platform != "wechat":
                update_failure(conn, row_id, body_attempts, "skipped", f"unsupported-platform:{platform}")
                failed += 1
                log_event("skip", id=row_id, platform=platform, source=source_handle, reason="unsupported-platform")
                continue

            status, title, content, info = hydrate_wechat(row, timeout_sec=max(1, body_timeout_sec))
            if status == "success":
                update_success(conn, row_id, title, content, info)
                success += 1
                consecutive_timeouts_by_source[source_handle] = 0
                consecutive_failures_by_source[source_handle] = 0
                log_event("success", id=row_id, source=source_handle, method=info, attempts=body_attempts)
                continue

            retry_after = update_failure(conn, row_id, body_attempts, status, info)
            if status == "timeout":
                timeout += 1
                consecutive_timeouts_by_source[source_handle] = consecutive_timeouts_by_source.get(source_handle, 0) + 1
                consecutive_failures_by_source[source_handle] = consecutive_failures_by_source.get(source_handle, 0) + 1
            else:
                failed += 1
                consecutive_timeouts_by_source[source_handle] = 0
                consecutive_failures_by_source[source_handle] = consecutive_failures_by_source.get(source_handle, 0) + 1

            if consecutive_timeouts_by_source.get(source_handle, 0) >= max(1, breaker_timeouts) or consecutive_failures_by_source.get(source_handle, 0) >= max(1, breaker_failures):
                breaker_open_sources.add(source_handle)
                log_event(
                    "circuit-open",
                    source=source_handle,
                    timeout_streak=consecutive_timeouts_by_source.get(source_handle, 0),
                    failure_streak=consecutive_failures_by_source.get(source_handle, 0),
                )

            log_event(
                status,
                id=row_id,
                source=source_handle,
                attempts=body_attempts,
                retry_after=retry_after,
                err=info[:180],
            )

        stats = get_body_queue_stats(conn, platforms=platforms)
        return {"service": "body_worker", "platforms": ",".join(platforms), "pending": len(pending), "claimed": len(claimed), "success": success, "failed": failed, "timeout": timeout, "skipped": skipped, "forced": forced, "duration_sec": round(time.time() - started_at, 2), **stats}
    finally:
        conn.close()


def run_daemon(limit: int, poll_sec: int, platforms: list[str], body_timeout_sec: int, breaker_timeouts: int, breaker_failures: int, max_timeouts_per_run: int) -> None:
    while True:
        try:
            summary = run_once(
                limit=limit,
                dry_run=False,
                platforms=platforms,
                body_timeout_sec=body_timeout_sec,
                breaker_timeouts=breaker_timeouts,
                breaker_failures=breaker_failures,
                max_timeouts_per_run=max_timeouts_per_run,
            )
            log(f"[summary] service=body_worker platforms={summary.get('platforms','')} duration_sec={summary.get('duration_sec', 0)} pending={summary['pending']} claimed={summary['claimed']} success={summary['success']} failed={summary['failed']} timeout={summary['timeout']} skipped={summary.get('skipped', 0)} forced={summary.get('forced', 0)} body_due_now={summary.get('body_due_now', 0)} body_retry_waiting={summary.get('body_retry_waiting', 0)} body_in_progress={summary.get('body_in_progress', 0)} body_success={summary.get('body_success', 0)}")
        except Exception as exc:  # noqa: BLE001
            log(f"[fatal-loop-error] {exc}")
        time.sleep(max(1, poll_sec))


def main() -> None:
    args = parse_args()
    if args.once or args.dry_run or args.force_id:
        summary = run_once(
            limit=args.limit,
            dry_run=bool(args.dry_run),
            platforms=args.platforms,
            force_ids=args.force_id,
            body_timeout_sec=args.body_timeout_sec,
            breaker_timeouts=args.breaker_timeouts,
            breaker_failures=args.breaker_failures,
            max_timeouts_per_run=args.max_timeouts_per_run,
        )
        log(f"[summary] service=body_worker platforms={summary.get('platforms','')} duration_sec={summary.get('duration_sec', 0)} pending={summary['pending']} claimed={summary['claimed']} success={summary['success']} failed={summary['failed']} timeout={summary['timeout']} skipped={summary.get('skipped', 0)} forced={summary.get('forced', 0)} body_due_now={summary.get('body_due_now', 0)} body_retry_waiting={summary.get('body_retry_waiting', 0)} body_in_progress={summary.get('body_in_progress', 0)} body_success={summary.get('body_success', 0)}")
        return
    run_daemon(
        limit=args.limit,
        poll_sec=args.poll_sec,
        platforms=args.platforms,
        body_timeout_sec=args.body_timeout_sec,
        breaker_timeouts=args.breaker_timeouts,
        breaker_failures=args.breaker_failures,
        max_timeouts_per_run=args.max_timeouts_per_run,
    )


if __name__ == "__main__":
    main()
