from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
from typing import Any

from content_object_models import content_object_from_raw_item
from create_studio_config import load_create_studio_config
from create_studio_store import CreateStudioStore

SYNC_SOURCE_NAME = "event_radar.raw_items"
SYNC_PHASE_NAME = "index_raw_items"
SYNC_FETCHED_AT_META_KEY = "raw_items_last_synced_fetched_at"
SYNC_ROW_ID_META_KEY = "raw_items_last_synced_row_id"


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return 0


def _load_event_links(conn: sqlite3.Connection) -> dict[int, list[dict[str, Any]]]:
    if not _table_exists(conn, "event_candidates") or not _table_exists(conn, "event_evidence"):
        return {}

    event_candidate_columns = _column_names(conn, "event_candidates")
    event_evidence_columns = _column_names(conn, "event_evidence")

    role_expr = "ee.role AS evidence_role" if "role" in event_evidence_columns else "'' AS evidence_role"
    event_type_expr = "ec.event_type AS event_type" if "event_type" in event_candidate_columns else "'' AS event_type"
    summary_expr = "ec.summary AS event_summary" if "summary" in event_candidate_columns else "'' AS event_summary"
    status_expr = "ec.status AS event_status" if "status" in event_candidate_columns else "'' AS event_status"
    heat_expr = "ec.heat_score AS heat_score" if "heat_score" in event_candidate_columns else "0 AS heat_score"
    novelty_expr = "ec.novelty_score AS novelty_score" if "novelty_score" in event_candidate_columns else "0 AS novelty_score"
    confidence_expr = "ec.confidence AS confidence" if "confidence" in event_candidate_columns else "0 AS confidence"

    rows = conn.execute(
        f"""
        SELECT
          ee.raw_item_id,
          ec.id AS event_id,
          {event_type_expr},
          ec.title AS event_title,
          {summary_expr},
          {status_expr},
          {heat_expr},
          {novelty_expr},
          {confidence_expr},
          {role_expr}
        FROM event_evidence ee
        JOIN event_candidates ec ON ec.id = ee.event_id
        ORDER BY ee.raw_item_id ASC, ec.id ASC
        """
    ).fetchall()

    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        raw_item_id = _safe_int(row["raw_item_id"])
        if raw_item_id <= 0:
            continue
        grouped[raw_item_id].append(
            {
                "event_id": _safe_int(row["event_id"]),
                "event_type": str(row["event_type"] or "").strip(),
                "title": str(row["event_title"] or "").strip(),
                "summary": str(row["event_summary"] or "").strip(),
                "status": str(row["event_status"] or "").strip(),
                "heat_score": float(row["heat_score"] or 0),
                "novelty_score": float(row["novelty_score"] or 0),
                "confidence": float(row["confidence"] or 0),
                "role": str(row["evidence_role"] or "").strip(),
            }
        )
    return grouped


def _fetch_candidate_rows(
    conn: sqlite3.Connection,
    *,
    last_fetched_at: str,
    last_row_id: int,
    full: bool,
    limit: int = 0,
) -> list[sqlite3.Row]:
    raw_item_columns = _column_names(conn, "raw_items")
    body_status_expr = "body_status" if "body_status" in raw_item_columns else "'' AS body_status"
    base_sql = f"""
        SELECT
          id,
          platform,
          source_handle,
          item_id,
          title,
          content,
          url,
          published_at,
          fetched_at,
          metrics_json,
          {body_status_expr}
        FROM raw_items
    """
    params: list[Any] = []
    where_parts: list[str] = []
    if not full and last_fetched_at:
        where_parts.append("(COALESCE(fetched_at, '') > ? OR (COALESCE(fetched_at, '') = ? AND id > ?))")
        params.extend([last_fetched_at, last_fetched_at, last_row_id])
    if where_parts:
        base_sql = f"{base_sql} WHERE {' AND '.join(where_parts)}"
    base_sql = f"{base_sql} ORDER BY COALESCE(fetched_at, '') ASC, id ASC"
    if limit > 0:
        base_sql = f"{base_sql} LIMIT ?"
        params.append(limit)
    return conn.execute(base_sql, tuple(params)).fetchall()


def sync_raw_items_to_create_studio(
    *,
    nighthawk_db_path: str | Path,
    create_studio_db_path: str | Path,
    full: bool = False,
    limit: int = 0,
) -> dict[str, Any]:
    source_db_path = Path(nighthawk_db_path)
    store = CreateStudioStore(create_studio_db_path)
    store.initialize()

    run = store.start_sync_run(
        SYNC_SOURCE_NAME,
        SYNC_PHASE_NAME,
        {
            "mode": "full" if full else "incremental",
            "source_db_path": str(source_db_path),
            "target_db_path": str(Path(create_studio_db_path)),
        },
    )
    run_id = run["run_id"]

    try:
        if not source_db_path.exists():
            raise FileNotFoundError(f"NightHawk DB not found: {source_db_path}")

        with sqlite3.connect(source_db_path) as conn:
            conn.row_factory = sqlite3.Row
            if not _table_exists(conn, "raw_items"):
                raise RuntimeError("NightHawk DB does not contain raw_items")

            last_fetched_at = "" if full else store.get_meta_value(SYNC_FETCHED_AT_META_KEY, "")
            last_row_id = 0 if full else _safe_int(store.get_meta_value(SYNC_ROW_ID_META_KEY, "0"))
            event_links_by_raw_item_id = _load_event_links(conn)
            rows = _fetch_candidate_rows(
                conn,
                last_fetched_at=last_fetched_at,
                last_row_id=last_row_id,
                full=full,
                limit=limit,
            )

        metrics = {
            "mode": "full" if full else "incremental",
            "source_db_path": str(source_db_path),
            "target_db_path": str(Path(create_studio_db_path)),
            "scanned": len(rows),
            "upserted": 0,
            "body_ready": 0,
            "metadata_only": 0,
            "event_linked": 0,
            "last_synced_fetched_at": last_fetched_at,
            "last_synced_row_id": last_row_id,
        }

        latest_fetched_at = last_fetched_at
        latest_row_id = last_row_id
        for row in rows:
            obj = content_object_from_raw_item(dict(row))
            event_links = event_links_by_raw_item_id.get(_safe_int(row["id"]), [])
            metadata = dict(obj.metadata or {})
            metadata["sync_source"] = SYNC_SOURCE_NAME
            metadata["event_links"] = event_links
            metadata["event_candidate_ids"] = [item["event_id"] for item in event_links if item.get("event_id")]
            metadata["event_packet_refs"] = list(metadata.get("event_packet_refs") or [])
            metadata["cluster_ready"] = bool(event_links)
            obj.metadata = metadata
            if event_links:
                metrics["event_linked"] += 1

            store.upsert_content_object(obj.to_store_payload())
            metrics["upserted"] += 1
            if obj.body_ready:
                metrics["body_ready"] += 1
            else:
                metrics["metadata_only"] += 1

            latest_fetched_at = str(row["fetched_at"] or latest_fetched_at or "").strip()
            latest_row_id = _safe_int(row["id"] or latest_row_id)

        store.set_meta_value(SYNC_FETCHED_AT_META_KEY, latest_fetched_at)
        store.set_meta_value(SYNC_ROW_ID_META_KEY, latest_row_id)
        metrics["last_synced_fetched_at"] = latest_fetched_at
        metrics["last_synced_row_id"] = latest_row_id

        finished = store.finish_sync_run(run_id, "completed", metrics)
        return {
            "ok": True,
            "run": finished,
            "metrics": metrics,
            "store_status": store.get_status(),
        }
    except Exception as exc:  # noqa: BLE001
        error_metrics = {
            "mode": "full" if full else "incremental",
            "source_db_path": str(source_db_path),
            "target_db_path": str(Path(create_studio_db_path)),
        }
        store.finish_sync_run(run_id, "failed", error_metrics, error_text=str(exc))
        raise


def sync_raw_items_from_config(*, full: bool = False, limit: int = 0) -> dict[str, Any]:
    config = load_create_studio_config()
    nighthawk_db_path = ((config.get("database_sources") or {}).get("nighthawk_db_path") or "").strip()
    create_studio_db_path = ((config.get("indexing") or {}).get("content_index_db_path") or "").strip()
    return sync_raw_items_to_create_studio(
        nighthawk_db_path=nighthawk_db_path,
        create_studio_db_path=create_studio_db_path,
        full=full,
        limit=limit,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync NightHawk raw_items into create_studio content_objects")
    parser.add_argument("--full", action="store_true", help="force full sync instead of incremental sync")
    parser.add_argument("--limit", type=int, default=0, help="limit number of candidate rows for one sync run")
    parser.add_argument("--nighthawk-db", default="", help="override NightHawk DB path")
    parser.add_argument("--create-studio-db", default="", help="override create_studio DB path")
    args = parser.parse_args()

    if args.nighthawk_db and args.create_studio_db:
        result = sync_raw_items_to_create_studio(
            nighthawk_db_path=args.nighthawk_db,
            create_studio_db_path=args.create_studio_db,
            full=args.full,
            limit=max(0, int(args.limit or 0)),
        )
    else:
        result = sync_raw_items_from_config(full=args.full, limit=max(0, int(args.limit or 0)))

    print(_json_dumps(result))


if __name__ == "__main__":
    main()
