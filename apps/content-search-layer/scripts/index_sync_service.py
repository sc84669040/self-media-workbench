from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any

from content_chunker import chunk_content_objects_to_store
from create_studio_config import load_create_studio_config
from create_studio_store import CreateStudioStore, now_iso
from fts_search_service import rebuild_fts_index
from index_raw_items import sync_raw_items_to_create_studio
from index_vault_notes import sync_vault_notes_to_create_studio

INDEX_SYNC_SUMMARY_META_KEY = "last_index_sync_summary_json"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def run_create_studio_index_sync(
    *,
    config_path: str | Path | None = None,
    full: bool = False,
    limit_per_phase: int = 0,
) -> dict[str, Any]:
    config = load_create_studio_config(config_path=config_path)
    config_meta = dict(config.get("_meta") or {})
    database_sources = dict(config.get("database_sources") or {})
    indexing = dict(config.get("indexing") or {})
    semantic_search = dict(config.get("semantic_search") or {})

    create_studio_db_path = str(indexing.get("content_index_db_path") or "").strip()
    nighthawk_db_path = str(database_sources.get("nighthawk_db_path") or "").strip()
    chunk_size = _safe_int(indexing.get("chunk_size"), 1200)
    chunk_overlap = _safe_int(indexing.get("chunk_overlap"), 180)

    if not create_studio_db_path:
        raise ValueError("create studio index db path is missing")
    if not nighthawk_db_path:
        raise ValueError("NightHawk DB path is missing")

    store = CreateStudioStore(create_studio_db_path)
    store.initialize()

    started_at = now_iso()
    started_perf = time.perf_counter()
    summary: dict[str, Any] = {
        "ok": False,
        "status": "running",
        "mode": "full" if full else "incremental",
        "started_at": started_at,
        "finished_at": "",
        "duration_seconds": 0.0,
        "config_path": str(config_meta.get("config_path") or ""),
        "message": "创作台索引同步进行中",
        "phases": [],
        "totals": {
            "new_objects": 0,
            "new_chunks": 0,
            "failed_phases": 0,
        },
    }
    store.set_meta_value(INDEX_SYNC_SUMMARY_META_KEY, _json_dumps(summary))

    try:
        raw_result = sync_raw_items_to_create_studio(
            nighthawk_db_path=nighthawk_db_path,
            create_studio_db_path=create_studio_db_path,
            full=full,
            limit=limit_per_phase,
        )
        summary["phases"].append(
            {
                "name": "NightHawk 数据同步",
                "status": "completed",
                "objects": int((raw_result.get("metrics") or {}).get("upserted") or 0),
                "chunks": 0,
            }
        )

        vault_result = sync_vault_notes_to_create_studio(
            config_path=config_meta.get("config_path") or config_path,
            full=full,
            limit=limit_per_phase,
        )
        summary["phases"].append(
            {
                "name": "知识库笔记同步",
                "status": "completed",
                "objects": int((vault_result.get("metrics") or {}).get("upserted") or 0),
                "chunks": 0,
            }
        )

        chunk_result = chunk_content_objects_to_store(
            create_studio_db_path=create_studio_db_path,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            full=full,
            limit=limit_per_phase,
        )
        summary["phases"].append(
            {
                "name": "正文切块同步",
                "status": "completed",
                "objects": int((chunk_result.get("metrics") or {}).get("rechunked_objects") or 0),
                "chunks": int((chunk_result.get("metrics") or {}).get("new_chunks") or 0),
            }
        )

        fts_result = None
        if bool(semantic_search.get("enable_fts")):
            fts_result = rebuild_fts_index(create_studio_db_path)
            summary["phases"].append(
                {
                    "name": "全文检索索引刷新",
                    "status": "completed",
                    "objects": 0,
                    "chunks": 0,
                    "indexed_objects": int((fts_result or {}).get("indexed_count") or 0),
                }
            )

        finished_at = now_iso()
        summary["ok"] = True
        summary["status"] = "completed"
        summary["finished_at"] = finished_at
        summary["duration_seconds"] = round(time.perf_counter() - started_perf, 3)
        summary["totals"]["new_objects"] = sum(int(phase.get("objects") or 0) for phase in summary["phases"])
        summary["totals"]["new_chunks"] = sum(int(phase.get("chunks") or 0) for phase in summary["phases"])
        summary["message"] = "创作台索引同步完成"
        store.set_meta_value(INDEX_SYNC_SUMMARY_META_KEY, _json_dumps(summary))
        return {
            "ok": True,
            "message": summary["message"],
            "index_sync_summary": summary,
            "store_status": store.get_status(),
            "phase_results": {
                "raw_items": raw_result,
                "vault_notes": vault_result,
                "chunking": chunk_result,
                "fts": fts_result or {"ok": False, "skipped": True},
            },
        }
    except Exception as exc:  # noqa: BLE001
        finished_at = now_iso()
        summary["status"] = "failed"
        summary["finished_at"] = finished_at
        summary["duration_seconds"] = round(time.perf_counter() - started_perf, 3)
        summary["message"] = f"创作台索引同步失败：{exc}"
        summary["totals"]["failed_phases"] = 1
        if not summary["phases"] or summary["phases"][-1].get("status") == "completed":
            summary["phases"].append(
                {
                    "name": "索引同步",
                    "status": "failed",
                    "error": str(exc),
                    "objects": 0,
                    "chunks": 0,
                }
            )
        else:
            summary["phases"][-1]["status"] = "failed"
            summary["phases"][-1]["error"] = str(exc)
        store.set_meta_value(INDEX_SYNC_SUMMARY_META_KEY, _json_dumps(summary))
        raise
