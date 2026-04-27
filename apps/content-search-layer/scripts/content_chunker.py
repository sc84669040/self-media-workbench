from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from typing import Any

from create_studio_config import load_create_studio_config
from create_studio_store import CreateStudioStore

SYNC_SOURCE_NAME = "create_studio.content_chunks"
SYNC_PHASE_NAME = "chunk_content_objects"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


def _parse_json(text: Any, default: Any) -> Any:
    raw = str(text or "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return default


def _normalize_chunk_settings(chunk_size: int, chunk_overlap: int) -> tuple[int, int]:
    normalized_size = max(200, int(chunk_size or 0))
    normalized_overlap = max(0, int(chunk_overlap or 0))
    if normalized_overlap >= normalized_size:
        normalized_overlap = max(0, normalized_size // 5)
    return normalized_size, normalized_overlap


def estimate_token_count(text: str) -> int:
    normalized = str(text or "").strip()
    if not normalized:
        return 0
    ascii_like = sum(1 for char in normalized if ord(char) < 128 and not char.isspace())
    non_ascii = sum(1 for char in normalized if ord(char) >= 128)
    return max(1, ascii_like // 4 + non_ascii)


def split_text_into_chunks(text: str, chunk_size: int, chunk_overlap: int) -> list[dict[str, Any]]:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return []

    normalized_size, normalized_overlap = _normalize_chunk_settings(chunk_size, chunk_overlap)
    step = max(1, normalized_size - normalized_overlap)
    chunks: list[dict[str, Any]] = []

    if len(normalized_text) <= normalized_size:
        return [
            {
                "chunk_index": 0,
                "chunk_text": normalized_text,
                "char_start": 0,
                "char_end": len(normalized_text),
                "token_estimate": estimate_token_count(normalized_text),
            }
        ]

    start = 0
    chunk_index = 0
    while start < len(normalized_text):
        end = min(len(normalized_text), start + normalized_size)
        if end < len(normalized_text):
            boundary = max(
                normalized_text.rfind("\n\n", start, end),
                normalized_text.rfind("\n", start, end),
                normalized_text.rfind("。", start, end),
                normalized_text.rfind("！", start, end),
                normalized_text.rfind("？", start, end),
                normalized_text.rfind(".", start, end),
                normalized_text.rfind("!", start, end),
                normalized_text.rfind("?", start, end),
                normalized_text.rfind(" ", start, end),
            )
            if boundary > start + max(80, normalized_size // 3):
                end = boundary + 1

        chunk_text = normalized_text[start:end].strip()
        if chunk_text:
            chunks.append(
                {
                    "chunk_index": chunk_index,
                    "chunk_text": chunk_text,
                    "char_start": start,
                    "char_end": end,
                    "token_estimate": estimate_token_count(chunk_text),
                }
            )
            chunk_index += 1

        if end >= len(normalized_text):
            break
        start = max(0, end - normalized_overlap)

    return chunks


def _connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _load_chunk_signatures(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    signatures: dict[str, dict[str, Any]] = {}
    rows = conn.execute(
        """
        SELECT object_uid, metadata_json
        FROM content_chunks
        WHERE chunk_index = 0
        """
    ).fetchall()
    for row in rows:
        metadata = _parse_json(row["metadata_json"], {})
        signatures[str(row["object_uid"] or "")] = {
            "content_hash": str(metadata.get("content_hash") or ""),
            "chunk_size": _safe_int(metadata.get("chunk_size"), 0),
            "chunk_overlap": _safe_int(metadata.get("chunk_overlap"), 0),
        }
    return signatures


def _object_needs_rechunk(
    row: sqlite3.Row,
    signatures: dict[str, dict[str, Any]],
    *,
    chunk_size: int,
    chunk_overlap: int,
    full: bool,
) -> bool:
    if full:
        return True
    signature = signatures.get(str(row["object_uid"] or ""))
    if not signature:
        return True
    if str(signature.get("content_hash") or "") != str(row["content_hash"] or ""):
        return True
    if _safe_int(signature.get("chunk_size"), 0) != chunk_size:
        return True
    if _safe_int(signature.get("chunk_overlap"), 0) != chunk_overlap:
        return True
    return False


def _build_chunk_payloads(row: sqlite3.Row, chunk_size: int, chunk_overlap: int) -> list[dict[str, Any]]:
    metadata = _parse_json(row["metadata_json"], {})
    tags = _parse_json(row["tags_json"], [])
    related_topics = _parse_json(row["related_topics_json"], [])
    text = str(row["body_text"] or "").strip()
    raw_chunks = split_text_into_chunks(text, chunk_size, chunk_overlap)
    payloads: list[dict[str, Any]] = []

    for chunk in raw_chunks:
        chunk_index = _safe_int(chunk.get("chunk_index"), 0)
        chunk_metadata = {
            "content_hash": str(row["content_hash"] or ""),
            "chunk_size": chunk_size,
            "chunk_overlap": chunk_overlap,
            "char_start": _safe_int(chunk.get("char_start"), 0),
            "char_end": _safe_int(chunk.get("char_end"), 0),
            "source_kind": str(row["source_kind"] or ""),
            "platform": str(row["platform"] or ""),
            "title": str(row["title"] or ""),
            "source_ref": str(row["source_ref"] or ""),
            "canonical_url": str(row["canonical_url"] or ""),
            "published_at": str(row["published_at"] or ""),
            "tags": tags,
            "related_topics": related_topics,
            "event_candidate_ids": list(metadata.get("event_candidate_ids") or []),
            "event_packet_refs": list(metadata.get("event_packet_refs") or []),
            "cluster_ready": bool(metadata.get("cluster_ready")),
        }
        payloads.append(
            {
                "chunk_id": f"{row['object_uid']}::chunk::{chunk_index}",
                "chunk_text": str(chunk.get("chunk_text") or "").strip(),
                "token_estimate": _safe_int(chunk.get("token_estimate"), 0),
                "metadata": chunk_metadata,
            }
        )
    return payloads


def chunk_content_objects_to_store(
    *,
    create_studio_db_path: str | Path,
    chunk_size: int,
    chunk_overlap: int,
    full: bool = False,
    limit: int = 0,
) -> dict[str, Any]:
    normalized_chunk_size, normalized_chunk_overlap = _normalize_chunk_settings(chunk_size, chunk_overlap)
    store = CreateStudioStore(create_studio_db_path)
    store.initialize()

    run = store.start_sync_run(
        SYNC_SOURCE_NAME,
        SYNC_PHASE_NAME,
        {
            "mode": "full" if full else "incremental",
            "chunk_size": normalized_chunk_size,
            "chunk_overlap": normalized_chunk_overlap,
            "target_db_path": str(Path(create_studio_db_path)),
        },
    )
    run_id = run["run_id"]

    try:
        with _connect_readonly(create_studio_db_path) as conn:
            signatures = _load_chunk_signatures(conn)
            rows = conn.execute(
                """
                SELECT
                  object_uid,
                  source_kind,
                  platform,
                  source_ref,
                  canonical_url,
                  title,
                  body_text,
                  body_ready,
                  published_at,
                  tags_json,
                  related_topics_json,
                  metadata_json,
                  content_hash,
                  updated_at
                FROM content_objects
                WHERE body_ready = 1 AND COALESCE(body_text, '') != ''
                ORDER BY updated_at ASC, object_uid ASC
                """
            ).fetchall()

        metrics = {
            "mode": "full" if full else "incremental",
            "eligible_objects": len(rows),
            "rechunked_objects": 0,
            "skipped_objects": 0,
            "new_chunks": 0,
            "objects_with_event_links": 0,
            "chunk_size": normalized_chunk_size,
            "chunk_overlap": normalized_chunk_overlap,
        }

        processed = 0
        for row in rows:
            if limit > 0 and processed >= limit:
                break
            if not _object_needs_rechunk(
                row,
                signatures,
                chunk_size=normalized_chunk_size,
                chunk_overlap=normalized_chunk_overlap,
                full=full,
            ):
                metrics["skipped_objects"] += 1
                continue

            chunk_payloads = _build_chunk_payloads(row, normalized_chunk_size, normalized_chunk_overlap)
            store.replace_content_chunks(str(row["object_uid"] or ""), chunk_payloads)
            processed += 1
            metrics["rechunked_objects"] += 1
            metrics["new_chunks"] += len(chunk_payloads)

            metadata = _parse_json(row["metadata_json"], {})
            if metadata.get("event_candidate_ids") or metadata.get("event_packet_refs"):
                metrics["objects_with_event_links"] += 1

        finished = store.finish_sync_run(run_id, "completed", metrics)
        return {
            "ok": True,
            "run": finished,
            "metrics": metrics,
            "store_status": store.get_status(),
        }
    except Exception as exc:  # noqa: BLE001
        store.finish_sync_run(run_id, "failed", {"mode": "full" if full else "incremental"}, error_text=str(exc))
        raise


def chunk_content_objects_from_config(*, config_path: str | Path | None = None, full: bool = False, limit: int = 0) -> dict[str, Any]:
    config = load_create_studio_config(config_path=config_path)
    indexing = dict(config.get("indexing") or {})
    return chunk_content_objects_to_store(
        create_studio_db_path=str(indexing.get("content_index_db_path") or "").strip(),
        chunk_size=_safe_int(indexing.get("chunk_size"), 1200),
        chunk_overlap=_safe_int(indexing.get("chunk_overlap"), 180),
        full=full,
        limit=limit,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Chunk create_studio content objects into content_chunks")
    parser.add_argument("--config", default="", help="override create studio config path")
    parser.add_argument("--full", action="store_true", help="force full rechunk instead of incremental")
    parser.add_argument("--limit", type=int, default=0, help="limit number of objects to rechunk")
    parser.add_argument("--chunk-size", type=int, default=0, help="override chunk size")
    parser.add_argument("--chunk-overlap", type=int, default=0, help="override chunk overlap")
    parser.add_argument("--create-studio-db", default="", help="override create studio DB path")
    args = parser.parse_args()

    if args.create_studio_db:
        result = chunk_content_objects_to_store(
            create_studio_db_path=args.create_studio_db,
            chunk_size=args.chunk_size or 1200,
            chunk_overlap=args.chunk_overlap or 180,
            full=args.full,
            limit=max(0, int(args.limit or 0)),
        )
    else:
        config = load_create_studio_config(config_path=args.config or None)
        indexing = dict(config.get("indexing") or {})
        result = chunk_content_objects_to_store(
            create_studio_db_path=str(indexing.get("content_index_db_path") or "").strip(),
            chunk_size=args.chunk_size or _safe_int(indexing.get("chunk_size"), 1200),
            chunk_overlap=args.chunk_overlap or _safe_int(indexing.get("chunk_overlap"), 180),
            full=args.full,
            limit=max(0, int(args.limit or 0)),
        )

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
