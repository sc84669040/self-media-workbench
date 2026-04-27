from __future__ import annotations

import argparse
from glob import glob
from pathlib import Path
import re
from typing import Any

from content_object_models import content_object_from_markdown_document
from create_studio_config import load_create_studio_config
from create_studio_store import CreateStudioStore

SYNC_SOURCE_NAME = "knowledge_base.vault_notes"
SYNC_PHASE_NAME = "index_vault_notes"
SYNC_MTIME_META_KEY = "vault_notes_last_synced_mtime_ns"
SYNC_PATH_META_KEY = "vault_notes_last_synced_path"


def _normalize_config_pattern(pattern: str) -> str:
    text = str(pattern or "").strip()
    if not text:
        return ""

    match = re.match(r"^/mnt/([a-zA-Z])/(.*)$", text)
    if match:
        drive = match.group(1).upper()
        remainder = match.group(2).replace("/", "\\")
        return f"{drive}:\\{remainder}"
    return text


def _ensure_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return 0


def _sorted_paths_from_globs(glob_patterns: list[str]) -> list[Path]:
    discovered: dict[str, Path] = {}
    for pattern in glob_patterns:
        normalized = _normalize_config_pattern(pattern)
        if not normalized:
            continue
        for matched in glob(normalized, recursive=True):
            path = Path(matched)
            if path.is_file():
                discovered[str(path.resolve()).lower()] = path.resolve()
    return sorted(discovered.values(), key=lambda item: str(item).lower())


def _default_source_note_globs(vault_roots: list[str]) -> list[str]:
    globs: list[str] = []
    for root in vault_roots:
        normalized_root = _normalize_config_pattern(root)
        if normalized_root:
            globs.append(str(Path(normalized_root) / "**" / "*.md"))
    return globs


def _load_candidate_files(config: dict[str, Any]) -> tuple[list[Path], list[Path]]:
    knowledge_sources = dict(config.get("knowledge_sources") or {})
    vault_roots = _ensure_list(knowledge_sources.get("vault_roots"))
    analysis_card_globs = _ensure_list(knowledge_sources.get("analysis_card_globs"))
    source_note_globs = _ensure_list(knowledge_sources.get("source_note_globs")) or _default_source_note_globs(vault_roots)

    analysis_card_paths = _sorted_paths_from_globs(analysis_card_globs)
    analysis_card_keys = {str(path).lower() for path in analysis_card_paths}

    source_note_paths = [
        path
        for path in _sorted_paths_from_globs(source_note_globs)
        if str(path).lower() not in analysis_card_keys
    ]
    return analysis_card_paths, source_note_paths


def _should_process_file(path: Path, *, full: bool, last_mtime_ns: int, last_path: str) -> bool:
    if full:
        return True
    stat = path.stat()
    current_key = str(path.resolve())
    if stat.st_mtime_ns > last_mtime_ns:
        return True
    if stat.st_mtime_ns == last_mtime_ns and current_key > last_path:
        return True
    return False


def _update_watermark(path: Path, latest_mtime_ns: int, latest_path: str) -> tuple[int, str]:
    stat = path.stat()
    current_mtime_ns = int(stat.st_mtime_ns)
    current_path = str(path.resolve())
    if current_mtime_ns > latest_mtime_ns:
        return current_mtime_ns, current_path
    if current_mtime_ns == latest_mtime_ns and current_path > latest_path:
        return current_mtime_ns, current_path
    return latest_mtime_ns, latest_path


def sync_vault_notes_to_create_studio(
    *,
    config_path: str | Path | None = None,
    full: bool = False,
    limit: int = 0,
) -> dict[str, Any]:
    config = load_create_studio_config(config_path=config_path)
    create_studio_db_path = ((config.get("indexing") or {}).get("content_index_db_path") or "").strip()
    if not create_studio_db_path:
        raise ValueError("create studio index db path is missing from config")

    analysis_card_paths, source_note_paths = _load_candidate_files(config)
    store = CreateStudioStore(create_studio_db_path)
    store.initialize()

    run = store.start_sync_run(
        SYNC_SOURCE_NAME,
        SYNC_PHASE_NAME,
        {
            "mode": "full" if full else "incremental",
            "config_path": ((config.get("_meta") or {}).get("config_path") or ""),
            "analysis_card_candidates": len(analysis_card_paths),
            "source_note_candidates": len(source_note_paths),
        },
    )
    run_id = run["run_id"]

    try:
        last_mtime_ns = 0 if full else _safe_int(store.get_meta_value(SYNC_MTIME_META_KEY, "0"))
        last_path = "" if full else store.get_meta_value(SYNC_PATH_META_KEY, "")

        ordered_candidates: list[tuple[str, Path]] = [
            ("analysis_card", path) for path in analysis_card_paths
        ] + [
            ("source_note", path) for path in source_note_paths
        ]
        ordered_candidates.sort(key=lambda item: (item[1].stat().st_mtime_ns, str(item[1].resolve())))

        metrics = {
            "mode": "full" if full else "incremental",
            "scanned": 0,
            "upserted": 0,
            "analysis_cards": 0,
            "source_notes": 0,
            "body_ready": 0,
            "cluster_hint_objects": 0,
            "last_synced_mtime_ns": last_mtime_ns,
            "last_synced_path": last_path,
        }

        latest_mtime_ns = last_mtime_ns
        latest_path = last_path
        processed = 0
        for source_kind, path in ordered_candidates:
            if not _should_process_file(path, full=full, last_mtime_ns=last_mtime_ns, last_path=last_path):
                continue
            if limit > 0 and processed >= limit:
                break

            markdown_text = path.read_text(encoding="utf-8")
            obj = content_object_from_markdown_document(
                path,
                markdown_text,
                default_source_kind=source_kind,
            )
            metadata = dict(obj.metadata or {})
            metadata["sync_source"] = SYNC_SOURCE_NAME
            metadata.setdefault("event_packet_refs", [])
            metadata.setdefault("cluster_ready", False)
            cluster_hints = dict(metadata.get("cluster_hints") or {})
            cluster_hints.setdefault("related_topics", list(obj.related_topics or []))
            cluster_hints.setdefault("tags", list(obj.tags or []))
            cluster_hints.setdefault("source_kind", obj.source_kind)
            metadata["cluster_hints"] = cluster_hints
            obj.metadata = metadata

            store.upsert_content_object(obj.to_store_payload())
            processed += 1
            metrics["scanned"] += 1
            metrics["upserted"] += 1
            if obj.source_kind == "analysis_card":
                metrics["analysis_cards"] += 1
            else:
                metrics["source_notes"] += 1
            if obj.body_ready:
                metrics["body_ready"] += 1
            if obj.related_topics or obj.tags:
                metrics["cluster_hint_objects"] += 1

            latest_mtime_ns, latest_path = _update_watermark(path, latest_mtime_ns, latest_path)

        store.set_meta_value(SYNC_MTIME_META_KEY, latest_mtime_ns)
        store.set_meta_value(SYNC_PATH_META_KEY, latest_path)
        metrics["last_synced_mtime_ns"] = latest_mtime_ns
        metrics["last_synced_path"] = latest_path

        finished = store.finish_sync_run(run_id, "completed", metrics)
        return {
            "ok": True,
            "run": finished,
            "metrics": metrics,
            "store_status": store.get_status(),
        }
    except Exception as exc:  # noqa: BLE001
        store.finish_sync_run(
            run_id,
            "failed",
            {"mode": "full" if full else "incremental"},
            error_text=str(exc),
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync vault markdown notes and analysis cards into create_studio")
    parser.add_argument("--config", default="", help="override create studio config path")
    parser.add_argument("--full", action="store_true", help="force full sync instead of incremental sync")
    parser.add_argument("--limit", type=int, default=0, help="limit number of files for one sync run")
    args = parser.parse_args()

    result = sync_vault_notes_to_create_studio(
        config_path=args.config or None,
        full=args.full,
        limit=max(0, int(args.limit or 0)),
    )
    print(result)


if __name__ == "__main__":
    main()
