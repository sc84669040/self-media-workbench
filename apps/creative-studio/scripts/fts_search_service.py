from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from typing import Any

from create_studio_store import now_iso

FTS_TABLE_NAME = "content_objects_fts"
FTS_META_INDEXED_AT = "last_fts_indexed_at"
FTS_META_INDEXED_COUNT = "last_fts_indexed_count"
FTS_META_SOURCE_MUTATION_AT = "last_fts_source_mutation_at"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


def _parse_json(value: Any, default: Any) -> Any:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        return default


def _join_terms(value: Any) -> str:
    parsed = _parse_json(value, [])
    if isinstance(parsed, list):
        return " ".join(str(item).strip() for item in parsed if str(item).strip())
    return str(value or "").strip()


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _write_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        """
        INSERT INTO create_studio_meta(key, value_text, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value_text = excluded.value_text,
          updated_at = excluded.updated_at
        """,
        (str(key or "").strip(), str(value), now_iso()),
    )


def _read_meta(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value_text FROM create_studio_meta WHERE key = ? LIMIT 1", (key,)).fetchone()
    if not row:
        return default
    return str(row["value_text"] or default)


def ensure_fts_index(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {FTS_TABLE_NAME}
        USING fts5(
          object_uid UNINDEXED,
          title,
          summary,
          body_text,
          related_topics,
          tags,
          source_name,
          source_kind,
          tokenize = 'unicode61 remove_diacritics 2'
        )
        """
    )


def rebuild_fts_index(create_studio_db_path: str | Path) -> dict[str, Any]:
    with _connect(create_studio_db_path) as conn:
        ensure_fts_index(conn)
        rows = conn.execute(
            """
            SELECT
              object_uid,
              title,
              summary,
              body_text,
              related_topics_json,
              tags_json,
              source_name,
              source_kind
            FROM content_objects
            ORDER BY object_uid ASC
            """
        ).fetchall()
        conn.execute(f"DELETE FROM {FTS_TABLE_NAME}")
        for row in rows:
            conn.execute(
                f"""
                INSERT INTO {FTS_TABLE_NAME}(
                  object_uid, title, summary, body_text, related_topics, tags, source_name, source_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row["object_uid"] or ""),
                    str(row["title"] or ""),
                    str(row["summary"] or ""),
                    str(row["body_text"] or ""),
                    _join_terms(row["related_topics_json"]),
                    _join_terms(row["tags_json"]),
                    str(row["source_name"] or ""),
                    str(row["source_kind"] or ""),
                ),
            )

        current_mutation = _read_meta(conn, "last_mutation_at", "")
        indexed_at = now_iso()
        _write_meta(conn, FTS_META_INDEXED_AT, indexed_at)
        _write_meta(conn, FTS_META_INDEXED_COUNT, len(rows))
        _write_meta(conn, FTS_META_SOURCE_MUTATION_AT, current_mutation)
        conn.commit()
        return {
            "ok": True,
            "indexed_count": len(rows),
            "indexed_at": indexed_at,
            "source_mutation_at": current_mutation,
        }


def _fts_needs_refresh(conn: sqlite3.Connection) -> bool:
    ensure_fts_index(conn)
    indexed_count = _safe_int(_read_meta(conn, FTS_META_INDEXED_COUNT, "0"))
    current_count = _safe_int(conn.execute("SELECT COUNT(*) FROM content_objects").fetchone()[0], 0)
    current_mutation = _read_meta(conn, "last_mutation_at", "")
    indexed_mutation = _read_meta(conn, FTS_META_SOURCE_MUTATION_AT, "")
    return indexed_count != current_count or current_mutation != indexed_mutation


def _build_match_query(query: str, query_terms: list[str]) -> str:
    phrases = [str(item or "").strip() for item in [query, *list(query_terms or [])] if str(item or "").strip()]
    normalized: list[str] = []
    for item in phrases:
        escaped = item.replace('"', " ").strip()
        if not escaped:
            continue
        normalized.append(f'"{escaped}"')
        normalized.extend(f'"{part}"' for part in escaped.split() if len(part) >= 2)

    deduped: list[str] = []
    seen: set[str] = set()
    for item in normalized:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return " OR ".join(deduped[:12])


def _escape_match_literal(value: str) -> str:
    return str(value or "").replace("'", "''")


def _field_hits(row: sqlite3.Row, query_terms: list[str]) -> list[str]:
    joined_terms = [str(item or "").strip().lower() for item in query_terms if str(item or "").strip()]
    haystacks = {
        "title": str(row["title"] or "").lower(),
        "summary": str(row["summary"] or "").lower(),
        "body_text": str(row["body_text"] or "").lower(),
        "related_topics": _join_terms(row["related_topics_json"]).lower(),
        "tags": _join_terms(row["tags_json"]).lower(),
        "source_name": str(row["source_name"] or "").lower(),
    }
    hits: list[str] = []
    for field_name, content in haystacks.items():
        if any(term in content for term in joined_terms):
            hits.append(field_name)
    return hits


def _why_relevant(body_ready: bool, field_hits: list[str]) -> str:
    body_state = "正文完整" if body_ready else "暂时只有摘要"
    hit_summary = f"命中字字段：{', '.join(field_hits)}" if field_hits else "命中全文索引"
    return "；".join([body_state, hit_summary])


def search_content_objects_fts(
    *,
    create_studio_db_path: str | Path,
    query: str,
    query_terms: list[str] | None = None,
    limit: int = 20,
    body_ready_only: bool = False,
    source_kinds: list[str] | None = None,
) -> dict[str, Any]:
    query_terms = [str(item or "").strip() for item in (query_terms or []) if str(item or "").strip()]
    normalized_query = str(query or "").strip()
    if not normalized_query and not query_terms:
        return {"ok": True, "query": "", "match_query": "", "count": 0, "items": [], "fts_refreshed": False}

    with _connect(create_studio_db_path) as conn:
        needs_refresh = _fts_needs_refresh(conn)

    refreshed = False
    if needs_refresh:
        rebuild_fts_index(create_studio_db_path)
        refreshed = True

    match_query = _build_match_query(normalized_query, query_terms)
    if not match_query:
        return {"ok": True, "query": normalized_query, "match_query": "", "count": 0, "items": [], "fts_refreshed": refreshed}

    with _connect(create_studio_db_path) as conn:
        ensure_fts_index(conn)
        where_parts = [f"{FTS_TABLE_NAME} MATCH '{_escape_match_literal(match_query)}'"]
        params: list[Any] = []
        if body_ready_only:
            where_parts.append("co.body_ready = 1")
        source_kind_filters = [str(item or "").strip() for item in (source_kinds or []) if str(item or "").strip()]
        if source_kind_filters:
            placeholders = ", ".join(["?"] * len(source_kind_filters))
            where_parts.append(f"co.source_kind IN ({placeholders})")
            params.extend(source_kind_filters)
        params.append(max(1, int(limit or 20)))

        rows = conn.execute(
            f"""
            SELECT
              co.object_uid,
              co.source_kind,
              co.platform,
              co.source_ref,
              co.canonical_url,
              co.title,
              co.summary,
              co.body_text,
              co.body_ready,
              co.published_at,
              co.source_name,
              co.tags_json,
              co.related_topics_json,
              co.metadata_json,
              bm25({FTS_TABLE_NAME}, 5.0, 3.0, 1.0, 2.5, 2.0, 1.5, 1.0) AS relevance_score
            FROM {FTS_TABLE_NAME}
            JOIN content_objects co ON co.object_uid = {FTS_TABLE_NAME}.object_uid
            WHERE {' AND '.join(where_parts)}
            ORDER BY relevance_score ASC, co.published_at DESC, co.object_uid ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    items: list[dict[str, Any]] = []
    explanation_terms = [normalized_query, *query_terms]
    for row in rows:
        field_hits = _field_hits(row, explanation_terms)
        items.append(
            {
                "object_uid": str(row["object_uid"] or ""),
                "source_kind": str(row["source_kind"] or ""),
                "platform": str(row["platform"] or ""),
                "source_ref": str(row["source_ref"] or ""),
                "canonical_url": str(row["canonical_url"] or ""),
                "title": str(row["title"] or ""),
                "summary": str(row["summary"] or ""),
                "body_text": str(row["body_text"] or ""),
                "body_ready": bool(row["body_ready"]),
                "published_at": str(row["published_at"] or ""),
                "source_name": str(row["source_name"] or ""),
                "tags": _parse_json(row["tags_json"], []),
                "related_topics": _parse_json(row["related_topics_json"], []),
                "metadata": _parse_json(row["metadata_json"], {}),
                "relevance_score": float(row["relevance_score"] or 0.0),
                "field_hits": field_hits,
                "why_relevant": _why_relevant(bool(row["body_ready"]), field_hits),
            }
        )

    return {
        "ok": True,
        "query": normalized_query,
        "match_query": match_query,
        "count": len(items),
        "items": items,
        "fts_refreshed": refreshed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Search create_studio content objects with SQLite FTS5")
    parser.add_argument("db_path", help="create_studio.db path")
    parser.add_argument("query", help="search query")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--body-ready-only", action="store_true")
    parser.add_argument("--source-kind", action="append", default=[])
    args = parser.parse_args()

    payload = search_content_objects_fts(
        create_studio_db_path=args.db_path,
        query=args.query,
        query_terms=[],
        limit=args.limit,
        body_ready_only=args.body_ready_only,
        source_kinds=args.source_kind,
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
