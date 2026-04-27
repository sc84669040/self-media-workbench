from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

SQLITE_BUSY_TIMEOUT_MS = 30_000
SCHEMA_VERSION = 1
REQUIRED_TABLES = (
    "create_studio_meta",
    "content_objects",
    "content_chunks",
    "topic_packets",
    "event_packets",
    "sync_runs",
)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return 0


class CreateStudioStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except Exception:  # noqa: BLE001
            pass
        try:
            conn.execute("PRAGMA synchronous = NORMAL")
        except Exception:  # noqa: BLE001
            pass
        return conn

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
        return bool(row)

    def _read_meta(self, conn: sqlite3.Connection, key: str, default: str = "") -> str:
        if not self._table_exists(conn, "create_studio_meta"):
            return default
        row = conn.execute("SELECT value_text FROM create_studio_meta WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        return str(row["value_text"] or default)

    def _write_meta(self, conn: sqlite3.Connection, key: str, value: Any) -> None:
        conn.execute(
            """
            INSERT INTO create_studio_meta(key, value_text, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value_text = excluded.value_text,
              updated_at = excluded.updated_at
            """,
            (key, str(value), now_iso()),
        )

    def _touch(self, conn: sqlite3.Connection) -> None:
        self._write_meta(conn, "last_mutation_at", now_iso())

    def _ensure_initialized(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS create_studio_meta (
              key TEXT PRIMARY KEY,
              value_text TEXT,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS content_objects (
              object_uid TEXT PRIMARY KEY,
              source_kind TEXT NOT NULL,
              platform TEXT,
              source_ref TEXT,
              canonical_url TEXT,
              title TEXT,
              summary TEXT,
              body_text TEXT,
              body_ready INTEGER NOT NULL DEFAULT 0,
              published_at TEXT,
              source_name TEXT,
              tags_json TEXT NOT NULL DEFAULT '[]',
              related_topics_json TEXT NOT NULL DEFAULT '[]',
              metadata_json TEXT NOT NULL DEFAULT '{}',
              origin_path TEXT,
              content_hash TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS content_chunks (
              chunk_id TEXT PRIMARY KEY,
              object_uid TEXT NOT NULL,
              chunk_index INTEGER NOT NULL,
              chunk_text TEXT NOT NULL,
              token_estimate INTEGER NOT NULL DEFAULT 0,
              embedding_provider TEXT,
              embedding_model TEXT,
              embedding_vector_json TEXT,
              metadata_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              UNIQUE(object_uid, chunk_index),
              FOREIGN KEY(object_uid) REFERENCES content_objects(object_uid) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_packets (
              packet_id TEXT PRIMARY KEY,
              topic TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'draft',
              query_text TEXT,
              packet_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_packets (
              packet_id TEXT PRIMARY KEY,
              event_key TEXT NOT NULL,
              title TEXT,
              status TEXT NOT NULL DEFAULT 'draft',
              packet_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_runs (
              run_id TEXT PRIMARY KEY,
              source_name TEXT NOT NULL,
              phase TEXT NOT NULL,
              status TEXT NOT NULL,
              started_at TEXT NOT NULL,
              finished_at TEXT,
              metrics_json TEXT NOT NULL DEFAULT '{}',
              error_text TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_content_objects_published_at ON content_objects(published_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_content_objects_platform ON content_objects(platform)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_content_objects_body_ready ON content_objects(body_ready)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_content_chunks_object_uid ON content_chunks(object_uid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_packets_updated_at ON topic_packets(updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_packets_updated_at ON event_packets(updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(started_at)")
        if not self._read_meta(conn, "initialized_at"):
            self._write_meta(conn, "initialized_at", now_iso())
        self._write_meta(conn, "schema_version", SCHEMA_VERSION)
        self._write_meta(conn, "schema_updated_at", now_iso())

    def initialize(self) -> dict[str, Any]:
        with self._connect() as conn:
            self._ensure_initialized(conn)
            self._touch(conn)
            conn.commit()
        return self.get_status()

    def get_meta_value(self, key: str, default: str = "") -> str:
        normalized_key = str(key or "").strip()
        if not normalized_key:
            return default
        with self._connect() as conn:
            self._ensure_initialized(conn)
            return self._read_meta(conn, normalized_key, default)

    def set_meta_value(self, key: str, value: Any) -> str:
        normalized_key = str(key or "").strip()
        if not normalized_key:
            raise ValueError("key is required")
        with self._connect() as conn:
            self._ensure_initialized(conn)
            self._write_meta(conn, normalized_key, value)
            self._touch(conn)
            conn.commit()
        return str(value)

    def get_status(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": True,
            "db_path": str(self.db_path),
            "db_exists": self.db_path.exists(),
            "initialized": False,
            "schema_version": 0,
            "required_tables": list(REQUIRED_TABLES),
            "missing_tables": list(REQUIRED_TABLES),
            "table_counts": {},
            "file_size_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0,
            "file_size_human": self._format_size(self.db_path.stat().st_size if self.db_path.exists() else 0),
            "meta": {},
            "latest_sync_run": None,
        }
        if not self.db_path.exists():
            return payload

        with self._connect() as conn:
            existing_tables = {
                str(row["name"])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
            }
            missing_tables = [table for table in REQUIRED_TABLES if table not in existing_tables]
            payload["missing_tables"] = missing_tables
            payload["initialized"] = not missing_tables
            if "create_studio_meta" in existing_tables:
                payload["schema_version"] = _safe_int(self._read_meta(conn, "schema_version", "0"))
                payload["meta"] = {
                    "initialized_at": self._read_meta(conn, "initialized_at", ""),
                    "schema_updated_at": self._read_meta(conn, "schema_updated_at", ""),
                    "last_mutation_at": self._read_meta(conn, "last_mutation_at", ""),
                }
            table_counts: dict[str, int] = {}
            for table_name in REQUIRED_TABLES:
                if table_name not in existing_tables:
                    continue
                row = conn.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}").fetchone()
                table_counts[table_name] = _safe_int(row["row_count"] if row else 0)
            payload["table_counts"] = table_counts
            if "sync_runs" in existing_tables:
                row = conn.execute(
                    """
                    SELECT run_id, source_name, phase, status, started_at, finished_at, metrics_json, error_text
                    FROM sync_runs
                    ORDER BY started_at DESC, run_id DESC
                    LIMIT 1
                    """
                ).fetchone()
                if row:
                    payload["latest_sync_run"] = {
                        "run_id": str(row["run_id"] or ""),
                        "source_name": str(row["source_name"] or ""),
                        "phase": str(row["phase"] or ""),
                        "status": str(row["status"] or ""),
                        "started_at": str(row["started_at"] or ""),
                        "finished_at": str(row["finished_at"] or ""),
                        "metrics": self._parse_json(row["metrics_json"], default={}),
                        "error_text": str(row["error_text"] or ""),
                    }
            payload["index_sync_summary"] = self._parse_json(
                self._read_meta(conn, "last_index_sync_summary_json", ""),
                default={},
            )
        return payload

    def upsert_content_object(self, payload: dict[str, Any]) -> dict[str, Any]:
        object_uid = str(payload.get("object_uid") or "").strip()
        if not object_uid:
            raise ValueError("object_uid is required")
        timestamp = now_iso()
        record = {
            "object_uid": object_uid,
            "source_kind": str(payload.get("source_kind") or "").strip() or "unknown",
            "platform": str(payload.get("platform") or "").strip(),
            "source_ref": str(payload.get("source_ref") or "").strip(),
            "canonical_url": str(payload.get("canonical_url") or "").strip(),
            "title": str(payload.get("title") or "").strip(),
            "summary": str(payload.get("summary") or "").strip(),
            "body_text": str(payload.get("body_text") or "").strip(),
            "body_ready": 1 if payload.get("body_ready") else 0,
            "published_at": str(payload.get("published_at") or "").strip(),
            "source_name": str(payload.get("source_name") or "").strip(),
            "tags_json": _json_dumps(payload.get("tags") or []),
            "related_topics_json": _json_dumps(payload.get("related_topics") or []),
            "metadata_json": _json_dumps(payload.get("metadata") or {}),
            "origin_path": str(payload.get("origin_path") or "").strip(),
            "content_hash": str(payload.get("content_hash") or "").strip(),
            "created_at": str(payload.get("created_at") or "").strip() or timestamp,
            "updated_at": timestamp,
        }
        with self._connect() as conn:
            self._ensure_initialized(conn)
            conn.execute(
                """
                INSERT INTO content_objects(
                  object_uid, source_kind, platform, source_ref, canonical_url, title, summary,
                  body_text, body_ready, published_at, source_name, tags_json, related_topics_json,
                  metadata_json, origin_path, content_hash, created_at, updated_at
                )
                VALUES(
                  :object_uid, :source_kind, :platform, :source_ref, :canonical_url, :title, :summary,
                  :body_text, :body_ready, :published_at, :source_name, :tags_json, :related_topics_json,
                  :metadata_json, :origin_path, :content_hash, :created_at, :updated_at
                )
                ON CONFLICT(object_uid) DO UPDATE SET
                  source_kind = excluded.source_kind,
                  platform = excluded.platform,
                  source_ref = excluded.source_ref,
                  canonical_url = excluded.canonical_url,
                  title = excluded.title,
                  summary = excluded.summary,
                  body_text = excluded.body_text,
                  body_ready = excluded.body_ready,
                  published_at = excluded.published_at,
                  source_name = excluded.source_name,
                  tags_json = excluded.tags_json,
                  related_topics_json = excluded.related_topics_json,
                  metadata_json = excluded.metadata_json,
                  origin_path = excluded.origin_path,
                  content_hash = excluded.content_hash,
                  updated_at = excluded.updated_at
                """,
                record,
            )
            self._touch(conn)
            conn.commit()
        return record

    def replace_content_chunks(self, object_uid: str, chunks: list[dict[str, Any]]) -> dict[str, Any]:
        normalized_object_uid = str(object_uid or "").strip()
        if not normalized_object_uid:
            raise ValueError("object_uid is required")
        timestamp = now_iso()
        with self._connect() as conn:
            self._ensure_initialized(conn)
            conn.execute("DELETE FROM content_chunks WHERE object_uid = ?", (normalized_object_uid,))
            for index, chunk in enumerate(chunks):
                chunk_id = str(chunk.get("chunk_id") or f"{normalized_object_uid}::chunk::{index}").strip()
                conn.execute(
                    """
                    INSERT INTO content_chunks(
                      chunk_id, object_uid, chunk_index, chunk_text, token_estimate, embedding_provider,
                      embedding_model, embedding_vector_json, metadata_json, created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chunk_id,
                        normalized_object_uid,
                        index,
                        str(chunk.get("chunk_text") or "").strip(),
                        _safe_int(chunk.get("token_estimate") or 0),
                        str(chunk.get("embedding_provider") or "").strip(),
                        str(chunk.get("embedding_model") or "").strip(),
                        _json_dumps(chunk.get("embedding_vector") or []),
                        _json_dumps(chunk.get("metadata") or {}),
                        str(chunk.get("created_at") or "").strip() or timestamp,
                        timestamp,
                    ),
                )
            self._touch(conn)
            conn.commit()
        return {"object_uid": normalized_object_uid, "chunk_count": len(chunks), "updated_at": timestamp}

    def save_topic_packet(self, payload: dict[str, Any]) -> dict[str, Any]:
        packet_id = str(payload.get("packet_id") or f"TP-{uuid.uuid4().hex[:10]}").strip()
        topic = str(payload.get("topic") or "").strip()
        if not topic:
            raise ValueError("topic is required")
        timestamp = now_iso()
        record = {
            "packet_id": packet_id,
            "topic": topic,
            "status": str(payload.get("status") or "draft").strip() or "draft",
            "query_text": str(payload.get("query_text") or "").strip(),
            "packet_json": _json_dumps(payload.get("packet") or payload),
            "created_at": str(payload.get("created_at") or "").strip() or timestamp,
            "updated_at": timestamp,
        }
        with self._connect() as conn:
            self._ensure_initialized(conn)
            conn.execute(
                """
                INSERT INTO topic_packets(packet_id, topic, status, query_text, packet_json, created_at, updated_at)
                VALUES(:packet_id, :topic, :status, :query_text, :packet_json, :created_at, :updated_at)
                ON CONFLICT(packet_id) DO UPDATE SET
                  topic = excluded.topic,
                  status = excluded.status,
                  query_text = excluded.query_text,
                  packet_json = excluded.packet_json,
                  updated_at = excluded.updated_at
                """,
                record,
            )
            self._touch(conn)
            conn.commit()
        return record

    def save_event_packet(self, payload: dict[str, Any]) -> dict[str, Any]:
        packet_id = str(payload.get("packet_id") or f"EP-{uuid.uuid4().hex[:10]}").strip()
        event_key = str(payload.get("event_key") or packet_id).strip()
        timestamp = now_iso()
        record = {
            "packet_id": packet_id,
            "event_key": event_key,
            "title": str(payload.get("title") or "").strip(),
            "status": str(payload.get("status") or "draft").strip() or "draft",
            "packet_json": _json_dumps(payload.get("packet") or payload),
            "created_at": str(payload.get("created_at") or "").strip() or timestamp,
            "updated_at": timestamp,
        }
        with self._connect() as conn:
            self._ensure_initialized(conn)
            conn.execute(
                """
                INSERT INTO event_packets(packet_id, event_key, title, status, packet_json, created_at, updated_at)
                VALUES(:packet_id, :event_key, :title, :status, :packet_json, :created_at, :updated_at)
                ON CONFLICT(packet_id) DO UPDATE SET
                  event_key = excluded.event_key,
                  title = excluded.title,
                  status = excluded.status,
                  packet_json = excluded.packet_json,
                  updated_at = excluded.updated_at
                """,
                record,
            )
            self._touch(conn)
            conn.commit()
        return record

    def start_sync_run(self, source_name: str, phase: str, metrics: dict[str, Any] | None = None) -> dict[str, Any]:
        run_id = f"CSR-{uuid.uuid4().hex[:10]}"
        record = {
            "run_id": run_id,
            "source_name": str(source_name or "").strip(),
            "phase": str(phase or "").strip(),
            "status": "running",
            "started_at": now_iso(),
            "finished_at": "",
            "metrics_json": _json_dumps(metrics or {}),
            "error_text": "",
        }
        with self._connect() as conn:
            self._ensure_initialized(conn)
            conn.execute(
                """
                INSERT INTO sync_runs(run_id, source_name, phase, status, started_at, finished_at, metrics_json, error_text)
                VALUES(:run_id, :source_name, :phase, :status, :started_at, :finished_at, :metrics_json, :error_text)
                """,
                record,
            )
            self._touch(conn)
            conn.commit()
        return {
            "run_id": run_id,
            "source_name": record["source_name"],
            "phase": record["phase"],
            "status": record["status"],
            "started_at": record["started_at"],
        }

    def finish_sync_run(
        self,
        run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        error_text: str = "",
    ) -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            raise ValueError("run_id is required")
        finished_at = now_iso()
        with self._connect() as conn:
            self._ensure_initialized(conn)
            conn.execute(
                """
                UPDATE sync_runs
                SET status = ?, finished_at = ?, metrics_json = ?, error_text = ?
                WHERE run_id = ?
                """,
                (
                    str(status or "").strip() or "completed",
                    finished_at,
                    _json_dumps(metrics or {}),
                    str(error_text or "").strip(),
                    normalized_run_id,
                ),
            )
            self._touch(conn)
            conn.commit()
        return {
            "run_id": normalized_run_id,
            "status": str(status or "").strip() or "completed",
            "finished_at": finished_at,
            "metrics": metrics or {},
            "error_text": str(error_text or "").strip(),
        }

    def _parse_json(self, raw: Any, default: Any) -> Any:
        text = str(raw or "").strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except Exception:  # noqa: BLE001
            return default

    def _format_size(self, size_bytes: int) -> str:
        size = float(max(0, size_bytes))
        units = ("B", "KB", "MB", "GB")
        unit = units[0]
        for unit in units:
            if size < 1024 or unit == units[-1]:
                break
            size /= 1024
        if unit == "B":
            return f"{int(size)} {unit}"
        return f"{size:.1f} {unit}"
