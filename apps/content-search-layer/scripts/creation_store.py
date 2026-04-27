from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class CreationStore:
    def __init__(self, data_root: str | Path):
        self.data_root = Path(data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.writer_packets_dir = self.data_root / "writer_packets"
        self.writer_packets_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, object_type: str) -> Path:
        return self.data_root / f"{object_type}.jsonl"

    def save(self, object_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        path = self._path(object_type)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return payload

    def _load_latest_map(self, object_type: str) -> dict[str, dict[str, Any]]:
        path = self._path(object_type)
        latest: dict[str, dict[str, Any]] = {}
        if not path.exists():
            return latest
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            item = json.loads(text)
            item_id = str(item.get("id") or "").strip()
            if item_id:
                latest[item_id] = item
        return latest

    def get(self, object_type: str, item_id: str) -> dict[str, Any] | None:
        return self._load_latest_map(object_type).get(item_id)

    def list(self, object_type: str, limit: int = 20) -> list[dict[str, Any]]:
        indexed_items = list(enumerate(self._load_latest_map(object_type).values()))
        indexed_items.sort(
            key=lambda pair: (
                pair[1].get("updated_at") or pair[1].get("created_at") or "",
                pair[0],
            ),
            reverse=True,
        )
        return [item for _, item in indexed_items[:limit]]

    def find_by_task(self, object_type: str, task_id: str) -> list[dict[str, Any]]:
        indexed_items = [
            (idx, item)
            for idx, item in enumerate(self._load_latest_map(object_type).values())
            if str(item.get("creation_task_id") or "") == task_id
        ]
        indexed_items.sort(
            key=lambda pair: (
                pair[1].get("updated_at") or pair[1].get("created_at") or "",
                pair[0],
            ),
            reverse=True,
        )
        return [item for _, item in indexed_items]

    def latest_by_task(self, object_type: str, task_id: str) -> dict[str, Any] | None:
        items = self.find_by_task(object_type, task_id)
        return items[0] if items else None

    def write_writer_packet(self, writer_job_id: str, task_id: str, packet: dict[str, Any]) -> str:
        target_dir = self.writer_packets_dir / task_id
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{writer_job_id}.json"
        path.write_text(json.dumps(packet, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)
