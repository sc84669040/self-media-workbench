from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from creation_service import CreationWorkspaceService  # noqa: E402


def test_creation_retrieval_uses_topic_intent_when_query_terms_are_missing(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")
    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "Hermes 的价值",
            "platform": "wechat",
            "audience": "AI 从业者",
            "goal": "输出一篇选题资料包",
            "style_notes": ["有判断"],
            "banned_patterns": ["空话"],
        }
    )
    task_id = created["task"]["id"]
    captured: list[dict] = []

    def fake_search(payload: dict) -> dict:
        captured.append(dict(payload))
        return {"ok": True, "results": []}

    result = service.run_retrieval(task_id, {"filters": {"limit": 6}}, fake_search)

    assert result["ok"] is True
    assert captured
    assert captured[0]["topic_intent"]["normalized_topic"] == "Hermes 在现有内容生产链路中的价值"
    assert "重要性" in captured[0]["topic_intent"]["topic_facets"]
    assert "Hermes 重要性" in captured[0]["query_terms"]
    assert result["retrieval_batch"]["topic_intent"]["entities"] == ["Hermes"]
    assert "Hermes 创新点" in result["retrieval_batch"]["query_terms"]
