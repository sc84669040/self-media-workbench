from __future__ import annotations

import importlib
import json
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
WEB_DIR = ROOT / "web"
for path in (SCRIPTS_DIR, WEB_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture()
def creation_api_server(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_SEARCH_CREATION_DATA_ROOT", str(tmp_path / "creation"))
    monkeypatch.setenv("CREATE_STUDIO_INDEX_DB_PATH", str(tmp_path / "create_studio.db"))
    monkeypatch.setenv("CREATE_STUDIO_ENABLE_FTS", "0")
    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)
    search_calls: list[dict] = []

    def fake_search_candidates(payload: dict) -> dict:
        normalized = dict(payload)
        search_calls.append(normalized)
        query = str(normalized.get("query") or "").strip() or "AI 编排层"
        return {
            "ok": True,
            "provider": "x",
            "providers": ["x"],
            "query": query,
            "mode": "keyword_search",
            "count": 4,
            "results": [
                {
                    "id": "ANA-001",
                    "title": "AI 编排层分析卡",
                    "url": "https://example.com/analysis-1",
                    "channel": "knowledge_base",
                    "source": "分析卡片库",
                    "published_at": "2026-04-01T10:00:00+00:00",
                    "summary": "解释为什么编排层比单模型调用更稳定。",
                    "why_pick": "直接回答主题判断",
                    "bucket": "strong",
                    "content_type": "analysis_card",
                    "text": "编排层让检索、引用、结构和写作解耦。",
                },
                {
                    "id": "RAW-001",
                    "title": "AI 工作流原文拆解",
                    "url": "https://example.com/raw-1",
                    "channel": "wechat",
                    "source": "原文资料",
                    "published_at": "2026-04-02T10:00:00+00:00",
                    "summary": "说明为什么原文应该作为辅助证据。",
                    "why_pick": "可作为补充例子",
                    "bucket": "watch",
                    "content_type": "raw_article",
                    "text": "原文提供案例，但不该替代结构化判断。",
                    "recommend_full_fetch": "yes",
                    "raw": {
                        "source_kind": "official_feed",
                        "body_fetch_ok": True,
                        "content": "这是一段已抓到的公众号正文。",
                    },
                },
                {
                    "id": "OTH-001",
                    "title": "无关样本",
                    "url": "https://example.com/other-1",
                    "channel": "x",
                    "source": "噪音来源",
                    "published_at": "2026-04-03T10:00:00+00:00",
                    "summary": "相关性偏低。",
                    "why_pick": "仅供对照",
                    "bucket": "skip",
                    "content_type": "other",
                    "text": "这条更多是背景噪音。",
                },
                {
                    "id": "YT-001",
                    "title": "AI 工作流视频拆解",
                    "url": "https://example.com/youtube-1",
                    "channel": "youtube",
                    "source": "YouTube 资料",
                    "published_at": "2026-04-04T10:00:00+00:00",
                    "summary": "字幕资料适合继续下钻。",
                    "why_pick": "能补强方法论案例",
                    "bucket": "strong",
                    "content_type": "transcript",
                    "recommend_full_fetch": "maybe",
                    "raw": {
                        "recommendation": "watch",
                        "confidence": "high",
                        "route_bucket": "youtube-watch",
                        "transcript_language": "zh-Hans",
                        "transcript_text": "这里是一段视频字幕正文。",
                        "transcript_excerpt": "这里是一段视频字幕摘要。",
                    },
                },
            ],
            "grouped": {"strong": [], "watch": [], "skip": []},
            "counts": {"strong": 2, "watch": 1, "skip": 1},
            "searched_handles": [],
            "success_handles": [],
            "filters": {},
            "errors": [],
            "error_count": 0,
            "observability": {},
            "per_provider": {"x": {"count": 4}},
            "schema_version": "candidate-pack-v1",
            "message": "",
        }

    monkeypatch.setattr(dashboard, "search_candidates", fake_search_candidates)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield base_url, search_calls
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def _request(base_url: str, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(f"{base_url}{path}", data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _create_valid_task(base_url: str, overrides: dict | None = None) -> dict:
    request_payload = {
        "trigger_type": "manual_topic",
        "topic": "AI 编排层为什么比单模型输出更值钱",
        "platform": "wechat",
        "audience": "AI 产品经理",
        "goal": "输出一篇可直接进入写作的长文任务单",
        "angle": "从流程稳定性切入",
        "style_notes": ["有判断", "少空话"],
        "banned_patterns": ["正确的废话"],
        "source_scope": ["analysis_cards", "raw_articles"],
    }
    if overrides:
        request_payload.update(overrides)
    status, payload = _request(
        base_url,
        "POST",
        "/api/create/tasks",
        request_payload,
    )
    assert status == 200
    return payload


def test_creation_api_can_run_retrieval_and_persist_manual_screening(creation_api_server):
    base_url, search_calls = creation_api_server
    created = _create_valid_task(base_url, overrides={"source_scope": []})
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    run_status, run_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/run",
        {
            "query_terms": ["AI 编排层", "工作流稳定性"],
            "filters": {"limit": 5, "sort_by": "time"},
        },
    )
    get_status, retrieval_payload = _request(base_url, "GET", f"/api/create/tasks/{task_id}/retrieval")
    keep_status, keep_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/keep",
        {"source_id": "ANA-001", "reason": "作为主判断依据"},
    )
    classify_status, classify_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/classify",
        {"source_id": "ANA-001", "classification": "primary", "reason": "最贴近主题判断"},
    )
    exclude_status, exclude_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/exclude",
        {"source_id": "OTH-001", "reason": "相关性太弱"},
    )

    assert run_status == 200
    assert run_payload["retrieval_batch"]["status"] == "retrieved"
    assert run_payload["retrieval_batch"]["query_terms"] == ["AI 编排层", "工作流稳定性"]
    assert run_payload["retrieval_batch"]["results"][0]["source_id"] == "ANA-001"
    assert run_payload["gates"]["gate_2_retrieval_ready"]["pass"] is False
    assert get_status == 200
    assert retrieval_payload["retrieval_batch"]["results"][1]["content_type"] == "raw_article"
    assert keep_status == 200
    assert keep_payload["retrieval_batch"]["results"][0]["decision"] == "keep"
    assert keep_payload["gates"]["gate_2_retrieval_ready"]["pass"] is False
    assert classify_status == 200
    assert classify_payload["retrieval_batch"]["results"][0]["classification"] == "primary"
    assert exclude_status == 200
    assert exclude_payload["retrieval_batch"]["results"][2]["decision"] == "exclude"
    assert exclude_payload["gates"]["gate_2_retrieval_ready"]["pass"] is True
    assert search_calls[0]["query"] == "AI 编排层 工作流稳定性"


def test_creation_retrieval_uses_task_platform_and_source_scope_constraints(creation_api_server):
    base_url, search_calls = creation_api_server
    created = _create_valid_task(
        base_url,
        overrides={
            "platform": "wechat",
            "source_scope": ["analysis_cards"],
        },
    )
    task_id = created["task"]["id"]

    run_status, run_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/run",
        {
            "query_terms": ["AI 编排层", "工作流稳定性"],
            "filters": {"limit": 5, "sort_by": "time"},
        },
    )

    assert run_status == 200
    assert search_calls[0]["provider"] == "wechat"
    assert search_calls[0]["providers"] == ["wechat"]
    assert search_calls[0]["source_scope"] == ["analysis_cards"]
    assert search_calls[0]["filters"]["source_scope"] == ["analysis_cards"]
    assert [item["content_type"] for item in run_payload["retrieval_batch"]["results"]] == ["analysis_card"]


def test_creation_retrieval_preserves_nighthawk_p1_signals(creation_api_server):
    base_url, _ = creation_api_server
    created = _create_valid_task(base_url, overrides={"source_scope": []})
    task_id = created["task"]["id"]

    run_status, run_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/run",
        {
            "query_terms": ["AI 编排层", "工作流稳定性"],
            "filters": {"limit": 6, "sort_by": "time"},
        },
    )

    assert run_status == 200
    results = {item["source_id"]: item for item in run_payload["retrieval_batch"]["results"]}
    assert results["RAW-001"]["recommend_full_fetch"] == "yes"
    assert results["RAW-001"]["raw"]["source_kind"] == "official_feed"
    assert results["RAW-001"]["raw"]["body_fetch_ok"] is True
    assert results["RAW-001"]["raw"]["content"] == "这是一段已抓到的公众号正文。"
    assert results["RAW-001"]["text"] == "原文提供案例，但不该替代结构化判断。"
    assert results["YT-001"]["recommend_full_fetch"] == "maybe"
    assert results["YT-001"]["raw"]["recommendation"] == "watch"
    assert results["YT-001"]["raw"]["confidence"] == "high"
    assert results["YT-001"]["raw"]["route_bucket"] == "youtube-watch"
    assert results["YT-001"]["raw"]["transcript_language"] == "zh-Hans"
    assert results["YT-001"]["raw"]["transcript_text"] == "这里是一段视频字幕正文。"
    assert results["YT-001"]["text"] == "这里是一段视频字幕正文。"


def test_creation_retrieval_gate_requires_citable_primary_body(creation_api_server):
    base_url, _ = creation_api_server
    created = _create_valid_task(base_url, overrides={"source_scope": []})
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/run",
        {"query_terms": ["AI 编排层"], "filters": {"limit": 5}},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/keep",
        {"source_id": "RAW-001", "reason": "先保留原文"},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/exclude",
        {"source_id": "ANA-001", "reason": "保留一个剔除样本"},
    )
    classify_status, classify_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/classify",
        {"source_id": "RAW-001", "classification": "primary", "reason": "尝试拿原文做主资料"},
    )

    assert classify_status == 200
    assert classify_payload["gates"]["gate_2_retrieval_ready"]["pass"] is True

    import creation_service  # noqa: WPS433

    service = creation_service.CreationWorkspaceService()
    retrieval_batch = service.get_retrieval_batch(task_id)
    raw_item = next(item for item in retrieval_batch["results"] if item["source_id"] == "RAW-001")
    raw_item["raw"]["body_fetch_ok"] = False
    raw_item["raw"]["content"] = ""
    service.store.save("retrieval_batches", retrieval_batch)

    get_status, retrieval_payload = _request(base_url, "GET", f"/api/create/tasks/{task_id}/retrieval")

    assert get_status == 200
    assert retrieval_payload["gates"]["gate_2_retrieval_ready"]["pass"] is False
    assert "缺少可生成引用的主资料" in "；".join(retrieval_payload["gates"]["gate_2_retrieval_ready"]["reasons"])
    assert "正文未就绪" in "；".join(retrieval_payload["gates"]["gate_2_retrieval_ready"]["reasons"])


def test_creation_api_can_generate_and_edit_citations_from_primary_sources(creation_api_server):
    base_url, _ = creation_api_server
    created = _create_valid_task(base_url)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/run",
        {"query_terms": ["AI 编排层"], "filters": {"limit": 5}},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/keep",
        {"source_id": "ANA-001", "reason": "核心证据"},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/classify",
        {"source_id": "ANA-001", "classification": "primary", "reason": "主资料"},
    )

    generate_status, generate_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/citations/generate",
        {},
    )
    citation_id = generate_payload["citation_list"]["citations"][0]["citation_id"]
    edit_status, edit_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/citations/{citation_id}",
        {
            "normalized_claim": "创作编排层的核心价值，是把检索、证据和写作阶段拆开。",
            "usage_scope": "must_use",
        },
    )

    assert generate_status == 200
    assert generate_payload["citation_list"]["status"] == "ready"
    assert generate_payload["citation_list"]["citations"][0]["source_id"] == "ANA-001"
    assert generate_payload["citation_list"]["citations"][0]["claim_type"] == "fact"
    assert generate_payload["gates"]["gate_3_citation_ready"]["pass"] is True
    assert edit_status == 200
    assert edit_payload["citation_list"]["citations"][0]["normalized_claim"].startswith("创作编排层的核心价值")
    assert edit_payload["citation_list"]["citations"][0]["usage_scope"] == "must_use"


def test_creation_api_prefers_full_body_text_for_raw_primary_citations(creation_api_server):
    base_url, _ = creation_api_server
    created = _create_valid_task(base_url, overrides={"source_scope": []})
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/run",
        {"query_terms": ["AI 编排层"], "filters": {"limit": 5}},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/keep",
        {"source_id": "RAW-001", "reason": "正文已抓到"},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/classify",
        {"source_id": "RAW-001", "classification": "primary", "reason": "主资料"},
    )

    generate_status, generate_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/citations/generate",
        {},
    )

    assert generate_status == 200
    assert generate_payload["citation_list"]["citations"][0]["source_id"] == "RAW-001"
    assert generate_payload["citation_list"]["citations"][0]["quote_text"] == "这是一段已抓到的公众号正文。"


def test_creation_api_can_generate_outline_with_citation_refs_and_gate_4(creation_api_server):
    base_url, _ = creation_api_server
    created = _create_valid_task(base_url)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/run",
        {"query_terms": ["AI 编排层"], "filters": {"limit": 5}},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/keep",
        {"source_id": "ANA-001", "reason": "核心证据"},
    )
    _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/classify",
        {"source_id": "ANA-001", "classification": "primary", "reason": "主资料"},
    )
    _request(base_url, "POST", f"/api/create/tasks/{task_id}/citations/generate", {})

    outline_status, outline_payload = _request(
        base_url,
        "POST",
        f"/api/create/tasks/{task_id}/outline/generate",
        {
            "core_judgement": "AI 内容生产真正稀缺的不是模型，而是编排层。",
            "angle": "从流程稳定性切入",
            "content_template": "方法论型",
        },
    )

    assert outline_status == 200
    assert outline_payload["outline_packet"]["status"] == "ready"
    assert len(outline_payload["outline_packet"]["outline"]) >= 3
    assert outline_payload["outline_packet"]["outline"][0]["citation_refs"]
    assert outline_payload["gates"]["gate_4_outline_ready"]["pass"] is True
