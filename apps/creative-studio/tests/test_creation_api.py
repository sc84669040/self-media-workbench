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
    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_PROVIDER", "mock")
    monkeypatch.setenv("CREATE_STUDIO_TRANSLATION_CACHE_DB_PATH", str(tmp_path / "translation_cache.db"))
    monkeypatch.setenv("CREATE_STUDIO_AUTOFILL_PROVIDER", "mock")
    monkeypatch.setenv("CREATE_STUDIO_WRITING_PROVIDER", "mock")
    import search_dashboard  # noqa: WPS433

    dashboard = importlib.reload(search_dashboard)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield base_url
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


def _create_valid_task(base_url: str) -> dict:
    status, payload = _request(
        base_url,
        "POST",
        "/api/create/tasks",
        {
            "trigger_type": "manual_topic",
            "topic": "AI 编排层为什么比单个模型更值钱",
            "platform": "wechat",
            "audience": "AI 产品从业者",
            "goal": "输出一篇可直接进入写作的长文任务单",
            "style_notes": ["有判断", "少空话"],
            "banned_patterns": ["正确的废话"],
        },
    )
    assert status == 200
    return payload



def _run_retrieval_and_mark_primary(base_url: str, task_id: str, batch_id: str, source_id: str) -> None:
    import search_dashboard  # noqa: WPS433

    service = search_dashboard.get_creation_workspace_service()
    batch = service.get_retrieval_batch(task_id)
    assert batch is not None

    results = [
        {
            "source_id": "RAW-001",
            "title": "夜鹰正文抓取策略调整",
            "url": "https://example.com/raw-001",
            "source": "NightHawk",
            "channel": "wechat",
            "published_at": "2026-04-19T09:00:00+08:00",
            "summary": "metadata-only 主链路与异步补正文",
            "why_pick": "解释为何 /create 可能拿到未补全正文",
            "bucket": "raw_articles",
            "content_type": "raw_article",
            "text": "夜鹰已经改成 metadata-only，正文需要后续 worker 补全。",
            "recommend_full_fetch": "",
            "matched_handle": "",
            "author": "NightHawk",
            "created_at": "2026-04-19T09:00:00+08:00",
            "lang": "zh",
            "metrics": {},
            "raw": {
                "body_fetch_ok": True,
                "content": "夜鹰已经改成 metadata-only，正文需要后续 worker 补全。",
                "source_kind": "official_feed",
            },
            "decision": "keep" if source_id == "RAW-001" else "exclude",
            "decision_reason": "核心证据" if source_id == "RAW-001" else "对照材料",
            "classification": "primary" if source_id == "RAW-001" else "secondary",
            "classification_reason": "主资料" if source_id == "RAW-001" else "辅助资料",
        },
        {
            "source_id": "ANA-001",
            "title": "创作工作台现状分析",
            "url": "https://example.com/ana-001",
            "source": "Hermes",
            "channel": "analysis",
            "published_at": "2026-04-19T10:00:00+08:00",
            "summary": "需要在引用前增加正文就绪 gate",
            "why_pick": "能当主资料测试空正文场景",
            "bucket": "analysis_cards",
            "content_type": "analysis_card",
            "text": "创作工作台已经具备检索、引用和大纲链路。",
            "recommend_full_fetch": "",
            "matched_handle": "",
            "author": "Hermes",
            "created_at": "2026-04-19T10:00:00+08:00",
            "lang": "zh",
            "metrics": {},
            "raw": {},
            "decision": "keep" if source_id == "ANA-001" else "exclude",
            "decision_reason": "核心证据" if source_id == "ANA-001" else "对照材料",
            "classification": "primary" if source_id == "ANA-001" else "secondary",
            "classification_reason": "主资料" if source_id == "ANA-001" else "辅助资料",
        },
    ]

    updated_batch = dict(batch)
    updated_batch["status"] = "retrieved"
    updated_batch["results"] = results
    service._save_retrieval_batch(updated_batch)



def _patch_retrieval_result(task_id: str, source_id: str, updates: dict) -> dict:
    import search_dashboard  # noqa: WPS433

    service = search_dashboard.get_creation_workspace_service()
    batch = service.get_retrieval_batch(task_id)
    assert batch is not None

    current = next(item for item in list(batch.get("results") or []) if item.get("source_id") == source_id)
    normalized_updates = dict(updates)
    raw_updates = normalized_updates.pop("raw", None)
    if raw_updates is not None:
        merged_raw = dict(current.get("raw") or {})
        merged_raw.update(dict(raw_updates))
        normalized_updates["raw"] = merged_raw
    return service._update_result_in_batch(task_id, batch["id"], source_id, normalized_updates)


def test_creation_page_is_served_as_independent_entry(creation_api_server):
    req = Request(f"{creation_api_server}/create.html", method="GET")

    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "Creation Workspace" in body
    assert "target-list" in body
    assert "material-list" in body
    assert "result-panel" in body
    assert "generate-packet-btn" in body
    assert "/api/create/tasks" in body
    assert "/api/create/targets" in body
    assert "/create/nighthawk" in body
    assert "/create/topic" in body


def test_create_root_now_serves_studio_front(creation_api_server):
    req = Request(f"{creation_api_server}/create", method="GET")

    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "创作前台" in body
    assert "Create Studio" in body


def test_studio_front_page_is_served_as_unified_entry(creation_api_server):
    req = Request(f"{creation_api_server}/create/studio", method="GET")

    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "创作前台" in body
    assert "Create Studio" in body
    assert "entries-three" in body
    assert "status-bar" in body
    assert "/api/create/config" in body
    assert "/api/create/nighthawk/profile" in body
    assert "/create/topic" in body
    assert "/create/nighthawk" in body
    assert "/create/topic" in body


def test_creation_workspace_alias_is_served(creation_api_server):
    req = Request(f"{creation_api_server}/create/workspace", method="GET")

    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "Creation Workspace" in body
    assert "generate-packet-btn" in body


def test_creation_targets_api_returns_configurable_targets(creation_api_server):
    status, payload = _request(creation_api_server, "GET", "/api/create/targets")

    assert status == 200
    assert payload["ok"] is True
    assert payload["default_target"]
    target_ids = {item["id"] for item in payload["targets"]}
    assert "khazix-wechat" in target_ids
    assert "voiceover-script" in target_ids


def test_topic_search_page_is_served_as_independent_entry(creation_api_server):
    req = Request(f"{creation_api_server}/create/topic", method="GET")

    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "主题搜索台" in body
    assert "topic-search-form" in body
    assert "/api/create/topic-search" in body
    assert "/api/create/topic-packets" in body
    assert "/api/topics?sort=overall&limit=4&page=1" not in body
    assert "主动主题搜索" in body
    assert "主题池筛选" in body
    assert "已有主题池" in body
    assert "detail-modal" in body
    assert "search-progress" in body
    assert "progress-stages" in body
    assert "check-topic-sync-btn" in body
    assert "/api/topics/sync-status" in body
    assert "Topic Packet" in body
    assert "/create/nighthawk" in body
    assert "/create" in body
    assert "/create/workspace" in body


def test_topic_detail_can_create_packet_and_enter_workspace(creation_api_server, monkeypatch, tmp_path):
    import search_dashboard  # noqa: WPS433

    tmp_index_db = tmp_path / "create_studio.db"

    monkeypatch.setattr(
        search_dashboard,
        "get_create_studio_config",
        lambda: {
            "indexing": {"content_index_db_path": str(tmp_index_db)},
            "creation_workspace": {"data_root": str(tmp_path / "creation")},
        },
    )
    monkeypatch.setattr(
        search_dashboard,
        "get_topic_detail",
        lambda topic_id, event_page=1, event_limit=10, article_page=1, article_limit=8: {
            "ok": True,
            "topic": {
                "id": int(topic_id),
                "title": "Claude Code 使用方法",
                "topic_key": "claude-code-how-to",
                "summary": "这是一个适合直接进入创作的主题。",
                "status": "ready",
                "topic_type": "article_theme_v2",
                "last_seen_at": "2026-04-23 10:00:00",
                "evidence_article_count": 2,
                "evidence_source_count": 2,
                "evidence_platform_count": 2,
            },
            "articles": [
                {
                    "raw_item_id": 101,
                    "title": "Claude Code 最新实测",
                    "content_excerpt": "一篇很适合进入创作的正文。",
                    "body_text": "一篇很适合进入创作的正文，包含足够多的主资料信息。",
                    "platform": "wechat",
                    "published_at": "2026-04-23 09:00:00",
                    "body_status": "success",
                    "url": "https://example.com/claude-code-101",
                },
                {
                    "raw_item_id": 102,
                    "title": "Claude Code 工作流整理",
                    "content_excerpt": "另一篇辅助材料。",
                    "body_text": "另一篇辅助材料，适合作为次级证据。",
                    "platform": "x",
                    "published_at": "2026-04-23 08:00:00",
                    "body_status": "success",
                    "url": "https://example.com/claude-code-102",
                },
            ],
        },
    )

    packet_status, packet_payload = _request(creation_api_server, "POST", "/api/topics/321/packet", {})
    assert packet_status == 200
    assert packet_payload["ok"] is True
    assert packet_payload["topic_packet"]["topic"] == "Claude Code 使用方法"
    assert len(packet_payload["topic_packet"]["articles"]) == 2

    task_status, task_payload = _request(creation_api_server, "POST", "/api/topics/321/to-task", {})
    assert task_status == 200
    assert task_payload["ok"] is True
    assert task_payload["creation_task"]["trigger_type"] == "topic_packet"
    assert task_payload["packet_type"] == "topic"
    assert "/create/workspace?" in task_payload["next_url"]
    assert "task_id=" in task_payload["next_url"]


def test_packet_detail_page_is_served_as_independent_entry(creation_api_server):
    req = Request(f"{creation_api_server}/create/packets/TP-DEMO-001", method="GET")

    with urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        content_type = resp.headers.get_content_type()

    assert resp.status == 200
    assert content_type == "text/html"
    assert "资料包详情页" in body
    assert "/api/create/topic-packets/" in body
    assert "packet-to-task-btn" in body
    assert "/api/create/packets/" in body
    assert "资料包标题与摘要" in body
    assert "文章列表与正文展开" in body
    assert "导出 JSON / Markdown" in body
    assert "进入后续编排" in body
    assert "/create/nighthawk" in body
    assert "/create/studio" in body
    assert "/create/workspace" in body


def test_create_config_api_returns_effective_config_with_env_override(creation_api_server):
    base_url = creation_api_server
    status, payload = _request(base_url, "GET", "/api/create/config")

    assert status == 200
    assert payload["ok"] is True
    assert payload["creation_workspace"]["data_root"].endswith("creation")
    assert payload["config_path"].endswith("config\\local\\local.yaml") or payload["config_path"].endswith("config/local/local.yaml")
    assert payload["config_file_loaded"] is False
    assert "CONTENT_SEARCH_CREATION_DATA_ROOT" in payload["env_overrides"]
    assert payload["database_sources"]["nighthawk_db_path"].endswith("event_radar.db")
    assert payload["semantic_search"]["enable_fts"] is True
    assert payload["event_clustering"]["mode"] == "semi_auto"


def test_create_writers_api_returns_registry_summary(creation_api_server):
    status, payload = _request(creation_api_server, "GET", "/api/create/writers")

    assert status == 200
    assert payload["ok"] is True
    assert payload["default_writer"] == "generic-longform"
    assert len(payload["writers"]) >= 2
    assert {item["skill"] for item in payload["writers"]} >= {"khazix-writer", "generic-longform"}


def test_creation_api_can_create_list_and_get_bundle(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]

    list_status, listed = _request(creation_api_server, "GET", "/api/create/tasks")
    bundle_status, bundle = _request(creation_api_server, "GET", f"/api/create/tasks/{task_id}")

    assert list_status == 200
    assert listed["count"] == 1
    assert listed["items"][0]["id"] == task_id
    assert bundle_status == 200
    assert bundle["task"]["id"] == task_id
    assert bundle["retrieval_batch"]["id"].startswith("RB-")
    assert bundle["outline_packet"]["id"].startswith("OP-")


def test_creation_api_blocks_status_progress_when_gate_1_not_ready(creation_api_server):
    status, created = _request(
        creation_api_server,
        "POST",
        "/api/create/tasks",
        {
            "trigger_type": "manual_topic",
            "topic": "只有主题，没有目标",
            "platform": "wechat",
            "audience": "公众号读者",
            "goal": "",
        },
    )
    assert status == 200
    task_id = created["task"]["id"]

    update_status, payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}",
        {"status": "retrieval_ready"},
    )

    assert update_status == 400
    assert payload["ok"] is False
    assert "Gate 1" in payload["message"]



def test_generate_citations_blocks_primary_source_when_body_not_ready(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _run_retrieval_and_mark_primary(creation_api_server, task_id, batch_id, "RAW-001")
    _patch_retrieval_result(
        task_id,
        "RAW-001",
        {
            "text": "这只是元数据摘要，不是可引用正文。",
            "recommend_full_fetch": "yes",
            "raw": {
                "body_fetch_ok": False,
                "content": "",
                "source_kind": "official_feed",
            },
        },
    )

    status, payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/citations/generate",
        {},
    )

    assert status == 400
    assert payload["ok"] is False
    assert "Gate 3" in payload["message"]
    assert "正文未就绪" in payload["message"]



def test_generate_citations_blocks_primary_source_when_body_is_empty(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _run_retrieval_and_mark_primary(creation_api_server, task_id, batch_id, "ANA-001")
    _patch_retrieval_result(
        task_id,
        "ANA-001",
        {
            "text": "",
            "summary": "",
        },
    )

    status, payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/citations/generate",
        {},
    )

    assert status == 400
    assert payload["ok"] is False
    assert "Gate 3" in payload["message"]
    assert "缺少可引用正文" in payload["message"]



def test_generate_citations_blocks_primary_source_when_body_too_short_but_still_needs_full_fetch(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _run_retrieval_and_mark_primary(creation_api_server, task_id, batch_id, "RAW-001")
    _patch_retrieval_result(
        task_id,
        "RAW-001",
        {
            "text": "太短了",
            "recommend_full_fetch": "yes",
            "raw": {
                "body_fetch_ok": True,
                "content": "太短了",
            },
        },
    )

    status, payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/citations/generate",
        {},
    )

    assert status == 400
    assert payload["ok"] is False
    assert "Gate 3" in payload["message"]
    assert "正文过短" in payload["message"]



def test_create_primary_source_fetch_task_when_body_not_ready(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _run_retrieval_and_mark_primary(creation_api_server, task_id, batch_id, "RAW-001")
    _patch_retrieval_result(
        task_id,
        "RAW-001",
        {
            "text": "这只是元数据摘要，不是可引用正文。",
            "recommend_full_fetch": "yes",
            "raw": {
                "body_fetch_ok": False,
                "content": "",
                "source_kind": "official_feed",
            },
        },
    )

    status, payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/citations/fetch-primary-body",
        {},
    )

    assert status == 200
    assert payload["ok"] is True
    assert payload["source_id"] == "RAW-001"
    assert payload["fetch_task"]["status"] in {"queued", "running"}
    assert payload["fetch_task"]["urls"] == ["https://example.com/raw-001"]


def test_create_fetch_task_for_specific_imported_material_when_body_not_ready(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _run_retrieval_and_mark_primary(creation_api_server, task_id, batch_id, "RAW-001")
    _patch_retrieval_result(
        task_id,
        "RAW-001",
        {
            "text": "",
            "summary": "只有摘要，还没有正文",
            "recommend_full_fetch": "yes",
            "raw": {
                "body_fetch_ok": False,
                "content": "",
                "source_kind": "official_feed",
            },
        },
    )

    status, payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/retrieval/{batch_id}/RAW-001/fetch-body",
        {},
    )

    assert status == 200
    assert payload["ok"] is True
    assert payload["source_id"] == "RAW-001"
    assert payload["fetch_task"]["status"] in {"queued", "running"}
    assert payload["fetch_task"]["urls"] == ["https://example.com/raw-001"]



def test_creation_api_can_generate_writer_job_and_push_status_forward(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]

    citation_status, citation_payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/citations",
        {
            "citations": [
                {
                    "citation_id": "C1",
                    "claim_type": "fact",
                    "source_id": "NOTE-001",
                    "usable_excerpt": "编排层能把检索、引用、结构和写作拆开。",
                    "normalized_claim": "创作工作流需要编排层，而不是把原料直接喂给 writer。",
                    "usage_scope": "must_use",
                    "confidence": "high",
                }
            ]
        },
    )
    outline_status, outline_payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/outline",
        {
            "core_judgement": "编排层比单点模型能力更稀缺。",
            "angle": "从生产流程解释价值",
            "content_template": "方法论型",
            "title_candidates": ["先做编排层，再谈爆款自动化"],
            "outline": [
                {
                    "section": "开场",
                    "goal": "先指出直喂 raw 的问题",
                    "citation_refs": ["C1"],
                }
            ],
        },
    )
    writer_status, writer_payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/writer-job/generate",
        {
            "primary_output": ["long_article"],
            "optional_followups": ["oral_adaptation"],
            "article_archetype": "方法论分享型",
        },
    )
    get_status, current_writer = _request(creation_api_server, "GET", f"/api/create/tasks/{task_id}/writer-job")
    packet_status, packet_payload = _request(creation_api_server, "GET", f"/api/create/tasks/{task_id}/writer-packet")
    submit_status, submitted = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/writer-job",
        {"status": "submitted"},
    )

    assert citation_status == 200
    assert citation_payload["citation_list"]["status"] == "ready"
    assert outline_status == 200
    assert outline_payload["outline_packet"]["status"] == "ready"
    assert writer_status == 200
    assert writer_payload["writer_job"]["status"] == "ready"
    assert get_status == 200
    assert current_writer["writer_job"]["id"] == writer_payload["writer_job"]["id"]
    assert packet_status == 200
    assert packet_payload["packet"]["task"]["id"] == task_id
    assert submit_status == 200
    assert submitted["writer_job"]["status"] == "submitted"


def test_creation_api_can_generate_edit_and_download_article_draft(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]
    _run_retrieval_and_mark_primary(creation_api_server, task_id, batch_id, "RAW-001")

    generate_status, generated = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/article/generate",
        {
            "creation_target_id": "khazix-wechat",
            "preferred_writer_skill": "khazix-writer",
            "writer_skill": "khazix-writer",
            "article_archetype": "方法论分享型",
            "angle": "编排层比单点模型能力更稀缺。",
            "opening_hook": "故事是这样的，我最近越来越觉得创作台最重要的不是写作按钮。",
            "topic_reason": "这个选题能解释为什么创作需要从检索走到编排。",
        },
    )
    draft_id = generated["article_draft"]["id"]
    get_status, current = _request(creation_api_server, "GET", f"/api/create/drafts/{draft_id}")
    save_status, saved = _request(
        creation_api_server,
        "POST",
        f"/api/create/drafts/{draft_id}",
        {"article_markdown": current["article_markdown"] + "\n\n补一行人工编辑。", "title": "编辑后的文章"},
    )
    req = Request(f"{creation_api_server}/api/create/drafts/{draft_id}/download?format=md", method="GET")
    with urlopen(req, timeout=5) as resp:
        download_status = resp.status
        downloaded = resp.read().decode("utf-8")

    assert generate_status == 200
    assert generated["next_url"].startswith("/create/write?draft_id=")
    assert generated["article_markdown"]
    assert generated["article_draft"]["quality_report"]["enabled"] is True
    assert get_status == 200
    assert current["article_draft"]["id"] == draft_id
    assert save_status == 200
    assert saved["article_draft"]["title"] == "编辑后的文章"
    assert download_status == 200
    assert "补一行人工编辑。" in downloaded


def test_creation_api_can_autofill_khazix_target_from_imported_materials(creation_api_server):
    created = _create_valid_task(creation_api_server)
    task_id = created["task"]["id"]
    batch_id = created["retrieval_batch"]["id"]

    _run_retrieval_and_mark_primary(creation_api_server, task_id, batch_id, "RAW-001")

    status, payload = _request(
        creation_api_server,
        "POST",
        f"/api/create/tasks/{task_id}/autofill",
        {
            "creation_target_id": "khazix-wechat",
        },
    )

    assert status == 200
    assert payload["ok"] is True
    assert payload["autofill"]["source"] == "mock"
    assert payload["autofill"]["target_id"] == "khazix-wechat"
    assert payload["autofill"]["material_count"] >= 1
    assert payload["autofill"]["fields"]["topic_reason"]
    assert payload["autofill"]["fields"]["angle"]
    assert payload["autofill"]["fields"]["opening_hook"]
    assert payload["bundle"]["task"]["metadata"]["creation_target_autofill"]["target_id"] == "khazix-wechat"
    assert payload["bundle"]["writer_jobs"][-1]["article_archetype"] == payload["autofill"]["fields"]["article_archetype"]
