from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from creation_service import CreationWorkspaceService  # noqa: E402
from writer_adapter_registry import resolve_writer_profile  # noqa: E402


def test_create_task_builds_p0_scaffold(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")

    bundle = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "AI 编程助手产品化",
            "platform": "wechat",
            "audience": "想做 AI 产品的中文创作者",
            "goal": "产出一篇可直接交给 writer 的公众号长文任务单",
            "style_notes": ["有判断", "少空话"],
            "banned_patterns": ["假大空", "纯概念堆砌"],
            "source_scope": ["analysis_cards", "raw_articles"],
        }
    )

    task = bundle["task"]
    assert task["id"].startswith("CT-")
    assert task["status"] == "draft"
    assert bundle["retrieval_batch"]["id"].startswith("RB-")
    assert bundle["citation_list"]["id"].startswith("CL-")
    assert bundle["outline_packet"]["id"].startswith("OP-")
    assert bundle["writer_jobs"][0]["id"].startswith("WJ-")
    assert bundle["gates"]["gate_1_ready"]["pass"] is True
    assert (tmp_path / "creation" / "tasks.jsonl").exists()
    assert (tmp_path / "creation" / "writer_packets").exists()


def test_create_task_persists_preferred_writer_skill(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")

    bundle = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "多 writer 创作链路",
            "platform": "wechat",
            "audience": "内容系统设计者",
            "goal": "验证 writer 选择能稳定进入任务元数据",
            "preferred_writer_skill": "generic-longform",
        }
    )

    assert bundle["task"]["metadata"]["preferred_writer_skill"] == "generic-longform"
    assert bundle["writer_jobs"][0]["writer_skill"] == "generic-longform"


def test_list_and_update_task_returns_latest_snapshot(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")
    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "多模型协作",
            "platform": "wechat",
            "audience": "产品经理",
            "goal": "沉淀一篇方法论长文",
            "style_notes": ["直给"],
            "banned_patterns": ["正确的废话"],
            "source_scope": ["analysis_cards"],
        }
    )

    task_id = created["task"]["id"]
    updated = service.update_task(task_id, {"status": "retrieval_ready", "angle": "从工作流设计切入"})
    bundle = service.get_task_bundle(task_id)
    recent = service.list_tasks(limit=5)

    assert updated["status"] == "retrieval_ready"
    assert bundle["task"]["angle"] == "从工作流设计切入"
    assert recent[0]["id"] == task_id
    assert recent[0]["status"] == "retrieval_ready"


def test_delete_task_removes_related_workspace_objects(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")
    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "删除素材包验证",
            "platform": "wechat",
            "audience": "测试",
            "goal": "验证删除任务时会一并清理关联数据",
        }
    )

    task_id = created["task"]["id"]
    service.save_citation_list(
        task_id,
        {
            "citations": [
                {
                    "citation_id": "C1",
                    "claim_type": "fact",
                    "source_id": "SRC-001",
                    "usable_excerpt": "这是一个可用于测试删除任务的数据点。",
                    "normalized_claim": "删除任务前先创建完整关联数据。",
                    "usage_scope": "must_use",
                    "confidence": "high",
                }
            ]
        },
    )
    service.save_outline_packet(
        task_id,
        {
            "core_judgement": "test",
            "outline": [{"section": "开头", "goal": "验证删除", "citation_refs": ["C1"]}],
        },
    )
    service.generate_writer_job(task_id, {})

    result = service.delete_task(task_id)

    assert result["ok"] is True
    assert service.get_task(task_id) is None
    assert service.get_retrieval_batch(task_id) is None
    assert service.get_citation_list(task_id) is None
    assert service.get_outline_packet(task_id) is None
    assert service.get_latest_writer_job(task_id) is None


def test_generate_writer_job_persists_packet_file(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")
    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "AI 搜索工作台如何变成创作中台",
            "platform": "wechat",
            "audience": "自媒体操盘手",
            "goal": "输出长文选题",
            "style_notes": ["像熟人说人话", "有判断"],
            "banned_patterns": ["口号式总结"],
            "source_scope": ["analysis_cards", "raw_articles"],
        }
    )

    task_id = created["task"]["id"]
    service.save_citation_list(
        task_id,
        {
            "citations": [
                {
                    "citation_id": "C1",
                    "claim_type": "fact",
                    "source_id": "NOTE-001",
                    "usable_excerpt": "内容搜索工作台已经具备检索与抓取能力。",
                    "normalized_claim": "现有系统已有监控与抓取底座，可直接作为创作前置供料。",
                    "usage_scope": "must_use",
                    "confidence": "high",
                }
            ]
        },
    )
    service.save_outline_packet(
        task_id,
        {
            "core_judgement": "先把供料、引用、结构串起来，写作效率才会稳定。",
            "angle": "从系统化生产切入",
            "content_template": "方法论型",
            "hook_candidates": ["你缺的不是灵感，是编排层。"],
            "title_candidates": ["别再直接喂 raw 给 writer 了"],
            "outline": [
                {
                    "section": "开场",
                    "goal": "指出现有断点",
                    "citation_refs": ["C1"],
                }
            ],
        },
    )

    writer_job = service.generate_writer_job(
        task_id,
        {
            "primary_output": ["long_article"],
            "optional_followups": ["oral_adaptation"],
            "article_archetype": "方法论分享型",
            "user_voice_notes": ["保留一点攻击性"],
        },
    )

    packet_path = Path(writer_job["packet_path"])
    assert writer_job["status"] == "ready"
    assert packet_path.exists()
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    assert packet["task"]["id"] == task_id
    assert packet["outline_packet"]["core_judgement"]
    assert packet["citation_list"]["citations"][0]["citation_id"] == "C1"
    assert packet["writer_adapter"]["skill"] == "khazix-writer"
    assert packet["creation_packet"]["task_id"] == task_id
    assert packet["creation_packet"]["creation_intent"]["topic"]
    assert packet["creation_packet"]["evidence_pack"]["summary"]["must_use_citations"] == 1
    assert packet["creation_packet"]["narrative_plan"]["core_judgement"]
    assert packet["writer_ready_brief"]["must_use_citations"][0]["citation_id"] == "C1"


def test_generate_writer_job_uses_task_preferred_writer_when_payload_omits_it(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")
    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "通用 writer 适配",
            "platform": "wechat",
            "audience": "内容产品经理",
            "goal": "验证任务级 writer 偏好能继承到出包",
            "preferred_writer_skill": "generic-longform",
        }
    )
    task_id = created["task"]["id"]
    service.save_citation_list(
        task_id,
        {
            "citations": [
                {
                    "citation_id": "C1",
                    "claim_type": "fact",
                    "source_id": "NOTE-001",
                    "usable_excerpt": "先有中间层，再有多 writer 扩展。",
                    "normalized_claim": "中间层先立住，多 writer 才不会反复返工。",
                    "usage_scope": "must_use",
                    "confidence": "high",
                }
            ]
        },
    )
    service.save_outline_packet(
        task_id,
        {
            "core_judgement": "创作台应该先产出通用创作包，再去适配不同 writer。",
            "angle": "从中间层稳定性切入",
            "content_template": "方法论型",
            "hook_candidates": ["别让 writer 反过来决定你的系统结构。"],
            "title_candidates": ["先做创作中间层，再谈多 writer"],
            "outline": [{"section": "开场", "goal": "说明为什么先立中间层", "citation_refs": ["C1"]}],
        },
    )

    writer_job = service.generate_writer_job(task_id, {})
    packet = json.loads(Path(writer_job["packet_path"]).read_text(encoding="utf-8"))

    assert writer_job["writer_skill"] == "generic-longform"
    assert packet["writer_adapter"]["skill"] == "generic-longform"
    assert packet["writer_adapter"]["display_name"] == "Generic Longform"


def test_resolve_writer_profile_falls_back_to_default_registry():
    profile = resolve_writer_profile("unknown-writer-skill")

    assert profile["skill"] == "unknown-writer-skill"
    assert profile["adapter_name"] == "balanced-brief"
    assert "creation_intent" in profile["enabled_sections"]


def test_generate_article_draft_persists_markdown_and_quality_report(tmp_path, monkeypatch):
    monkeypatch.setenv("CREATE_STUDIO_WRITING_PROVIDER", "mock")
    service = CreationWorkspaceService(data_root=tmp_path / "creation")
    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "Claude Code 工作流",
            "platform": "wechat",
            "audience": "AI tool power users",
            "goal": "直接生成一篇 Khazix 长文草稿",
            "preferred_writer_skill": "khazix-writer",
        }
    )
    task_id = created["task"]["id"]
    batch = service.get_retrieval_batch(task_id)
    assert batch is not None
    updated_batch = dict(batch)
    updated_batch["status"] = "retrieval_ready"
    updated_batch["results"] = [
        {
            "source_id": "RAW-CLAUDE-001",
            "title": "Claude Code is workflow, not demo",
            "summary": "Claude Code 的价值在于把编码、调试和回滚放进同一条工作流。",
            "text": "Claude Code 不是一次性问答工具。它更像是把编码、调试、执行和回滚串起来的协作层，这会改变工程师处理复杂任务的方式。",
            "url": "https://example.com/claude-code",
            "source": "NightHawk",
            "channel": "x",
            "decision": "keep",
            "classification": "primary",
        }
    ]
    service._save_retrieval_batch(updated_batch)

    payload = service.generate_article_draft(
        task_id,
        {
            "creation_target_id": "khazix-wechat",
            "preferred_writer_skill": "khazix-writer",
            "writer_skill": "khazix-writer",
            "article_archetype": "产品体验型",
            "angle": "Claude Code 的变化不是模型更聪明，而是工作流开始成型。",
            "opening_hook": "故事是这样的，我最近越来越觉得 Claude Code 不是一个 demo。",
            "topic_reason": "这个选题适合写，因为它能解释为什么编程助手正在从工具变成工作流。",
            "personal_observations": "暂无，不要编造个人经历。",
            "user_voice_notes": ["像熟人聊天", "先下判断"],
            "banned_patterns": ["报告腔"],
        },
    )

    draft = payload["article_draft"]
    article_path = Path(draft["article_path"])
    assert payload["ok"] is True
    assert draft["id"].startswith("AD-")
    assert draft["writer_skill"] == "khazix-writer"
    assert article_path.exists()
    assert "Claude Code 工作流" in article_path.read_text(encoding="utf-8")
    assert draft["quality_report"]["enabled"] is True
    assert payload["next_url"].startswith("/create/write?draft_id=")
    saved = service.get_article_draft(draft["id"])
    assert saved["article_markdown"]


def test_create_task_from_topic_packet_prefills_retrieval_and_packet_metadata(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")

    packet = {
        "packet_id": "TP-20260420-001",
        "packet_type": "topic",
        "topic": "Hermes 的价值",
        "status": "ready",
        "query_text": "Hermes 价值",
        "generated_at": "2026-04-20T12:00:00+08:00",
        "topic_intent": {
            "normalized_topic": "Hermes 价值",
            "expanded_queries": ["Hermes 价值", "Hermes 创作台"],
            "entities": ["Hermes"],
            "topic_facets": ["创作台", "编排层"],
        },
        "summary": {
            "total_results": 3,
            "strong_results": 2,
            "watch_results": 1,
            "top_titles": ["Hermes 的价值判断", "为什么创作台需要编排层"],
        },
        "filters": {"source_scope": ["analysis_cards", "raw_articles"]},
        "results": [
            {
                "source_id": "OBJ-HERMES-001",
                "title": "Hermes 的价值判断",
                "summary": "解释 Hermes 为什么值得作为创作台底座。",
                "why_pick": "这是最直接的价值判断材料。",
                "source": "Hermes",
                "channel": "analysis",
                "published_at": "2026-04-20T09:00:00+08:00",
                "url": "https://example.com/hermes-1",
                "content_type": "analysis_card",
                "bucket": "strong",
                "text_excerpt": "Hermes 把检索、证据和写作编排成稳定工作流。",
                "scores": {"relevance_score": 0.98, "structured_score": 0.88},
                "event_packet_refs": [],
                "event_candidate_ids": ["EVT-CAND-1"],
            },
            {
                "source_id": "OBJ-HERMES-002",
                "title": "为什么创作台需要编排层",
                "summary": "补充编排层对内容生产的意义。",
                "why_pick": "解释系统为什么不能只靠一次搜索。",
                "source": "NightHawk",
                "channel": "feed",
                "published_at": "2026-04-20T10:00:00+08:00",
                "url": "https://example.com/hermes-2",
                "content_type": "raw_article",
                "bucket": "strong",
                "text_excerpt": "编排层决定了后续引用和写作怎么接上。",
                "scores": {"relevance_score": 0.81, "structured_score": 0.73},
                "event_packet_refs": ["EP-001"],
                "event_candidate_ids": [],
            },
            {
                "source_id": "OBJ-HERMES-003",
                "title": "Hermes 与热点事件连接",
                "summary": "说明后续热点事件如何并入创作。",
                "why_pick": "为事件接入预留空间。",
                "source": "NightHawk",
                "channel": "analysis",
                "published_at": "2026-04-20T11:00:00+08:00",
                "url": "https://example.com/hermes-3",
                "content_type": "analysis_card",
                "bucket": "watch",
                "text_excerpt": "事件能力接进来后，不需要回头重做底座。",
                "scores": {"relevance_score": 0.62, "structured_score": 0.61},
                "event_packet_refs": ["EP-002"],
                "event_candidate_ids": [],
            },
        ],
        "articles": [
            {
                "source_id": "OBJ-HERMES-001",
                "title": "Hermes 的价值判断",
                "summary": "解释 Hermes 为什么值得作为创作台底座。",
                "body_text": "Hermes 把检索、证据和写作编排成稳定工作流，因此适合做创作台底座。",
                "url": "https://example.com/hermes-1",
                "source": "Hermes",
                "channel": "analysis",
                "content_type": "analysis_card",
                "published_at": "2026-04-20T09:00:00+08:00",
                "why_pick": "这是最直接的价值判断材料。",
                "related_topics": ["Hermes", "创作台"],
                "tags": ["创作台", "编排层"],
                "event_packet_refs": [],
                "event_candidate_ids": ["EVT-CAND-1"],
                "search_explain": {"structured": {"matched_entities": ["Hermes"]}},
            },
            {
                "source_id": "OBJ-HERMES-002",
                "title": "为什么创作台需要编排层",
                "summary": "补充编排层对内容生产的意义。",
                "body_text": "编排层让引用、框架和写作不再断裂，所以它决定了创作效率能否稳定。",
                "url": "https://example.com/hermes-2",
                "source": "NightHawk",
                "channel": "feed",
                "content_type": "raw_article",
                "published_at": "2026-04-20T10:00:00+08:00",
                "why_pick": "解释系统为什么不能只靠一次搜索。",
                "related_topics": ["编排层"],
                "tags": ["方法论"],
                "event_packet_refs": ["EP-001"],
                "event_candidate_ids": [],
                "search_explain": {"structured": {"matched_facets": ["编排层"]}},
            },
            {
                "source_id": "OBJ-HERMES-003",
                "title": "Hermes 与热点事件连接",
                "summary": "说明后续热点事件如何并入创作。",
                "body_text": "热点事件能力接进来时，可以直接复用现有资料对象和切块层。",
                "url": "https://example.com/hermes-3",
                "source": "NightHawk",
                "channel": "analysis",
                "content_type": "analysis_card",
                "published_at": "2026-04-20T11:00:00+08:00",
                "why_pick": "为事件接入预留空间。",
                "related_topics": ["热点事件"],
                "tags": ["事件接入"],
                "event_packet_refs": ["EP-002"],
                "event_candidate_ids": [],
                "search_explain": {"structured": {"matched_facets": ["热点事件"]}},
            },
        ],
    }

    created = service.create_task_from_packet(packet, {"platform": "wechat"})

    task = created["creation_task"]
    retrieval_batch = created["bundle"]["retrieval_batch"]

    assert task["id"].startswith("CT-")
    assert task["trigger_type"] == "topic_packet"
    assert task["status"] == "retrieval_ready"
    assert task["metadata"]["source_packet"]["packet_id"] == "TP-20260420-001"
    assert task["metadata"]["packet_preview"]["strong_results"] == 2
    assert task["metadata"]["event_packet_refs"] == ["EP-001", "EP-002"]
    assert retrieval_batch["query_terms"] == ["Hermes 价值", "Hermes 创作台"]
    assert len(retrieval_batch["results"]) == 3
    assert retrieval_batch["results"][0]["decision"] == "keep"
    assert retrieval_batch["results"][0]["classification"] == "primary"
    assert retrieval_batch["results"][1]["classification"] == "secondary"
    assert retrieval_batch["results"][2]["decision"] == "exclude"
    assert created["bundle"]["gates"]["gate_2_retrieval_ready"]["pass"] is True
    assert f"task_id={task['id']}" in created["next_url"]


def test_create_task_from_topic_packet_treats_x_post_text_as_ready_body(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")

    packet = {
        "packet_id": "TP-X-001",
        "packet_type": "topic",
        "topic": "Claude Code usage",
        "status": "ready",
        "query_text": "Claude Code usage",
        "generated_at": "2026-04-23T10:00:00+08:00",
        "topic_intent": {"normalized_topic": "Claude Code usage", "expanded_queries": ["Claude Code usage"]},
        "summary": {"total_results": 1, "strong_results": 1, "watch_results": 0, "top_titles": ["Why Claude Code matters"]},
        "filters": {"source_scope": ["raw_articles"]},
        "results": [
            {
                "source_id": "RAW-X-001",
                "title": "Why Claude Code matters",
                "summary": "Claude Code changes the workflow because it links coding, debugging, execution and rollback into one loop.",
                "why_pick": "Primary evidence",
                "source": "NightHawk",
                "channel": "x",
                "published_at": "2026-04-23T09:00:00+08:00",
                "url": "https://example.com/x-1",
                "content_type": "raw_article",
                "bucket": "strong",
                "text_excerpt": "Claude Code changes the workflow because it links coding, debugging, execution and rollback into one loop.",
                "scores": {"relevance_score": 0.91},
            }
        ],
        "articles": [
            {
                "source_id": "RAW-X-001",
                "title": "Why Claude Code matters",
                "summary": "Claude Code changes the workflow because it links coding, debugging, execution and rollback into one loop.",
                "body_text": "",
                "url": "https://example.com/x-1",
                "source": "NightHawk",
                "channel": "x",
                "content_type": "raw_article",
                "published_at": "2026-04-23T09:00:00+08:00",
                "why_pick": "Primary evidence",
            }
        ],
    }

    created = service.create_task_from_packet(packet, {"platform": "wechat"})
    result = created["bundle"]["retrieval_batch"]["results"][0]

    assert result["recommend_full_fetch"] == ""
    assert result["raw"]["body_fetch_ok"] is True
    assert result["raw"]["body_status"] == "ready"
    assert "workflow" in result["raw"]["content"]
    assert "workflow" in result["text"]


def test_create_task_from_nighthawk_sources_treats_x_post_text_as_ready_body(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")

    creation_packet = {
        "creation_intent": {
            "topic": "Claude Code usage",
            "platform": "x",
            "audience": "AI builders",
            "goal": "Turn selected X posts into a writing task",
            "angle": "How Claude Code changes workflows",
            "source_scope": ["raw_articles"],
        },
        "source_trace": {
            "source_type": "nighthawk_raw_items",
            "raw_item_ids": [1001],
            "event_candidate_ids": [],
        },
        "evidence_pack": {
            "summary": {"body_ready_count": 0},
            "citations": [{"raw_item_id": 1001, "usage_scope": "must_use"}],
        },
        "metadata": {"selected_count": 1},
    }
    selected_items = [
        {
            "raw_item_id": 1001,
            "object_uid": "RAW-1001",
            "title": "Why Claude Code matters",
            "canonical_url": "https://example.com/x-1",
            "source_name": "NightHawk",
            "platform": "x",
            "published_at": "2026-04-23T09:00:00+08:00",
            "summary": "Claude Code changes the workflow because it links coding, debugging, execution and rollback into one loop.",
            "body_text": "",
            "body_ready": False,
            "source_kind": "raw_article",
            "event_links": [],
        }
    ]

    created = service.create_task_from_nighthawk_sources(creation_packet, selected_items, {})
    result = created["bundle"]["retrieval_batch"]["results"][0]

    assert result["recommend_full_fetch"] == ""
    assert result["raw"]["body_fetch_ok"] is True
    assert result["raw"]["body_status"] == "ready"
    assert "workflow" in result["raw"]["content"]
    assert "workflow" in result["text"]


def test_get_retrieval_batch_rewrites_stale_x_body_flags(tmp_path):
    service = CreationWorkspaceService(data_root=tmp_path / "creation")
    created = service.create_task(
        {
            "trigger_type": "topic_packet",
            "topic": "GPT Image 2 提示词与使用方法",
            "platform": "wechat",
            "audience": "AI creators",
            "goal": "Verify stale X body flags can be normalized on read",
        }
    )
    task_id = created["task"]["id"]
    batch = service.get_retrieval_batch(task_id)
    assert batch is not None

    stale_batch = dict(batch)
    stale_batch["status"] = "retrieval_ready"
    stale_batch["results"] = [
        {
            "source_id": "RAW-59567",
            "title": "GPT Image 2 蓝莓宣传图案例",
            "summary": "随手拍了一张蓝莓，让 GPT-Image-2 生成符合这个产品风格的宣传图，一致性还原非常好。",
            "text": "随手拍了一张蓝莓，让 GPT-Image-2 生成符合这个产品风格的宣传图，一致性还原非常好。",
            "url": "https://example.com/x-blueberry",
            "source": "NightHawk",
            "channel": "x",
            "published_at": "2026-04-23T09:00:00+08:00",
            "decision": "keep",
            "classification": "primary",
            "why_pick": "Primary evidence",
            "recommend_full_fetch": "yes",
            "raw": {
                "body_fetch_ok": False,
                "body_status": "missing",
                "content": "",
            },
        }
    ]
    service._save_retrieval_batch(stale_batch)

    normalized = service.get_retrieval_batch(task_id)
    assert normalized is not None
    result = normalized["results"][0]
    assert result["recommend_full_fetch"] == ""
    assert result["raw"]["body_fetch_ok"] is True
    assert result["raw"]["body_status"] == "ready"
    assert "蓝莓" in result["raw"]["content"]


"""
def test_autofill_task_target_generates_khazix_inputs_from_imported_materials(tmp_path, monkeypatch):
    monkeypatch.setenv("CREATE_STUDIO_AUTOFILL_PROVIDER", "mock")
    service = CreationWorkspaceService(data_root=tmp_path / "creation")

    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "Claude Code 浣跨敤鎶€宸т笌宸ヤ綔娴佸彉鍖?,
            "platform": "wechat",
            "audience": "AI 宸ュ叿閲嶅害鐢ㄦ埛",
            "goal": "浜у嚭涓€绡囬€傚悎 Khazix 闀挎枃妯″紡鐨勫垱浣滃寘",
        }
    )

    task_id = created["task"]["id"]
    batch = service.get_retrieval_batch(task_id)
    assert batch is not None

    updated_batch = dict(batch)
    updated_batch["status"] = "retrieval_ready"
    updated_batch["results"] = [
        {
            "source_id": "RAW-CLAUDE-001",
            "title": "Claude Code 鍊煎緱鐢ㄥ悧",
            "summary": "浠庣湡瀹炲紑鍙戞祦绋嬫潵鐪嬶紝Claude Code 鏈€鍊煎緱鎶曡祫鐨勪笉鏄ā鍨嬭兘鍔涳紝鑰屾槸瀹冨浣曟敼鍙樹綘鐨勫伐浣滄祦銆?,
            "text": "Claude Code 鐨勪环鍊间笉鍦ㄤ簬瀹冧竴涓寚浠ゅ氫箞鎯婅壋锛岃€屽湪浜庡畠鎶婄紪鐮併€佹煡閿欍€佹墽琛屽拰鍥為€€涓叉垚浜嗕竴鏉＄ǔ瀹氬伐浣滄祦銆?,
            "url": "https://example.com/claude-code-1",
            "source": "NightHawk",
            "channel": "x",
            "published_at": "2026-04-22T10:00:00+08:00",
            "decision": "keep",
            "classification": "primary",
            "why_pick": "浣滀负涓昏祫鏂欒鏄庡伐浣滄祦鍙樺寲",
        },
        {
            "source_id": "RAW-CLAUDE-002",
            "title": "Claude Code 浣跨敤蹇冩櫤妯″瀷",
            "summary": "鐪熸鏈夋剰涔夌殑涓嶆槸鍛戒护鎬庝箞鍐欙紝鑰屾槸浣犳€庝箞鎶婂畠鎽嗗湪鏁翠釜缂栫▼娴佺▼閲屻€?,
            "text": "濡傛灉浣犳妸 Claude Code 褰撴垚涓€娆℃€у伐鍏凤紝浣犲緢瀹规槗澶辨湜锛涗絾濡傛灉浣犳妸瀹冨綋鎴愪竴涓彲浠ユ寔缁凯浠ｇ殑缂栫▼鍚堜綔灞傦紝浣犱細鍙戠幇鏁堢巼鎻愬崌鏉ュ緱寰堝揩銆?,
            "url": "https://example.com/claude-code-2",
            "source": "NightHawk",
            "channel": "wechat",
            "published_at": "2026-04-22T11:00:00+08:00",
            "decision": "keep",
            "classification": "secondary",
            "why_pick": "浣滀负杈呭姪璇佹嵁鏀拺鍐欐硶鍜屽垏鍙?",
        },
    ]
    service._save_retrieval_batch(updated_batch)

    payload = service.autofill_task_target(
        task_id,
        {
            "creation_target_id": "khazix-wechat",
        },
    )

    assert payload["ok"] is True
    assert payload["autofill"]["source"] == "mock"
    assert payload["autofill"]["target_id"] == "khazix-wechat"
    assert payload["autofill"]["material_count"] == 2
    assert payload["autofill"]["fields"]["topic_reason"]
    assert payload["autofill"]["fields"]["angle"]
    assert payload["autofill"]["fields"]["opening_hook"]

    task = service.get_task(task_id)
    assert task is not None
    assert task["angle"] == payload["autofill"]["fields"]["angle"]
    assert task["metadata"]["creation_target_inputs"]["topic_reason"] == payload["autofill"]["fields"]["topic_reason"]
    assert task["metadata"]["creation_target_inputs"]["opening_hook"] == payload["autofill"]["fields"]["opening_hook"]
    assert task["metadata"]["creation_target_autofill"]["target_id"] == "khazix-wechat"

    writer_job = service.get_latest_writer_job(task_id)
    assert writer_job is not None
    assert writer_job["article_archetype"] == payload["autofill"]["fields"]["article_archetype"]
    assert writer_job["optional_followups"] == payload["autofill"]["fields"]["optional_followups"]
"""


def test_autofill_task_target_generates_khazix_inputs_from_imported_materials(tmp_path, monkeypatch):
    monkeypatch.setenv("CREATE_STUDIO_AUTOFILL_PROVIDER", "mock")
    service = CreationWorkspaceService(data_root=tmp_path / "creation")

    created = service.create_task(
        {
            "trigger_type": "manual_topic",
            "topic": "Claude Code workflow methods",
            "platform": "wechat",
            "audience": "AI tool power users",
            "goal": "Generate a Khazix longform creation bundle",
        }
    )

    task_id = created["task"]["id"]
    batch = service.get_retrieval_batch(task_id)
    assert batch is not None

    updated_batch = dict(batch)
    updated_batch["status"] = "retrieval_ready"
    updated_batch["results"] = [
        {
            "source_id": "RAW-CLAUDE-001",
            "title": "Why Claude Code is worth using",
            "summary": "Claude Code is more valuable as a workflow layer than as a one-shot model demo.",
            "text": "The value of Claude Code is not in a single surprising answer. It is in how it links coding, debugging, execution and rollback into one stable workflow.",
            "url": "https://example.com/claude-code-1",
            "source": "NightHawk",
            "channel": "x",
            "published_at": "2026-04-22T10:00:00+08:00",
            "decision": "keep",
            "classification": "primary",
            "why_pick": "Primary evidence for workflow change",
        },
        {
            "source_id": "RAW-CLAUDE-002",
            "title": "Claude Code usage mindset",
            "summary": "The key is not how to write one command, but how to place Claude Code inside the whole engineering flow.",
            "text": "If you treat Claude Code as a one-off tool, you will quickly be disappointed. If you treat it as a collaborative coding layer that can iterate continuously, the efficiency gains arrive much faster.",
            "url": "https://example.com/claude-code-2",
            "source": "NightHawk",
            "channel": "wechat",
            "published_at": "2026-04-22T11:00:00+08:00",
            "decision": "keep",
            "classification": "secondary",
            "why_pick": "Supporting evidence for writing angle",
        },
    ]
    service._save_retrieval_batch(updated_batch)

    payload = service.autofill_task_target(
        task_id,
        {
            "creation_target_id": "khazix-wechat",
        },
    )

    assert payload["ok"] is True
    assert payload["autofill"]["source"] == "mock"
    assert payload["autofill"]["target_id"] == "khazix-wechat"
    assert payload["autofill"]["material_count"] == 2
    assert payload["autofill"]["fields"]["topic_reason"]
    assert payload["autofill"]["fields"]["angle"]
    assert payload["autofill"]["fields"]["opening_hook"]

    task = service.get_task(task_id)
    assert task is not None
    assert task["angle"] == payload["autofill"]["fields"]["angle"]
    assert task["metadata"]["creation_target_inputs"]["topic_reason"] == payload["autofill"]["fields"]["topic_reason"]
    assert task["metadata"]["creation_target_inputs"]["opening_hook"] == payload["autofill"]["fields"]["opening_hook"]
    assert task["metadata"]["creation_target_autofill"]["target_id"] == "khazix-wechat"
    writer_job = service.get_latest_writer_job(task_id)
    assert writer_job is not None
    assert writer_job["article_archetype"] == payload["autofill"]["fields"]["article_archetype"]
    assert writer_job["optional_followups"] == payload["autofill"]["fields"]["optional_followups"]
