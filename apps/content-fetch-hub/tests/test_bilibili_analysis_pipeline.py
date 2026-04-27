from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from fetch_content_cli import maybe_build_analysis  # noqa: E402
from models import FetchResult  # noqa: E402
from video_transcript_analysis import build_structured_payload  # noqa: E402
from writer_obsidian import write_result_to_obsidian  # noqa: E402


def test_write_result_to_obsidian_routes_bilibili_into_bsite_dir(tmp_path: Path):
    result = FetchResult(
        ok=True,
        channel="bilibili",
        url="https://www.bilibili.com/video/BV1ZuQvB9EVr/",
        title="B站集成测试",
        content_markdown="正文",
        author="测试UP主",
    )

    saved_path = Path(write_result_to_obsidian(result, tmp_path))

    assert saved_path.exists()
    assert saved_path.parent == tmp_path / "抓取内容" / "B站"
    assert "_bilibili_" in saved_path.name


def test_build_structured_payload_supports_bilibili_url_and_bv_id():
    payload = {
        "title": "B站平台识别测试",
        "source_name": "测试UP主",
        "url": "https://www.bilibili.com/video/BV1ZuQvB9EVr/?spm_id_from=333.1007.tianma.1-1-1.click",
    }

    structured = build_structured_payload(payload)

    assert structured["platform"] == "bilibili"
    assert structured["platform_id"] == "BV1ZuQvB9EVr"
    assert structured["content_type"] == "中长视频 / 讲解 / 观察"
    assert structured["route_bucket"] == "bilibili-skip"
    assert structured["related_topics"] == ["bilibili"]


def test_maybe_build_analysis_writes_bilibili_card_under_bsite_dir(tmp_path: Path):
    payload = {
        "channel": "bilibili",
        "title": "B站分析卡测试",
        "author": "测试UP主",
        "url": "https://www.bilibili.com/video/BV1ZuQvB9EVr/",
        "published_at": "2026-04-16",
        "content_markdown": "第一段：这期视频详细拆解了 AI 工作流。\n第二段：作者给了 3 个执行建议。",
        "meta": {
            "transcript_available": True,
            "transcript_language": "zh-CN",
            "transcript_source": "yt_dlp_subtitles",
            "video_id": "BV1ZuQvB9EVr",
        },
    }

    analysis_path, error = maybe_build_analysis(payload, tmp_path)
    analysis_file = Path(analysis_path)
    json_file = analysis_file.with_suffix(".json")

    assert error == ""
    assert analysis_file.exists()
    assert json_file.exists()
    assert analysis_file.parent == tmp_path / "抓取内容" / "分析卡片" / "B站"
    assert "# 分析卡片：B站分析卡测试" in analysis_file.read_text(encoding="utf-8")
