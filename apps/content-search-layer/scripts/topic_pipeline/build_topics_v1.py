#!/usr/bin/env python3
import argparse
import importlib.util
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from runtime_config import event_radar_db_path  # noqa: E402

DB_PATH = event_radar_db_path()
TITLE_RULES_PATH = Path(__file__).with_name('topic_title_rules_v1.py')
WINDOW_HOURS = 72

BAD_SUBJECTS = {
    'https', 'http', 'ai', 'china', 'unknown', 'github user', '4小时闭门会', '2026', '2025', '2024'
}
BAD_TITLE_KEYWORDS = ['https://', 'http://', 't.co/', '旅游攻略', '电摩', '摩托车']
ROUNDUP_KEYWORDS = ['周报', '盘点', '汇总', '合辑', '速览', '日报', '月报']
AI_BRANDS = ['openai', 'anthropic', 'claude', 'gpt', 'gemini', 'gemma', 'deepseek', 'kimi', 'cursor', 'copilot', 'midjourney', 'nvidia', 'seedance', 'byteplus', 'cal', 'hermes agent']
SPECIFIC_SUBJECT_RULES = [
    ('claude design', 'claude design'),
    ('claude code', 'claude code'),
    ('claude opus', 'claude opus'),
    ('gpt image 2', 'gpt image 2'),
    ('gpt-image-2', 'gpt image 2'),
    ('codex', 'codex'),
    ('sora', 'sora'),
    ('mcp', 'mcp'),
    ('gemma', 'gemma'),
    ('gemini', 'gemini'),
    ('deepseek', 'deepseek'),
    ('kimi', 'kimi'),
    ('cursor', 'cursor'),
    ('midjourney', 'midjourney'),
]
CANONICAL_SUBJECT_MAP = {
    'open ai': 'openai',
    'openai': 'openai',
    'gpt': 'openai',
    'anthropic labs': 'anthropic',
    'anthropic': 'anthropic',
    'claude': 'anthropic',
    'gemini ai': 'gemini',
    'gemini': 'gemini',
    'gemma': 'gemma',
}
SUBTOPIC_RULES = {
    'anthropic': {
        'ceo': 'management',
        'amodei': 'management',
        'leadership': 'management',
        'exit': 'management',
        'depart': 'management',
        'mcp': 'mcp',
    },
    'openai': {
        'executive': 'management',
        'leadership': 'management',
        'acquisition': 'acquisition',
        'acquisitions': 'acquisition',
        '收购': 'acquisition',
        '融资': 'financing',
        '投资': 'financing',
    },
}
AI_STRONG_KEYWORDS = [
    'openai', 'anthropic', 'claude', 'gpt', 'gemini', 'deepseek', 'llm', 'aigc', 'agi',
    'agent', 'agents', 'copilot', 'cursor', 'langchain', 'rag', 'embedding', 'midjourney',
    '大模型', '智能体', '生成式', '多模态', '模型推理', '模型训练', 'prompt', '提示词',
    '蒸馏', '微调', '量化', '向量', '知识库', '工作流', '推理模型', '开源模型'
]
AI_WEAK_KEYWORDS = ['ai', '模型', '推理', '机器人', '具身', '自动驾驶', '算力']
AI_EXCLUDE_KEYWORDS = [
    '电摩', '摩托车', '高速', '半程马拉松', '彩票', '调岗', '旅游攻略', '大会', '论坛',
    'serverless', '全球数据库', '数据库切换', '本地化能力', 'sql 自动调优', '制造业', '汉诺威', '手机渲染图', '运动科技'
]


def load_title_rules():
    spec = importlib.util.spec_from_file_location('topic_title_rules_v1', TITLE_RULES_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def normalize_subject(subject: str) -> str:
    s = (subject or '').strip().lower()
    s = re.sub(r'https?://\S+', '', s)
    s = re.sub(r'\bt\.co/\S+', '', s)
    s = re.sub(r'^(曝|传|消息称|报道称)', '', s)
    s = s.replace('open ai', 'openai')
    s = s.replace('anthropic labs', 'anthropic')
    s = s.replace('gpt-image-2', 'gpt image 2')
    s = re.sub(r'\b(v|V)\d+(\.\d+)*\b', '', s)
    s = re.sub(r'\b\d{4}\b', '', s)
    s = re.sub(r'[^\w\u4e00-\u9fff\-\+\. ]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip(' -—:：,.，。!！?？')
    if not s:
        return ''
    if s in BAD_SUBJECTS or re.fullmatch(r'\d+', s):
        return ''
    if len(s) > 30 and all(k not in s for k in ['openai', 'anthropic', 'claude', 'gpt image 2', 'codex', 'deepseek', 'gemini', 'gemma', 'kimi', 'cursor']):
        return ''
    return s[:40]


def canonicalize_subject(subject: str) -> str:
    s = normalize_subject(subject)
    return CANONICAL_SUBJECT_MAP.get(s, s)


def derive_specific_subject(text: str) -> str:
    lower = (text or '').lower()
    lower = lower.replace('open ai', 'openai').replace('gpt-image-2', 'gpt image 2')
    for needle, subject in SPECIFIC_SUBJECT_RULES:
        if needle in lower:
            return subject
    return ''


def derive_subject(row):
    title = (row['title'] or '').lower()
    subject = normalize_subject(row['subject'] or '')
    combined = f"{subject}\n{title}\n{(row['summary'] or '').lower()}"

    specific_subject = derive_specific_subject(combined)
    if specific_subject:
        return specific_subject

    if subject and subject not in BAD_SUBJECTS and len(subject) >= 2:
        for brand in AI_BRANDS:
            if brand in subject:
                return canonicalize_subject(brand)
        if subject in CANONICAL_SUBJECT_MAP:
            return canonicalize_subject(subject)
    for brand in AI_BRANDS:
        if brand in title:
            return canonicalize_subject(brand)
    if re.search(r'deepseek\s*v?\d+(?:\.\d+)*', title):
        return 'deepseek'
    return ''


def is_ai_relevant(title: str, summary: str = '', subject: str = '') -> bool:
    text = f"{title or ''}\n{summary or ''}\n{subject or ''}".lower()
    strong_hits = [k for k in AI_STRONG_KEYWORDS if k.lower() in text]
    weak_hits = [k for k in AI_WEAK_KEYWORDS if k.lower() in text]
    exclude_hits = [k for k in AI_EXCLUDE_KEYWORDS if k.lower() in text]
    if exclude_hits and not strong_hits:
        return False
    if strong_hits:
        return True
    return len(weak_hits) >= 2 and not is_roundup(title)


def is_bad_title(title: str) -> bool:
    title = (title or '').strip().lower()
    if len(title) < 8:
        return True
    if any(k.lower() in title for k in BAD_TITLE_KEYWORDS):
        return True
    return False


def is_roundup(title: str) -> bool:
    return any(k in (title or '') for k in ROUNDUP_KEYWORDS)


def fetch_events(conn):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    return cur.execute(
        """
        SELECT ec.id, ec.event_key, ec.event_type, ec.title, ec.summary, ec.subject,
               ec.keywords_json, ec.platforms_json, ec.first_seen_at, ec.last_seen_at, ec.confidence,
               COUNT(ee.raw_item_id) AS article_count
        FROM event_candidates ec
        LEFT JOIN event_evidence ee ON ee.event_id = ec.id
        WHERE datetime(COALESCE(ec.last_seen_at, ec.first_seen_at, ec.created_at)) >= datetime('now', ?)
        GROUP BY ec.id
        ORDER BY ec.id DESC
        """,
        (f'-{WINDOW_HOURS} hours',)
    ).fetchall()


def derive_subtopic(subject: str, row):
    text = ((row['title'] or '') + '\n' + (row['summary'] or '')).lower()
    rules = SUBTOPIC_RULES.get(subject, {})
    for needle, sub in rules.items():
        if needle in text:
            return sub
    return ''


def topic_key_for_event(row):
    subject = derive_subject(row)
    if not subject:
        return None
    event_type = (row['event_type'] or 'other').strip().lower()
    if event_type == 'other' and any(k in ((row['title'] or '') + '\n' + (row['summary'] or '')) for k in ['发布', '上线', '推出', '内测', '开源']):
        event_type = 'launch'
    if subject in {'claude design', 'claude code', 'claude opus', 'gpt image 2', 'codex', 'sora', 'mcp', 'gemma'}:
        return f'{event_type}|{subject}'
    subtopic = derive_subtopic(subject, row)
    if subtopic:
        return f'{event_type}|{subject}|{subtopic}'
    return f'{event_type}|{subject}'


def build_groups(events):
    groups = defaultdict(list)
    dropped = 0
    for e in events:
        if is_bad_title(e['title']) or not is_ai_relevant(e['title'], e['summary'], e['subject']):
            dropped += 1
            continue
        key = topic_key_for_event(e)
        if not key:
            dropped += 1
            continue
        groups[key].append(dict(e))
    return groups, dropped


def detect_reason(group):
    texts = '\n'.join((x['title'] or '') + '\n' + (x['summary'] or '') for x in group)
    lower = texts.lower()
    if any(k in texts for k in ['抄袭', '争议', '闭源']):
        return '争议'
    if 'gpt image 2' in lower and any(k in lower for k in ['海报', '插画', '设计能力', '提示词', '图片', '图像']):
        return '创作能力'
    if any(k in lower for k in ['claude design', 'claude code', 'codex', 'claude opus']) and any(k in texts for k in ['发布', '上线', '推出', '内测', '开源', '正式登场']):
        return '发布'
    if any(k in lower for k in ['ceo', 'executive', 'leadership', 'amodei', 'exit', 'depart']):
        return '组织变化'
    if any(k in texts for k in ['融资', '收购', '投资']):
        return '融资'
    if any(k in lower for k in ['海报', '插画', '设计能力', '提示词', '图片', '图像']):
        return '创作能力'
    if any(k in texts for k in ['发布', '上线', '推出', '内测', '开源']):
        return '发布'
    if any(k in texts for k in ['Agent', '智能体', '工作流', '运行时', 'Claude Code', 'Claude Design', 'Codex']):
        return '能力演进'
    return '动态'


def compute_group_metrics(group):
    article_count = sum(int(x['article_count'] or 0) for x in group)
    source_titles = [x['title'] for x in group if x['title']]
    subjects = [derive_subject(x) for x in group]
    subjects = [x for x in subjects if x]
    summaries = [x['summary'] for x in group if x['summary']]
    event_count = len(group)
    source_count = len(set((x['title'] or '')[:80] for x in group if x['title']))
    platform_count = len(set(p for x in group for p in json.loads(x['platforms_json'] or '[]')))
    has_roundup = any(is_roundup(x['title']) for x in group)
    importance = min(100, 35 + event_count * 8 + article_count * 3)
    impact = min(100, 25 + article_count * 6 + platform_count * 8)
    creation = min(100, 20 + (10 if has_roundup else 20) + (15 if any('争议' in (x['title'] or '') or '抄袭' in (x['title'] or '') for x in group) else 0))
    overall = round(importance * 0.5 + impact * 0.3 + creation * 0.2, 2)
    return {
        'event_count': event_count,
        'article_count': article_count,
        'source_count': source_count,
        'platform_count': platform_count,
        'importance': round(importance, 2),
        'impact': round(impact, 2),
        'creation': round(creation, 2),
        'overall': overall,
        'subjects': subjects,
        'titles': source_titles,
        'summaries': summaries,
        'has_roundup': has_roundup,
        'reason': detect_reason(group),
    }


def ensure_schema(conn):
    cur = conn.cursor()
    for name in ['topics', 'topic_events', 'topic_scores']:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
        if not cur.fetchone():
            raise RuntimeError(f'{name} table missing')


def clear_topics(conn):
    cur = conn.cursor()
    cur.execute('DELETE FROM topic_reviews')
    cur.execute('DELETE FROM topic_scores')
    cur.execute('DELETE FROM topic_events')
    cur.execute('DELETE FROM topics')


def write_topics(conn, groups, title_rules):
    cur = conn.cursor()
    written = 0
    for key, group in sorted(groups.items(), key=lambda kv: compute_group_metrics(kv[1])['article_count'], reverse=True):
        metrics = compute_group_metrics(group)
        display_title = title_rules.generate_display_title_cn(metrics['subjects'], metrics['titles'], metrics['summaries'], metrics.get('reason'))
        if metrics['has_roundup'] and metrics['event_count'] == 1 and metrics['article_count'] <= 2:
            continue
        first_seen = min((x['first_seen_at'] for x in group if x['first_seen_at']), default=None)
        last_seen = max((x['last_seen_at'] for x in group if x['last_seen_at']), default=None)
        risk_flags = []
        if metrics['event_count'] == 1:
            risk_flags.append('single_event')
        if metrics['platform_count'] == 1:
            risk_flags.append('single_platform')
        if metrics['has_roundup']:
            risk_flags.append('contains_roundup')

        cur.execute(
            """
            INSERT INTO topics (
                topic_key, title, summary, topic_type, status, time_window_hours,
                first_seen_at, last_seen_at, primary_platforms_json, primary_entities_json,
                risk_flags_json, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'emerging', ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
            """,
            (
                key, display_title, (metrics['summaries'][0][:180] if metrics['summaries'] else display_title),
                (group[0]['event_type'] or 'other'), WINDOW_HOURS, first_seen, last_seen,
                json.dumps(sorted(set(p for x in group for p in json.loads(x['platforms_json'] or '[]'))), ensure_ascii=False),
                json.dumps(metrics['subjects'][:5], ensure_ascii=False),
                json.dumps(risk_flags, ensure_ascii=False),
            )
        )
        topic_id = cur.lastrowid
        for e in group:
            cur.execute(
                "INSERT OR IGNORE INTO topic_events (topic_id, event_id, relation_weight, is_core, created_at) VALUES (?, ?, 1, 1, datetime('now'))",
                (topic_id, e['id'])
            )
        cur.execute(
            """
            INSERT INTO topic_scores (
                topic_id, importance_score, impact_score, creation_potential_score, overall_score,
                evidence_event_count, evidence_article_count, evidence_source_count, evidence_platform_count,
                trend_status, recommended_angles_json, recommended_formats_json, emotion_points_json,
                debate_points_json, card_summary, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'emerging', ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                topic_id, metrics['importance'], metrics['impact'], metrics['creation'], metrics['overall'],
                metrics['event_count'], metrics['article_count'], metrics['source_count'], metrics['platform_count'],
                json.dumps(['趋势解读', '产品观察'], ensure_ascii=False),
                json.dumps(['解读', '观点'], ensure_ascii=False),
                json.dumps(['机会感', '焦虑感'] if metrics['creation'] >= 40 else [], ensure_ascii=False),
                json.dumps(['路线之争'] if metrics['has_roundup'] else [], ensure_ascii=False),
                display_title,
            )
        )
        written += 1
    return written


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()

    title_rules = load_title_rules()
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    events = fetch_events(conn)
    groups, dropped = build_groups(events)

    preview = []
    sorted_groups = sorted(groups.items(), key=lambda kv: compute_group_metrics(kv[1])['article_count'], reverse=True)
    for key, group in sorted_groups[:10]:
        metrics = compute_group_metrics(group)
        preview.append({
            'topic_key': key,
            'display_title_cn': title_rules.generate_display_title_cn(metrics['subjects'], metrics['titles'], metrics['summaries'], metrics.get('reason')),
            'event_count': metrics['event_count'],
            'article_count': metrics['article_count'],
            'importance': metrics['importance'],
            'impact': metrics['impact'],
            'overall': metrics['overall'],
        })

    if not args.write:
        print(json.dumps({
            'events_scanned': len(events),
            'groups_built': len(groups),
            'dropped_events': dropped,
            'preview': preview,
        }, ensure_ascii=False, indent=2))
        conn.close()
        return

    clear_topics(conn)
    written = write_topics(conn, groups, title_rules)
    conn.commit()
    conn.close()
    print(json.dumps({
        'events_scanned': len(events),
        'groups_built': len(groups),
        'dropped_events': dropped,
        'topics_written': written,
    }, ensure_ascii=False))


if __name__ == '__main__':
    main()
