#!/usr/bin/env python3
import argparse
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
WINDOW_HOURS = 72

EVENT_TYPE_RULES = {
    'launch': ['发布', '上线', '推出', '内测', '开源', '正式发布'],
    'funding': ['融资', '收购', '并购', '投资'],
    'policy': ['监管', '政策', '规定', '禁止', '法规'],
    'controversy': ['抄袭', '闭源', '争议', '翻车', '事故', '失败'],
    'benchmark': ['破纪录', '排名', '第一名', '半马', '跑分', '比赛'],
    'trend': ['周报', '观察', '趋势', '盘点', '解读'],
}

AI_STRONG_KEYWORDS = [
    'openai', 'anthropic', 'claude', 'gpt', 'gemini', 'deepseek', 'llm', 'aigc', 'agi',
    'agent', 'agents', 'copilot', 'cursor', 'langchain', 'rag', 'embedding', 'midjourney',
    '大模型', '智能体', '生成式', '多模态', '模型推理', '模型训练', 'prompt', '提示词',
    '蒸馏', '微调', '量化', '向量', '知识库', '工作流', '推理模型', '开源模型'
]

AI_WEAK_KEYWORDS = [
    'ai', '模型', '推理', '机器人', '具身', '自动驾驶', '算力'
]

AI_EXCLUDE_KEYWORDS = [
    '电摩', '摩托车', '高速', '半程马拉松', '彩票', '调岗', '旅游攻略', '大会', '论坛',
    'serverless', '全球数据库', '数据库切换', '本地化能力', 'sql 自动调优', '制造业', '汉诺威'
]

ROUNDUP_KEYWORDS = ['周报', '盘点', '一周', '汇总', '合辑', '速览', '日报', '月报']


def detect_event_type(title: str, content: str = '') -> str:
    text = f"{title or ''}\n{content or ''}"
    for event_type, keywords in EVENT_TYPE_RULES.items():
        if any(k in text for k in keywords):
            return event_type
    return 'other'


def is_ai_relevant(title: str, content: str = '', source_handle: str = '') -> bool:
    text = f"{title or ''}\n{content or ''}\n{source_handle or ''}".lower()
    strong_hits = [k for k in AI_STRONG_KEYWORDS if k.lower() in text]
    weak_hits = [k for k in AI_WEAK_KEYWORDS if k.lower() in text]
    exclude_hits = [k for k in AI_EXCLUDE_KEYWORDS if k.lower() in text]

    if exclude_hits and not strong_hits:
        return False
    if strong_hits:
        return True
    return len(weak_hits) >= 2


def extract_subject(title: str, source_handle: str = '') -> str:
    if not title:
        return source_handle or 'unknown'
    patterns = [
        r'^([A-Za-z0-9\-\+\.# ]{2,40})',
        r'^([^，。！!？?：:]{2,30})',
    ]
    for p in patterns:
        m = re.search(p, title.strip())
        if m:
            subject = m.group(1).strip(' -—:：,.，。!！?？"“”')
            if subject:
                return subject
    return source_handle or 'unknown'


def build_event_key(event_type: str, subject: str, published_at: str) -> str:
    day = (published_at or '')[:10]
    return f"{event_type}|{subject}|{day}".lower()


def fetch_raw_items(conn):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    return cur.execute(
        """
        SELECT id, platform, source_handle, item_id, title, content, url,
               published_at, fetched_at
        FROM raw_items
        WHERE datetime(COALESCE(published_at, fetched_at)) >= datetime('now', ?)
          AND TRIM(COALESCE(title, '')) <> ''
        ORDER BY datetime(COALESCE(published_at, fetched_at)) DESC
        """,
        (f'-{WINDOW_HOURS} hours',)
    ).fetchall()


def candidate_from_row(row):
    title = row['title'] or ''
    content = row['content'] or ''
    event_type = detect_event_type(title, content)
    subject = extract_subject(title, row['source_handle'] or '')
    published_at = row['published_at'] or row['fetched_at'] or ''
    event_key = build_event_key(event_type, subject, published_at)
    summary = (content or title)[:180]
    keywords = []
    for kws in EVENT_TYPE_RULES.values():
        keywords.extend([k for k in kws if k in title or k in content])
    text = f"{title}\n{content}".lower()
    matched_ai = [k for k in (AI_STRONG_KEYWORDS + AI_WEAK_KEYWORDS) if k.lower() in text]
    is_roundup = any(k in title for k in ROUNDUP_KEYWORDS)
    return {
        'raw_item_id': row['id'],
        'event_key': event_key,
        'event_type': event_type,
        'title': title,
        'summary': summary,
        'subject': subject,
        'action': event_type,
        'object': None,
        'entities_json': json.dumps([subject], ensure_ascii=False),
        'keywords_json': json.dumps(sorted(set(keywords + matched_ai)), ensure_ascii=False),
        'platforms_json': json.dumps([row['platform']], ensure_ascii=False),
        'first_seen_at': published_at,
        'last_seen_at': published_at,
        'status': 'new',
        'confidence': 0.45 if is_roundup else 0.65,
        'is_roundup': is_roundup,
    }


def group_candidates(candidates):
    grouped = defaultdict(list)
    for c in candidates:
        grouped[c['event_key']].append(c)
    return grouped


def ensure_required_schema(conn):
    cur = conn.cursor()
    for name in ['event_candidates', 'event_evidence']:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
        if not cur.fetchone():
            raise RuntimeError(f'{name} table missing')


def upsert_event(conn, group):
    cur = conn.cursor()
    rep = group[0]
    titles = [x['title'] for x in group if x['title']]
    summary = max((x['summary'] for x in group), key=len, default=rep['summary'])
    platforms = sorted(set(x for c in group for x in json.loads(c['platforms_json'])))
    first_seen = min((c['first_seen_at'] for c in group if c['first_seen_at']), default=None)
    last_seen = max((c['last_seen_at'] for c in group if c['last_seen_at']), default=None)
    has_roundup = any(c.get('is_roundup') for c in group)
    confidence = min(0.95, (0.5 if has_roundup else 0.6) + 0.05 * len(group))
    row = cur.execute("SELECT id FROM event_candidates WHERE event_key = ? LIMIT 1", (rep['event_key'],)).fetchone()
    if row:
        event_id = row[0]
        cur.execute(
            """
            UPDATE event_candidates
            SET title=?, summary=?, event_type=?, subject=?, action=?,
                entities_json=?, keywords_json=?, platforms_json=?,
                first_seen_at=COALESCE(first_seen_at, ?), last_seen_at=?,
                updated_at=datetime('now'), confidence=?
            WHERE id=?
            """,
            (
                max(titles, key=len) if titles else rep['title'], summary,
                rep['event_type'], rep['subject'], rep['action'],
                rep['entities_json'], rep['keywords_json'], json.dumps(platforms, ensure_ascii=False),
                first_seen, last_seen, confidence, event_id,
            )
        )
    else:
        cur.execute(
            """
            INSERT INTO event_candidates (
                event_key, event_type, title, summary, confidence, status,
                created_at, subject, action, object, entities_json, keywords_json,
                platforms_json, first_seen_at, last_seen_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'new', datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                rep['event_key'], rep['event_type'], max(titles, key=len) if titles else rep['title'],
                summary, confidence, rep['subject'], rep['action'], rep['object'],
                rep['entities_json'], rep['keywords_json'], json.dumps(platforms, ensure_ascii=False),
                first_seen, last_seen,
            )
        )
        event_id = cur.lastrowid
    for c in group:
        cur.execute(
            "INSERT OR IGNORE INTO event_evidence (event_id, raw_item_id, relation_type, confidence, created_at) VALUES (?, ?, 'supporting', ?, datetime('now'))",
            (event_id, c['raw_item_id'], confidence)
        )
    return event_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    ensure_required_schema(conn)
    rows = fetch_raw_items(conn)
    ai_rows = [r for r in rows if is_ai_relevant(r['title'] or '', r['content'] or '', r['source_handle'] or '')]
    candidates = [candidate_from_row(r) for r in ai_rows]
    grouped = group_candidates(candidates)

    if args.dry_run:
        sample = []
        for i, (_, group) in enumerate(grouped.items()):
            if i >= 10:
                break
            rep = group[0]
            sample.append({
                'event_key': rep['event_key'],
                'event_type': rep['event_type'],
                'title': rep['title'],
                'group_size': len(group),
                'is_roundup': rep.get('is_roundup', False),
            })
        print(json.dumps({
            'raw_items_scanned': len(rows),
            'ai_relevant_items': len(ai_rows),
            'candidate_groups': len(grouped),
            'sample': sample,
        }, ensure_ascii=False, indent=2))
        conn.close()
        return

    event_ids = []
    for _, group in grouped.items():
        event_ids.append(upsert_event(conn, group))
    conn.commit()
    conn.close()
    print(json.dumps({
        'raw_items_scanned': len(rows),
        'ai_relevant_items': len(ai_rows),
        'candidate_groups': len(grouped),
        'events_written': len(event_ids),
        'window_hours': WINDOW_HOURS,
    }, ensure_ascii=False))


if __name__ == '__main__':
    main()
