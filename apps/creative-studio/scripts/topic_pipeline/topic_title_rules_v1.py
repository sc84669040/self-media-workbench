#!/usr/bin/env python3
"""
Topic display title naming rules V1
目标：根据聚类结果生成更像中文主题名的展示标题
核心要求：标题要说明“为什么这些文章会被聚成同一主题”
当前定位：规则版，不依赖 LLM
"""

from collections import Counter
import re

BAD_TITLE_PARTS = [
    'http://', 'https://', 't.co/', '周报', '盘点', '汇总', '合辑', '速览', '日报', '月报'
]

ACTION_WORDS = ['发布', '上线', '推出', '开源', '闭源', '融资', '收购', '抄袭', '内测', '升级']
ACTION_PRIORITY = ['发布', '上线', '推出', '开源', '闭源', '融资', '收购', '抄袭', '内测', '升级']
OBJECT_HINTS = [
    '大模型', '模型', 'Agent', '智能体', '平台', '工具链', '工作流', '推理', '创作平台', '编程工具', 'API', '产品'
]
GENERIC_SUBJECTS = {'ai', '2026', '2025', '2024', 'china', 'unknown', 'https', 'http'}
BRAND_HINTS = ['Claude Design', 'Claude Code', 'Claude Opus', 'GPT Image 2', 'Codex', 'OpenAI', 'Anthropic', 'Claude', 'DeepSeek', 'Gemini', 'Gemma', 'Kimi', 'Cursor', 'Canva', 'NVIDIA', 'Seedance', 'BytePlus', 'Cal', 'Hermes Agent', 'MCP']
SUBJECT_ALIAS_MAP = {
    'claude design': 'Claude Design',
    'claude code': 'Claude Code',
    'claude opus': 'Claude Opus',
    'gpt image 2': 'GPT Image 2',
    'codex': 'Codex',
    'openai': 'OpenAI',
    'anthropic': 'Anthropic',
    'deepseek': 'DeepSeek',
    'gemini': 'Gemini',
    'gemma': 'Gemma',
    'kimi': 'Kimi',
    'cursor': 'Cursor',
    'mcp': 'MCP',
}


def clean_title(title: str) -> str:
    title = (title or '').strip()
    for x in BAD_TITLE_PARTS:
        title = title.replace(x, '')
    title = re.sub(r'https?://\S+', '', title)
    title = re.sub(r'\bt\.co/\S+', '', title)
    return ' '.join(title.split())[:80]


def normalize_subject(subject: str) -> str:
    s = clean_title(subject).lower()
    s = re.sub(r'^[“"\']|[”"\']$', '', s)
    s = re.sub(r'^(曝|传|传出|消息称|报道称)', '', s).strip()
    s = s.replace('open ai', 'openai').replace('gpt-image-2', 'gpt image 2')
    s = re.sub(r'\b(v|V)\d+(\.\d+)*\b', '', s).strip()
    s = re.sub(r'\b\d{4}\b', '', s).strip()
    s = re.sub(r'\s+', ' ', s).strip(' -—:：,.，。!！?？')
    if not s or s.lower() in GENERIC_SUBJECTS:
        return ''
    if re.fullmatch(r'\d+', s):
        return ''
    return SUBJECT_ALIAS_MAP.get(s, s[:30])


def pick_main_subject(subjects, texts):
    norm_subjects = [normalize_subject(s) for s in subjects]
    norm_subjects = [s for s in norm_subjects if s and len(s) <= 24 and s.count(' ') <= 3]
    if norm_subjects:
        return Counter(norm_subjects).most_common(1)[0][0]

    joined = '\n'.join(texts)
    brand_hits = []
    for brand in BRAND_HINTS:
        if re.search(re.escape(brand), joined, re.I):
            brand_hits.append(brand)
    if brand_hits:
        return Counter(brand_hits).most_common(1)[0][0]

    for text in texts:
        m = re.search(r'(Claude Design|Claude Code|Claude Opus|GPT Image 2|Codex|OpenAI|Anthropic|DeepSeek|Gemini|Gemma|Claude|Kimi|Cursor|Canva|NVIDIA|Seedance|BytePlus|Cal|Hermes Agent|MCP)', text, re.I)
        if m:
            return normalize_subject(m.group(1))
    return 'AI'


def pick_actions(texts):
    joined = '\n'.join(texts)
    hits = [a for a in ACTION_WORDS if a in joined]
    if not hits:
        return ['动态']
    ordered = [a for a in ACTION_PRIORITY if a in hits]
    return ordered[:2]


def pick_object(texts):
    joined = '\n'.join(texts)
    for o in OBJECT_HINTS:
        if o.lower() in joined.lower():
            return o
    return ''


def generate_display_title_cn(subjects, titles, summaries=None, reason=None):
    summaries = summaries or []
    texts = [clean_title(x) for x in (titles + summaries) if x]
    subject = pick_main_subject(subjects, texts)
    actions = pick_actions(texts)
    obj = pick_object(texts)
    reason = reason or ('动态' if actions == ['动态'] else actions[0])

    if reason == '争议':
        if obj:
            return f'{subject}{obj}争议升温'
        return f'{subject}相关争议升温'
    if reason == '融资':
        return f'{subject}融资动态升温'
    if reason == '组织变化':
        return f'{subject}组织动态变化'
    if reason == '创作能力':
        if subject == 'GPT Image 2':
            return 'GPT Image 2 创作案例升温'
        if subject == 'Claude Design':
            return 'Claude Design 产品体验持续升温'
        return f'{subject}创作能力升温'
    if reason == '发布':
        if subject == 'Claude Design':
            return 'Claude Design 发布与体验热度升温'
        if subject == 'Claude Code':
            return 'Claude Code 更新热度升温'
        if subject == 'Claude Opus':
            return 'Claude Opus 发布推进'
        if subject == 'Codex':
            return 'Codex 更新热度升温'
        if obj and obj not in subject and subject not in {'Claude Code', 'Claude Design'}:
            return f'{subject}{obj}发布推进'
        return f'{subject}发布动态升温'
    if reason == '能力演进':
        joined = '\n'.join(texts)
        if subject == 'Anthropic' and obj in {'Agent', '智能体'}:
            return 'Anthropic 智能体生态能力演进'
        if 'Claude Design' in joined:
            return 'Claude Design 产品体验持续升温'
        if 'Claude Code' in joined:
            return 'Claude Code 使用经验持续扩散'
        if 'Codex' in joined:
            return 'Codex 产品能力持续迭代'
        if 'GPT Image 2' in joined or 'gpt image 2' in joined.lower():
            return 'GPT Image 2 创作能力持续升温'
        if obj:
            return f'{subject}{obj}能力演进'
        return f'{subject}能力演进'

    if actions == ['动态']:
        if obj and obj not in subject:
            return f'{subject}{obj}相关动态'
        return f'{subject}相关动态'
    if len(actions) >= 2 and actions[0] != actions[1]:
        action_part = f'{actions[0]}与{actions[1]}'
    else:
        action_part = actions[0]

    if obj and obj not in subject:
        return f'{subject}{action_part}{obj}'
    return f'{subject}{action_part}动态'


if __name__ == '__main__':
    sample_subjects = ['DeepSeek V4', 'DeepSeek', '梁文锋']
    sample_titles = [
        '曝DeepSeek V4将于本周发布，梁文锋启动首次外部融资',
        'DeepSeek V4 发布临近，融资传闻升温',
    ]
    print(generate_display_title_cn(sample_subjects, sample_titles))
