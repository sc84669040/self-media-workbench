#!/usr/bin/env python3
"""
content-search-layer 最小可用入口

当前状态：
- Feed / 官方博客：仍由既有流程处理
- X：已接入最小可用搜索能力（基于 Agent-Reach / twitter-cli / Cookie 登录）
- 其他渠道：暂保留骨架占位
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from shutil import which

import yaml

from runtime_config import config as root_config


def load_agent_reach_twitter_env():
    env = os.environ.copy()
    cfg = root_config()
    credentials = dict(cfg.get("credentials") or {})
    auth_env = str(credentials.get("twitter_auth_token_env") or "TWITTER_AUTH_TOKEN").strip()
    ct0_env = str(credentials.get("twitter_ct0_env") or "TWITTER_CT0").strip()
    if auth_env and os.environ.get(auth_env) and not env.get("TWITTER_AUTH_TOKEN"):
        env["TWITTER_AUTH_TOKEN"] = str(os.environ.get(auth_env) or "")
    if ct0_env and os.environ.get(ct0_env) and not env.get("TWITTER_CT0"):
        env["TWITTER_CT0"] = str(os.environ.get(ct0_env) or "")
    return env


def run_twitter_command(cmd):
    completed = subprocess.run(cmd, capture_output=True, text=True, env=load_agent_reach_twitter_env())
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout).strip())
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'twitter-cli 输出不是合法 JSON：{exc}') from exc


def build_x_candidates(payload, mode):
    items = payload.get('data', []) or []
    strong_recommend = []
    watch_list = []

    for idx, item in enumerate(items):
        text = (item.get('text') or '').strip()
        author = item.get('author') or {}
        screen_name = author.get('screenName') or author.get('username') or 'unknown'
        tweet_id = item.get('id')
        url = f'https://x.com/{screen_name}/status/{tweet_id}' if tweet_id else ''
        metrics = item.get('metrics') or {}
        likes = metrics.get('likes') or 0
        retweets = metrics.get('retweets') or 0
        quotes = metrics.get('quotes') or 0
        summary = text.replace('\n', ' ').strip()[:140]

        candidate = {
            'title': summary or f'@{screen_name} 的推文',
            'url': url,
            'channel': 'X',
            'source': f'@{screen_name}',
            'published_at': item.get('createdAtISO') or item.get('createdAtLocal') or item.get('createdAt') or '',
            'summary': summary,
            'why_pick': f'互动数据：{likes} likes / {retweets} retweets / {quotes} quotes',
            'recommend_full_fetch': 'yes' if idx < 3 else 'no',
            'raw': item,
        }

        if mode == 'source' or idx < 3:
            strong_recommend.append(candidate)
        else:
            watch_list.append(candidate)

    return strong_recommend, watch_list


def handle_x(args):
    if which('twitter') is None:
        raise RuntimeError('未找到 twitter 命令，请先安装 twitter-cli。')

    if args.mode == 'source':
        if not args.x_from:
            raise RuntimeError('X 定向来源搜索需要提供 --x-from。')
        cmd = [
            'twitter', 'user-posts', args.x_from,
            '-n', str(args.limit),
            '--json',
        ]
    else:
        query = args.query or args.topic
        if not query:
            raise RuntimeError('X 搜索需要提供 --query 或 --topic。')
        cmd = ['twitter', 'search', query, '--json', '-n', str(args.limit)]
        if args.search_type:
            cmd += ['--type', args.search_type]
        if args.x_from:
            cmd += ['--from', args.x_from]
        if args.since:
            cmd += ['--since', args.since]
        if args.until:
            cmd += ['--until', args.until]
        if args.lang:
            cmd += ['--lang', args.lang]

    payload = run_twitter_command(cmd)
    strong_recommend, watch_list = build_x_candidates(payload, args.mode)

    return {
        'ok': True,
        'mode': args.mode,
        'topic': args.topic,
        'query': args.query,
        'channels': args.channels,
        'limit': args.limit,
        'status': 'x-enabled-minimal',
        'message': 'X 已接入最小可用搜索能力，可用于指定账号最近内容和时间窗口搜索。',
        'strong_recommend': strong_recommend,
        'watch_list': watch_list,
        'skip_list': [],
    }


def build_skeleton_result(args):
    return {
        'ok': True,
        'mode': args.mode,
        'topic': args.topic,
        'query': args.query,
        'channels': args.channels,
        'limit': args.limit,
        'status': 'skeleton-only',
        'message': '这是 content-search-layer 的当前实现：X 已最小接入，其余渠道仍主要是骨架或既有外部流程。',
        'strong_recommend': [],
        'watch_list': [],
        'skip_list': [],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['hot', 'topic', 'source'], required=True)
    parser.add_argument('--topic', default='')
    parser.add_argument('--query', default='')
    parser.add_argument('--channels', nargs='*', default=[])
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--x-from', dest='x_from', default='')
    parser.add_argument('--since', default='')
    parser.add_argument('--until', default='')
    parser.add_argument('--lang', default='')
    parser.add_argument('--search-type', choices=['top', 'latest', 'photos', 'videos'], default='latest')
    args = parser.parse_args()

    try:
        channels = set(args.channels or [])
        if 'x' in channels or 'twitter' in channels:
            result = handle_x(args)
        else:
            result = build_skeleton_result(args)
    except Exception as exc:
        result = {
            'ok': False,
            'mode': args.mode,
            'topic': args.topic,
            'query': args.query,
            'channels': args.channels,
            'limit': args.limit,
            'status': 'error',
            'message': str(exc),
            'strong_recommend': [],
            'watch_list': [],
            'skip_list': [],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
