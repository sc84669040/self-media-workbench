from __future__ import annotations

import argparse
import re
from pathlib import Path

from self_media_config import REPO_ROOT


EXCLUDED_PARTS = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    "runtime",
    "config/local",
}
EXCLUDED_FILES = {
    ".env",
    "HANDOFF_FOR_CODEX.md",
    "scripts/scan_secrets.py",
}

PATTERNS = [
    ("openai-style-key", re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}\b")),
    ("private-windows-root", re.compile(r"D:[/\\]self-media(?:[/\\]|$)", re.I)),
    ("private-ai-vault", re.compile(r"D:[/\\]AI|/mnt/d/AI|AI知识库|AI鐭", re.I)),
    ("private-wsl-root", re.compile(r"/mnt/d/self-media|/home/nightwish", re.I)),
    ("private-user-path", re.compile(r"C:[/\\]Users[/\\]49615|/mnt/c/Users/49615", re.I)),
    ("cookie-file", re.compile(r"Netscape HTTP Cookie File|__Secure-", re.I)),
    ("x-token-value", re.compile(r"(auth_token|ct0)\s*[:=]\s*['\"][A-Za-z0-9%._\-]{20,}", re.I)),
    ("telegram-token-value", re.compile(r"\b\d{6,}:[A-Za-z0-9_\-]{24,}\b")),
]


def should_skip(path: Path) -> bool:
    text = str(path.relative_to(REPO_ROOT)).replace("\\", "/")
    if text in EXCLUDED_FILES:
        return True
    return any(part in text.split("/") or text.startswith(part + "/") for part in EXCLUDED_PARTS)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    findings: list[tuple[str, str, int]] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file() or should_skip(path):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for line_no, line in enumerate(text.splitlines(), start=1):
            for name, pattern in PATTERNS:
                if pattern.search(line):
                    findings.append((name, str(path.relative_to(REPO_ROOT)), line_no))
                    if args.verbose:
                        print(f"{name}: {path.relative_to(REPO_ROOT)}:{line_no}: {line[:160]}")

    if findings:
        print("Secret/path scan failed:")
        for name, rel, line_no in findings[:80]:
            print(f"- {name}: {rel}:{line_no}")
        if len(findings) > 80:
            print(f"- ... {len(findings) - 80} more")
        return 1

    print("Secret/path scan passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
