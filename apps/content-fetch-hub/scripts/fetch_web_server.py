#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fetch_content_cli import load_settings, run_batch

REPO_SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
if str(REPO_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(REPO_SCRIPTS))

from self_media_config import get_config, get_path, service  # noqa: E402

BASE_DIR = Path(__file__).resolve().parents[1]
WEB_INDEX = BASE_DIR / "web" / "index.html"


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, code: int, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/", "/index.html"):
            self._send_json(404, {"ok": False, "error": "not-found"})
            return
        if not WEB_INDEX.exists():
            self._send_html(500, "<h1>index.html not found</h1>")
            return
        self._send_html(200, WEB_INDEX.read_text(encoding="utf-8"))

    def do_POST(self) -> None:  # noqa: N802
        compat_aliases = {"/api/content-fetch", "/api/fetch-content"}
        if self.path not in {"/api/fetch", *compat_aliases}:
            self._send_json(404, {"ok": False, "error": "not-found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            payload = json.loads(raw or "{}")
        except Exception:
            self._send_json(400, {"ok": False, "error": "bad-json"})
            return

        urls: list[str] = []
        url = str(payload.get("url") or "").strip()
        if url:
            urls.append(url)

        urls_payload = payload.get("urls") or []
        if isinstance(urls_payload, list):
            urls.extend([str(x or "").strip() for x in urls_payload if str(x or "").strip()])

        urls_text = str(payload.get("urls_text") or "").strip()
        if urls_text:
            urls.extend([line.strip() for line in urls_text.splitlines() if line.strip() and not line.strip().startswith("#")])

        if not urls:
            self._send_json(400, {"ok": False, "error": "url-empty"})
            return

        try:
            settings = load_settings()
            vault = str(payload.get("vault") or settings.get("vault_path") or get_path(get_config(), "paths.sample_vault_path")).strip()
            retry_count = int(payload.get("retry_count", payload.get("retry", settings.get("retry_count", 1))))
            analyze = bool(payload.get("analyze", False))

            summary = run_batch(urls, vault=vault, retry_count=retry_count, analyze=analyze)
            if self.path in compat_aliases:
                summary["api_warning"] = "deprecated-path: use /api/fetch"
                summary["api_path"] = self.path
            self._send_json(200, summary)
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"ok": False, "error": f"server-error: {exc}"})


def main() -> None:
    fetch_service = service(get_config(), "content_fetch_hub")
    p = argparse.ArgumentParser(description="content-fetch-hub web server")
    p.add_argument("--host", default=str(os.getenv("FETCH_HUB_HOST") or fetch_service.get("host") or "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("FETCH_HUB_PORT") or fetch_service.get("port") or 8788))
    args = p.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"content-fetch-hub web ui running: http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
