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


APP_ROOT = Path(__file__).resolve().parents[1]


def _drop_modules_from_app_root() -> None:
    for name, module in list(sys.modules.items()):
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        try:
            Path(module_file).resolve().relative_to(APP_ROOT)
        except ValueError:
            continue
        except OSError:
            continue
        sys.modules.pop(name, None)


@pytest.fixture()
def search_layer_server():
    _drop_modules_from_app_root()
    module_name = "_content_search_layer_boundary_dashboard"
    module_path = APP_ROOT / "web" / "search_dashboard.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    dashboard = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = dashboard
    spec.loader.exec_module(dashboard)
    assert dashboard.NIGHTHAWK_ONLY_MODE is True

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
        _drop_modules_from_app_root()
        sys.modules.pop(module_name, None)


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


def test_search_layer_does_not_serve_create_api(search_layer_server):
    status, payload = _request(search_layer_server, "GET", "/api/create/config")
    assert status == 404
    assert payload == {"ok": False, "message": "Not Found"}

    status, payload = _request(search_layer_server, "POST", "/api/create/tasks", {"topic": "sample"})
    assert status == 404
    assert payload == {"ok": False, "message": "Not Found"}
