from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"


def test_fetch_router_imports_without_optional_bs4_installed():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                f"sys.path.insert(0, {str(SCRIPTS_DIR)!r}); "
                "import fetch_router; "
                "print('ok')"
            ),
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT_DIR),
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "ok"
