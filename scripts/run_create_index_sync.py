from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CREATIVE_STUDIO_SCRIPTS = REPO_ROOT / "apps" / "creative-studio" / "scripts"
if str(CREATIVE_STUDIO_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(CREATIVE_STUDIO_SCRIPTS))

from index_sync_service import run_create_studio_index_sync  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync NightHawk and vault notes into the Creative Studio index.")
    parser.add_argument("--config-path", default="", help="Optional create-studio compatible config path.")
    parser.add_argument("--full", action="store_true", help="Run a full sync instead of an incremental sync.")
    parser.add_argument("--limit-per-phase", type=int, default=0, help="Optional per-phase limit; 0 means unlimited.")
    args = parser.parse_args()

    result = run_create_studio_index_sync(
        config_path=args.config_path or None,
        full=bool(args.full),
        limit_per_phase=max(0, int(args.limit_per_phase or 0)),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
