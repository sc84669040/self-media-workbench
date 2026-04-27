from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def resolve_workspace_root(project_root: Path | None = None) -> Path:
    current_project_root = Path(project_root or PROJECT_ROOT).resolve()
    for candidate in [current_project_root, *current_project_root.parents]:
        if (candidate / "config" / "default.yaml").exists():
            return candidate
    return current_project_root.parent


WORKSPACE_ROOT = resolve_workspace_root()
APPS_ROOT = WORKSPACE_ROOT / "apps"
SKILLS_ROOT = APPS_ROOT
MONITORING_ROOT = WORKSPACE_ROOT / "config" / "sources"
VENV_ROOT = WORKSPACE_ROOT / ".venvs"


def resolve_skill_project_root(skill_name: str) -> Path:
    return APPS_ROOT / skill_name
