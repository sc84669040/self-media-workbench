# self-media workbench

Local-first content discovery, body fetching, topic packaging, and writing workspace.

This repository is the public, deployable version of the original local project. The source project remains untouched; all runtime data and private settings are kept outside tracked files.

## Quick Start

```powershell
cd <your-clone>\self-media-github
python -m venv .venv
.\.venv\Scripts\pip install -e ".[dev]"
copy config\examples\local.example.yaml config\local\local.yaml
python scripts\check_env.py
python scripts\init_runtime.py --profile sample
python scripts\start_local.py
```

Open `http://127.0.0.1:8791`.

The Creative Studio root page is now the local entry portal:

- `/`: entry portal for content intelligence, Creative Studio, and configuration.
- `/create`: Creative Studio front stage.
- `/config`: configuration center for paths, ports, credentials, channels, tools, scheduler jobs, template download, and template import.
- `/search`: Creative Studio's embedded search dashboard. The dedicated content-search-layer service still runs on its configured port.

The sample profile works without API keys, cookies, X credentials, RSSHub, or a private knowledge vault. Advanced channels stay disabled until configured.

## Configuration

Edit only:

```text
config/local/local.yaml
.env
```

Everything users need to fill in is centralized there: paths, ports, API keys, cookies paths, channel source lists, RSSHub URL, and scheduler jobs. `.env` is loaded automatically and never overrides variables already set in your shell.

Tracked defaults live in `config/default.yaml`. Private local config is ignored by Git.

You can edit the same private config from the web UI at `/config`. The page writes the currently selected config path, which defaults to `config/local/local.yaml`; if `SELF_MEDIA_CONFIG_PATH` is set, the page edits that selected file.

## Apps

- `apps/content-fetch-hub`: URL body extraction and optional vault writing.
- `apps/content-search-layer`: candidate discovery and NightHawk-style event storage.
- `apps/creative-studio`: topic packets, material retrieval, writing workspace, and UI.
- `services/rsshub-local`: optional RSSHub local service notes.

## Safety Rules

- Do not commit `runtime/`, `config/local/local.yaml`, `.env`, cookies, tokens, database files, or logs.
- Run `python scripts\scan_secrets.py` before publishing.
- If a key was ever copied into a tracked file, rotate it before pushing.

## Scheduler

All scheduled jobs are configured in `config/local/local.yaml` under `scheduler.jobs`.

Run enabled jobs once:

```powershell
python scripts\scheduler.py --once
```

Run continuously:

```powershell
python scripts\scheduler.py
```

By default, jobs that require external accounts or network credentials are disabled.

`python scripts\start_local.py` starts the search layer and Creative Studio. Add `--with-fetch-hub` only when you also want the optional fetch hub API.
