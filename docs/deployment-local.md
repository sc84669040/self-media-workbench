# Local deployment

1. Install Python 3.11 or newer.
2. Create and activate a virtual environment.
3. Install dependencies with `pip install -e ".[dev]"`.
4. Copy `config/examples/local.example.yaml` to `config/local/local.yaml`.
5. Run `python scripts/init_runtime.py --profile sample`.
6. Run `python scripts/start_local.py`.

All generated files stay under `runtime/` unless you explicitly change paths in `config/local/local.yaml`.

`scripts/start_local.py` starts `content-search-layer` and `creative-studio` by default. Use `--with-fetch-hub` when you also want the optional fetch API.

Values in `.env` are loaded automatically, but existing shell environment variables take precedence.
