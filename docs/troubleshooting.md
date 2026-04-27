# Troubleshooting

- Page opens but advanced channels are unavailable: fill the relevant credentials or cookies path in `config/local/local.yaml` and `.env`.
- YouTube or Bilibili transcript fetch fails: install `yt-dlp` or set `YT_DLP_BIN`.
- X search fails: configure `TWITTER_AUTH_TOKEN`, `TWITTER_CT0`, and optionally `TWITTER_BIN`.
- Writing is disabled: set `creative_studio.writing.provider` and provide `CREATE_STUDIO_WRITING_API_KEY`.
- Creative Studio is slow to show the NightHawk status: keep `creative_studio.nighthawk.enable_upstream_raw_sync` disabled unless `content-search-layer` is running and reachable.
- Secret scan fails: remove private paths or credentials from tracked files, then rotate any exposed key.
