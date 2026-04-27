---
name: content-fetch-hub
description: Unified content body fetching hub for URL, batch, transcript, and normalized markdown workflows.
---

# Content Fetch Hub

This app fetches body content from supported URLs and can return normalized JSON or write markdown into the configured vault/output directory.

## Run

```bash
python apps/content-fetch-hub/scripts/fetch_content_cli.py "<url>" --json
```

Batch mode:

```bash
python apps/content-fetch-hub/scripts/fetch_content_cli.py --file urls.txt --json
```

Transcript analysis mode:

```bash
python apps/content-fetch-hub/scripts/fetch_content_cli.py "<youtube-or-douyin-url>" --analyze --json
```

All paths, cookies, optional external tools, and write behavior are configured from `config/local/local.yaml`.
