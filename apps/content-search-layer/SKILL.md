---
name: content-search-layer
description: Local content search and event radar layer for collected self-media sources.
---

# Content Search Layer

This app provides the local search dashboard, event database, source collectors, and topic pipeline.

## Run

```bash
python apps/content-search-layer/web/search_dashboard.py
```

The default service binding is read from `config/default.yaml` and can be overridden in `config/local/local.yaml`.

All databases, source lists, credentials, and scheduler settings are managed from the root config directory.
