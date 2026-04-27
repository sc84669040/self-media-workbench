---
name: creative-studio
description: Local creative workflow UI built on top of the sample vault, content index, event radar, and topic packets.
---

# Creative Studio

This app provides the creative dashboard, topic packets, event packets, creation workspace, and writing workflow.

## Run

```bash
python apps/creative-studio/web/search_dashboard.py
```

The default service binding is read from `config/default.yaml` and can be overridden in `config/local/local.yaml`.

All databases, vault roots, model providers, writer adapters, and scheduler settings are managed from the root config directory.
