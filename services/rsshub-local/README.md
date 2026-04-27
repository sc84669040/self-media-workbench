# Optional RSSHub service

RSSHub is treated as an optional external service. The public workbench does not vendor a full RSSHub checkout or `node_modules`.

If you need RSSHub locally, run an official image or an independently managed checkout, then set:

```yaml
services:
  rsshub:
    enabled: true
    base_url: http://127.0.0.1:1200
```

