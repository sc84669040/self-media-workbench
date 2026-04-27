#!/usr/bin/env python3
from pathlib import Path
import sqlite3

from runtime_config import event_radar_db_path

p = event_radar_db_path()
conn = sqlite3.connect(p)
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
print('tables', [r[0] for r in cur.fetchall()])
for t in ['accounts', 'raw_items', 'event_candidates', 'event_evidence']:
    cur.execute(f'SELECT COUNT(*) FROM {t}')
    print(t, cur.fetchone()[0])
conn.close()
