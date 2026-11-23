import json
import sqlite3
from uuid import uuid4
from config import METADATA_DB

class SourceRegistry:
    def __init__(self):
        with sqlite3.connect(METADATA_DB) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                connection TEXT NOT NULL)""")

    def add_source(self, name, kind, connection):
        source_id = uuid4().hex
        with sqlite3.connect(METADATA_DB) as db:
            db.execute("INSERT INTO sources VALUES (?, ?, ?, ?)",
                       (source_id, name, kind, connection))
        return {"id": source_id, "name": name, "kind": kind}

    def list_sources(self):
        with sqlite3.connect(METADATA_DB) as db:
            rows = db.execute("SELECT id,name,kind FROM sources ORDER BY name").fetchall()
        return [{"id": r[0], "name": r[1], "kind": r[2]} for r in rows]

    def get_source(self, source_id):
        with sqlite3.connect(METADATA_DB) as db:
            row = db.execute("SELECT id,name,kind,connection FROM sources WHERE id=?",
                             (source_id,)).fetchone()
        return dict(zip(("id","name","kind","connection"), row)) if row else None

    def delete_source(self, source_id):
        with sqlite3.connect(METADATA_DB) as db:
            cur = db.execute("DELETE FROM sources WHERE id=?", (source_id,))
        return cur.rowcount > 0
