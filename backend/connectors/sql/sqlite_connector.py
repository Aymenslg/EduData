import sqlite3
import re
from pathlib import Path

_READ_ONLY = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)

class SQLiteConnector:
    def __init__(self, database_path):
        self.database_path = database_path

    def test(self):
        with sqlite3.connect(self._uri(), uri=True, timeout=5) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("SELECT 1").fetchone()

    def _uri(self):
        return Path(self.database_path).resolve().as_uri() + "?mode=ro"

    def query(self, sql, limit=500):
        if not _READ_ONLY.match(sql):
            raise ValueError("Seules les requêtes SELECT/CTE sont autorisées.")
        # Refuse plusieurs instructions SQL.
        if ";" in sql:
            raise ValueError("Une seule instruction SQL est autorisée.")
        with sqlite3.connect(self._uri(), uri=True, timeout=5) as db:
            db.execute("PRAGMA query_only=ON")
            db.row_factory = sqlite3.Row
            cursor = db.execute(sql)
            rows = [dict(row) for row in cursor.fetchmany(limit)]
            return {"columns": [c[0] for c in cursor.description or []], "rows": rows}
