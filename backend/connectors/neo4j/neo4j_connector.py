from neo4j import GraphDatabase, READ_ACCESS
import re

class Neo4jConnector:
    def __init__(self, uri, username=None, password=None):
        # Format de connexion : bolt://user:password@localhost:7687
        from urllib.parse import urlparse, unquote
        parsed = urlparse(uri)
        self.uri = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 7687}"
        self.username = unquote(parsed.username or username or "")
        self.password = unquote(parsed.password or password or "")

    def test(self):
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        try:
            driver.verify_connectivity()
        finally:
            driver.close()

    def read(self, cypher, parameters=None, limit=200):
        # Barrière applicative : autorise seulement MATCH/OPTIONAL MATCH/RETURN/WITH.
        if not re.match(r"^\s*(MATCH|OPTIONAL\s+MATCH|WITH)\b", cypher, re.I):
            raise ValueError("Seules les requêtes Cypher de lecture sont autorisées.")
        forbidden = r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|CALL|LOAD\s+CSV)\b"
        if re.search(forbidden, cypher, re.I):
            raise ValueError("Instruction Cypher non autorisée.")
        if ";" in cypher:
            raise ValueError("Une seule instruction Cypher est autorisée.")
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        try:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                def read_transaction(tx):
                    result = tx.run(cypher, parameters or {})
                    rows = []
                    for record in result:
                        if len(rows) >= min(max(limit, 1), 5000):
                            break
                        rows.append(record.data())
                    return rows
                return session.execute_read(read_transaction, timeout=10)
        finally:
            driver.close()
