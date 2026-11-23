from __future__ import annotations

from ia.ollama_client import OllamaClient
from ia.query_planner import parse_plan, planner_prompt
from ia.fallback_planner import combined_department_average_plan, department_average_plan
from backend.connectors.sql.sqlite_connector import SQLiteConnector
from backend.connectors.mongodb.mongo_connector import MongoConnector
from backend.connectors.neo4j.neo4j_connector import Neo4jConnector
from backend.query_engine import QueryEngine
from backend.sources.source_registry import SourceRegistry
from config import UPLOAD_DIR


class Orchestrator:
    def __init__(self, registry: SourceRegistry | None = None, upload_dir=UPLOAD_DIR, llm=None):
        self.registry = registry or SourceRegistry()
        self.llm = llm or OllamaClient()
        self.query_engine = QueryEngine(self.registry, upload_dir)

    def test_source(self, source):
        kind, connection = source["kind"], source["connection"]
        try:
            if kind == "sqlite":
                SQLiteConnector(connection).test()
            elif kind == "mongodb":
                MongoConnector(connection).test()
            elif kind == "neo4j":
                Neo4jConnector(connection).test()
            else:
                return {"ok": False, "message": "Type non supporté"}
            return {"ok": True, "message": "Connexion réussie"}
        except Exception:
            # Avoid returning URIs that can contain usernames/passwords.
            return {"ok": False, "message": "Connexion impossible. Vérifie l'adresse, le serveur et les identifiants de la source."}

    def model_status(self):
        return self.llm.status()

    def discover_schema(self, source_id: str):
        inventory = self.query_engine.discover([source_id], [])
        return self._public_schema(inventory[source_id])

    def answer(self, question: str, source_ids: list[str], file_ids: list[str] | None = None):
        file_ids = file_ids or []
        try:
            inventory_map = self.query_engine.discover(source_ids, file_ids)
        except Exception:
            return self._failed(question, "schema_error",
                                "Impossible de découvrir les schémas ou d'accéder aux sources sélectionnées. Vérifie les connexions et les autorisations de lecture.")

        status = self.llm.status()
        if not status.get("available"):
            return self._failed(question, "planner_unavailable",
                                 "Ollama n'est pas disponible. Démarre Ollama avec le modèle configuré avant d'interroger les données.")
        fallback = lambda: (combined_department_average_plan(question, inventory_map) or
                            department_average_plan(question, inventory_map))
        try:
            raw_plan = self.llm.generate_json(planner_prompt(question, list(inventory_map.values())))
            plan = parse_plan(raw_plan)
            asks_to_combine = len(inventory_map) > 1 and bool(
                __import__("re").search(r"(relie|relier|joint|join|combine|fusionne)", question.casefold()))
            plan_sources = {operation.source_id for operation in plan.operations}
            if not plan.operations or (asks_to_combine and plan_sources != set(inventory_map)):
                plan = fallback()
                if plan is None:
                    return {**self._failed(question, "refused",
                                           "Le modèle n'a pas produit de jointure exploitable. Vérifie les clés communes entre les sources."),
                            "plan": None, "source_ids": source_ids, "file_ids": file_ids}
            try:
                result = self.query_engine.execute(plan, inventory_map)
            except Exception:
                alternate = fallback()
                if alternate is None:
                    raise
                result = self.query_engine.execute(alternate, inventory_map)
            result.update({"question": question, "source_ids": source_ids, "file_ids": file_ids})
            return result
        except Exception:
            alternate = fallback()
            if alternate is not None:
                try:
                    result = self.query_engine.execute(alternate, inventory_map)
                    result.update({"question": question, "source_ids": source_ids, "file_ids": file_ids})
                    return result
                except Exception:
                    pass
            return self._failed(question, "plan_error", "Le plan généré n'a pas pu être validé ou exécuté. Vérifie les colonnes et les clés de jointure, puis reformule la question.",
                                source_ids=source_ids, file_ids=file_ids)

    @staticmethod
    def _public_schema(schema):
        return {key: value for key, value in schema.items() if key not in {"connection", "storage_id"}}

    @staticmethod
    def _failed(question, status, notice, source_ids=None, file_ids=None):
        return {"question": question, "data_executed": False, "execution_status": status,
                "answer": notice, "notice": notice, "rows": [], "columns": [], "row_count": 0,
                "provenance": [], "warnings": [], "source_ids": source_ids or [], "file_ids": file_ids or [],
                "read_only": True}
