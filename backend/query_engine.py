"""Schema discovery, validation and read-only execution for selected data sources."""
from __future__ import annotations

import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd

from ia.query_planner import QueryPlan, QueryOperation, FilterCondition
from backend.connectors.mongodb.mongo_connector import MongoConnector
from backend.connectors.neo4j.neo4j_connector import Neo4jConnector
from backend.ingestion.tabular_processor import load_table
from backend.sources.source_registry import SourceRegistry

_FILE_ID = re.compile(r"[0-9a-f]{32}(?:\.(?:pdf|csv|xlsx|xls|json)|_[\w .-]{1,100})\Z")
_SAFE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_OPERATOR_SQL = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
_OPERATOR_MONGO = {"eq": "$eq", "ne": "$ne", "gt": "$gt", "gte": "$gte", "lt": "$lt", "lte": "$lte"}
MAX_SOURCE_ROWS = 5000
MAX_RESULT_ROWS = 10000


def _sqlite_uri(database: str | Path) -> str:
    return Path(database).resolve().as_uri() + "?mode=ro"


def _json_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return json.loads(frame.to_json(orient="records", date_format="iso", force_ascii=False))


class QueryEngine:
    def __init__(self, registry: SourceRegistry, upload_dir: Path):
        self.registry = registry
        self.upload_dir = Path(upload_dir)

    def discover(self, source_ids: list[str], file_ids: list[str]) -> dict[str, dict[str, Any]]:
        selected = list(dict.fromkeys(source_ids + file_ids))
        if len(selected) > 5:
            raise ValueError("Sélectionne au maximum cinq sources/fichiers pour une analyse.")
        if not selected:
            raise ValueError("Sélectionne au moins une base ou un fichier.")
        inventory: dict[str, dict[str, Any]] = {}
        for source_id in source_ids:
            source = self.registry.get_source(source_id)
            if not source:
                raise ValueError("Une source sélectionnée n'existe plus.")
            inventory[source_id] = self._discover_source(source)
        file_number = 0
        for file_id in file_ids:
            if not _FILE_ID.fullmatch(file_id):
                raise ValueError("Identifiant de fichier invalide.")
            path = (self.upload_dir.resolve() / file_id).resolve()
            if path.parent != self.upload_dir.resolve() or not path.is_file():
                raise ValueError("Un fichier sélectionné est introuvable.")
            if path.suffix.lower() == ".pdf":
                raise ValueError("Les PDF ne sont pas des tables. Utilise un fichier CSV, Excel ou JSON.")
            if path.suffix.lower() in {".xlsx", ".xls"}:
                excel = pd.ExcelFile(path)
                objects = []
                for sheet_name in excel.sheet_names:
                    frame = pd.read_excel(excel, sheet_name=sheet_name, nrows=20000)
                    objects.append({"name": sheet_name, "columns": self._frame_columns(frame), "rows_available": len(frame)})
            else:
                frame = load_table(path)
                objects = [{"name": "table", "columns": self._frame_columns(frame), "rows_available": len(frame)}]
            file_number += 1
            alias = f"file{file_number}"
            metadata_path = path.with_name(path.name + ".meta.json")
            if metadata_path.is_file():
                try:
                    display_name = json.loads(metadata_path.read_text(encoding="utf-8")).get("filename", path.name)
                except Exception:
                    display_name = "Fichier " + path.suffix.lstrip(".")
            elif "_" in path.name[:33]:
                display_name = path.name[33:]
            else:
                display_name = "Fichier " + path.suffix.lstrip(".")
            inventory[alias] = {
                "source_id": alias, "storage_id": file_id, "name": display_name, "kind": "file", "objects": objects,
            }
        return inventory

    def _discover_source(self, source: dict[str, str]) -> dict[str, Any]:
        kind, connection = source["kind"], source["connection"]
        objects: list[dict[str, Any]] = []
        if kind == "sqlite":
            with sqlite3.connect(_sqlite_uri(connection), uri=True, timeout=5) as db:
                names = db.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
                for name, object_type in names:
                    quoted_name = '"' + name.replace('"', '""') + '"'
                    cols = db.execute(f"PRAGMA table_info({quoted_name})").fetchall()
                    objects.append({"name": name, "type": object_type,
                                    "columns": [{"name": c[1], "type": c[2] or "unknown"} for c in cols]})
        elif kind == "mongodb":
            connector = MongoConnector(connection)
            database = connector.database_name()
            for name in connector.list_collections(database):
                sample = connector.sample(database, name)
                cols: dict[str, str] = {}
                for doc in sample:
                    for key, value in doc.items():
                        cols.setdefault(key, type(value).__name__)
                objects.append({"name": name, "columns": [{"name": k, "type": v} for k, v in sorted(cols.items())]})
        elif kind == "neo4j":
            connector = Neo4jConnector(connection)
            labels: dict[str, dict[str, str]] = {}
            for row in connector.read("MATCH (n) RETURN labels(n) AS labels, properties(n) AS properties LIMIT 500", limit=500):
                properties = row.get("properties") or {}
                for label in row.get("labels") or []:
                    label_schema = labels.setdefault(label, {})
                    for prop, value in properties.items():
                        value_type = self._value_type(value)
                        if prop not in label_schema:
                            label_schema[prop] = value_type
                        elif label_schema[prop] != value_type:
                            label_schema[prop] = "mixed"
            relationships: set[tuple[str, str, str]] = set()
            for row in connector.read("MATCH (a)-[r]->(b) RETURN labels(a) AS from_labels, type(r) AS type, labels(b) AS to_labels LIMIT 500", limit=500):
                for source_label in row.get("from_labels") or []:
                    for target_label in row.get("to_labels") or []:
                        relationships.add((source_label, row.get("type", ""), target_label))
            objects = [{"name": label, "type": "node_label", "columns": [{"name": p, "type": props[p]} for p in sorted(props)]}
                       for label, props in sorted(labels.items())]
            return {"source_id": source["id"], "name": source["name"], "kind": kind,
                    "connection": connection, "objects": objects,
                    "relationships": [{"from": a, "type": b, "to": c} for a, b, c in sorted(relationships)]}
        else:
            raise ValueError(f"Le type de source {kind} n'est pas pris en charge pour les requêtes.")
        if not objects:
            raise ValueError(f"Aucune table/collection lisible trouvée dans {source['name']}.")
        return {"source_id": source["id"], "name": source["name"], "kind": kind,
                "connection": connection, "objects": objects}

    @staticmethod
    def _frame_columns(frame: pd.DataFrame) -> list[dict[str, str]]:
        return [{"name": str(c), "type": str(frame[c].dtype)} for c in frame.columns]

    @staticmethod
    def _value_type(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        return type(value).__name__

    def validate_plan(self, plan: QueryPlan, inventory: dict[str, dict[str, Any]]) -> None:
        allowed_ids = set(inventory)
        operations = {op.operation_id: op for op in plan.operations}
        if len(operations) != len(plan.operations):
            raise ValueError("Chaque opération du plan doit avoir un identifiant unique.")
        source_objects = [(op.source_id, op.object_name) for op in plan.operations]
        if len(set(source_objects)) != len(source_objects):
            raise ValueError("Le même objet ne peut pas apparaître deux fois dans le plan.")
        if not plan.operations:
            return
        for op in plan.operations:
            if len(set(op.columns)) != len(op.columns):
                raise ValueError("Une opération ne doit pas répéter une colonne.")
            if op.source_id not in allowed_ids:
                raise ValueError("Le plan essaie d'interroger une source qui n'a pas été sélectionnée.")
            meta = inventory[op.source_id]
            obj = next((item for item in meta["objects"] if item["name"] == op.object_name), None)
            if not obj:
                raise ValueError(f"Objet {op.object_name} absent du schéma autorisé de {meta['name']}.")
            schema_columns = {column["name"] for column in obj["columns"]}
            if meta["kind"] == "neo4j":
                if op.target_label:
                    edge = {"from": op.object_name, "type": op.relationship_type, "to": op.target_label}
                    reverse = {"from": op.target_label, "type": op.relationship_type, "to": op.object_name}
                    edge_ok = edge in meta.get("relationships", [])
                    reverse_ok = reverse in meta.get("relationships", [])
                    if not (edge_ok or (op.direction == "incoming" and reverse_ok) or (op.direction == "both" and (edge_ok or reverse_ok))):
                        raise ValueError("La relation Neo4j demandée n'existe pas dans le schéma découvert.")
                    target = next((it for it in meta["objects"] if it["name"] == op.target_label), None)
                    if not target:
                        raise ValueError("Le label Neo4j cible est inconnu.")
                    valid_columns = {f"source_{c}" for c in schema_columns} | {f"target_{c['name']}" for c in target["columns"]}
                else:
                    valid_columns = schema_columns
            else:
                valid_columns = schema_columns
            for column in op.columns:
                if column not in valid_columns:
                    raise ValueError(f"Colonne {column} absente du schéma de {meta['name']}.{op.object_name}.")
            for condition in op.filters:
                if condition.column not in valid_columns:
                    raise ValueError(f"Filtre sur une colonne inconnue : {condition.column}.")
                if meta["kind"] == "mongodb" and condition.column.startswith("$"):
                    raise ValueError("Les noms de champs MongoDB commençant par $ ne sont pas pris en charge.")
                if condition.operator == "in" and (not isinstance(condition.value, list) or len(condition.value) > 100):
                    raise ValueError("Un filtre 'in' doit contenir une liste de 100 valeurs maximum.")
                if condition.operator == "contains" and not isinstance(condition.value, str):
                    raise ValueError("Le filtre 'contains' attend du texte.")
        for join in plan.joins:
            self._validate_ref(join.left, operations)
            self._validate_ref(join.right, operations)
            if join.left.operation_id == join.right.operation_id:
                raise ValueError("Une jointure doit relier deux sources différentes.")
        for ref in plan.group_by:
            self._validate_ref(ref, operations)
        for metric in plan.metrics:
            if metric.operation_id not in operations:
                raise ValueError("Une mesure référence une source qui n'est pas interrogée.")
            if metric.column and metric.column not in operations[metric.operation_id].columns:
                raise ValueError(f"La mesure {metric.column} ne fait pas partie des colonnes demandées.")
            if metric.function != "count" and metric.column:
                metric_op = operations[metric.operation_id]
                metric_meta = inventory[metric_op.source_id]
                metric_obj = next(item for item in metric_meta["objects"] if item["name"] == metric_op.object_name)
                metric_column = metric.column
                if metric_meta["kind"] == "neo4j":
                    metric_label = metric_op.object_name
                    if metric_op.target_label:
                        if metric_column.startswith("source_"):
                            metric_column = metric_column.removeprefix("source_")
                        elif metric_column.startswith("target_"):
                            metric_column = metric_column.removeprefix("target_")
                            metric_label = metric_op.target_label
                    metric_obj = next(item for item in metric_meta["objects"] if item["name"] == metric_label)
                metric_type = next((c.get("type", "").lower() for c in metric_obj["columns"]
                                    if c["name"] == metric_column), "")
                if metric_type != "property" and not any(token in metric_type for token in
                    ("int", "real", "float", "double", "decimal", "numeric", "number", "long")):
                    raise ValueError(f"La mesure {metric.column} doit être numérique pour {metric.function}.")
        aliases = [metric.alias for metric in plan.metrics]
        if len(set(aliases)) != len(aliases):
            raise ValueError("Les noms des mesures doivent être uniques.")
        group_refs = [(ref.operation_id, ref.column) for ref in plan.group_by]
        if len(set(group_refs)) != len(group_refs):
            raise ValueError("Une dimension ne doit pas être répétée.")
        if len(plan.operations) > 1:
            joined = {plan.joins[0].left.operation_id, plan.joins[0].right.operation_id} if plan.joins else set()
            changed = True
            while changed:
                changed = False
                for join in plan.joins:
                    if join.left.operation_id in joined or join.right.operation_id in joined:
                        before = len(joined)
                        joined.update((join.left.operation_id, join.right.operation_id))
                        changed = changed or len(joined) != before
            if joined != set(operations):
                raise ValueError("Les sources doivent être reliées par des clés de jointure explicites.")

    @staticmethod
    def _validate_ref(ref, operations):
        op = operations.get(ref.operation_id)
        if not op:
            raise ValueError("Une jointure ou agrégation référence une source non interrogée.")
        if ref.column not in op.columns:
            raise ValueError(f"Ajoute la colonne {ref.column} aux colonnes à lire pour cette source.")

    def execute(self, plan: QueryPlan, inventory: dict[str, dict[str, Any]]) -> dict[str, Any]:
        self.validate_plan(plan, inventory)
        if not plan.operations:
            return {"data_executed": False, "execution_status": "refused", "rows": [],
                    "notice": plan.explanation, "provenance": [], "warnings": []}
        frames: dict[str, pd.DataFrame] = {}
        provenance = []
        warnings: list[str] = []
        labels: dict[tuple[str, str], str] = {}
        for op in plan.operations:
            meta = inventory[op.source_id]
            obj = next(item for item in meta["objects"] if item["name"] == op.object_name)
            required = list(dict.fromkeys(op.columns + [f.column for f in op.filters]))
            frame = self._execute_operation(op, meta, obj, required)
            if len(frame) >= op.limit:
                warnings.append(f"{meta['name']}: résultat limité à {op.limit} lignes.")
            display_columns = {}
            for column in frame.columns:
                display = f"{meta['name']} · {op.object_name} · {column}"
                labels[(op.operation_id, column)] = display
                display_columns[column] = display
            frame = frame.rename(columns=display_columns)
            frames[op.operation_id] = frame
            provenance.append({"source": meta["name"], "kind": meta["kind"], "object": op.object_name,
                               "filters": [f.model_dump(mode="json") for f in op.filters],
                               "rows_read": int(len(frame)), "columns": required,
                               "read_only": True})

        first_operation = plan.operations[0].operation_id
        result = frames[first_operation].copy()
        merged_operations = {first_operation}
        for join in plan.joins:
            if join.left.operation_id in merged_operations and join.right.operation_id not in merged_operations:
                left_ref, right_ref = join.left, join.right
            elif join.right.operation_id in merged_operations and join.left.operation_id not in merged_operations:
                left_ref, right_ref = join.right, join.left
            else:
                raise ValueError("Les jointures doivent former un arbre reliant les sources sélectionnées.")
            left_key = labels[(left_ref.operation_id, left_ref.column)]
            right_key = labels[(right_ref.operation_id, right_ref.column)]
            left_nulls = int(result[left_key].isna().sum())
            right_nulls = int(frames[right_ref.operation_id][right_key].isna().sum())
            left = result.copy()
            right = frames[right_ref.operation_id].copy()
            if left_nulls or right_nulls:
                warnings.append(f"Jointure {left_ref.column} ↔ {right_ref.column}: {left_nulls + right_nulls} clé(s) vide(s) ne correspondent pas.")
            left_counts = left.loc[left[left_key].notna(), left_key].value_counts()
            right_counts = right.loc[right[right_key].notna(), right_key].value_counts()
            matched_rows = sum(int(n) * int(right_counts.get(key, 0)) for key, n in left_counts.items())
            matched_left = sum(int(n) for key, n in left_counts.items() if key in right_counts)
            matched_right = sum(int(n) for key, n in right_counts.items() if key in left_counts)
            estimated = matched_rows
            if join.how in {"left", "outer"}:
                estimated += len(left) - matched_left
            if join.how == "outer":
                estimated += len(right) - matched_right
            if estimated > MAX_RESULT_ROWS * 2:
                raise ValueError("La jointure produirait trop de lignes. Ajoute des filtres ou une agrégation.")
            duplicate_keys = int(bool(left.loc[left[left_key].notna(), left_key].duplicated().sum())) + int(bool(right.loc[right[right_key].notna(), right_key].duplicated().sum()))
            if duplicate_keys:
                warnings.append(f"Jointure {left_ref.column} ↔ {right_ref.column}: clés répétées détectées; la cardinalité peut multiplier les lignes.")
            before = len(left)
            left_sentinel, right_sentinel = object(), object()
            left[left_key] = left[left_key].astype(object).where(left[left_key].notna(), left_sentinel)
            right[right_key] = right[right_key].astype(object).where(right[right_key].notna(), right_sentinel)
            result = left.merge(right, how=join.how, left_on=left_key, right_on=right_key,
                                suffixes=("_gauche", "_droite"), indicator="_correspondance")
            result[left_key] = result[left_key].map(lambda value: pd.NA if value is left_sentinel else value)
            if right_key in result.columns:
                result[right_key] = result[right_key].map(lambda value: pd.NA if value is right_sentinel else value)
            matched = int((result["_correspondance"] == "both").sum())
            not_matched = int(len(result) - matched)
            provenance.append({"join": f"{left_ref.column} = {right_ref.column}", "type": join.how,
                               "rows_before": before, "rows_after": int(len(result)),
                               "matched_rows": matched, "unmatched_rows": not_matched})
            result = result.drop(columns=["_correspondance"])
            merged_operations.add(right_ref.operation_id)
            if len(result) > MAX_RESULT_ROWS:
                raise ValueError("Le résultat dépasse la limite de lignes. Ajoute une agrégation.")

        if plan.group_by or plan.metrics:
            group_cols = [labels[(ref.operation_id, ref.column)] for ref in plan.group_by]
            if group_cols:
                grouped = result.groupby(group_cols, dropna=False, sort=True)
                output = grouped.size().rename("_row_count").reset_index()[group_cols]
            else:
                grouped = None
                output = pd.DataFrame([{}])
            for metric in plan.metrics:
                if metric.function == "count" and not metric.column:
                    values = grouped.size() if grouped is not None else pd.Series([len(result)])
                else:
                    metric_col = labels[(metric.operation_id, metric.column)]
                    values = pd.to_numeric(result[metric_col], errors="coerce")
                    if metric.function == "count":
                        values = (grouped[metric_col].count() if grouped is not None else pd.Series([values.count()]))
                    else:
                        fn = {"sum": "sum", "avg": "mean", "min": "min", "max": "max"}[metric.function]
                        if grouped is None:
                            values = getattr(values, fn)()
                        else:
                            numeric = pd.to_numeric(result[metric_col], errors="coerce")
                            values = getattr(numeric.groupby([result[c] for c in group_cols], dropna=False, sort=True), fn)()
                output[metric.alias] = values.to_numpy() if hasattr(values, "to_numpy") else values
            result = output

        if len(result) > MAX_RESULT_ROWS:
            warnings.append(f"Le résultat affiché est limité à {MAX_RESULT_ROWS} lignes.")
        result = result.head(MAX_RESULT_ROWS)
        records = _json_records(result)
        executed = bool(records)
        group_labels = [labels[(ref.operation_id, ref.column)] for ref in plan.group_by]
        summary = self._answer_from_results(plan, result, len(plan.operations), group_labels) if executed else "Les opérations de lecture ont été exécutées, mais aucun résultat n'a été trouvé."
        return {
            "data_executed": executed,
            "execution_status": "success" if executed else "no_data",
            "answer": summary,
            "plan": plan.model_dump(mode="json"),
            "plan_explanation": plan.explanation,
            "columns": [str(c) for c in result.columns],
            "rows": records,
            "row_count": len(records),
            "provenance": provenance,
            "warnings": warnings,
            "read_only": True,
        }

    @staticmethod
    def _answer_from_results(plan: QueryPlan, frame: pd.DataFrame, operation_count: int, group_columns: list[str]) -> str:
        sentences = [f"Résultat calculé à partir de {operation_count} opération(s) de lecture : {len(frame)} ligne(s)."]
        for metric in plan.metrics:
            if metric.alias not in frame.columns:
                continue
            values = pd.to_numeric(frame[metric.alias], errors="coerce")
            if values.notna().sum() == 0:
                continue
            if group_columns:
                position = int(values.idxmax())
                highest = frame.iloc[position]
                group_label = ", ".join(f"{column}={highest[column]}" for column in group_columns if column in frame.columns)
                sentences.append(f"{metric.alias} le plus élevé : {group_label}, {values.iloc[position]:.2f}.")
            else:
                sentences.append(f"{metric.alias} : {values.iloc[0]:.2f}.")
        return " ".join(sentences)

    def _execute_operation(self, op: QueryOperation, meta: dict[str, Any], obj: dict[str, Any], columns: list[str]) -> pd.DataFrame:
        kind = meta["kind"]
        if kind == "sqlite":
            quote_id = lambda value: '"' + value.replace('"', '""') + '"'
            params: list[Any] = []
            sql = f"SELECT {', '.join(quote_id(c) for c in columns)} FROM {quote_id(op.object_name)}"
            where = []
            for condition in op.filters:
                col = quote_id(condition.column)
                if condition.operator in _OPERATOR_SQL:
                    if condition.value is None and condition.operator in {"eq", "ne"}:
                        where.append(f"{col} IS {'NOT ' if condition.operator == 'ne' else ''}NULL")
                    else:
                        where.append(f"{col} {_OPERATOR_SQL[condition.operator]} ?")
                        params.append(condition.value)
                elif condition.operator == "in":
                    values = condition.value
                    if not isinstance(values, list) or not values:
                        raise ValueError("Le filtre 'in' doit contenir au moins une valeur.")
                    where.append(f"{col} IN ({','.join('?' for _ in values)})")
                    params.extend(values)
                elif condition.operator == "contains":
                    needle = str(condition.value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                    where.append(f"CAST({col} AS TEXT) LIKE ? ESCAPE '\\'")
                    params.append(f"%{needle}%")
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " LIMIT ?"
            params.append(min(op.limit, MAX_SOURCE_ROWS))
            with sqlite3.connect(_sqlite_uri(meta["connection"]), uri=True, timeout=8) as db:
                db.execute("PRAGMA query_only=ON")
                started = time.monotonic()
                db.set_progress_handler(lambda: 1 if time.monotonic() - started > 10 else 0, 1000)
                db.row_factory = sqlite3.Row
                return pd.DataFrame([dict(row) for row in db.execute(sql, params).fetchall()], columns=columns)
        if kind == "mongodb":
            query = self._mongo_filter(op.filters)
            database = MongoConnector(meta["connection"]).database_name()
            docs = MongoConnector(meta["connection"]).find(database, op.object_name, query,
                                                            limit=min(op.limit, MAX_SOURCE_ROWS), projection={c: 1 for c in columns})
            return pd.DataFrame(docs).reindex(columns=columns)
        if kind == "neo4j":
            return self._execute_neo4j(op, meta)
        path = (self.upload_dir.resolve() / meta["storage_id"]).resolve()
        frame = load_table(path, sheet_name=op.object_name if path.suffix.lower() in {".xlsx", ".xls"} else 0)
        frame = frame.loc[:, columns]
        for condition in op.filters:
            frame = self._filter_frame(frame, condition)
        return frame.head(min(op.limit, MAX_SOURCE_ROWS)).copy()

    @staticmethod
    def _mongo_filter(filters: list[FilterCondition]) -> dict[str, Any]:
        clauses = []
        for item in filters:
            if item.operator in _OPERATOR_MONGO:
                clauses.append({item.column: {_OPERATOR_MONGO[item.operator]: item.value}})
            elif item.operator == "contains":
                import re
                clauses.append({item.column: {"$regex": re.escape(str(item.value)), "$options": "i"}})
            elif item.operator == "in":
                if not isinstance(item.value, list) or not item.value:
                    raise ValueError("Le filtre 'in' doit contenir une liste non vide.")
                clauses.append({item.column: {"$in": item.value}})
        return {"$and": clauses} if clauses else {}

    @staticmethod
    def _filter_frame(frame: pd.DataFrame, condition: FilterCondition) -> pd.DataFrame:
        series = frame[condition.column]
        value = condition.value
        if condition.operator == "eq":
            mask = series.isna() if value is None else series == value
        elif condition.operator == "ne":
            mask = series.notna() if value is None else series != value
        elif condition.operator == "gt":
            mask = pd.to_numeric(series, errors="coerce") > value
        elif condition.operator == "gte":
            mask = pd.to_numeric(series, errors="coerce") >= value
        elif condition.operator == "lt":
            mask = pd.to_numeric(series, errors="coerce") < value
        elif condition.operator == "lte":
            mask = pd.to_numeric(series, errors="coerce") <= value
        elif condition.operator == "contains":
            mask = series.astype("string").str.contains(re.escape(str(value)), case=False, na=False)
        elif condition.operator == "in":
            mask = series.isin(value if isinstance(value, list) else [])
        else:
            raise ValueError("Opérateur de filtre non autorisé.")
        return frame.loc[mask]

    @staticmethod
    def _execute_neo4j(op: QueryOperation, meta: dict[str, Any]) -> pd.DataFrame:
        if not _SAFE_IDENTIFIER.fullmatch(op.object_name):
            raise ValueError("Label Neo4j non pris en charge.")
        params: dict[str, Any] = {"_limit": min(op.limit, MAX_SOURCE_ROWS)}
        start_props = {c["name"] for c in next(it for it in meta["objects"] if it["name"] == op.object_name)["columns"]}
        if op.target_label:
            if not _SAFE_IDENTIFIER.fullmatch(op.target_label) or not _SAFE_IDENTIFIER.fullmatch(op.relationship_type or ""):
                raise ValueError("Relation Neo4j non prise en charge.")
            target_obj = next(it for it in meta["objects"] if it["name"] == op.target_label)
            target_props = {c["name"] for c in target_obj["columns"]}
            rel = f"-[r:{op.relationship_type}]->" if op.direction == "outgoing" else (f"<-[r:{op.relationship_type}]-" if op.direction == "incoming" else f"-[r:{op.relationship_type}]-")
            pattern = f"(s:{op.object_name}){rel}(t:{op.target_label})"
            properties = {f"source_{p}": f"s.`{p.replace('`', '``')}`" for p in start_props} | {f"target_{p}": f"t.`{p.replace('`', '``')}`" for p in target_props}
        else:
            pattern = f"(s:{op.object_name})"
            properties = {p: f"s.`{p.replace('`', '``')}`" for p in start_props}
        if any(c not in properties for c in op.columns + [f.column for f in op.filters]):
            raise ValueError("La requête Neo4j référence une propriété inconnue.")
        where = []
        for i, condition in enumerate(op.filters):
            expr = properties[condition.column]
            param = f"_filter_{i}"
            if condition.operator in _OPERATOR_SQL:
                if condition.value is None and condition.operator in {"eq", "ne"}:
                    where.append(f"{expr} IS {'NOT ' if condition.operator == 'ne' else ''}NULL")
                else:
                    where.append(f"{expr} {_OPERATOR_SQL[condition.operator]} ${param}")
                    params[param] = condition.value
            elif condition.operator == "in":
                if not isinstance(condition.value, list) or not condition.value:
                    raise ValueError("Le filtre 'in' doit contenir une liste non vide.")
                where.append(f"{expr} IN ${param}")
                params[param] = condition.value
            elif condition.operator == "contains":
                where.append(f"toLower(toString({expr})) CONTAINS toLower(${param})")
                params[param] = str(condition.value)
        returns = ", ".join(f"{properties[c]} AS `{c.replace('`', '``')}`" for c in op.columns)
        query = f"MATCH {pattern}" + (" WHERE " + " AND ".join(where) if where else "") + f" RETURN {returns} LIMIT $_limit"
        rows = Neo4jConnector(meta["connection"]).read(query, params, limit=op.limit)
        return pd.DataFrame(rows).reindex(columns=op.columns)
