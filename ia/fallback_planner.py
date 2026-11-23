"""Small schema-driven fallback for common grade averages by department."""
from __future__ import annotations

import re

from ia.query_planner import QueryPlan


def department_average_plan(question: str, inventory: dict) -> QueryPlan | None:
    """Build a safe, validated-shape plan when the local model returns no operations.

    This deliberately supports one narrow, common case and only SQLite schemas:
    average of a grade/score, optionally grouped by a department field.
    """
    normalized = question.casefold()
    if not re.search(r"\b(moyenne|moyen|average|avg)\b", normalized):
        return None
    if not re.search(r"(département|departement|department)", normalized):
        return None

    table_refs = []
    for source_id, schema in inventory.items():
        if schema.get("kind") != "sqlite":
            continue
        for obj in schema.get("objects", []):
            columns = obj.get("columns", [])
            by_name = {column["name"].casefold(): column for column in columns}
            for column in columns:
                name = column["name"].casefold()
                numeric = any(t in column.get("type", "").casefold()
                              for t in ("int", "real", "float", "double", "numeric", "decimal"))
                if numeric and any(token in name for token in ("note", "score", "grade")):
                    table_refs.append({"source_id": source_id, "object": obj["name"],
                                       "columns": by_name, "metric": column["name"]})

    group_refs = []
    for source_id, schema in inventory.items():
        if schema.get("kind") != "sqlite":
            continue
        for obj in schema.get("objects", []):
            for column in obj.get("columns", []):
                if any(token in column["name"].casefold()
                       for token in ("departement", "department")):
                    group_refs.append({"source_id": source_id, "object": obj["name"],
                                       "columns": {c["name"].casefold(): c for c in obj.get("columns", [])},
                                       "group": column["name"]})

    candidates = []
    for metric in table_refs:
        for group in group_refs:
            if metric["source_id"] != group["source_id"]:
                continue
            common = set(metric["columns"]) & set(group["columns"])
            common = {key for key in common if key.endswith("_id") or key == "id"}
            same_table = metric["object"] == group["object"]
            if not same_table and not common:
                continue
            key = sorted(common, key=lambda item: ("etudiant" not in item and "student" not in item,
                                                    "inscription" not in item, item))[0] if common else None
            rank = ("etudiant" in metric["object"].casefold() or "student" in metric["object"].casefold(),
                    "resultat" in metric["object"].casefold() or "result" in metric["object"].casefold(),
                    key is not None and ("etudiant" in key or "student" in key))
            candidates.append((rank, metric, group, key))
    if not candidates:
        return None

    _, metric, group, join_key = max(candidates, key=lambda candidate: candidate[0])
    operations = [{"operation_id": "grades", "source_id": metric["source_id"],
                   "object_name": metric["object"],
                   "columns": list(dict.fromkeys([metric["metric"]] + ([join_key] if join_key else []))),
                   "limit": 5000}]
    group_operation_id = "grades"
    joins = []
    if metric["object"] != group["object"]:
        group_operation_id = "departments"
        operations.append({"operation_id": group_operation_id, "source_id": group["source_id"],
                           "object_name": group["object"],
                           "columns": list(dict.fromkeys([group["group"], join_key])), "limit": 5000})
        joins.append({"left": {"operation_id": "grades", "column": join_key},
                      "right": {"operation_id": "departments", "column": join_key}, "how": "inner"})
    return QueryPlan.model_validate({
        "operations": operations,
        "joins": joins,
        "group_by": [{"operation_id": group_operation_id, "column": group["group"]}],
        "metrics": [{"operation_id": "grades", "column": metric["metric"],
                     "function": "avg", "alias": "moyenne_note"}],
        "explanation": "Moyenne calculée depuis les tables SQLite et regroupée par département.",
    })


def combined_department_average_plan(question: str, inventory: dict) -> QueryPlan | None:
    """Fallback for grade averages that explicitly combine selected sources.

    It joins one schema object per selected source using a shared *_id field and
    uses only columns discovered in those schemas.
    """
    normalized = question.casefold()
    if len(inventory) < 2 or not re.search(r"\b(moyenne|moyen|average|avg)\b", normalized):
        return None
    if not re.search(r"(département|departement|department)", normalized):
        return None
    if not re.search(r"(relie|relier|joint|join|combine|fusionne)", normalized):
        return None

    nodes = []
    for source_id, schema in inventory.items():
        for obj in schema.get("objects", []):
            cols = obj.get("columns", [])
            columns = {c["name"].casefold(): c["name"] for c in cols}
            numeric_types = ("int", "real", "float", "double", "numeric", "decimal", "number", "long", "property")
            metrics = [c["name"] for c in cols if
                       any(t in c.get("type", "").casefold() for t in numeric_types) and
                       any(t in c["name"].casefold() for t in ("note", "score", "grade"))]
            departments = [c["name"] for c in cols if
                           any(t in c["name"].casefold() for t in ("departement", "department"))]
            nodes.append({"source_id": source_id, "kind": schema.get("kind"),
                          "schema": schema, "object": obj["name"], "columns": columns,
                          "metrics": metrics, "departments": departments})

    metric_nodes = [node for node in nodes if node["metrics"]]
    if not metric_nodes:
        return None
    mention_file = bool(re.search(r"(csv|excel|fichier|xlsx)", normalized))
    metric_nodes.sort(key=lambda n: (
        "note_sur_20" in normalized and "note_sur_20" in n["columns"],
        mention_file and n["kind"] == "file",
        any(x in n["object"].casefold() for x in ("resultat", "result", "grade")),
    ), reverse=True)
    metric_node = metric_nodes[0]
    metric_column = next((c for c in metric_node["metrics"] if c.casefold() in normalized),
                         metric_node["metrics"][0])

    chosen = [metric_node]
    edges = []
    # Connect each additional selected source to the growing join tree.
    remaining_sources = {sid for sid in inventory if sid != metric_node["source_id"]}
    while remaining_sources:
        options = []
        for node in nodes:
            if node in chosen or node["source_id"] not in remaining_sources:
                continue
            for existing in chosen:
                common = {c for c in node["columns"] if c in existing["columns"] and
                          (c.endswith("_id") or c == "id")}
                if common:
                    key = sorted(common, key=lambda c: ("etudiant" not in c and "student" not in c,
                                                         "inscription" not in c, c))[0]
                    options.append(("etudiant" in key or "student" in key, node, existing, key))
        if not options:
            return None
        _, node, existing, key = max(options, key=lambda x: x[0])
        chosen.append(node)
        edges.append((existing, node, key))
        remaining_sources.remove(node["source_id"])

    group_node = next((node for node in chosen if node["departments"]), None)
    if group_node is None:
        for node in nodes:
            if node["source_id"] != metric_node["source_id"] or not node["departments"] or node in chosen:
                continue
            for existing in chosen:
                common = set(node["columns"]) & set(existing["columns"])
                common = {c for c in common if c.endswith("_id") or c == "id"}
                if common:
                    key = sorted(common)[0]
                    chosen.append(node)
                    edges.append((existing, node, key))
                    group_node = node
                    break
            if group_node:
                break
    if group_node is None:
        return None

    op_ids = {id(node): f"op{i + 1}" for i, node in enumerate(chosen)}
    columns_by_node = {id(node): [] for node in chosen}
    columns_by_node[id(metric_node)].append(metric_column)
    group_column = group_node["departments"][0]
    columns_by_node[id(group_node)].append(group_column)
    for left, right, key in edges:
        columns_by_node[id(left)].append(left["columns"][key])
        columns_by_node[id(right)].append(right["columns"][key])

    operations = [{"operation_id": op_ids[id(node)], "source_id": node["source_id"],
                   "object_name": node["object"],
                   "columns": list(dict.fromkeys(columns_by_node[id(node)])), "limit": 5000}
                  for node in chosen]
    joins = [{"left": {"operation_id": op_ids[id(left)], "column": left["columns"][key]},
              "right": {"operation_id": op_ids[id(right)], "column": right["columns"][key]}, "how": "inner"}
             for left, right, key in edges]
    return QueryPlan.model_validate({
        "operations": operations, "joins": joins,
        "group_by": [{"operation_id": op_ids[id(group_node)], "column": group_column}],
        "metrics": [{"operation_id": op_ids[id(metric_node)], "column": metric_column,
                     "function": "avg", "alias": "moyenne_note"}],
        "explanation": "Moyenne calculée après jointure des sources sélectionnées sur leurs clés communes.",
    })
