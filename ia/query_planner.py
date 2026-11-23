"""Structured, schema-grounded planning for natural-language data questions."""
from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FilterCondition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    column: str = Field(min_length=1, max_length=128)
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "contains", "in"]
    value: str | int | float | bool | list[str | int | float] | None


class QueryOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation_id: str = Field(min_length=1, max_length=32)
    source_id: str = Field(min_length=1, max_length=64)
    object_name: str = Field(min_length=1, max_length=128)
    columns: list[str] = Field(min_length=1, max_length=40)
    filters: list[FilterCondition] = Field(default_factory=list, max_length=20)
    limit: int = Field(default=5000, ge=1, le=5000)
    # For Neo4j only: request a single, schema-validated relationship hop.
    target_label: str | None = None
    relationship_type: str | None = None
    direction: Literal["outgoing", "incoming", "both"] = "outgoing"

    @model_validator(mode="after")
    def graph_fields_are_paired(self):
        if bool(self.target_label) != bool(self.relationship_type):
            raise ValueError("target_label et relationship_type doivent être fournis ensemble")
        return self


class FieldReference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation_id: str = Field(min_length=1, max_length=32)
    column: str = Field(min_length=1, max_length=160)


class JoinSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    left: FieldReference
    right: FieldReference
    how: Literal["inner", "left", "outer"] = "inner"


class MetricSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation_id: str = Field(min_length=1, max_length=32)
    column: str | None = None
    function: Literal["count", "sum", "avg", "min", "max"]
    alias: str = Field(min_length=1, max_length=80)

    @model_validator(mode="after")
    def count_may_omit_column(self):
        if self.function != "count" and self.column is None:
            raise ValueError("La colonne est obligatoire pour cette agrégation")
        return self


class QueryPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operations: list[QueryOperation] = Field(max_length=5)
    joins: list[JoinSpec] = Field(default_factory=list, max_length=4)
    group_by: list[FieldReference] = Field(default_factory=list, max_length=10)
    metrics: list[MetricSpec] = Field(default_factory=list, max_length=10)
    explanation: str = Field(min_length=1, max_length=500)


def planner_prompt(question: str, inventory: list[dict[str, Any]]) -> str:
    # Never send connection strings, credentials, or local file paths to Ollama.
    safe_inventory = [{key: value for key, value in item.items()
                       if key in {"source_id", "kind", "objects", "relationships"}}
                      for item in inventory]
    schema = json.dumps(safe_inventory, ensure_ascii=False, separators=(",", ":"))
    return f"""Tu es un planificateur de questions de données. Réponds uniquement en JSON strict conforme au schéma décrit ci-dessous. Ne rédige jamais de SQL, Cypher, code, commandes, texte de réponse ni résultats inventés. Choisis uniquement les sources, objets et colonnes présents dans le catalogue fourni. Sélectionne le minimum de colonnes nécessaires. Pour plusieurs sources, indique les jointures explicites avec les colonnes correspondantes. Si aucune opération n'est raisonnablement possible, renvoie une liste operations vide et explique pourquoi.

Question utilisateur : {question}

Catalogue autorisé : {schema}

Forme JSON attendue :
{{"operations":[{{"operation_id":"op1","source_id":"ID_SOURCE","object_name":"table, collection, fichier ou label Neo4j","columns":["colonne exacte"],"filters":[{{"column":"colonne exacte","operator":"eq|ne|gt|gte|lt|lte|contains|in","value":"valeur"}}],"limit":5000,"target_label":null,"relationship_type":null,"direction":"outgoing"}}],"joins":[{{"left":{{"operation_id":"op1","column":"colonne exacte"}},"right":{{"operation_id":"op2","column":"colonne exacte"}},"how":"inner|left|outer"}}],"group_by":[{{"operation_id":"op1","column":"colonne exacte"}}],"metrics":[{{"operation_id":"op1","column":"colonne exacte ou null pour compter les lignes","function":"count|sum|avg|min|max","alias":"nom court"}}],"explanation":"Brève explication en français du plan, sans résultat."}}

Pour une relation Neo4j, renseigne target_label, relationship_type et direction, puis nomme les colonnes source par source_<propriété> et les colonnes cible par target_<propriété>, en prenant les propriétés dans le catalogue des labels. Les filtres utilisent les mêmes alias. Pour les autres sources, utilise les noms exacts de colonne. Le backend valide le JSON et chaque opération avant exécution."""


def parse_plan(raw: str) -> QueryPlan:
    """Parse a strict JSON response; tolerate only a surrounding Markdown fence."""
    value = raw.strip()
    if value.startswith("```"):
        value = value.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return QueryPlan.model_validate_json(value)
