"""Descriptive statistics for uploaded educational tables."""
from __future__ import annotations

import math
from typing import Any

import pandas as pd


EDUCATIONAL_METRICS = {
    "note_sur_20": (0, 20, "moyenne des notes sur 20"),
    "taux_presence_pct": (0, 100, "taux moyen de présence (%)"),
    "devoirs_realises_pct": (0, 100, "taux moyen de devoirs réalisés (%)"),
}


def _clean(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def analyze_dataframe(frame: pd.DataFrame) -> dict[str, Any]:
    """Return JSON-safe descriptive statistics and clearly flagged assumptions."""
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("L'analyse attend un tableau de données.")

    missing = frame.isna().sum()
    columns: dict[str, Any] = {}
    numeric: dict[str, Any] = {}
    categorical: dict[str, Any] = {}
    warnings: list[str] = []

    for name in frame.columns:
        series = frame[name]
        columns[str(name)] = {
            "dtype": str(series.dtype),
            "missing_count": int(missing[name]),
            "missing_rate": float(missing[name] / len(frame)) if len(frame) else 0.0,
            "constant": bool(series.nunique(dropna=True) <= 1),
        }
        if pd.api.types.is_numeric_dtype(series):
            desc = series.describe()
            numeric[str(name)] = {key: _clean(desc.get(key)) for key in
                                  ("count", "mean", "50%", "std", "min", "max")}
            numeric[str(name)]["median"] = numeric[str(name)].pop("50%")
        else:
            counts = series.astype("string").value_counts(dropna=True).head(20)
            categorical[str(name)] = {str(k): int(v) for k, v in counts.items()}

    if frame.empty:
        warnings.append("Le fichier ne contient aucune ligne.")
    if frame.isna().any().any():
        warnings.append("Certaines colonnes contiennent des valeurs manquantes.")
    duplicate_count = int(frame.duplicated().sum())
    if duplicate_count:
        warnings.append(f"{duplicate_count} ligne(s) dupliquée(s) détectée(s).")

    metrics: dict[str, Any] = {}
    for column, (low, high, label) in EDUCATIONAL_METRICS.items():
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        invalid = values.notna() & ~values.between(low, high)
        metrics[column] = {
            "label": label,
            "mean": _clean(values.where(values.between(low, high)).mean()),
            "valid_count": int(values.between(low, high).sum()),
            "invalid_count": int(invalid.sum()),
            "valid_range": [low, high],
        }
        if invalid.any():
            warnings.append(f"{column}: {int(invalid.sum())} valeur(s) hors plage [{low}, {high}]; conservées dans les données.")

    return {
        "rows": int(len(frame)),
        "columns_count": int(len(frame.columns)),
        "columns": columns,
        "numeric_statistics": numeric,
        "categorical_distributions": categorical,
        "duplicate_rows": duplicate_count,
        "educational_metrics": metrics,
        "warnings": warnings,
        "assumptions": ["Les indicateurs éducatifs ne sont calculés que pour les colonnes portant exactement le nom attendu."],
    }
