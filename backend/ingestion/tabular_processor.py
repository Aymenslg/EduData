from pathlib import Path
import pandas as pd
import json

def load_table(path, sheet_name=0):
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name)
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        return pd.json_normalize(data if isinstance(data, list) else [data])
    raise ValueError("Format tabulaire non supporté")
