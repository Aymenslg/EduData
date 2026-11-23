from pathlib import Path
import json
import pandas as pd
import fitz

def inspect_file(path):
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        doc = fitz.open(path)
        text = "\n".join(page.get_text() for page in doc)
        return {"type": "pdf", "pages": len(doc), "text_chars": len(text),
                "text_preview": text[:1200]}
    if suffix == ".csv":
        df = pd.read_csv(path, nrows=100)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path, nrows=100)
    elif suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            df = pd.json_normalize(data[:100])
        elif isinstance(data, dict):
            df = pd.json_normalize(data)
        else:
            return {"type": "json", "preview": str(data)[:1200]}
    else:
        raise ValueError("Format non pris en charge")
    return {"type": suffix.lstrip("."), "columns": list(df.columns),
            "rows_preview": df.head(10).where(pd.notna(df), None).to_dict(orient="records"),
            "rows_sampled": len(df)}
