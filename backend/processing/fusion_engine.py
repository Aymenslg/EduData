import pandas as pd

def merge_sources(left, right, key, how="inner"):
    if key not in left.columns or key not in right.columns:
        raise ValueError(f"La clé '{key}' doit exister dans les deux sources.")
    return pd.merge(left, right, on=key, how=how, validate="many_to_many")
