import pandas as pd

def normalize_columns(frame):
    result = frame.copy()
    result.columns = [str(c).strip().lower().replace(" ", "_") for c in result.columns]
    return result
