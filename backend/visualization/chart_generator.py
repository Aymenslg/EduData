def chart_recommendation(columns):
    numeric = [c for c, dtype in columns.items() if str(dtype).startswith(("int", "float"))]
    categorical = [c for c in columns if c not in numeric]
    if categorical and numeric:
        return {"type": "bar", "x": categorical[0], "y": numeric[0]}
    return {"type": "table"}
