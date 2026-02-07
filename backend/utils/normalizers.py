import pandas as pd

def df_to_json_safe(df: pd.DataFrame):
    return df.where(pd.notnull(df), None).astype(object).to_dict(orient="records")
