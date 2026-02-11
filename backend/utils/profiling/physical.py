from utils.profiling.constants import PANDAS_DTYPE_MAP
import pandas as pd


def extract_physical_signals(df: pd.DataFrame, column: str):
    s = df[column]
    N = len(s)

    sum_null = s.isna().sum()
    null_ratio = sum_null / N if N else 0

    unique_count = s.nunique(dropna=True)
    unique_ratio = unique_count / N if N else 0

    non_null_count = N - sum_null
    cardinality_ratio = unique_count / non_null_count if non_null_count else 0

    raw_dtype = str(s.dtype)
    physical_dtype = PANDAS_DTYPE_MAP.get(raw_dtype, "unknown")

    return {
        "dtype": physical_dtype,
        "raw_dtype": raw_dtype,
        "null_ratio": round(null_ratio, 4),
        "unique_ratio": round(unique_ratio, 4),
        "cardinality_ratio": round(cardinality_ratio, 4),
    }
