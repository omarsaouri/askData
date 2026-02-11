import pandas as pd
from utils.profiling.builder import build_column_profiles


def profile_dataframe(df: pd.DataFrame) -> dict:
    return build_column_profiles(df)
