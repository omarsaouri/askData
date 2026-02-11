from column_classifier import ColumnClassifier
from utils.profiling.physical import extract_physical_signals
from utils.profiling.semantic import extract_semantic_signals
import pandas as pd


def build_column_profiles(df: pd.DataFrame):
    try:
        classifier = ColumnClassifier()
    except Exception as init_error:
        print(f"Initialization error: {init_error}")
        raise

    column_profiles = {}

    for col in df.columns:
        # Extract physical signals
        # physical = extract_physical_signals(df, col)

        # Prepare column info dictionary for classify_column
        col_info = {
            "name": col,
            "data": df[col].tolist(),  # or df[col].values.tolist()
            "dtype": str(df[col].dtype),
        }

        # Classify individual column
        semantic_raw = classifier.classify_column(col_info)

        print(semantic_raw)

        # Extract semantic hints
        # semantic_hints = extract_semantic_signals(semantic_raw)

    return column_profiles
