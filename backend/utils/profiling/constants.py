PANDAS_DTYPE_MAP = {
    "int64": "numeric",
    "float64": "numeric",
    "bool": "boolean",
    "object": "string",
    "datetime64[ns]": "datetime",
}

SEMANTIC_LABEL_MAP = {
    "NUMBER": "numeric_value",
    "DATE": "time",
    "LOCATION": "geographic",
    "ORGANIZATION": "category_like",
    "PERSON": "entity",
    "STRING": "generic_text",
    "OTHER": "unknown",
}
