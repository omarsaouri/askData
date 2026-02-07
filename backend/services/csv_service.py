from fastapi import UploadFile
import pandas as pd
import io
from sqlalchemy.ext.asyncio import AsyncSession
from utils import detectors
from utils import semantic
from models.eda_models import Dataset, ColumnProfile, Relationship, Insight

async def process_csv(file:UploadFile):
    contents = await file.read()
    #detecting
    encoding = detectors.detect_encoding(contents)
    text_sample = contents[:5000].decode(encoding, errors="ignore")
    delimiter = detectors.detect_delimiter(text_sample)

    try:
        return pd.read_csv(
            io.BytesIO(contents),
            encoding=encoding,
            sep=delimiter,
            engine="pyarrow"
        )
    except Exception:
        pass

    try:
        return pd.read_csv(
            io.BytesIO(contents),
            encoding=encoding,
            sep=delimiter,
            engine="python"
        )
        
    except Exception:
        pass

    # Controlled fallback (warn user)
    print("final approach")
    return pd.read_csv(
        io.BytesIO(contents),
        encoding=encoding,
        sep=delimiter,
        engine="python",
        on_bad_lines="skip"
    )


def _profile_column(series: pd.Series):
    total = len(series)
    missing = int(series.isna().sum())
    unique = int(series.nunique(dropna=True))
    unique_ratio = unique / max(total, 1)

    stats = {
        "count": total,
        "missing": missing,
        "unique": unique,
        "unique_ratio": unique_ratio,
    }

    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        stats.update({
            "min": float(numeric.min()) if numeric.notna().any() else None,
            "max": float(numeric.max()) if numeric.notna().any() else None,
            "mean": float(numeric.mean()) if numeric.notna().any() else None,
            "median": float(numeric.median()) if numeric.notna().any() else None,
            "std": float(numeric.std()) if numeric.notna().any() else None,
        })
    elif pd.api.types.is_datetime64_any_dtype(series):
        stats.update({
            "min": series.min().isoformat() if series.notna().any() else None,
            "max": series.max().isoformat() if series.notna().any() else None,
        })
    else:
        top = series.dropna().astype(str).value_counts().head(5)
        stats["top_values"] = [{"value": k, "count": int(v)} for k, v in top.items()]

    return stats


def _infer_schema(df: pd.DataFrame):
    return {col: str(dtype) for col, dtype in df.dtypes.items()}


def _semantic_classification(df: pd.DataFrame):
    result = {}
    for col in df.columns:
        result[col] = semantic.detect_semantic_type(df[col], col)
    return result


def _relationship_detection(df: pd.DataFrame, semantic_types: dict):
    relationships = []

    # Numeric correlations
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr(numeric_only=True)
        for i, col_a in enumerate(numeric_cols):
            for col_b in numeric_cols[i + 1:]:
                value = corr.loc[col_a, col_b]
                if pd.notna(value) and abs(value) >= 0.8:
                    relationships.append({
                        "col_a": col_a,
                        "col_b": col_b,
                        "relation_type": "correlation",
                        "strength": float(value),
                        "details": {"method": "pearson"},
                    })

    # Potential key/foreign-key patterns
    for col_a in df.columns:
        series_a = df[col_a]
        unique_ratio = series_a.nunique(dropna=True) / max(len(series_a), 1)
        if unique_ratio < 0.98:
            continue
        if "id" not in col_a.lower():
            continue

        values_a = set(series_a.dropna().astype(str).tolist())
        if not values_a:
            continue

        for col_b in df.columns:
            if col_b == col_a:
                continue
            series_b = df[col_b].dropna().astype(str)
            if len(series_b) == 0:
                continue
            overlap = series_b.isin(values_a).mean()
            if overlap >= 0.8:
                relationships.append({
                    "col_a": col_a,
                    "col_b": col_b,
                    "relation_type": "potential_fk",
                    "strength": float(overlap),
                    "details": {"overlap_ratio": float(overlap)},
                })

    return relationships


def _insight_generation(df: pd.DataFrame, profiles: dict, relationships: list):
    insights = []
    for col, stats in profiles.items():
        if stats.get("missing", 0) / max(stats.get("count", 1), 1) >= 0.3:
            insights.append({
                "title": f"High missing values in {col}",
                "detail": f"{stats['missing']} of {stats['count']} rows are missing ({stats['missing'] / max(stats['count'], 1):.0%}).",
                "severity": "warning",
            })
        if stats.get("unique_ratio", 1) <= 0.01:
            insights.append({
                "title": f"Low variance in {col}",
                "detail": f"{col} has very low uniqueness (unique ratio {stats['unique_ratio']:.2f}).",
                "severity": "info",
            })

        if "top_values" in stats and stats["top_values"]:
            top = stats["top_values"][0]
            if top["count"] / max(stats["count"], 1) >= 0.9:
                insights.append({
                    "title": f"Dominant category in {col}",
                    "detail": f"Value '{top['value']}' appears in {top['count']} of {stats['count']} rows.",
                    "severity": "info",
                })

    for rel in relationships:
        if rel["relation_type"] == "correlation" and abs(rel["strength"]) >= 0.9:
            insights.append({
                "title": f"Strong correlation between {rel['col_a']} and {rel['col_b']}",
                "detail": f"Pearson correlation is {rel['strength']:.2f}.",
                "severity": "info",
            })

    return insights


def analyze_dataframe(df: pd.DataFrame):
    schema = _infer_schema(df)
    semantic_types = _semantic_classification(df)
    profiles = {col: _profile_column(df[col]) for col in df.columns}
    relationships = _relationship_detection(df, semantic_types)
    insights = _insight_generation(df, profiles, relationships)

    return {
        "schema": schema,
        "semantic_types": semantic_types,
        "profiles": profiles,
        "relationships": relationships,
        "insights": insights,
    }


def _json_safe_value(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


async def persist_analysis(
    db: AsyncSession,
    filename: str,
    df: pd.DataFrame,
    analysis: dict,
):
    dataset = Dataset(
        filename=filename,
        row_count=int(len(df)),
        column_count=int(len(df.columns)),
    )
    db.add(dataset)
    await db.flush()

    for col in df.columns:
        stats = analysis["profiles"][col]
        sample_values = [
            _json_safe_value(v)
            for v in df[col].dropna().astype(object).head(5).tolist()
        ]
        profile = ColumnProfile(
            dataset_id=dataset.id,
            name=col,
            inferred_type=analysis["schema"][col],
            semantic_type=analysis["semantic_types"][col],
            stats=stats,
            sample_values=sample_values,
        )
        db.add(profile)

    for rel in analysis["relationships"]:
        db.add(Relationship(
            dataset_id=dataset.id,
            col_a=rel["col_a"],
            col_b=rel["col_b"],
            relation_type=rel["relation_type"],
            strength=rel.get("strength"),
            details=rel.get("details"),
        ))

    for ins in analysis["insights"]:
        db.add(Insight(
            dataset_id=dataset.id,
            title=ins["title"],
            detail=ins["detail"],
            severity=ins["severity"],
        ))

    await db.commit()
    await db.refresh(dataset)
    return dataset.id
