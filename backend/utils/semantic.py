import re
import pandas as pd


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
URL_RE = re.compile(r"^https?://|^www\.", re.IGNORECASE)
PHONE_RE = re.compile(r"^\+?\d[\d\-\s\(\)]{6,}\d$")
ZIP_RE = re.compile(r"^\d{5}(-\d{4})?$")


def _sample_non_null(series: pd.Series, max_items: int = 100):
    values = series.dropna()
    if len(values) == 0:
        return []
    return values.astype(str).head(max_items).tolist()


def detect_semantic_type(series: pd.Series, col_name: str):
    name = (col_name or "").lower()
    samples = _sample_non_null(series)
    sample_count = len(samples)

    if sample_count == 0:
        return "unknown"

    # Column name hints
    if any(token in name for token in ["email", "e-mail"]):
        return "email"
    if any(token in name for token in ["url", "link", "website"]):
        return "url"
    if any(token in name for token in ["phone", "mobile", "tel"]):
        return "phone"
    if any(token in name for token in ["zip", "postal"]):
        return "postal_code"
    if any(token in name for token in ["address", "street", "city", "state", "country"]):
        return "address"
    if any(token in name for token in ["first_name", "last_name", "fullname", "name"]):
        return "person_name"

    # Value pattern hints
    email_hits = sum(1 for v in samples if EMAIL_RE.match(v))
    if email_hits / sample_count >= 0.8:
        return "email"

    url_hits = sum(1 for v in samples if URL_RE.search(v))
    if url_hits / sample_count >= 0.8:
        return "url"

    phone_hits = sum(1 for v in samples if PHONE_RE.match(v))
    if phone_hits / sample_count >= 0.8:
        return "phone"

    zip_hits = sum(1 for v in samples if ZIP_RE.match(v))
    if zip_hits / sample_count >= 0.8:
        return "postal_code"

    # Datetime detection
    common_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y", 
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%Y%m%d",  # 20231231
        "%d.%m.%Y",
        "%m.%d.%Y"
    ]
    for fmt in common_formats:        
        parsed = pd.to_datetime(series, errors="coerce", format=fmt)
        if parsed.notna().mean() >= 0.9:
            return "datetime"

    # Numeric detection
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"

    # Boolean detection
    lower_vals = [v.strip().lower() for v in samples]
    if all(v in ["true", "false", "0", "1", "yes", "no"] for v in lower_vals):
        return "boolean"

    # Categorical vs free text
    unique_ratio = series.nunique(dropna=True) / max(len(series), 1)
    if unique_ratio <= 0.2 and series.nunique(dropna=True) <= 50:
        return "categorical"

    return "text"
