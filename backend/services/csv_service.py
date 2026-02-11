from fastapi import UploadFile
import pandas as pd
import io
from utils.uploading import detection_utils


async def process_csv(file: UploadFile):
    contents = await file.read()
    # detecting
    encoding = detection_utils.detect_encoding(contents)
    text_sample = contents[:5000].decode(encoding, errors="ignore")
    delimiter = detection_utils.detect_delimiter(text_sample)

    try:
        return pd.read_csv(
            io.BytesIO(contents), encoding=encoding, sep=delimiter, engine="pyarrow"
        )
    except Exception:
        pass

    try:
        return pd.read_csv(
            io.BytesIO(contents), encoding=encoding, sep=delimiter, engine="python"
        )

    except Exception:
        pass

    # Controlled fallback (warn user)
    return pd.read_csv(
        io.BytesIO(contents),
        encoding=encoding,
        sep=delimiter,
        engine="python",
        on_bad_lines="skip",
    )
