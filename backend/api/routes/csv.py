from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from services import csv_service
from db import get_db
from services.column_recognition_service import profile_dataframe
import pandas as pd


router = APIRouter(prefix="/csv", tags=["CSV"])


@router.post("/upload")
async def analyze_csv(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type")

    df = await csv_service.process_csv(file)

    # DEBUG: Let's see what we actually got
    print(f"Type of df: {type(df)}")
    print(f"Is DataFrame? {isinstance(df, pd.DataFrame)}")
    if isinstance(df, pd.DataFrame):
        print(f"Columns: {df.columns.tolist()}")
    else:
        print(f"Value: {df}")

    analysis = profile_dataframe(df)

    return {
        "columns": df.columns.tolist(),
        "row_count": len(df),
        "analysis": analysis,
    }
