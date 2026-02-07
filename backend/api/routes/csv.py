from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from services import csv_service
from utils import normalizers
from db import get_db

router = APIRouter(prefix="/csv", tags=["CSV"])


@router.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code = 400, detail = "Invalid file type")
    df = await csv_service.process_csv(file)
    analysis = csv_service.analyze_dataframe(df)
    dataset_id = await csv_service.persist_analysis(
        db=db,
        filename=file.filename,
        df=df,
        analysis=analysis,
    )
    return {
    "dataset_id": dataset_id,
    "columns": df.columns.tolist(),
    "row_count": len(df),
    "analysis": analysis,
    }
