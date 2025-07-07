from fastapi import APIRouter, UploadFile, File, HTTPException, Form
import pandas as pd
from app.dependencies import es_manager

router = APIRouter()

@router.post("/upload-csv", summary="Upload and index a CSV file", description="Uploads a CSV file and indexes its content in Elasticsearch.")
async def upload_csv(file: UploadFile = File(...), index_name: str = Form(...)):
    """Upload a CSV file and index it in Elasticsearch."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")
    if not index_name or not index_name.replace('_', '').replace('-', '').isalnum():
        raise HTTPException(status_code=400, detail="Nombre de índice inválido")
    try:
        contents = await file.read()
        df = pd.read_csv(pd.io.common.StringIO(contents.decode('utf-8')))
        mapping = {
            "mappings": {
                "properties": {
                    col: {"type": "double"} if df[col].dtype in ['int64', 'float64']
                    else {"type": "text", "analyzer": "standard"}
                    for col in df.columns
                }
            }
        }
        if not es_manager.create_index(index_name, mapping):
            raise HTTPException(status_code=500, detail="Error creando índice")
        stats = es_manager.index_dataframe(df, index_name)
        return {
            "message": "CSV cargado e indexado exitosamente",
            "filename": file.filename,
            "index_name": index_name,
            "columns": list(df.columns),
            "stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")
