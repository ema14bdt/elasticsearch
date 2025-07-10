from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from ..dependencies import es_manager
from ..utils import process_csv, infer_mapping
from ..schemas import UploadResponse

router = APIRouter()

@router.post("/upload-csv", response_model=UploadResponse)
async def upload_csv(
    file: UploadFile = File(...), 
    index_name: str = Form(...),
    session_id: str = Form(...)
):
    """
    Sube un archivo CSV, infiere el mapping y lo indexa en Elasticsearch
    en un índice temporal basado en la sesión.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")

    try:
        # Construir un nombre de índice temporal y seguro
        temp_index_name = f"temp-{session_id}-{index_name}".lower()

        df = process_csv(file.file)
        mapping = infer_mapping(df)
        
        # Crear índice temporal
        es_manager.create_index(temp_index_name, mapping)
        
        # Indexar datos en el índice temporal
        stats = es_manager.index_dataframe(df, temp_index_name)
        
        return UploadResponse(
            filename=file.filename,
            index_name=temp_index_name, # Devolver el nombre real del índice temporal
            mapping=mapping,
            stats=stats
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando el archivo: {str(e)}")
