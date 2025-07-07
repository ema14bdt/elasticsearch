from fastapi import APIRouter, UploadFile, File, HTTPException, Form
import pandas as pd
from pydantic import BaseModel, create_model
from typing import Optional, Type
from app.dependencies import es_manager

router = APIRouter()

def create_dynamic_model(df: pd.DataFrame) -> Type[BaseModel]:
    """Crea un modelo Pydantic dinámicamente desde un DataFrame."""
    fields = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_numeric_dtype(dtype):
            field_type = Optional[float]
        else:
            field_type = Optional[str]
        fields[col] = (field_type, None)
    return create_model('DynamicRow', **fields)

@router.post("/upload-csv", summary="Upload, validate, and index a CSV file", description="Uploads a CSV, validates each row, and indexes valid data in Elasticsearch.")
async def upload_csv(file: UploadFile = File(...), index_name: str = Form(...)):
    """Upload a CSV file, validate its data, and index it in Elasticsearch."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")
    if not index_name or not index_name.replace('_', '').replace('-', '').isalnum():
        raise HTTPException(status_code=400, detail="Nombre de índice inválido")
    
    try:
        contents = await file.read()
        df = pd.read_csv(pd.io.common.StringIO(contents.decode('utf-8')))
        
        # Crear modelo Pydantic dinámico y validar filas
        DynamicRowModel = create_dynamic_model(df)
        valid_rows = []
        validation_errors = []
        for index, row in df.iterrows():
            try:
                validated_data = DynamicRowModel(**row.to_dict())
                valid_rows.append(validated_data.dict())
            except Exception as e:
                validation_errors.append({"row": index + 2, "errors": str(e)})

        if not valid_rows:
            raise HTTPException(status_code=400, detail="No se encontraron filas válidas en el CSV.",
                                headers={"X-Validation-Errors": str(validation_errors)})

        valid_df = pd.DataFrame(valid_rows)

        mapping = {
            "mappings": {
                "properties": {
                    col: {"type": "double"} if valid_df[col].dtype in ['int64', 'float64']
                    else {"type": "text", "analyzer": "standard", "fields": {"keyword": {"type": "keyword"}}}
                    for col in valid_df.columns
                }
            }
        }
        
        if not es_manager.create_index(index_name, mapping):
            raise HTTPException(status_code=500, detail="Error creando índice")
            
        stats = es_manager.index_dataframe(valid_df, index_name)
        
        return {
            "message": "CSV procesado. Filas válidas indexadas.",
            "filename": file.filename,
            "index_name": index_name,
            "columns": list(df.columns),
            "validation_summary": {
                "total_rows": len(df),
                "valid_rows": len(valid_rows),
                "invalid_rows": len(validation_errors),
            },
            "validation_errors": validation_errors,
            "indexing_stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")
