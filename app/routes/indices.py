from fastapi import APIRouter, HTTPException, Query, Header
from ..dependencies import es_manager, es
import os
from datetime import datetime, timedelta

router = APIRouter()

# Clave secreta para el cron job, obtenida de variables de entorno
CRON_SECRET = os.getenv("CRON_SECRET")

@router.get("/indices")
async def list_indices(session_id: str = Query(...)):
    """
    Lista los índices temporales asociados a una sesión de usuario.
    """
    try:
        # El prefijo debe coincidir con el que se usa en upload_csv
        index_prefix = f"temp-{session_id}-"
        indices = es_manager.get_indices(prefix=index_prefix)
        return {"indices": indices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo índices: {str(e)}")

@router.post("/cleanup")
async def cleanup_indices(authorization: str = Header(None)):
    """
    Limpia los índices temporales que tienen más de 1 hora de antigüedad.
    Este endpoint está protegido por una clave secreta.
    """
    if not CRON_SECRET or authorization != f"Bearer {CRON_SECRET}":
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        indices = es.indices.get(index="temp-*")
        deleted_indices = []
        for index_name in indices:
            creation_date_str = es.indices.get_settings(index=index_name)[index_name]['settings']['index']['creation_date']
            creation_date = datetime.fromtimestamp(int(creation_date_str) / 1000)
            
            if datetime.utcnow() - creation_date > timedelta(hours=1):
                es.indices.delete(index=index_name)
                deleted_indices.append(index_name)
        
        return {"message": "Limpieza completada", "deleted_indices": deleted_indices}
    except Exception as e:
        return {"message": f"Error durante la limpieza: {str(e)}"}
