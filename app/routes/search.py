from fastapi import APIRouter, HTTPException, Body
from ..dependencies import es_manager
from ..schemas import SearchRequest, SearchResult

router = APIRouter()

@router.post("/search", response_model=SearchResult)
async def perform_search(request: SearchRequest):
    """
    Realiza una búsqueda en un índice temporal, validando la sesión del usuario.
    """
    # Medida de seguridad: asegurar que el índice pertenece a la sesión
    if not request.index_name.startswith(f"temp-{request.session_id}-"):
        raise HTTPException(status_code=403, detail="Acceso no autorizado al índice")

    if not request.query.strip():
        raise HTTPException(status_code=400, detail="La consulta no puede estar vacía")

    try:
        search_results = es_manager.search(
            index_name=request.index_name,
            query=request.query,
            size=request.size,
            agg_fields=request.agg_fields
        )
        return search_results
    except HTTPException as e:
        # Re-lanzar excepciones HTTP para que FastAPI las maneje
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en la búsqueda: {str(e)}")
