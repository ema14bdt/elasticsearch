from fastapi import APIRouter, HTTPException
from app.dependencies import es_manager
from app.schemas import SearchRequest


router = APIRouter()

@router.post("/search")
async def perform_search(request: SearchRequest):
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="La consulta no puede estar vacía")
    return es_manager.search(request.index_name, request.query, request.size, request.agg_fields)
