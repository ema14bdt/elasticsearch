from fastapi import APIRouter, HTTPException
from app.dependencies import es

router = APIRouter()

@router.get("/indices")
async def list_indices():
    try:
        indices_info = es.cat.indices(format='json')
        user_indices = [
            {
                "name": idx['index'],
                "docs_count": int(idx['docs.count']) if idx['docs.count'] != '0' else 0,
                "size": idx['store.size'] if idx['store.size'] else '0b'
            }
            for idx in indices_info if not idx['index'].startswith('.')
        ]
        return {"indices": user_indices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo índices: {str(e)}")
