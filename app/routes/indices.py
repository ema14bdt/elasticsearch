from fastapi import APIRouter, HTTPException
from app.dependencies import es

router = APIRouter()

@router.get("/indices")
async def list_indices():
    try:
        indices_info = es.cat.indices(format='json')
        user_indices = []
        for idx in indices_info:
            if not idx['index'].startswith('.'):
                mapping = es.indices.get_mapping(index=idx['index'])
                properties = mapping[idx['index']]['mappings']['properties']
                columns = [
                    {"name": k, "type": v['type']} 
                    for k, v in properties.items()
                ]
                user_indices.append({
                    "name": idx['index'],
                    "docs_count": int(idx['docs.count']) if idx['docs.count'] != '0' else 0,
                    "size": idx['store.size'] if idx['store.size'] else '0b',
                    "columns": columns
                })
        return {"indices": user_indices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo índices: {str(e)}")
