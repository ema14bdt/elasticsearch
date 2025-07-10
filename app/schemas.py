from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# Esquemas para subida de archivos
class Stats(BaseModel):
    total_documents: int
    success_count: int
    error_count: int
    indexing_time_seconds: float
    documents_per_second: float

class UploadResponse(BaseModel):
    filename: str
    index_name: str
    mapping: Dict[str, Any]
    stats: Stats

# Esquemas para búsqueda
class SearchRequest(BaseModel):
    index_name: str
    query: str
    session_id: str  # Añadido para la validación
    size: int = 10
    agg_fields: Optional[List[str]] = Field(None, description="Fields to aggregate on")

class SearchResult(BaseModel):
    query: str
    total_results: int
    returned_results: int
    search_time_seconds: float
    elasticsearch_took_ms: int
    results: List[Dict[str, Any]]
    aggregations: Optional[Dict[str, Any]] = None

# Esquemas para índices
class Column(BaseModel):
    name: str
    type: str

class IndexInfo(BaseModel):
    name: str
    status: str
    health: str
    doc_count: str
    columns: List[Column]