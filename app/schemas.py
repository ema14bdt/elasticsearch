from pydantic import BaseModel

class SearchRequest(BaseModel):
    index_name: str
    query: str
    size: int = 10