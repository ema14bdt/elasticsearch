from pydantic import BaseModel, Field
from typing import List, Optional

class SearchRequest(BaseModel):
    index_name: str
    query: str
    size: int = 10
    agg_fields: Optional[List[str]] = Field(None, description="Fields to aggregate on")