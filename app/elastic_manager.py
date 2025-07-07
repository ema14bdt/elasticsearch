import logging
import time
import pandas as pd
from typing import Dict, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError
from fastapi import HTTPException

logger = logging.getLogger(__name__)

class ElasticsearchManager:
    def __init__(self, elasticsearch_client: Elasticsearch):
        self.es = elasticsearch_client
        logger.info("🔧 ElasticsearchManager inicializado")

    def create_index(self, index_name: str, mapping: Dict) -> bool:
        try:
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)
            self.es.indices.create(index=index_name, body=mapping)
            return True
        except Exception as e:
            logger.error(f"❌ Error creando índice {index_name}: {e}")
            return False

    def index_dataframe(self, df: pd.DataFrame, index_name: str) -> Dict[str, Any]:
        start_time = time.time()
        
        def generate_actions():
            for index, row in df.iterrows():
                doc = {k: (v if pd.notna(v) else None) for k, v in row.to_dict().items()}
                yield {
                    "_index": index_name,
                    "_id": index,
                    "_source": doc
                }

        try:
            success_count, errors = bulk(self.es, generate_actions(), raise_on_error=False)
            error_count = len(errors)
            
            self.es.indices.refresh(index=index_name)
            indexing_time = time.time() - start_time
            
            return {
                "total_documents": len(df),
                "success_count": success_count,
                "error_count": error_count,
                "indexing_time_seconds": round(indexing_time, 2),
                "documents_per_second": round(success_count / indexing_time, 2) if indexing_time > 0 else 0,
                "errors": errors
            }
        except Exception as e:
            logger.error(f"❌ Error general en indexación masiva: {e}")
            return {
                "error": str(e),
                "success_count": 0,
                "error_count": len(df)
            }

    def search(self, index_name: str, query: str, size: int = 10) -> Dict[str, Any]:
        start_time = time.time()
        try:
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["*"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                "size": size,
                "highlight": {"fields": {"*": {}}}
            }
            response = self.es.search(index=index_name, body=search_body)
            hits = response['hits']
            results = [
                {
                    "score": hit['_score'],
                    "source": hit['_source'],
                    "highlights": hit.get('highlight', {})
                }
                for hit in hits['hits']
            ]
            return {
                "query": query,
                "total_results": hits['total']['value'],
                "returned_results": len(results),
                "search_time_seconds": round(time.time() - start_time, 3),
                "elasticsearch_took_ms": response['took'],
                "results": results
            }
        except NotFoundError:
            raise HTTPException(status_code=404, detail=f"Índice '{index_name}' no encontrado")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error en búsqueda: {str(e)}")
