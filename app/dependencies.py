import os
import logging
from elasticsearch import Elasticsearch
from .elastic_manager import ElasticsearchManager

logger = logging.getLogger(__name__)

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")

es = Elasticsearch([f"http://{ES_HOST}:{ES_PORT}"])
if not es.ping():
    logger.error("❌ No se pudo conectar con Elasticsearch")
    raise Exception("No se pudo conectar con Elasticsearch")

es_manager = ElasticsearchManager(es)
