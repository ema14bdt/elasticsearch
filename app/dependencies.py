import logging
from elasticsearch import Elasticsearch
from .elastic_manager import ElasticsearchManager
from .config import ELASTICSEARCH_HOST, ELASTICSEARCH_PORT

logger = logging.getLogger(__name__)

es = Elasticsearch([f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"])
if not es.ping():
    logger.error("❌ No se pudo conectar con Elasticsearch")
    raise Exception("No se pudo conectar con Elasticsearch")

es_manager = ElasticsearchManager(es)
