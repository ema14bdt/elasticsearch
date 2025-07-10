import logging
import os
from elasticsearch import Elasticsearch
from .elastic_manager import ElasticsearchManager
from .config import ELASTICSEARCH_HOST, ELASTICSEARCH_PORT

logger = logging.getLogger(__name__)

# Priorizar la conexión por URL si está disponible (para Vercel)
elastic_url = os.getenv("ELASTIC_URL")

if elastic_url:
    es = Elasticsearch([elastic_url])
else:
    # Conexión local
    es = Elasticsearch([f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"])

if not es.ping():
    logger.error("❌ No se pudo conectar con Elasticsearch")
    # En un entorno serverless, es mejor no lanzar una excepción al inicio
    # La aplicación puede funcionar parcialmente o el problema puede ser temporal
    # raise Exception("No se pudo conectar con Elasticsearch")

es_manager = ElasticsearchManager(es)
