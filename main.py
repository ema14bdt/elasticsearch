import logging
import time
import os
from typing import Dict, List, Any, Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, NotFoundError
import uvicorn

# Configuración de logging para ver el flujo de ejecución
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Inicialización de FastAPI
app = FastAPI(
    title="Elasticsearch CSV Search",
    description="Aplicación para cargar CSVs y realizar búsquedas con Elasticsearch",
    version="1.0.0"
)

# Configuración de Elasticsearch
# Conecta a Elasticsearch local en el puerto por defecto
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")

logger.info(f"Intentando conectar a Elasticsearch en {ES_HOST}:{ES_PORT}")

try:
    # Inicializa el cliente de Elasticsearch
    es = Elasticsearch([f"http://{ES_HOST}:{ES_PORT}"])
    # Verifica la conexión
    if es.ping():
        logger.info("✅ Conexión exitosa con Elasticsearch")
    else:
        logger.error("❌ No se pudo conectar con Elasticsearch")
        raise ConnectionError("No se pudo conectar con Elasticsearch")
except Exception as e:
    logger.error(f"❌ Error al conectar con Elasticsearch: {e}")
    raise e

class ElasticsearchManager:
    """
    Clase para manejar todas las operaciones con Elasticsearch
    """
    
    def __init__(self, elasticsearch_client: Elasticsearch):
        self.es = elasticsearch_client
        logger.info("🔧 ElasticsearchManager inicializado")
    
    def create_index(self, index_name: str, mapping: Dict) -> bool:
        """
        Crea un índice en Elasticsearch con el mapping especificado
        
        Args:
            index_name: Nombre del índice a crear
            mapping: Estructura de campos del índice
            
        Returns:
            bool: True si se creó exitosamente
        """
        try:
            logger.info(f"🔄 Creando índice: {index_name}")
            
            # Elimina el índice si ya existe
            if self.es.indices.exists(index=index_name):
                logger.info(f"🗑️ Eliminando índice existente: {index_name}")
                self.es.indices.delete(index=index_name)
            
            # Crea el nuevo índice con el mapping
            self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"✅ Índice '{index_name}' creado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando índice {index_name}: {e}")
            return False
    
    def index_dataframe(self, df: pd.DataFrame, index_name: str) -> Dict[str, Any]:
        """
        Indexa un DataFrame de pandas en Elasticsearch
        
        Args:
            df: DataFrame con los datos a indexar
            index_name: Nombre del índice donde guardar los datos
            
        Returns:
            Dict con estadísticas de la indexación
        """
        start_time = time.time()
        logger.info(f"🔄 Iniciando indexación de {len(df)} documentos en '{index_name}'")
        
        success_count = 0
        error_count = 0
        
        try:
            # Itera sobre cada fila del DataFrame
            for index, row in df.iterrows():
                try:
                    # Convierte la fila a diccionario
                    doc = row.to_dict()
                    
                    # Limpia valores NaN que pueden causar problemas
                    doc = {k: (v if pd.notna(v) else None) for k, v in doc.items()}
                    
                    # Indexa el documento
                    response = self.es.index(
                        index=index_name,
                        id=index,  # Usa el índice de la fila como ID
                        body=doc
                    )
                    
                    success_count += 1
                    
                    # Log cada 100 documentos indexados
                    if success_count % 100 == 0:
                        logger.info(f"📈 {success_count} documentos indexados...")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Error indexando documento {index}: {e}")
            
            # Fuerza la actualización del índice
            self.es.indices.refresh(index=index_name)
            
            end_time = time.time()
            indexing_time = end_time - start_time
            
            stats = {
                "total_documents": len(df),
                "success_count": success_count,
                "error_count": error_count,
                "indexing_time_seconds": round(indexing_time, 2),
                "documents_per_second": round(success_count / indexing_time, 2) if indexing_time > 0 else 0
            }
            
            logger.info(f"✅ Indexación completada: {success_count} éxitos, {error_count} errores en {indexing_time:.2f}s")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error general en indexación: {e}")
            return {
                "error": str(e),
                "success_count": success_count,
                "error_count": error_count
            }
    
    def search(self, index_name: str, query: str, size: int = 10) -> Dict[str, Any]:
        """
        Realiza una búsqueda en Elasticsearch
        
        Args:
            index_name: Nombre del índice donde buscar
            query: Texto a buscar
            size: Número máximo de resultados
            
        Returns:
            Dict con los resultados y métricas de búsqueda
        """
        start_time = time.time()
        logger.info(f"🔍 Realizando búsqueda: '{query}' en índice '{index_name}'")
        
        try:
            # Estructura de la consulta de Elasticsearch
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["*"],  # Busca en todos los campos
                        "type": "best_fields",
                        "fuzziness": "AUTO"  # Permite búsquedas aproximadas
                    }
                },
                "size": size,
                "highlight": {
                    "fields": {
                        "*": {}  # Resalta coincidencias en todos los campos
                    }
                }
            }
            
            # Ejecuta la búsqueda
            response = self.es.search(index=index_name, body=search_body)
            
            end_time = time.time()
            search_time = end_time - start_time
            
            # Extrae información de la respuesta
            hits = response['hits']
            total_results = hits['total']['value']
            
            # Procesa los resultados
            results = []
            for hit in hits['hits']:
                result = {
                    "score": hit['_score'],
                    "source": hit['_source'],
                    "highlights": hit.get('highlight', {})
                }
                results.append(result)
            
            search_stats = {
                "query": query,
                "total_results": total_results,
                "returned_results": len(results),
                "search_time_seconds": round(search_time, 3),
                "elasticsearch_took_ms": response['took'],
                "results": results
            }
            
            logger.info(f"✅ Búsqueda completada: {total_results} resultados encontrados en {search_time:.3f}s")
            return search_stats
            
        except NotFoundError:
            logger.error(f"❌ Índice '{index_name}' no encontrado")
            raise HTTPException(status_code=404, detail=f"Índice '{index_name}' no encontrado")
            
        except Exception as e:
            logger.error(f"❌ Error en búsqueda: {e}")
            raise HTTPException(status_code=500, detail=f"Error en búsqueda: {str(e)}")

# Inicializa el manager de Elasticsearch
es_manager = ElasticsearchManager(es)

@app.get("/", response_class=HTMLResponse)
async def home():
    """
    Página principal de la aplicación - Información básica
    """
    logger.info("🏠 Acceso a página principal")
    return """
    <html>
        <head>
            <title>Elasticsearch CSV Search</title>
        </head>
        <body>
            <h1>🔍 Elasticsearch CSV Search API</h1>
            <p>Aplicación para cargar CSVs y realizar búsquedas con Elasticsearch</p>
            <h2>Endpoints disponibles:</h2>
            <ul>
                <li><a href="/docs">📚 Documentación interactiva</a></li>
                <li><a href="/health">🏥 Estado de la aplicación</a></li>
                <li><a href="/indices">📋 Lista de índices</a></li>
            </ul>
            <h2>Uso:</h2>
            <p>1. Usa POST /upload-csv para cargar un archivo CSV</p>
            <p>2. Usa POST /search para realizar búsquedas</p>
        </body>
    </html>
    """

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), index_name: str = Form(...)):
    """
    Endpoint para cargar y procesar archivos CSV
    
    Args:
        file: Archivo CSV subido
        index_name: Nombre del índice donde guardar los datos
    """
    logger.info(f"📤 Recibiendo archivo CSV: {file.filename} para índice: {index_name}")
    
    # Validaciones básicas
    if not file.filename.endswith('.csv'):
        logger.error("❌ Archivo no es CSV")
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")
    
    if not index_name or not index_name.replace('_', '').replace('-', '').isalnum():
        logger.error("❌ Nombre de índice inválido")
        raise HTTPException(status_code=400, detail="Nombre de índice inválido")
    
    try:
        # Lee el contenido del archivo
        contents = await file.read()
        logger.info(f"📖 Leyendo archivo CSV de {len(contents)} bytes")
        
        # Convierte a DataFrame
        df = pd.read_csv(pd.io.common.StringIO(contents.decode('utf-8')))
        logger.info(f"📊 DataFrame creado: {len(df)} filas, {len(df.columns)} columnas")
        logger.info(f"📋 Columnas: {list(df.columns)}")
        
        # Crea el mapping dinámico basado en las columnas del CSV
        mapping = {
            "mappings": {
                "properties": {}
            }
        }
        
        # Define tipos de campos automáticamente
        for column in df.columns:
            if df[column].dtype in ['int64', 'float64']:
                mapping["mappings"]["properties"][column] = {"type": "double"}
            else:
                mapping["mappings"]["properties"][column] = {
                    "type": "text",
                    "analyzer": "standard"
                }
        
        logger.info(f"🗂️ Mapping creado: {list(mapping['mappings']['properties'].keys())}")
        
        # Crea el índice
        if not es_manager.create_index(index_name, mapping):
            raise HTTPException(status_code=500, detail="Error creando índice")
        
        # Indexa los datos
        stats = es_manager.index_dataframe(df, index_name)
        
        logger.info("✅ Proceso de carga completado exitosamente")
        
        return {
            "message": "CSV cargado e indexado exitosamente",
            "filename": file.filename,
            "index_name": index_name,
            "columns": list(df.columns),
            "stats": stats
        }
        
    except Exception as e:
        logger.error(f"❌ Error procesando CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")

@app.get("/search-page", response_class=HTMLResponse)
async def search_page():
    """
    Página simple de búsqueda - Solo para testing
    """
    logger.info("🔍 Acceso a página de búsqueda")
    return """
    <html>
        <head>
            <title>Buscar</title>
        </head>
        <body>
            <h1>🔍 Búsqueda</h1>
            <p>Usa la <a href="/docs">documentación interactiva</a> para realizar búsquedas</p>
            <p>O usa curl/Postman con POST /search</p>
        </body>
    </html>
    """

@app.post("/search")
async def perform_search(
    index_name: str = Form(...),
    query: str = Form(...),
    size: int = Form(10)
):
    """
    Realiza búsqueda en el índice especificado
    
    Args:
        index_name: Nombre del índice donde buscar
        query: Consulta de búsqueda
        size: Número de resultados a retornar
    """
    logger.info(f"🔍 Búsqueda solicitada: '{query}' en '{index_name}'")
    
    if not query.strip():
        raise HTTPException(status_code=400, detail="La consulta no puede estar vacía")
    
    try:
        results = es_manager.search(index_name, query, size)
        return results
        
    except Exception as e:
        logger.error(f"❌ Error en búsqueda: {e}")
        raise e

@app.get("/indices")
async def list_indices():
    """
    Lista todos los índices disponibles en Elasticsearch
    """
    logger.info("📋 Listando índices disponibles")
    
    try:
        # Obtiene información de todos los índices
        indices_info = es.cat.indices(format='json')
        
        # Filtra solo los índices creados por la aplicación (no los del sistema)
        user_indices = [
            {
                "name": idx['index'],
                "docs_count": int(idx['docs.count']) if idx['docs.count'] != '0' else 0,
                "size": idx['store.size'] if idx['store.size'] else '0b'
            }
            for idx in indices_info 
            if not idx['index'].startswith('.')  # Excluye índices del sistema
        ]
        
        logger.info(f"📊 {len(user_indices)} índices de usuario encontrados")
        return {"indices": user_indices}
        
    except Exception as e:
        logger.error(f"❌ Error listando índices: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo índices: {str(e)}")

@app.get("/health")
async def health_check():
    """
    Endpoint para verificar el estado de la aplicación y Elasticsearch
    """
    logger.info("🏥 Verificación de salud solicitada")
    
    try:
        # Verifica conexión con Elasticsearch
        es_health = es.cluster.health()
        es_status = es_health['status']
        
        return {
            "app_status": "ok",
            "elasticsearch_status": es_status,
            "elasticsearch_cluster": es_health['cluster_name'],
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"❌ Error en verificación de salud: {e}")
        return {
            "app_status": "ok",
            "elasticsearch_status": "error",
            "error": str(e),
            "timestamp": time.time()
        }

if __name__ == "__main__":
    logger.info("🚀 Iniciando servidor FastAPI")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Recarga automáticamente en desarrollo
        log_level="info"
    )
    