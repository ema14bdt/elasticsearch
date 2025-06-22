import logging
import time
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse
from .dependencies import es_manager, es

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Elasticsearch CSV Search",
    description="Aplicación para cargar CSVs y realizar búsquedas con Elasticsearch",
    version="1.0.0"
)

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <html>
        <head><title>Elasticsearch CSV Search</title></head>
        <body>
            <h1>🔍 Elasticsearch CSV Search API</h1>
            <p>Aplicación para cargar CSVs y realizar búsquedas con Elasticsearch</p>
            <ul>
                <li><a href="/docs">📚 Documentación interactiva</a></li>
                <li><a href="/health">🏥 Estado de la aplicación</a></li>
                <li><a href="/indices">📋 Lista de índices</a></li>
            </ul>
        </body>
    </html>
    """

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), index_name: str = Form(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")
    if not index_name or not index_name.replace('_', '').replace('-', '').isalnum():
        raise HTTPException(status_code=400, detail="Nombre de índice inválido")
    try:
        contents = await file.read()
        df = pd.read_csv(pd.io.common.StringIO(contents.decode('utf-8')))
        mapping = {
            "mappings": {
                "properties": {
                    col: {"type": "double"} if df[col].dtype in ['int64', 'float64']
                    else {"type": "text", "analyzer": "standard"}
                    for col in df.columns
                }
            }
        }
        if not es_manager.create_index(index_name, mapping):
            raise HTTPException(status_code=500, detail="Error creando índice")
        stats = es_manager.index_dataframe(df, index_name)
        return {
            "message": "CSV cargado e indexado exitosamente",
            "filename": file.filename,
            "index_name": index_name,
            "columns": list(df.columns),
            "stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")

@app.get("/search-page", response_class=HTMLResponse)
async def search_page():
    return """
    <html>
        <head><title>Buscar</title></head>
        <body>
            <h1>🔍 Búsqueda</h1>
            <p>Usa la <a href="/docs">documentación interactiva</a> para realizar búsquedas</p>
        </body>
    </html>
    """

@app.post("/search")
async def perform_search(index_name: str = Form(...), query: str = Form(...), size: int = Form(10)):
    if not query.strip():
        raise HTTPException(status_code=400, detail="La consulta no puede estar vacía")
    return es_manager.search(index_name, query, size)

@app.get("/indices")
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

@app.get("/health")
async def health_check():
    try:
        es_health = es.cluster.health()
        es_status = es_health['status']
        return {
            "app_status": "ok",
            "elasticsearch_status": es_status,
            "elasticsearch_cluster": es_health['cluster_name'],
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "app_status": "ok",
            "elasticsearch_status": "error",
            "error": str(e),
            "timestamp": time.time()
        }
