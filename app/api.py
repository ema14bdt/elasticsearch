import logging
import time
from fastapi.staticfiles import StaticFiles
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from .dependencies import es_manager, es

from .routes.upload import router as upload_router
from .routes.search import router as search_router
from .routes.indices import router as indices_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Elasticsearch CSV Search",
    description="Aplicación para cargar CSVs y realizar búsquedas con Elasticsearch",
    version="1.0.0"
)

# Manejo global de errores HTTPException
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error": str(exc)
        },
    )

from fastapi.exceptions import RequestValidationError, HTTPException

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()}
    )

# Montar la carpeta 'frontend' en /static
app.mount("/static", StaticFiles(directory="frontend", html=True), name="static")

app.include_router(upload_router)
app.include_router(search_router)
app.include_router(indices_router)


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

@app.get("/", response_class=FileResponse)
async def root():
    return "frontend/index.html"
