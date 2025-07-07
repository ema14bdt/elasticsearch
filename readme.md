# Elasticsearch CSV Search

A FastAPI application to upload CSV files and perform searches using Elasticsearch.

## Features

- Upload CSV files and index them in Elasticsearch
- Search indexed data with fuzzy queries
- Simple web frontend (Vue.js)
- REST API documentation with Swagger

## Requirements

- Python 3.8+
- Elasticsearch 7.x or 8.x
- Node.js (optional, for advanced frontend)

## Usage

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Start Elasticsearch server.

3. Run the FastAPI app:
   ```
   uvicorn main:app --reload
   ```

4. Access the app at [http://localhost:8000/](http://localhost:8000/)

## API Endpoints

- `POST /upload-csv`: Upload and index a CSV file
- `POST /search`: Search indexed data
- `GET /indices`: List available indices
- `GET /health`: Health check

## Configuration

Set environment variables for Elasticsearch connection:

```
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
```

## License

MIT