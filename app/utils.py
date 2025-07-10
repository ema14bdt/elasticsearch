import pandas as pd
from io import StringIO

def process_csv(file) -> pd.DataFrame:
    """
    Lee un archivo CSV en un DataFrame de Pandas.
    """
    content = file.read().decode('utf-8')
    return pd.read_csv(StringIO(content))

def infer_mapping(df: pd.DataFrame) -> dict:
    """
    Infiere un mapping de Elasticsearch a partir de los tipos de un DataFrame.
    """
    properties = {}
    for column, dtype in df.dtypes.items():
        if pd.api.types.is_numeric_dtype(dtype):
            properties[column] = {"type": "float"} # Usar float para números
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            properties[column] = {"type": "date"}
        else:
            # Por defecto, tratar como texto con un sub-campo .keyword para agregaciones
            properties[column] = {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }
    return {"properties": properties}
