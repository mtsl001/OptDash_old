"""
deps.py — FastAPI dependencies shared across all routers.
Provides a request-scoped DuckDB connection.
"""
from typing import Generator
import duckdb
from config import DUCKDB_PATH

def get_duckdb() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    FastAPI dependency that yields a DuckDB connection per request.
    DuckDB allows multiple concurrent READ connections.
    """
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        yield conn
    finally:
        conn.close()
