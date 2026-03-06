"""
main.py — FastAPI application with CORS, health check, and all routers.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Routers are packages within src/api
from routers import market, screener, microstructure, position, ai

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure DuckDB views are current
    from duckdb_setup import initialize_duckdb
    initialize_duckdb()
    # Initialise AI trade journal (SQLite)
    from journal import init_db
    init_db()
    yield
    # Shutdown logic if any


app = FastAPI(
    title="Option Buying Dashboard API",
    version="2.0.0",
    description="Real-time analytics for NIFTY/BANKNIFTY option buying",
    lifespan=lifespan,
)

# CORS — allow Vite dev server on port 5173
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173",
                   "http://localhost:4173", "http://127.0.0.1:4173"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Include all routers
app.include_router(market.router,          prefix="/market",      tags=["Market"])
app.include_router(screener.router,        prefix="/screener",    tags=["Screener"])
app.include_router(microstructure.router,  prefix="/micro",       tags=["Microstructure"])
app.include_router(position.router,        prefix="/position",    tags=["Position"])
app.include_router(ai.router,              prefix="/ai",          tags=["AI Bot"])


@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/")
def root():
    return {"message": "Option Buying Dashboard API. Visit /docs for Swagger UI."}
