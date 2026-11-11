
"""
SchemaGuard - FastAPI Application
"""

from fastapi import FastAPI

from backend.api.routes.pipeline_routes import router

from backend.database.connection import initialize_database


# --------------------------------------------------
# APPLICATION CONFIGURATION
# --------------------------------------------------

app = FastAPI(
    title="SchemaGuard API",
    description=(
        "Intelligent Parquet Schema Evolution "
        "and Data Quality Pipeline"
    ),
    version="1.0.0"
)


# --------------------------------------------------
# DATABASE INITIALIZATION
# --------------------------------------------------

@app.on_event("startup")
def startup_event():

    initialize_database()


# --------------------------------------------------
# REGISTER API ROUTES
# --------------------------------------------------

app.include_router(router)


# --------------------------------------------------
# ROOT ENDPOINT
# --------------------------------------------------

@app.get("/")
def root():

    return {
        "application": "SchemaGuard",
        "message": "SchemaGuard API is running",
        "documentation": "/docs",
        "health": "/api/health"
    }
