
"""
SchemaGuard - Pipeline API Routes
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.services.pipeline_service import run_pipeline

from backend.database.repository import (
    get_pipeline_run,
    get_all_pipeline_runs,
    get_batches_for_run,
    get_quarantine_logs,
    get_metrics_for_run
)


router = APIRouter(
    prefix="/api",
    tags=["SchemaGuard Pipeline"]
)


# --------------------------------------------------
# REQUEST MODEL
# --------------------------------------------------

class PipelineRunRequest(BaseModel):

    source_filename: str = Field(
        default="batch_01.parquet",
        min_length=1,
        max_length=255
    )


# --------------------------------------------------
# 1. HEALTH CHECK
# --------------------------------------------------

@router.get("/health")
def health_check():

    return {
        "application": "SchemaGuard",
        "status": "HEALTHY"
    }


# --------------------------------------------------
# 2. RUN PIPELINE
# --------------------------------------------------

@router.post("/pipeline/run")
def execute_pipeline(request: PipelineRunRequest):

    try:

        return run_pipeline(
            source_filename=request.source_filename
        )

    except FileNotFoundError as error:

        raise HTTPException(
            status_code=404,
            detail=str(error)
        )

    except (ValueError, NotImplementedError) as error:

        raise HTTPException(
            status_code=400,
            detail=str(error)
        )

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(error)
        )


# --------------------------------------------------
# 3. GET ALL PIPELINE RUNS
# --------------------------------------------------

@router.get("/runs")
def list_pipeline_runs(limit: int = 20):

    return {
        "runs": get_all_pipeline_runs(limit)
    }


# --------------------------------------------------
# 4. GET ONE PIPELINE RUN
# --------------------------------------------------

@router.get("/runs/{run_id}")
def pipeline_run_details(run_id: str):

    result = get_pipeline_run(run_id)

    if result is None:

        raise HTTPException(
            status_code=404,
            detail="Pipeline run not found"
        )

    return result


# --------------------------------------------------
# 5. GET BATCH DETAILS
# --------------------------------------------------

@router.get("/runs/{run_id}/batches")
def batch_details(run_id: str):

    run = get_pipeline_run(run_id)

    if run is None:

        raise HTTPException(
            status_code=404,
            detail="Pipeline run not found"
        )

    return {
        "batches": get_batches_for_run(run_id)
    }


# --------------------------------------------------
# 6. GET QUARANTINE HISTORY
# --------------------------------------------------

@router.get("/runs/{run_id}/quarantine")
def quarantine_history(run_id: str):

    run = get_pipeline_run(run_id)

    if run is None:

        raise HTTPException(
            status_code=404,
            detail="Pipeline run not found"
        )

    return {
        "quarantine_logs": get_quarantine_logs(run_id)
    }


# --------------------------------------------------
# 7. GET PIPELINE METRICS
# --------------------------------------------------

@router.get("/runs/{run_id}/metrics")
def pipeline_metrics(run_id: str):

    run = get_pipeline_run(run_id)

    if run is None:

        raise HTTPException(
            status_code=404,
            detail="Pipeline run not found"
        )

    return {
        "metrics": get_metrics_for_run(run_id)
    }
