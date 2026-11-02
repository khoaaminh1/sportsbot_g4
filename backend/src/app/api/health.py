from __future__ import annotations

from fastapi import APIRouter, Request, Response

from app import __version__
from app.schemas.errors import ErrorResponse
from app.schemas.health import HealthResponse, LivenessResponse

router = APIRouter(tags=["health"])


@router.get(
    "/health/live",
    response_model=LivenessResponse,
    summary="Liveness: the process is up (no dependency checks)",
)
async def live() -> LivenessResponse:
    return LivenessResponse(version=__version__)


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Readiness: PostgreSQL, Qdrant and (optional) HDFS checks",
    responses={503: {"model": HealthResponse, "description": "A required dependency failed"}},
)
async def health(request: Request, response: Response) -> HealthResponse:
    result = await request.app.state.health_service.run()
    if result.status == "error":
        response.status_code = 503
    return result


# Referenced so the shared error schema appears in the OpenAPI document.
ERROR_RESPONSES = {500: {"model": ErrorResponse, "description": "Unexpected error"}}
