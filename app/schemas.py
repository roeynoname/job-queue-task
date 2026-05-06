import uuid
from datetime import datetime
from typing import Optional, Any, Dict, List

from pydantic import BaseModel, Field

from app.models import JobStatus


class JobCreate(BaseModel):
    type: str = Field(..., description="Job type: email, webhook, report, batch")
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=100, description="Higher = more urgent")
    max_attempts: int = Field(default=3, ge=1, le=10)
    idempotency_key: Optional[str] = Field(default=None, max_length=255)
    scheduled_at: Optional[datetime] = Field(default=None, description="Future execution time")


class JobResponse(BaseModel):
    id: uuid.UUID
    type: str
    payload: Dict[str, Any]
    status: JobStatus
    priority: int
    max_attempts: int
    current_attempts: int
    error_message: Optional[str]
    progress: float
    result: Optional[Any]
    idempotency_key: Optional[str]
    scheduled_at: Optional[datetime]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]

    model_config = {"from_attributes": True}


class HealthResponse(BaseModel):
    status: str
    queue_size: int
    redis_ok: bool
    db_ok: bool
