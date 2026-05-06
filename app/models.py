import uuid
import enum
from datetime import datetime

from sqlalchemy import Column, String, Integer, JSON, DateTime, Text, Float, Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID

from app.database import Base


class JobStatus(str, enum.Enum):
    SCHEDULED = "scheduled"
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(String(50), nullable=False)
    payload = Column(JSON, nullable=False, default=dict)
    status = Column(SAEnum(JobStatus), nullable=False, default=JobStatus.PENDING, index=True)
    priority = Column(Integer, nullable=False, default=0, index=True)

    # Attempt tracking
    max_attempts = Column(Integer, nullable=False, default=3)
    current_attempts = Column(Integer, nullable=False, default=0)

    # Results / errors
    error_message = Column(Text, nullable=True)
    progress = Column(Float, nullable=False, default=0.0)
    result = Column(JSON, nullable=True)

    # Deduplication
    idempotency_key = Column(String(255), unique=True, nullable=True, index=True)

    # Timing
    scheduled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Crash recovery: the worker sets this deadline when claiming the job.
    # If now > processing_timeout_at and status is still PROCESSING,
    # the monitor knows the worker died.
    processing_timeout_at = Column(DateTime, nullable=True)


class JobLog(Base):
    __tablename__ = "job_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    level = Column(String(20), nullable=False, default="info")  # info, warning, error
    message = Column(Text, nullable=False)
    meta = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
