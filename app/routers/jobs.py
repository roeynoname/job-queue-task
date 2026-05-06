import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Job, JobStatus
from app.queue import PriorityQueue
from app.schemas import JobCreate, JobResponse

router = APIRouter(prefix="/jobs", tags=["jobs"])

# One queue instance shared across requests (thread-safe; Redis handles concurrency)
_queue = PriorityQueue()


@router.post("/", response_model=JobResponse, status_code=201)
def submit_job(body: JobCreate, db: Session = Depends(get_db)):
    """
    Submit a new job. Supports:
    - Idempotency: if `idempotency_key` matches an existing job, return that job.
    - Scheduling: if `scheduled_at` is in the future, job starts in SCHEDULED state.
    - Priority: 0–100, higher = processed first.
    """
    # ── Idempotency check ────────────────────────────────────────────────────
    if body.idempotency_key:
        existing = (
            db.query(Job)
            .filter(Job.idempotency_key == body.idempotency_key)
            .first()
        )
        if existing:
            return existing

    # ── Determine initial status ─────────────────────────────────────────────
    is_future = body.scheduled_at and body.scheduled_at > datetime.utcnow()
    initial_status = JobStatus.SCHEDULED if is_future else JobStatus.PENDING

    job = Job(
        type=body.type,
        payload=body.payload,
        priority=body.priority,
        max_attempts=body.max_attempts,
        idempotency_key=body.idempotency_key,
        scheduled_at=body.scheduled_at,
        status=initial_status,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Only enqueue immediately if PENDING; SCHEDULED jobs wait for the monitor
    if initial_status == JobStatus.PENDING:
        _queue.push(str(job.id), job.priority, job.created_at.timestamp())

    return job


@router.get("/", response_model=List[JobResponse])
def list_jobs(
    status: Optional[JobStatus] = None,
    type: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    q = db.query(Job)
    if status:
        q = q.filter(Job.status == status)
    if type:
        q = q.filter(Job.type == type)
    return q.order_by(Job.created_at.desc()).offset(offset).limit(limit).all()


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/{job_id}/cancel", response_model=JobResponse)
def cancel_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    """
    Cancel a PENDING or SCHEDULED job.
    We remove it from Redis atomically first, then update the DB.
    Order matters: remove from queue before DB update so no worker
    can pick it up between the two operations.
    """
    # FOR UPDATE: prevents a concurrent worker from claiming this job
    # at the exact moment we're trying to cancel it
    job = (
        db.query(Job)
        .filter(Job.id == job_id)
        .with_for_update()
        .first()
    )
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.PENDING, JobStatus.SCHEDULED):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel a job in '{job.status}' state. "
                   f"Only PENDING or SCHEDULED jobs can be cancelled.",
        )

    # Remove from Redis queue first (idempotent if not present)
    _queue.remove(str(job_id))

    job.status = JobStatus.CANCELLED
    job.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(job)
    return job


@router.post("/{job_id}/retry", response_model=JobResponse)
def retry_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    """Manually retry a permanently FAILED job. Resets attempt counter."""
    job = (
        db.query(Job)
        .filter(Job.id == job_id)
        .with_for_update()
        .first()
    )
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.FAILED:
        raise HTTPException(
            status_code=400,
            detail=f"Only FAILED jobs can be manually retried. Current status: {job.status}",
        )

    job.status = JobStatus.PENDING
    job.current_attempts = 0
    job.error_message = None
    job.result = None
    job.progress = 0.0
    job.completed_at = None
    db.commit()

    _queue.push(str(job.id), job.priority, job.created_at.timestamp())

    db.refresh(job)
    return job
