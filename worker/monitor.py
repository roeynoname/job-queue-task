"""
Background Monitor
==================
Runs in a dedicated thread alongside the worker. Handles two concerns:

1. CRASH RECOVERY
   If a worker dies mid-job, the job is stuck in PROCESSING forever.
   We detect this via `processing_timeout_at`: when a worker claims a job it
   writes a deadline. If now > deadline and status is still PROCESSING,
   the worker is presumed dead and we re-queue the job.

   We use `with_for_update(skip_locked=True)` so multiple monitor instances
   (if running) don't deadlock on the same rows.

2. SCHEDULED JOB PROMOTION
   Jobs with a future `scheduled_at` sit in SCHEDULED state.
   When their time arrives, we move them to PENDING and push to the queue.
   This also handles retry backoff: failed jobs set scheduled_at = now + delay
   and the monitor picks them up when ready.
"""

import logging
import time
from datetime import datetime

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Job, JobStatus
from app.queue import PriorityQueue

logger = logging.getLogger("monitor")


def _recover_stuck_jobs(db: Session, queue: PriorityQueue) -> int:
    """
    Find PROCESSING jobs whose deadline has passed → worker is dead.
    Re-queue them if attempts remain; otherwise mark FAILED.
    Returns count of recovered jobs.
    """
    stuck = (
        db.query(Job)
        .filter(
            Job.status == JobStatus.PROCESSING,
            Job.processing_timeout_at < datetime.utcnow(),
        )
        .with_for_update(skip_locked=True)
        .all()
    )

    count = 0
    for job in stuck:
        logger.warning(
            f"Recovering stuck job {job.id} "
            f"(type={job.type}, attempts={job.current_attempts}/{job.max_attempts})"
        )
        if job.current_attempts >= job.max_attempts:
            job.status = JobStatus.FAILED
            job.error_message = "Worker timeout: exceeded processing deadline after max attempts"
            job.completed_at = datetime.utcnow()
        else:
            # Re-queue; the worker's attempt count was already incremented when it claimed
            # the job, so we don't increment again here.
            job.status = JobStatus.PENDING
            job.processing_timeout_at = None
            db.flush()
            queue.push(str(job.id), job.priority, job.created_at.timestamp())

        count += 1

    db.commit()
    return count


def _promote_scheduled_jobs(db: Session, queue: PriorityQueue) -> int:
    """
    Find SCHEDULED jobs whose time has arrived → move to PENDING and enqueue.
    This covers:
      - User-submitted future jobs
      - Retry-backoff jobs (worker sets scheduled_at = now + delay)
    Returns count of promoted jobs.
    """
    due = (
        db.query(Job)
        .filter(
            Job.status == JobStatus.SCHEDULED,
            Job.scheduled_at <= datetime.utcnow(),
        )
        .with_for_update(skip_locked=True)
        .all()
    )

    count = 0
    for job in due:
        logger.info(f"Promoting scheduled job {job.id} (type={job.type}) to PENDING")
        job.status = JobStatus.PENDING
        job.scheduled_at = None
        db.flush()
        queue.push(str(job.id), job.priority, job.created_at.timestamp())
        count += 1

    db.commit()
    return count


def run_monitor(interval: int = 30, stop_event=None) -> None:
    """
    Main monitor loop. Runs indefinitely until stop_event is set (or process killed).

    Args:
        interval: Seconds between monitor cycles.
        stop_event: threading.Event — if set, the loop exits gracefully.
    """
    queue = PriorityQueue()
    logger.info(f"Monitor started (interval={interval}s)")

    while True:
        if stop_event and stop_event.is_set():
            logger.info("Monitor stopping")
            break

        db: Session = SessionLocal()
        try:
            recovered = _recover_stuck_jobs(db, queue)
            promoted = _promote_scheduled_jobs(db, queue)
            if recovered or promoted:
                logger.info(f"Monitor cycle: recovered={recovered}, promoted={promoted}")
        except Exception as exc:
            logger.error(f"Monitor error: {exc}", exc_info=True)
            try:
                db.rollback()
            except Exception:
                pass
        finally:
            db.close()

        time.sleep(interval)
