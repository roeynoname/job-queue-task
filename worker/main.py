"""
Worker Process
==============
Pulls jobs from the Redis priority queue and executes them.

KEY DESIGN CHOICES:

1. ATOMIC PICKUP
   BZPOPMAX is atomic at the Redis level — only one worker ever gets each job_id.
   BUT: we still do a `SELECT FOR UPDATE` in Postgres when claiming the job.
   Why? It's a safety net for the crash-recovery scenario:
     - Monitor re-queues a job (sets PENDING, pushes to Redis)
     - Original worker (slow, not dead) also tries to complete the same job
   The FOR UPDATE + status check ensures only one path wins.

2. RETRY BACKOFF
   Attempt 1 fails → re-queue immediately (delay = 0)
   Attempt 2 fails → scheduled_at = now + 30s (monitor promotes it)
   Attempt 3 fails → FAILED permanently
   
   We increment `current_attempts` BEFORE running the job (when claiming it).
   This way, if a worker crashes mid-job, current_attempts already reflects
   the attempt that was in-flight, and crash recovery doesn't get a "free" attempt.

3. GRACEFUL SHUTDOWN
   On SIGTERM/SIGINT: stop accepting new jobs, finish the current one, then exit.
"""

import logging
import signal
import threading
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models import Job, JobLog, JobStatus
from app.queue import PriorityQueue
from worker.handlers import JOB_HANDLERS
from worker.monitor import run_monitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("worker")

# Delay (seconds) to wait before the Nth retry.
# Index = current_attempts value AFTER the failed attempt was recorded.
# attempt 1 fails (current_attempts=1) → RETRY_DELAYS[1] = 0  (immediate)
# attempt 2 fails (current_attempts=2) → RETRY_DELAYS[2] = 30s
# attempt 3 fails (current_attempts=3) → >= max_attempts → FAILED
RETRY_DELAYS = {1: 0, 2: 30, 3: 120}
DEFAULT_RETRY_DELAY = 120


def _add_log(db: Session, job_id, level: str, message: str, meta: dict = None):
    log = JobLog(job_id=job_id, level=level, message=message, meta=meta)
    db.add(log)
    # Don't commit here; caller handles the transaction


class Worker:
    def __init__(self):
        self.queue = PriorityQueue()
        self.running = True
        self._current_job_id: Optional[str] = None

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info(f"Signal {signum} received — finishing current job then shutting down")
        self.running = False

    # ──────────────────────────────────────────────────────────────────────────
    # Core processing logic
    # ──────────────────────────────────────────────────────────────────────────

    def _claim_job(self, db: Session, job_id: str) -> Optional[Job]:
        """
        Atomically claim a job by:
          1. Acquiring a row-level lock (FOR UPDATE)
          2. Verifying the job is still PENDING
          3. Transitioning to PROCESSING and setting the timeout deadline

        Returns the job if claimed, None if it should be skipped.
        """
        job = (
            db.query(Job)
            .filter(Job.id == job_id)
            .with_for_update()
            .first()
        )
        if not job:
            logger.warning(f"Job {job_id} not found in DB — skipping")
            return None

        if job.status != JobStatus.PENDING:
            # Could be CANCELLED, already COMPLETED (crash recovery edge case), etc.
            logger.info(f"Job {job_id} has status={job.status}, not PENDING — skipping")
            return None

        job.status = JobStatus.PROCESSING
        job.current_attempts += 1
        job.started_at = datetime.utcnow()
        job.processing_timeout_at = (
            datetime.utcnow() + timedelta(seconds=settings.WORKER_TIMEOUT_SECONDS)
        )
        _add_log(
            db, job.id, "info",
            f"Starting attempt {job.current_attempts}/{job.max_attempts}",
        )
        db.commit()  # Releases the FOR UPDATE lock; job is now PROCESSING in DB
        return job

    def _complete_job(self, db: Session, job: Job, result: dict):
        """Mark a job as COMPLETED after successful execution."""
        # Re-read and lock before updating — guards against crash-recovery race
        # where monitor re-queued the job and another worker claimed it while
        # this (slow) worker was finishing.
        current = (
            db.query(Job)
            .filter(Job.id == job.id)
            .with_for_update()
            .first()
        )
        if current.status != JobStatus.PROCESSING:
            logger.warning(
                f"Job {job.id} status is now {current.status} (expected PROCESSING). "
                "Another worker likely took over. Discarding result."
            )
            db.rollback()
            return

        current.status = JobStatus.COMPLETED
        current.result = result
        current.progress = 100.0
        current.completed_at = datetime.utcnow()
        current.processing_timeout_at = None
        _add_log(db, current.id, "info", "Job completed successfully")
        db.commit()
        logger.info(f"Job {job.id} COMPLETED")

    def _fail_job(self, db: Session, job: Job, error: Exception):
        """
        Handle a failed job: either schedule a retry or mark as permanently FAILED.
        """
        current = (
            db.query(Job)
            .filter(Job.id == job.id)
            .with_for_update()
            .first()
        )
        if current.status != JobStatus.PROCESSING:
            logger.warning(f"Job {job.id} is {current.status}, skipping failure handling")
            db.rollback()
            return

        current.error_message = str(error)
        current.processing_timeout_at = None

        if current.current_attempts >= current.max_attempts:
            # Exhausted all attempts — permanently failed
            current.status = JobStatus.FAILED
            current.completed_at = datetime.utcnow()
            _add_log(
                db, current.id, "error",
                f"Permanently FAILED after {current.current_attempts} attempts: {error}",
            )
            db.commit()
            logger.error(f"Job {job.id} permanently FAILED")
        else:
            delay = RETRY_DELAYS.get(current.current_attempts, DEFAULT_RETRY_DELAY)
            _add_log(
                db, current.id, "warning",
                f"Attempt {current.current_attempts} failed, "
                f"retrying in {delay}s: {error}",
            )
            if delay == 0:
                current.status = JobStatus.PENDING
                db.commit()
                # Push back to queue immediately
                self.queue.push(
                    str(current.id), current.priority, current.created_at.timestamp()
                )
            else:
                current.status = JobStatus.SCHEDULED
                current.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
                db.commit()
                # Monitor will promote it to PENDING when scheduled_at arrives
            logger.info(f"Job {job.id} scheduled for retry in {delay}s")

    # ──────────────────────────────────────────────────────────────────────────
    # Main processing entry point
    # ──────────────────────────────────────────────────────────────────────────

    def process(self, job_id: str):
        db: Session = SessionLocal()
        job: Optional[Job] = None
        try:
            # Phase 1: claim
            job = self._claim_job(db, job_id)
            if not job:
                return

            logger.info(f"Processing job {job.id} (type={job.type}, priority={job.priority})")

            handler = JOB_HANDLERS.get(job.type)
            if not handler:
                raise ValueError(f"Unknown job type: '{job.type}'")

            def update_progress(pct: float):
                """Called by handlers (e.g. batch) to report intermediate progress."""
                try:
                    job.progress = round(pct, 1)
                    db.commit()
                except Exception as e:
                    logger.warning(f"Progress update failed: {e}")

            # Phase 2: execute (outside any DB lock)
            job.payload["current_attempt"] = job.current_attempts
            result = handler(job.payload, update_progress)

            # Phase 3: complete
            self._complete_job(db, job, result)

        except Exception as exc:
            logger.exception(f"Job {job_id} raised an exception")
            if job:
                try:
                    self._fail_job(db, job, exc)
                except Exception as inner:
                    logger.error(f"Failed to record failure for job {job_id}: {inner}")
                    db.rollback()
        finally:
            db.close()

    # ──────────────────────────────────────────────────────────────────────────
    # Event loop
    # ──────────────────────────────────────────────────────────────────────────

    def run(self):
        logger.info("Worker started")
        while self.running:
            job_id = self.queue.pop(timeout=2)
            if job_id:
                logger.info("Job id popped from queue: %s", job_id)
                self._current_job_id = job_id
                self.process(job_id)
                self._current_job_id = None

        logger.info("Worker stopped gracefully")


def main():
    # ── Start monitor in a background daemon thread ───────────────────────────
    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=run_monitor,
        kwargs={"interval": settings.MONITOR_INTERVAL_SECONDS, "stop_event": stop_event},
        daemon=True,
        name="monitor",
    )
    monitor_thread.start()

    # ── Run worker (blocks until shutdown signal) ─────────────────────────────
    worker = Worker()
    worker.run()

    # ── Cleanup ───────────────────────────────────────────────────────────────
    stop_event.set()
    monitor_thread.join(timeout=5)
    logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
