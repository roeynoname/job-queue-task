"""
Test suite for the Job Queue Service.

Tests are integration tests — they exercise the full stack (API + worker logic + DB + Redis).
Worker execution is driven by calling Worker.process() directly, without a running worker
process. This makes tests deterministic and fast.

Coverage:
  1. Job submission and retrieval
  2. Job completion flow (email job)
  3. Job failure and automatic retry scheduling
  4. Permanent failure after max attempts
  5. Job cancellation
  6. Idempotency key deduplication
  7. Priority ordering
  8. Scheduled job (future execution)
  9. Manual retry of a failed job
 10. Worker crash recovery (monitor logic)
"""

import time
import uuid
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from app.models import Job, JobStatus
from worker.main import Worker
from worker.monitor import _promote_scheduled_jobs, _recover_stuck_jobs


# ── Helpers ───────────────────────────────────────────────────────────────────

def submit(client, job_type="email", priority=0, payload=None, **kwargs):
    """Helper to POST /jobs and assert success."""
    body = {"type": job_type, "priority": priority, "payload": payload or {}, **kwargs}
    resp = client.post("/jobs/", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def run_job(job_id: str):
    """Run a single job through the worker without starting the event loop."""
    worker = Worker()
    worker.process(job_id)


# ── Test 1: Submission and retrieval ─────────────────────────────────────────

def test_submit_and_get_job(client):
    """Job is persisted with correct initial state after submission."""
    job = submit(client, job_type="email", priority=5, payload={"to": "a@b.com"})

    assert job["status"] == "pending"
    assert job["type"] == "email"
    assert job["priority"] == 5
    assert job["current_attempts"] == 0
    assert job["progress"] == 0.0
    assert job["result"] is None
    assert job["id"] is not None

    # Fetch by ID
    fetched = client.get(f"/jobs/{job['id']}").json()
    assert fetched["id"] == job["id"]
    assert fetched["status"] == "pending"


def test_list_jobs_filter_by_status(client):
    """Listing jobs with status filter returns only matching jobs."""
    submit(client, job_type="email")
    submit(client, job_type="report")

    resp = client.get("/jobs/?status=pending")
    assert resp.status_code == 200
    jobs = resp.json()
    assert len(jobs) >= 2
    assert all(j["status"] == "pending" for j in jobs)


# ── Test 2: Completion flow ───────────────────────────────────────────────────

def test_email_job_completion(client, db):
    """Email job runs to COMPLETED with a result containing message_id."""
    job = submit(client, job_type="email", payload={"to": "user@example.com"})
    job_id = job["id"]

    with patch("worker.handlers.time.sleep"):  # Skip actual sleeps in tests
        run_job(job_id)

    result = client.get(f"/jobs/{job_id}").json()
    assert result["status"] == "completed"
    assert result["progress"] == 100.0
    assert result["result"]["message_id"].startswith("msg_")
    assert result["current_attempts"] == 1
    assert result["started_at"] is not None
    assert result["completed_at"] is not None


# ── Test 3: Failure and automatic retry ──────────────────────────────────────

def test_job_failure_schedules_retry(client, db, queue):
    """
    When a job fails and has attempts remaining, it is scheduled for retry.
    The first retry has delay=0 (immediate → PENDING).
    """
    job = submit(client, job_type="webhook", payload={"url": "https://x.com"})
    job_id = job["id"]

    # Force the handler to always raise
    with patch.dict("worker.handlers.JOB_HANDLERS", {"webhook": Mock(side_effect=RuntimeError("Simulated failure"))}):
        run_job(job_id)

    result = client.get(f"/jobs/{job_id}").json()
    # Attempt 1 failed, delay[1]=0 → should be PENDING again (immediate retry)
    assert result["status"] == "pending"
    assert result["current_attempts"] == 1
    assert result["error_message"] == "Simulated failure"
    assert result["progress"] == 0.0


def test_job_failure_with_backoff(client, db, queue):
    """
    After the second failure, retry delay is 30s → job goes to SCHEDULED.
    """
    job = submit(client, job_type="webhook", payload={"url": "https://x.com"})
    job_id = job["id"]

    # Simulate two failures: after first failure job is PENDING → run again
    with patch.dict("worker.handlers.JOB_HANDLERS", {"webhook": Mock(side_effect=RuntimeError("fail"))}):
        run_job(job_id)  # attempt 1 → PENDING (delay=0)
        run_job(job_id)  # attempt 2 → SCHEDULED (delay=30s)

    result = client.get(f"/jobs/{job_id}").json()
    assert result["status"] == "scheduled"
    assert result["current_attempts"] == 2
    assert result["scheduled_at"] is not None


# ── Test 4: Permanent failure ─────────────────────────────────────────────────

def test_permanent_failure_after_max_attempts(client, db, queue):
    """After max_attempts (3) failures, job is permanently FAILED."""
    job = submit(client, job_type="webhook", max_attempts=3)
    job_id = job["id"]

    with patch.dict("worker.handlers.JOB_HANDLERS", {"webhook": Mock(side_effect=RuntimeError("always fails"))}):
        run_job(job_id)  # attempt 1 → PENDING (retry immediately)
        run_job(job_id)  # attempt 2 → SCHEDULED (retry in 30s)

        # Fast-forward the scheduled_at so it's "due now"
        db_job = db.query(Job).filter(Job.id == job_id).first()
        db_job.scheduled_at = datetime.utcnow() - timedelta(seconds=1)
        db_job.status = JobStatus.PENDING  # manually promote (monitor would do this)
        db.commit()
        from app.queue import PriorityQueue
        PriorityQueue().push(job_id, db_job.priority, db_job.created_at.timestamp())

        run_job(job_id)  # attempt 3 → FAILED
        db.expire_all()  # flush stale cache so next read hits the DB

    result = client.get(f"/jobs/{job_id}").json()
    assert result["status"] == "failed"
    assert result["current_attempts"] == 3
    assert "always fails" in result["error_message"]


# ── Test 5: Cancellation ──────────────────────────────────────────────────────

def test_cancel_pending_job(client, db, queue):
    """Cancelling a PENDING job removes it from the queue and marks CANCELLED."""
    job = submit(client, job_type="report")
    job_id = job["id"]

    # Verify it's in the Redis queue
    import redis as redis_lib, os
    r = redis_lib.from_url(os.environ["REDIS_URL"], decode_responses=True)
    assert r.zscore("jobs:priority_queue", job_id) is not None

    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"

    # Must be removed from the queue (worker won't pick it up)
    assert r.zscore("jobs:priority_queue", job_id) is None

    # Cannot cancel again
    resp2 = client.post(f"/jobs/{job_id}/cancel")
    assert resp2.status_code == 400


def test_cannot_cancel_completed_job(client, db):
    """Cancelling an already-completed job returns 400."""
    job = submit(client, job_type="email")
    job_id = job["id"]

    with patch("worker.handlers.time.sleep"):
        run_job(job_id)

    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 400


# ── Test 6: Idempotency ───────────────────────────────────────────────────────

def test_idempotency_returns_existing_job(client, db):
    """Submitting twice with the same idempotency_key returns the original job."""
    idem_key = f"test-idem-{uuid.uuid4().hex}"

    resp1 = client.post("/jobs/", json={
        "type": "email",
        "payload": {"to": "a@b.com"},
        "idempotency_key": idem_key,
    })
    assert resp1.status_code == 201
    job1 = resp1.json()

    resp2 = client.post("/jobs/", json={
        "type": "email",
        "payload": {"to": "completely-different@example.com"},  # Different payload!
        "idempotency_key": idem_key,
    })
    assert resp2.status_code == 201
    job2 = resp2.json()

    # Same ID returned, NOT a new job
    assert job1["id"] == job2["id"]
    # Original payload preserved
    assert job2["payload"]["to"] == "a@b.com"


# ── Test 7: Priority ordering ─────────────────────────────────────────────────

def test_priority_ordering(client, db, queue):
    """
    Higher-priority jobs are returned first by the queue.
    Submit low→high→medium, expect high→medium→low order from queue.
    """
    low = submit(client, job_type="email", priority=1)
    high = submit(client, job_type="email", priority=10)
    mid = submit(client, job_type="email", priority=5)

    popped = [queue.pop(timeout=1), queue.pop(timeout=1), queue.pop(timeout=1)]

    assert popped[0] == high["id"], "Highest priority should be first"
    assert popped[1] == mid["id"], "Medium priority should be second"
    assert popped[2] == low["id"], "Lowest priority should be last"


def test_fifo_within_same_priority(client, db, queue):
    """For jobs with equal priority, submission order (FIFO) is preserved."""
    first = submit(client, job_type="email", priority=5)
    time.sleep(0.01)  # Ensure distinct created_at timestamps
    second = submit(client, job_type="email", priority=5)

    p1 = queue.pop(timeout=1)
    p2 = queue.pop(timeout=1)

    assert p1 == first["id"]
    assert p2 == second["id"]


# ── Test 8: Scheduled jobs ────────────────────────────────────────────────────

def test_scheduled_job_not_immediately_queued(client, db, queue):
    """A job with future scheduled_at starts as SCHEDULED and is NOT in Redis yet."""
    future = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    job = submit(client, job_type="email", scheduled_at=future)

    assert job["status"] == "scheduled"

    # Must NOT be in Redis queue
    import redis as redis_lib, os
    r = redis_lib.from_url(os.environ["REDIS_URL"], decode_responses=True)
    assert r.zscore("jobs:priority_queue", job["id"]) is None


def test_monitor_promotes_due_scheduled_job(db, queue):
    """Monitor promotes a SCHEDULED job whose time has arrived to PENDING."""
    # Insert a SCHEDULED job directly (already due)
    job = Job(
        type="email",
        payload={"to": "x@y.com"},
        status=JobStatus.SCHEDULED,
        scheduled_at=datetime.utcnow() - timedelta(seconds=5),  # Past due
        priority=0,
    )
    db.add(job)
    db.commit()

    promoted = _promote_scheduled_jobs(db, queue)
    assert promoted == 1

    db.refresh(job)
    assert job.status == JobStatus.PENDING
    assert job.scheduled_at is None

    # Should now be in the queue
    import redis as redis_lib, os
    r = redis_lib.from_url(os.environ["REDIS_URL"], decode_responses=True)
    assert r.zscore("jobs:priority_queue", str(job.id)) is not None


# ── Test 9: Manual retry ──────────────────────────────────────────────────────

def test_manual_retry_of_failed_job(client, db, queue):
    """POST /jobs/{id}/retry resets a FAILED job and re-queues it."""
    # Create a job in FAILED state directly
    job = Job(
        type="email",
        payload={},
        status=JobStatus.FAILED,
        current_attempts=3,
        error_message="Something went wrong",
    )
    db.add(job)
    db.commit()

    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 200
    result = resp.json()
    assert result["status"] == "pending"
    assert result["current_attempts"] == 0
    assert result["error_message"] is None

    # Back in the Redis queue
    import redis as redis_lib, os
    r = redis_lib.from_url(os.environ["REDIS_URL"], decode_responses=True)
    assert r.zscore("jobs:priority_queue", str(job.id)) is not None

    # Cannot retry a non-failed job
    resp2 = client.post(f"/jobs/{job.id}/retry")
    assert resp2.status_code == 400


# ── Test 10: Crash recovery ───────────────────────────────────────────────────

def test_monitor_recovers_stuck_processing_job(db, queue):
    """
    If a job has been PROCESSING past its timeout (worker crashed),
    the monitor resets it to PENDING and re-queues it.
    """
    job = Job(
        type="email",
        payload={},
        status=JobStatus.PROCESSING,
        current_attempts=1,
        max_attempts=3,
        started_at=datetime.utcnow() - timedelta(minutes=20),
        # Deadline in the past — worker is "dead"
        processing_timeout_at=datetime.utcnow() - timedelta(minutes=10),
    )
    db.add(job)
    db.commit()

    recovered = _recover_stuck_jobs(db, queue)
    assert recovered == 1

    db.refresh(job)
    assert job.status == JobStatus.PENDING

    import redis as redis_lib, os
    r = redis_lib.from_url(os.environ["REDIS_URL"], decode_responses=True)
    assert r.zscore("jobs:priority_queue", str(job.id)) is not None


def test_monitor_fails_stuck_job_when_max_attempts_reached(db, queue):
    """
    If a job is stuck in PROCESSING and has already used all attempts,
    the monitor marks it as permanently FAILED (not re-queued).
    """
    job = Job(
        type="email",
        payload={},
        status=JobStatus.PROCESSING,
        current_attempts=3,
        max_attempts=3,
        processing_timeout_at=datetime.utcnow() - timedelta(minutes=10),
    )
    db.add(job)
    db.commit()

    recovered = _recover_stuck_jobs(db, queue)
    assert recovered == 1

    db.refresh(job)
    assert job.status == JobStatus.FAILED
    assert "timeout" in job.error_message.lower()

    # Must NOT be re-queued
    import redis as redis_lib, os
    r = redis_lib.from_url(os.environ["REDIS_URL"], decode_responses=True)
    assert r.zscore("jobs:priority_queue", str(job.id)) is None
