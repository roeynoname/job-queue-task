"""
Priority Queue backed by Redis Sorted Set.

WHY REDIS SORTED SET + BZPOPMAX?
---------------------------------
The core challenge in any distributed job queue is preventing duplicate pickup:
  "two workers must not claim the same job".

BZPOPMAX is an atomic operation — Redis processes it as a single command.
This means at most one worker will ever receive a given job_id. No locks
needed at the queue level; atomicity is guaranteed by Redis single-threading.

Priority scoring:
  score = priority * MAX_TS_MS + (MAX_TS_MS - created_at_ms)

  - Higher priority  → higher score → BZPOPMAX picks it first ✓
  - Same priority    → earlier created_at → larger (MAX - ts) → higher score ✓ (FIFO)

  MAX_TS_MS = 10_000_000_000_000 (year ~2286 in ms), safely larger than
  any real timestamp so each priority level is a fully isolated band.
"""

import redis
from app.config import settings

QUEUE_KEY = "jobs:priority_queue"
MAX_TS_MS = 10_000_000_000_000  # Far-future sentinel, larger than any real timestamp


class PriorityQueue:
    def __init__(self):
        self._redis = redis.from_url(settings.REDIS_URL, decode_responses=True)

    def _score(self, priority: int, created_at_ts: float) -> float:
        created_ms = int(created_at_ts * 1000)
        return priority * MAX_TS_MS + (MAX_TS_MS - created_ms)

    def push(self, job_id: str, priority: int, created_at_ts: float) -> None:
        """Add a job to the queue. Safe to call multiple times (ZADD overwrites)."""
        score = self._score(priority, created_at_ts)
        self._redis.zadd(QUEUE_KEY, {job_id: score})

    def pop(self, timeout: int = 2) -> str | None:
        """
        Atomically pop the highest-priority job.
        Blocks up to `timeout` seconds, returns job_id or None.

        BZPOPMAX guarantees: exactly one worker receives each job_id.
        """
        result = self._redis.bzpopmax(QUEUE_KEY, timeout=timeout)
        if result:
            # result = (queue_key, member, score)
            return result[1]
        return None

    def remove(self, job_id: str) -> bool:
        """Remove a specific job from the queue (used for cancellation)."""
        return self._redis.zrem(QUEUE_KEY, job_id) > 0

    def size(self) -> int:
        return self._redis.zcard(QUEUE_KEY)

    def ping(self) -> bool:
        try:
            return self._redis.ping()
        except Exception:
            return False
