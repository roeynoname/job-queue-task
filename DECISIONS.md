# Design Decisions

## 1. Job Pickup Strategy

**Approach chosen:** Redis Sorted Set (`ZADD` / `BZPOPMAX`) as the queue, with a secondary `SELECT FOR UPDATE` in Postgres when claiming.

**Why:**
`BZPOPMAX` is a single atomic Redis command — only one worker ever receives each job_id. Redis processes commands serially; there is no race window between "see the job" and "take the job". This is fundamentally different from a naive `GET` → `DELETE` sequence, which has a race between two workers both reading the same item.

The `SELECT FOR UPDATE` in Postgres is a secondary safety net for a specific crash-recovery edge case:
- Monitor detects a stuck job, sets it back to PENDING, re-pushes to Redis
- The original worker (slow, not actually dead) finishes and tries to mark COMPLETE
- Both paths hit the Postgres claim; FOR UPDATE ensures only one proceeds

**Trade-offs:**
- Gained: true atomicity at the queue level without distributed locks, O(log N) insert/pop
- Gave up: if Redis goes down we lose the queue (jobs remain safely in Postgres, but new jobs can't be picked up until Redis recovers). A recovery procedure can scan Postgres for PENDING jobs and re-populate Redis. This is acceptable for this system's requirements.
- Alternative considered: `SELECT FOR UPDATE SKIP LOCKED` on Postgres directly (no Redis). This works but puts more load on Postgres and loses the priority-queue semantics that Redis sorted sets provide cleanly.

---

## 2. Worker Crash Recovery

**Approach chosen:** Processing timeout deadline (`processing_timeout_at`) checked by a background monitor thread.

**Why:**
When a worker claims a job it writes `processing_timeout_at = now + 10 minutes` to the DB. The monitor runs every 30 seconds and queries:
```sql
SELECT ... WHERE status='processing' AND processing_timeout_at < now()
```
Jobs found this way are re-queued (or marked FAILED if max_attempts reached).

**What happens if worker crashes mid-job:**
1. Worker dies, job stays in PROCESSING
2. Monitor (in another worker process or same process via daemon thread) detects the expired deadline
3. `_recover_stuck_jobs` acquires `FOR UPDATE SKIP LOCKED` (so multiple monitors don't fight over the same row)
4. Resets status to PENDING, pushes back to Redis
5. Another worker picks it up; `current_attempts` reflects the attempt that was in-flight

**Why not heartbeats?**
Heartbeats require the worker to actively write to the DB every N seconds. The timeout approach is simpler: write once on claim, let the monitor do the checking. One failure mode of heartbeats is the heartbeat thread dying silently while the main job thread keeps running — you'd need to monitor the monitor. The timeout approach doesn't have this problem.

**Trade-offs:**
- Gave up: jobs can be "lost" for up to `MONITOR_INTERVAL_SECONDS` (30s) before recovery. Acceptable for this workload.
- Gained: simplicity, no need for distributed locks or heartbeat infrastructure.

---

## 3. Priority Queue Implementation

**Approach chosen:** Redis Sorted Set with composite score:
```
score = priority * MAX_TS_MS + (MAX_TS_MS - created_at_ms)
```
Where `MAX_TS_MS = 10_000_000_000_000` (larger than any real timestamp in ms).

**Why:**
- `BZPOPMAX` takes the element with the highest score — so higher priority = higher score = processed first ✓
- Within same priority level, earlier `created_at` produces a higher score (FIFO ordering) ✓
- The priority bands are completely isolated: a priority-100 job will always beat a priority-99 job, regardless of creation time ✓

**Trade-offs:**
- Timestamps in milliseconds fit safely below `MAX_TS_MS` (2024 timestamps are ~1.7 × 10¹²). This assumption holds until year ~2286.

---

## 4. Retry Backoff Strategy

**Approach chosen:** Attempt-indexed delays using the SCHEDULED status for delayed retries.

**Timing:**
- Attempt 1 fails → retry delay = **0s** (re-queued immediately as PENDING)
- Attempt 2 fails → retry delay = **30s** (set to SCHEDULED, monitor promotes when due)
- Attempt 3 fails → permanently **FAILED**

The SCHEDULED state doubles as the retry-backoff mechanism. When a job fails with `delay > 0`, we set `scheduled_at = now + delay`. The monitor's `_promote_scheduled_jobs` picks it up when the time arrives. This reuses existing infrastructure instead of needing a separate retry scheduler.

`current_attempts` is incremented **before** the job runs (when claiming it). This ensures crash recovery doesn't give the job an extra "free" attempt — if the worker dies on attempt 2, the DB already shows `current_attempts=2`.

---

## 5. One Thing I Would Do Differently With More Time

- I didnt implemnt all the nice to have points, Dead-letter queue, i didnt classify the errors, and cannot know when to it is error on the input.
- The monitor system is not stand alone, but depend on the workers (not on the logic, but the worker is initiate it.). 

