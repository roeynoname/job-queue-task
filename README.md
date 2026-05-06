# Job Queue Service

A distributed background job processing system built with FastAPI, PostgreSQL, and Redis.

## Architecture Overview

```
Client → POST /jobs → API → Redis (ZADD priority queue)
                   ↘ PostgreSQL (job state)

Worker (×2) → Redis BZPOPMAX → claim job in PG → execute → update PG
Monitor thread → scan PG for stuck/scheduled jobs → re-queue as needed
```

**Key components:**
- **API** (`app/`) — FastAPI service. Accepts job submissions, exposes status/cancel/retry endpoints.
- **Worker** (`worker/main.py`) — Separate process. Polls Redis, executes jobs, handles retries.
- **Monitor** (`worker/monitor.py`) — Background thread inside the worker. Handles crash recovery and scheduled job promotion.
- **Queue** (`app/queue.py`) — Redis Sorted Set with priority + FIFO ordering via composite score.

## Running the Project

**Prerequisites:** Docker + Docker Compose

```bash
# Start everything (API + 2 workers + Postgres + Redis)
docker-compose up --build

# API is available at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

## Running Tests

```bash
# Start dependencies only
docker-compose up -d db redis

# In a virtualenv:
pip install -r requirements.txt
pytest tests/ -v
```

Tests use a separate Redis DB (`redis://localhost:6379/1`) and PostgreSQL database (`jobqueue_test`) to avoid interfering with the dev environment.

## Submitting a Test Job

**Email job:**
```bash
curl -X POST http://localhost:8000/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email",
    "payload": {"to": "user@example.com", "subject": "Hello"},
    "priority": 5,
    "max_attempts": 3,
    "idempotency_key": "123" //(unique key)
  }'
```

**Webhook job (20% failure rate — good for testing retries):**
```bash
curl -X POST http://localhost:8000/jobs/ \
  -H "Content-Type: application/json" \
  -d '{"type": "webhook", "payload": {"url": "https://httpbin.org/post"},
      "max_attempts": 3,
      "idempotency_key": "1234" //(unique key)
  }'
```

**Scheduled job (runs in 60 seconds):**
```bash
curl -X POST http://localhost:8000/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "type": "report",
    "payload": {},
    "scheduled_at": "'"$(date -u -d '+60 seconds' '+%Y-%m-%dT%H:%M:%S')"'",
    "max_attempts": 3,
    "idempotency_key": "12345" //(unique key)
  }'
```

**Check job status:**
```bash
curl http://localhost:8000/jobs/{job_id}
```

**Cancel a pending job:**
```bash
curl -X POST http://localhost:8000/jobs/{job_id}/cancel
```

**Health check:**
```bash
curl http://localhost:8000/health
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/jobs/` | Submit a new job |
| GET | `/jobs/` | List jobs (filter by `status`, `type`) |
| GET | `/jobs/{id}` | Get job details |
| POST | `/jobs/{id}/cancel` | Cancel a pending/scheduled job |
| POST | `/jobs/{id}/retry` | Retry a failed job |
| GET | `/health` | Health check with queue stats |

## Job Types

| Type | Behavior |
|------|----------|
| `email` | Simulates email send. 1–3s delay. Always succeeds. |
| `webhook` | Simulates HTTP call. 1–2s delay. 80% success / 20% failure. |
| `report` | Simulates report generation. 3–5s delay. Always succeeds. |
| `batch` | Processes `payload.items` list with progress tracking. |
