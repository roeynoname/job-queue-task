from fastapi import FastAPI
from sqlalchemy import text

from app.database import create_tables, SessionLocal
from app.queue import PriorityQueue
from app.routers.jobs import router as jobs_router

app = FastAPI(
    title="Job Queue Service",
    description="Distributed background job processing system",
    version="1.0.0",
)

app.include_router(jobs_router)


@app.on_event("startup")
def on_startup():
    create_tables()


@app.get("/health", tags=["health"])
def health_check():
    """Returns service health including queue depth and DB connectivity."""
    q = PriorityQueue()
    redis_ok = q.ping()
    queue_size = q.size() if redis_ok else -1

    db_ok = False
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        db_ok = True
    except Exception:
        pass

    status = "ok" if (redis_ok and db_ok) else "degraded"
    return {
        "status": status,
        "queue_size": queue_size,
        "redis_ok": redis_ok,
        "db_ok": db_ok,
    }
