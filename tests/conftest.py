"""
Test configuration.

Tests use a real PostgreSQL test database and a real Redis instance.
These are the same services spun up by docker-compose; the test database
is a separate DB ("jobqueue_test") created automatically by this fixture.

Run with:
    docker-compose up -d db redis
    pytest tests/ -v
"""

import os
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

# ── Override settings BEFORE importing app modules ────────────────────────────
TEST_DB_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue_test",
)
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/1")  # DB 1 for tests

os.environ["DATABASE_URL"] = TEST_DB_URL
os.environ["REDIS_URL"] = TEST_REDIS_URL

from app.database import Base, get_db  # noqa: E402
from app.main import app               # noqa: E402
from app.queue import PriorityQueue, QUEUE_KEY  # noqa: E402


# ── Database fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def engine():
    """Create the test database and schema once per session."""
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="jobqueue", user="jobqueue", password="jobqueue",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'jobqueue_test'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE jobqueue_test")
    cur.close()
    conn.close()

    eng = create_engine(TEST_DB_URL)
    Base.metadata.create_all(bind=eng)
    yield eng
    Base.metadata.drop_all(bind=eng)
    eng.dispose()


@pytest.fixture
def db(engine):
    """
    Use a real committing session so the worker's own SessionLocal()
    can see the data. Tables are truncated after each test for cleanup.
    """
    from app.database import SessionLocal
    session = SessionLocal()
    yield session
    session.close()
    # Truncate all tables between tests
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())


@pytest.fixture
def client(db):
    """FastAPI test client with the DB dependency overridden to use the test session."""
    def override_get_db():
        try:
            yield db
        finally:
            pass  # rollback handled by the `db` fixture

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ── Redis fixture ─────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_redis():
    """Flush the test Redis DB before each test to avoid state leakage."""
    import redis as redis_lib
    r = redis_lib.from_url(TEST_REDIS_URL, decode_responses=True)
    r.flushdb()
    yield
    r.flushdb()


@pytest.fixture
def queue():
    return PriorityQueue()
