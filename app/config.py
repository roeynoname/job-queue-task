from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://jobqueue:jobqueue@db:5432/jobqueue"
    REDIS_URL: str = "redis://redis:6379/0"

    # How long a job can be in PROCESSING before the monitor considers the worker dead
    WORKER_TIMEOUT_SECONDS: int = 600  # 10 minutes

    # How often the monitor checks for stuck/scheduled jobs (seconds)
    MONITOR_INTERVAL_SECONDS: int = 30

    MAX_ATTEMPTS: int = 3

    class Config:
        env_file = ".env"


settings = Settings()
