FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command — overridden by docker-compose for worker service
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
