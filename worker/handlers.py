"""
Mock implementations of each job type.
In production these would call real services; here they simulate work with sleeps.
"""

import random
import time
import uuid
from typing import Callable


def handle_email(payload: dict, update_progress: Callable) -> dict:
    """Simulate sending an email. Always succeeds. Takes 1–3 seconds."""
    time.sleep(random.uniform(1, 3))
    return {
        "message_id": f"msg_{uuid.uuid4().hex[:12]}",
        "recipient": payload.get("to", "user@example.com"),
        "subject": payload.get("subject", "(no subject)"),
        "status": "delivered",
    }


def handle_webhook(payload: dict, update_progress: Callable) -> dict:
    """
    Simulate calling an external webhook.
    80% success rate, 20% failure — good for testing retry logic.
    """
    time.sleep(random.uniform(1, 2))
    if random.random() < 0.20:
        raise RuntimeError("Webhook endpoint returned HTTP 500: Internal Server Error")
    return {
        "url": payload.get("url", "https://example.com/webhook"),
        "response_status": 200,
        "response_body": '{"ok": true}',
    }


def handle_report(payload: dict, update_progress: Callable) -> dict:
    """Simulate generating a report. Takes 3–5 seconds."""
    time.sleep(random.uniform(3, 5))
    report_id = uuid.uuid4().hex[:8]
    return {
        "report_id": report_id,
        "file_url": f"https://storage.example.com/reports/{report_id}.pdf",
        "size_bytes": random.randint(50_000, 500_000),
        "generated_at": __import__("datetime").datetime.utcnow().isoformat(),
    }


def handle_batch(payload: dict, update_progress: Callable) -> dict:
    """
    Process a list of items with progress tracking.
    Calls update_progress(pct) so the caller can persist progress to the DB.
    """
    items = payload.get("items", list(range(10)))
    if not items:
        return {"total": 0, "processed": 0, "results": []}

    results = []
    for i, item in enumerate(items):
        time.sleep(random.uniform(0.1, 0.3))
        results.append({"item": item, "status": "ok"})
        update_progress((i + 1) / len(items) * 100)

    return {
        "total": len(items),
        "processed": len(results),
        "results": results,
    }

def handle_email_failed(payload: dict, update_progress) -> dict:
    attempt = payload.get("current_attempt", 1)  # passed in from the worker

    if attempt == 1:
        raise RuntimeError(f"email_failed: simulated failure on attempt {attempt}")

    time.sleep(random.uniform(1, 2))
    return {
        "message_id": f"msg_{uuid.uuid4().hex[:12]}",
        "recipient": payload.get("to", "user@example.com"),
        "succeeded_on_attempt": attempt,
    }

# Registry: job type string → handler function
JOB_HANDLERS = {
    "email": handle_email,
    "webhook": handle_webhook,
    "report": handle_report,
    "batch": handle_batch,
    "email_failed": handle_email_failed,
}
