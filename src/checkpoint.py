"""
Lightweight checkpoint/resume for long-running batch scans.

Saves progress stats to a JSON file so that:
1. If a process crashes, the next run auto-resumes from where it left off
2. External monitoring can poll the checkpoint file for status
3. Completion stats are preserved after the run

Checkpoint file: /tmp/ds_checkpoint_{job_name}.json
"""

import json
import os
import time
from datetime import datetime

import structlog

logger = structlog.get_logger("checkpoint")

CHECKPOINT_DIR = "/tmp"


def _checkpoint_path(job_name: str) -> str:
    return os.path.join(CHECKPOINT_DIR, f"ds_checkpoint_{job_name}.json")


def save_checkpoint(job_name: str, stats: dict, total: int, extra: dict = None):
    """Save current progress to checkpoint file."""
    data = {
        "job_name": job_name,
        "total": total,
        "stats": stats,
        "updated_at": datetime.now().isoformat(),
        "pid": os.getpid(),
    }
    if extra:
        data.update(extra)

    path = _checkpoint_path(job_name)
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        logger.debug("checkpoint_save_failed", error=str(e))


def load_checkpoint(job_name: str) -> dict:
    """Load checkpoint file if it exists. Returns empty dict if none."""
    path = _checkpoint_path(job_name)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def clear_checkpoint(job_name: str):
    """Remove checkpoint file after successful completion."""
    path = _checkpoint_path(job_name)
    if os.path.exists(path):
        os.remove(path)


def mark_complete(job_name: str, stats: dict, total: int, elapsed: float):
    """Write final completion checkpoint."""
    save_checkpoint(job_name, stats, total, extra={
        "status": "complete",
        "elapsed_sec": round(elapsed, 1),
        "completed_at": datetime.now().isoformat(),
    })
