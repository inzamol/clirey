"""Data models for Clirey Celery monitoring."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class WorkerInfo(BaseModel):
    """Detailed worker state and metrics."""
    name: str
    status: str = "ONLINE"  # ONLINE, OFFLINE, UNRESPONSIVE
    active_tasks_count: int = 0
    total_processed_count: int = 0
    concurrency: int = 0
    pool_type: str = "prefork"
    uptime_seconds: float = 0.0
    loadavg: List[float] = Field(default_factory=lambda: [0.0, 0.0, 0.0])
    pid: Optional[int] = None
    queues: List[str] = Field(default_factory=list)
    registered_tasks: List[str] = Field(default_factory=list)
    last_heartbeat: float = Field(default_factory=time.time)
    broker_connection: Dict[str, Any] = Field(default_factory=dict)
    raw_stats: Dict[str, Any] = Field(default_factory=dict)

    @property
    def is_online(self) -> bool:
        return self.status.upper() == "ONLINE"


class TaskInfo(BaseModel):
    """Information regarding a Celery task."""
    task_id: str
    name: str = "unknown"
    worker: Optional[str] = None
    state: str = "PENDING"  # PENDING, RECEIVED, STARTED, SUCCESS, FAILURE, RETRY, REVOKED
    args: Optional[str] = None
    kwargs: Optional[str] = None
    runtime: Optional[float] = None
    received_at: Optional[float] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    eta: Optional[str] = None
    exception: Optional[str] = None
    traceback: Optional[str] = None
    retries: int = 0
    queue: Optional[str] = None

    @property
    def duration_display(self) -> str:
        if self.runtime is not None:
            return f"{self.runtime:.2f}s"
        if self.started_at is not None:
            elapsed = max(0.0, time.time() - self.started_at)
            return f"{elapsed:.1f}s (running)"
        return "-"


class QueueInfo(BaseModel):
    """Queue metrics from broker and worker perspectives."""
    name: str
    consumer_count: int = 0
    pending_messages: Optional[int] = None  # Direct broker inspection count (LLEN / AMQP)
    workers: List[str] = Field(default_factory=list)


class EventRecord(BaseModel):
    """A captured event from Celery event stream."""
    timestamp: float = Field(default_factory=time.time)
    event_type: str  # task-started, task-succeeded, task-failed, worker-heartbeat, etc.
    worker: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    runtime: Optional[float] = None
    exception: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)


class ClusterOverview(BaseModel):
    """Aggregated cluster metrics."""
    total_workers: int = 0
    online_workers: int = 0
    active_tasks: int = 0
    scheduled_tasks: int = 0
    reserved_tasks: int = 0
    total_processed_tasks: int = 0
    events_per_second: float = 0.0
    failed_tasks_count: int = 0
    succeeded_tasks_count: int = 0
