"""Tests for Pydantic models in Clirey."""

from clirey.core.models import EventRecord, QueueInfo, TaskInfo, WorkerInfo


def test_worker_info_model():
    worker = WorkerInfo(
        name="celery@worker1",
        status="ONLINE",
        active_tasks_count=3,
        total_processed_count=150,
        concurrency=4,
        pool_type="prefork",
        uptime_seconds=3600.0,
        loadavg=[0.5, 0.4, 0.3],
        queues=["default", "high_priority"],
    )
    assert worker.is_online is True
    assert worker.name == "celery@worker1"
    assert len(worker.queues) == 2


def test_task_info_model():
    task = TaskInfo(
        task_id="abc-123",
        name="tasks.send_email",
        state="STARTED",
        started_at=100.0,
        runtime=2.45,
    )
    assert task.duration_display == "2.45s"


def test_queue_info_model():
    q = QueueInfo(
        name="default",
        consumer_count=4,
        pending_messages=42,
        workers=["worker1", "worker2"],
    )
    assert q.name == "default"
    assert q.pending_messages == 42
    assert q.consumer_count == 4
