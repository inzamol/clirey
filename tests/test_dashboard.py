"""Tests for Dashboard layout rendering, background inspection, and event monitor."""

import time

from clirey.core.client import CeleryClient
from clirey.core.events import EventMonitor
from clirey.core.models import ClusterOverview, EventRecord, QueueInfo, TaskInfo, WorkerInfo
from clirey.ui.dashboard import Dashboard


def test_dashboard_build_layout():
    dashboard = Dashboard(broker_url="redis://127.0.0.1:6379/0", refresh_rate=1.0)
    workers = [
        WorkerInfo(
            name="celery@worker-test",
            status="ONLINE",
            active_tasks_count=1,
            total_processed_count=100,
            concurrency=4,
            pool_type="prefork",
            uptime_seconds=3600.0,
            loadavg=[0.1, 0.2, 0.3],
            queues=["celery"],
        )
    ]
    tasks = [
        TaskInfo(
            task_id="abc-123-xyz",
            name="tasks.add",
            worker="celery@worker-test",
            state="STARTED",
            duration=1.5,
        )
    ]
    queues = [
        QueueInfo(name="celery", consumer_count=1, pending_messages=5, workers=["celery@worker-test"])
    ]
    events = [
        EventRecord(
            timestamp=time.time(),
            event_type="task-succeeded",
            worker="celery@worker-test",
            task_id="abc-123-xyz",
            task_name="tasks.add",
            runtime=0.42,
        )
    ]
    overview = ClusterOverview(
        total_workers=1,
        online_workers=1,
        active_tasks=1,
        total_processed_tasks=100,
        events_per_second=2.5,
        failed_tasks_count=0,
        succeeded_tasks_count=100,
    )

    layout = dashboard.build_layout(workers, tasks, queues, events, overview)
    assert layout is not None
    assert layout["header"] is not None
    assert layout["body"] is not None
    assert layout["footer"] is not None


def test_event_monitor_handling():
    client = CeleryClient(broker_url="redis://127.0.0.1:6379/0")
    monitor = EventMonitor(client)

    # Simulate task-received
    monitor._handle_event({
        "type": "task-received",
        "uuid": "test-task-1",
        "name": "tasks.process_data",
        "hostname": "celery@node1",
        "args": "('foo',)",
    })
    assert "test-task-1" in monitor.tasks_map
    assert monitor.tasks_map["test-task-1"].state == "RECEIVED"

    # Simulate task-started
    monitor._handle_event({
        "type": "task-started",
        "uuid": "test-task-1",
        "name": "tasks.process_data",
        "hostname": "celery@node1",
    })
    assert monitor.tasks_map["test-task-1"].state == "STARTED"
    assert len(monitor.get_active_tasks()) == 1

    # Simulate task-succeeded
    monitor._handle_event({
        "type": "task-succeeded",
        "uuid": "test-task-1",
        "hostname": "celery@node1",
        "runtime": 0.35,
    })
    assert monitor.tasks_map["test-task-1"].state == "SUCCESS"
    assert monitor.tasks_map["test-task-1"].runtime == 0.35
    assert len(monitor.get_active_tasks()) == 0
    assert monitor.total_succeeded == 1

    # Verify recent events
    recent = monitor.get_recent_events(10)
    assert len(recent) == 3


def test_inspect_cluster_offline():
    client = CeleryClient(broker_url="redis://127.0.0.1:9999/0", timeout=0.1)
    workers, tasks, queues = client.inspect_cluster()
    assert isinstance(workers, list)
    assert isinstance(tasks, list)
    assert isinstance(queues, list)
