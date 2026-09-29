"""Mock simulator for testing and demonstrating Clirey UI without a live cluster."""

import random
import threading
import time
import uuid
from typing import List, Optional

from clirey.core.models import EventRecord, QueueInfo, TaskInfo, WorkerInfo
from clirey.ui.dashboard import Dashboard


class MockDashboard(Dashboard):
    """Simulated dashboard generating realistic Celery events and worker metrics."""

    def __init__(self, refresh_rate: float = 1.0):
        # We don't connect to a real broker for demo
        self.broker_url = "redis://:mock_secret@10.0.1.45:6379/0"
        self.masked_url = "redis://:****@10.0.1.45:6379/0"
        self.refresh_rate = refresh_rate

        self.mock_workers: List[WorkerInfo] = [
            WorkerInfo(
                name="celery@worker-node-alpha.prod",
                status="ONLINE",
                active_tasks_count=2,
                total_processed_count=18492,
                concurrency=8,
                pool_type="prefork",
                uptime_seconds=84200.0,
                loadavg=[0.42, 0.38, 0.31],
                queues=["default", "high_priority", "mail"],
            ),
            WorkerInfo(
                name="celery@worker-node-beta.prod",
                status="ONLINE",
                active_tasks_count=3,
                total_processed_count=14320,
                concurrency=8,
                pool_type="prefork",
                uptime_seconds=84190.0,
                loadavg=[0.88, 0.65, 0.52],
                queues=["default", "reports", "ml_inference"],
            ),
            WorkerInfo(
                name="celery@worker-node-gamma.prod",
                status="ONLINE",
                active_tasks_count=1,
                total_processed_count=9810,
                concurrency=4,
                pool_type="gevent",
                uptime_seconds=36200.0,
                loadavg=[0.15, 0.20, 0.18],
                queues=["default", "webhooks"],
            ),
        ]

        self.mock_queues: List[QueueInfo] = [
            QueueInfo(name="default", consumer_count=3, pending_messages=14, workers=["alpha", "beta", "gamma"]),
            QueueInfo(name="high_priority", consumer_count=1, pending_messages=0, workers=["alpha"]),
            QueueInfo(name="reports", consumer_count=1, pending_messages=6, workers=["beta"]),
            QueueInfo(name="ml_inference", consumer_count=1, pending_messages=3, workers=["beta"]),
            QueueInfo(name="mail", consumer_count=1, pending_messages=1, workers=["alpha"]),
            QueueInfo(name="webhooks", consumer_count=1, pending_messages=0, workers=["gamma"]),
        ]

        self.task_names = [
            "tasks.send_transactional_email",
            "tasks.generate_pdf_report",
            "tasks.process_payment_webhook",
            "tasks.sync_salesforce_leads",
            "tasks.run_llm_embedding_pipeline",
            "tasks.cleanup_expired_sessions",
            "tasks.export_user_data_archive",
        ]

        self.mock_events: List[EventRecord] = []
        self.mock_active_tasks: List[TaskInfo] = []
        self._running = False
        self._sim_thread: Optional[threading.Thread] = None

    def _sim_loop(self) -> None:
        """Background loop simulating task lifecycle events."""
        while self._running:
            try:
                task_name = random.choice(self.task_names)
                tid = str(uuid.uuid4())
                w = random.choice(self.mock_workers)
                now = time.time()

                # Event: task-received
                self.mock_events.append(
                    EventRecord(
                        timestamp=now,
                        event_type="task-received",
                        worker=w.name,
                        task_id=tid,
                        task_name=task_name,
                    )
                )

                # Add active task
                active_task = TaskInfo(
                    task_id=tid,
                    name=task_name,
                    worker=w.name,
                    state="STARTED",
                    started_at=now,
                    args=f"('user_{random.randint(100, 999)}',)",
                )
                self.mock_active_tasks.append(active_task)

                # Event: task-started
                self.mock_events.append(
                    EventRecord(
                        timestamp=now + 0.05,
                        event_type="task-started",
                        worker=w.name,
                        task_id=tid,
                        task_name=task_name,
                    )
                )

                # Evict old events
                if len(self.mock_events) > 30:
                    self.mock_events.pop(0)

                # Finish older active tasks
                if len(self.mock_active_tasks) > 6:
                    finished = self.mock_active_tasks.pop(0)
                    runtime = random.uniform(0.12, 3.45)
                    is_fail = random.random() < 0.08

                    if is_fail:
                        self.mock_events.append(
                            EventRecord(
                                timestamp=time.time(),
                                event_type="task-failed",
                                worker=finished.worker,
                                task_id=finished.task_id,
                                task_name=finished.name,
                                runtime=runtime,
                                exception="ConnectionResetError('Remote host closed connection')",
                            )
                        )
                    else:
                        self.mock_events.append(
                            EventRecord(
                                timestamp=time.time(),
                                event_type="task-succeeded",
                                worker=finished.worker,
                                task_id=finished.task_id,
                                task_name=finished.name,
                                runtime=runtime,
                            )
                        )
                        w.total_processed_count += 1

                # Slightly fluctuate queue depths
                for q in self.mock_queues:
                    if q.pending_messages is not None:
                        q.pending_messages = max(0, q.pending_messages + random.randint(-1, 2))

                time.sleep(random.uniform(0.4, 1.2))
            except Exception:
                time.sleep(0.5)

    def run(self) -> None:
        """Run simulated dashboard."""
        from rich.live import Live

        from clirey.core.models import ClusterOverview
        from clirey.ui.console import console

        self._running = True
        self._sim_thread = threading.Thread(target=self._sim_loop, daemon=True)
        self._sim_thread.start()

        try:
            with Live(console=console, screen=True, refresh_per_second=4) as live:
                while True:
                    succeeded_count = sum(1 for e in self.mock_events if e.event_type == "task-succeeded")
                    failed_count = sum(1 for e in self.mock_events if e.event_type == "task-failed")
                    overview = ClusterOverview(
                        total_workers=len(self.mock_workers),
                        online_workers=len(self.mock_workers),
                        active_tasks=len(self.mock_active_tasks),
                        total_processed_tasks=sum(w.total_processed_count for w in self.mock_workers),
                        events_per_second=random.uniform(3.5, 9.2),
                        failed_tasks_count=failed_count,
                        succeeded_tasks_count=succeeded_count,
                    )

                    layout = self.build_layout(
                        self.mock_workers,
                        self.mock_active_tasks,
                        self.mock_queues,
                        self.mock_events,
                        overview,
                    )
                    live.update(layout)
                    time.sleep(self.refresh_rate)
        except KeyboardInterrupt:
            pass
        finally:
            self._running = False
            console.print("[bold yellow]Demo closed.[/]")
