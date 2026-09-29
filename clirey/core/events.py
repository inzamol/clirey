"""Real-time event capture and state tracking for Celery."""

from __future__ import annotations

import collections
import logging
import threading
import time
from typing import Callable, Deque, Dict, List, Optional

from celery.events import EventReceiver
from celery.events.state import State

from clirey.core.client import CeleryClient
from clirey.core.models import ClusterOverview, EventRecord, TaskInfo

logger = logging.getLogger(__name__)


class EventMonitor:
    """Listens to Celery event stream in background thread and maintains real-time state."""

    def __init__(
        self,
        client: CeleryClient,
        max_events: int = 200,
        max_tasks: int = 500,
        on_event_callback: Optional[Callable[[EventRecord], None]] = None,
    ):
        self.client = client
        self.app = client.app
        self.max_events = max_events
        self.max_tasks = max_tasks
        self.on_event_callback = on_event_callback

        self.state = State()
        self.events_buffer: Deque[EventRecord] = collections.deque(maxlen=max_events)
        self.tasks_map: Dict[str, TaskInfo] = {}
        self.lock = threading.RLock()

        # Throughput counters
        self.event_timestamps: Deque[float] = collections.deque(maxlen=500)
        self.total_succeeded: int = 0
        self.total_failed: int = 0
        self.total_started: int = 0

        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start listening to Celery events in a background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_receiver, daemon=True, name="ClireyEventReceiver")
        self._thread.start()

    def stop(self) -> None:
        """Stop event listening."""
        self._running = False

    def _run_receiver(self) -> None:
        """Background thread main loop for capturing events."""
        try:
            self.client.enable_events()
        except Exception:
            pass

        while self._running:
            try:
                with self.app.connection_for_read() as connection:
                    recv = EventReceiver(
                        connection,
                        app=self.app,
                        handlers={"*": self._handle_event},
                    )
                    for _ in recv.itercapture(limit=None, timeout=1.0, wakeup=True):
                        if not self._running:
                            break
            except Exception as e:
                logger.debug(f"Event receiver connection error: {e}")
                time.sleep(1.0)

    def _handle_event(self, event: Dict) -> None:
        """Process an individual event dictionary from Celery."""
        try:
            now = time.time()
            if "timestamp" not in event:
                event["timestamp"] = now
            if "clock" not in event:
                event["clock"] = 0
            if "local_received" not in event:
                event["local_received"] = now

            try:
                self.state.event(event)
            except Exception as e:
                logger.debug(f"Celery State update error: {e}")

            event_type = event.get("type", "unknown")
            uuid = event.get("uuid") or event.get("task_id")
            hostname = event.get("hostname")
            runtime = event.get("runtime")
            exception = event.get("exception")

            with self.lock:
                self.event_timestamps.append(now)

                # Track task state
                task_name = event.get("name")
                if uuid:
                    if uuid not in self.tasks_map:
                        if len(self.tasks_map) >= self.max_tasks:
                            # Evict oldest entry
                            oldest = next(iter(self.tasks_map))
                            del self.tasks_map[oldest]
                        self.tasks_map[uuid] = TaskInfo(task_id=uuid, name=task_name or "unknown", worker=hostname)

                    t_info = self.tasks_map[uuid]
                    if task_name:
                        t_info.name = task_name
                    if hostname:
                        t_info.worker = hostname
                    if runtime is not None:
                        t_info.runtime = float(runtime)
                    if exception:
                        t_info.exception = str(exception)
                    if event.get("traceback"):
                        t_info.traceback = str(event.get("traceback"))

                    # State transitions
                    if event_type == "task-received":
                        t_info.state = "RECEIVED"
                        t_info.received_at = now
                        if event.get("args"):
                            t_info.args = str(event.get("args"))
                        if event.get("kwargs"):
                            t_info.kwargs = str(event.get("kwargs"))
                        if event.get("eta"):
                            t_info.eta = str(event.get("eta"))
                    elif event_type == "task-started":
                        t_info.state = "STARTED"
                        t_info.started_at = now
                        self.total_started += 1
                    elif event_type == "task-succeeded":
                        t_info.state = "SUCCESS"
                        t_info.finished_at = now
                        self.total_succeeded += 1
                    elif event_type == "task-failed":
                        t_info.state = "FAILURE"
                        t_info.finished_at = now
                        self.total_failed += 1
                    elif event_type == "task-retried":
                        t_info.state = "RETRY"
                        t_info.retries += 1
                    elif event_type == "task-revoked":
                        t_info.state = "REVOKED"
                        t_info.finished_at = now

                # Record event in circular buffer
                record = EventRecord(
                    timestamp=now,
                    event_type=event_type,
                    worker=hostname,
                    task_id=uuid,
                    task_name=task_name or (self.tasks_map[uuid].name if uuid and uuid in self.tasks_map else None),
                    runtime=float(runtime) if runtime is not None else None,
                    exception=exception,
                    data=event,
                )
                self.events_buffer.append(record)

            if self.on_event_callback:
                try:
                    self.on_event_callback(record)
                except Exception:
                    pass

        except Exception as e:
            logger.debug(f"Error handling event: {e}")

    def get_recent_events(self, count: int = 20) -> List[EventRecord]:
        """Return the most recent captured events."""
        with self.lock:
            return list(self.events_buffer)[-count:]

    def get_active_tasks(self) -> List[TaskInfo]:
        """Return currently running/started tasks."""
        with self.lock:
            return [t for t in self.tasks_map.values() if t.state == "STARTED"]

    def get_recent_tasks(self, count: int = 50) -> List[TaskInfo]:
        """Return recent tasks tracked by events."""
        with self.lock:
            return list(self.tasks_map.values())[-count:]

    def get_events_per_second(self) -> float:
        """Calculate recent event throughput per second (last 5 seconds window)."""
        now = time.time()
        window = 5.0
        with self.lock:
            count = sum(1 for ts in self.event_timestamps if (now - ts) <= window)
            return count / window

    def get_cluster_overview(self) -> ClusterOverview:
        """Produce an aggregated cluster overview."""
        with self.lock:
            active_count = sum(1 for t in self.tasks_map.values() if t.state == "STARTED")
            scheduled_count = sum(1 for t in self.tasks_map.values() if t.state == "SCHEDULED" or t.eta)
            return ClusterOverview(
                total_workers=len(self.state.workers),
                online_workers=sum(1 for w in self.state.workers.values() if w.alive),
                active_tasks=active_count,
                scheduled_tasks=scheduled_count,
                total_processed_tasks=self.total_succeeded + self.total_failed,
                events_per_second=self.get_events_per_second(),
                failed_tasks_count=self.total_failed,
                succeeded_tasks_count=self.total_succeeded,
            )
