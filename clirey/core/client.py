"""Celery Client wrapper for dynamic inspection and control with only a broker URL."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from celery import Celery

from clirey.core.broker import BrokerInspector
from clirey.core.models import QueueInfo, TaskInfo, WorkerInfo

logger = logging.getLogger(__name__)


class CeleryClient:
    """Manager for Celery inspection and remote control commands."""

    def __init__(
        self,
        broker_url: str,
        backend_url: Optional[str] = None,
        timeout: float = 2.5,
        key_prefix: Optional[str] = None,
        control_exchange: Optional[str] = None,
        event_exchange: Optional[str] = None,
        auto_detect_prefix: bool = True,
    ):
        self.broker_url = broker_url
        self.backend_url = backend_url
        self.timeout = timeout

        if key_prefix is None and auto_detect_prefix:
            key_prefix = BrokerInspector.auto_detect_key_prefix(broker_url)

        self.key_prefix = key_prefix
        self.control_exchange = control_exchange
        self.event_exchange = event_exchange
        self.app = self._create_app()
        self.broker_inspector = BrokerInspector(broker_url, key_prefix=key_prefix)

    def _create_app(self) -> Celery:
        """Create dynamic generic Celery app without needing task definitions."""
        app = Celery("clirey", broker=self.broker_url, backend=self.backend_url)
        conf_updates: Dict[str, Any] = {
            "worker_enable_remote_control": True,
            "event_queue_expires": 30,
            "broker_connection_retry_on_startup": True,
            "task_serializer": "json",
            "accept_content": ["json", "pickle", "yaml", "msgpack"],
            "result_serializer": "json",
            "timezone": "UTC",
            "enable_utc": True,
        }
        if self.key_prefix:
            conf_updates["broker_transport_options"] = {"global_keyprefix": self.key_prefix}
            conf_updates["result_backend_transport_options"] = {"global_keyprefix": self.key_prefix}
        if self.control_exchange:
            conf_updates["control_exchange"] = self.control_exchange
        if self.event_exchange:
            conf_updates["event_exchange"] = self.event_exchange

        app.conf.update(**conf_updates)
        return app


    def ping(self) -> Dict[str, Any]:
        """Ping active Celery workers."""
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            res = inspect.ping()
            return res or {}
        except Exception as e:
            logger.debug(f"Ping error: {e}")
            return {}

    def inspect_cluster(self) -> tuple[List[WorkerInfo], List[TaskInfo], List[QueueInfo]]:
        """Inspect all workers, active tasks, and queues in a single coordinated pass."""
        workers: List[WorkerInfo] = []
        active_tasks: List[TaskInfo] = []
        queue_map: Dict[str, QueueInfo] = {}

        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            pings = inspect.ping() or {}
            if not pings:
                stats = {}
                active = {}
                registered = {}
                active_queues = {}
            else:
                stats = inspect.stats() or {}
                active = inspect.active() or {}
                registered = inspect.registered() or {}
                active_queues = inspect.active_queues() or {}

            all_worker_names = set(stats.keys()) | set(pings.keys()) | set(active.keys())

            for name in sorted(all_worker_names):
                w_stats = stats.get(name, {})
                w_active = active.get(name, [])
                w_reg = registered.get(name, [])
                w_queues_raw = active_queues.get(name, [])
                queue_names = [q.get("name") for q in w_queues_raw if isinstance(q, dict) and "name" in q]

                # Extract concurrency & pool info
                pool_info = w_stats.get("pool", {})
                concurrency = pool_info.get("max-concurrency", len(pool_info.get("processes", [])))
                pool_type = w_stats.get("pool", {}).get("implementation", "prefork")
                uptime = float(w_stats.get("uptime", 0))
                pid = w_stats.get("pid")
                loadavg = w_stats.get("loadavg", [0.0, 0.0, 0.0])
                if not isinstance(loadavg, list) or len(loadavg) < 3:
                    loadavg = [0.0, 0.0, 0.0]

                # Total processed count
                total_processed = 0
                total_dict = w_stats.get("total", {})
                if isinstance(total_dict, dict):
                    total_processed = sum(v for v in total_dict.values() if isinstance(v, (int, float)))

                status = "ONLINE" if (name in pings or name in stats) else "OFFLINE"

                workers.append(
                    WorkerInfo(
                        name=name,
                        status=status,
                        active_tasks_count=len(w_active),
                        total_processed_count=int(total_processed),
                        concurrency=concurrency,
                        pool_type=str(pool_type),
                        uptime_seconds=uptime,
                        loadavg=[float(x) for x in loadavg[:3]],
                        pid=pid,
                        queues=queue_names,
                        registered_tasks=w_reg,
                        raw_stats=w_stats,
                    )
                )

                # Extract active tasks from w_active
                for t in w_active:
                    task_id = t.get("id") or t.get("task_id", "")
                    task_name = t.get("name", "unknown")
                    time_start = t.get("time_start")
                    args = str(t.get("args", ""))
                    kwargs = str(t.get("kwargs", ""))
                    active_tasks.append(
                        TaskInfo(
                            task_id=task_id,
                            name=task_name,
                            worker=name,
                            state="STARTED",
                            args=args,
                            kwargs=kwargs,
                            started_at=float(time_start) if time_start else None,
                            received_at=None,
                        )
                    )

                # Track queue subscriptions
                for q_name in queue_names:
                    if q_name not in queue_map:
                        queue_map[q_name] = QueueInfo(name=q_name, consumer_count=0, workers=[])
                    queue_map[q_name].consumer_count += 1
                    queue_map[q_name].workers.append(name)

        except Exception as e:
            logger.debug(f"Error inspecting cluster: {e}")

        # Discover additional queues and queue depths
        try:
            broker_discovered = self.broker_inspector.discover_queues()
            for q in broker_discovered:
                if q not in queue_map:
                    queue_map[q] = QueueInfo(name=q, consumer_count=0, workers=[])

            if not queue_map:
                queue_map["celery"] = QueueInfo(name="celery", consumer_count=0, workers=[])

            depths = self.broker_inspector.get_queues_depths(list(queue_map.keys()))
            for q_name, depth in depths.items():
                if q_name in queue_map:
                    queue_map[q_name].pending_messages = depth
        except Exception as e:
            logger.debug(f"Error inspecting queues: {e}")

        return workers, active_tasks, list(queue_map.values())

    def get_workers(self) -> List[WorkerInfo]:
        """Inspect all workers and build WorkerInfo list."""
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            pings = inspect.ping() or {}
            if not pings:
                return []

            stats = inspect.stats() or {}
            active = inspect.active() or {}
            registered = inspect.registered() or {}
            active_queues = inspect.active_queues() or {}

            all_worker_names = set(stats.keys()) | set(pings.keys()) | set(active.keys())
            workers: List[WorkerInfo] = []

            for name in sorted(all_worker_names):
                w_stats = stats.get(name, {})
                w_active = active.get(name, [])
                w_reg = registered.get(name, [])
                w_queues_raw = active_queues.get(name, [])
                queue_names = [q.get("name") for q in w_queues_raw if isinstance(q, dict) and "name" in q]

                # Extract concurrency & pool info
                pool_info = w_stats.get("pool", {})
                concurrency = pool_info.get("max-concurrency", len(pool_info.get("processes", [])))
                pool_type = w_stats.get("pool", {}).get("implementation", "prefork")
                uptime = float(w_stats.get("uptime", 0))
                pid = w_stats.get("pid")
                loadavg = w_stats.get("loadavg", [0.0, 0.0, 0.0])
                if not isinstance(loadavg, list) or len(loadavg) < 3:
                    loadavg = [0.0, 0.0, 0.0]

                # Total processed count
                total_processed = 0
                total_dict = w_stats.get("total", {})
                if isinstance(total_dict, dict):
                    total_processed = sum(v for v in total_dict.values() if isinstance(v, (int, float)))

                status = "ONLINE" if (name in pings or name in stats) else "OFFLINE"

                workers.append(
                    WorkerInfo(
                        name=name,
                        status=status,
                        active_tasks_count=len(w_active),
                        total_processed_count=int(total_processed),
                        concurrency=concurrency,
                        pool_type=str(pool_type),
                        uptime_seconds=uptime,
                        loadavg=[float(x) for x in loadavg[:3]],
                        pid=pid,
                        queues=queue_names,
                        registered_tasks=w_reg,
                        raw_stats=w_stats,
                    )
                )
            return workers
        except Exception as e:
            logger.debug(f"Error fetching workers: {e}")
            return []

    def get_active_tasks(self) -> List[TaskInfo]:
        """Fetch all currently running tasks across all workers."""
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            active_data = inspect.active() or {}
            tasks: List[TaskInfo] = []

            for worker_name, task_list in active_data.items():
                for t in task_list:
                    task_id = t.get("id") or t.get("task_id", "")
                    name = t.get("name", "unknown")
                    time_start = t.get("time_start")
                    args = str(t.get("args", ""))
                    kwargs = str(t.get("kwargs", ""))
                    tasks.append(
                        TaskInfo(
                            task_id=task_id,
                            name=name,
                            worker=worker_name,
                            state="STARTED",
                            args=args,
                            kwargs=kwargs,
                            started_at=float(time_start) if time_start else None,
                            received_at=None,
                        )
                    )
            return tasks
        except Exception as e:
            logger.debug(f"Error getting active tasks: {e}")
            return []

    def get_scheduled_tasks(self) -> List[TaskInfo]:
        """Fetch tasks scheduled with ETA/countdown."""
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            scheduled_data = inspect.scheduled() or {}
            tasks: List[TaskInfo] = []
            for worker_name, task_list in scheduled_data.items():
                for item in task_list:
                    req = item.get("request", item)
                    tasks.append(
                        TaskInfo(
                            task_id=req.get("id", ""),
                            name=req.get("name", "unknown"),
                            worker=worker_name,
                            state="SCHEDULED",
                            args=str(req.get("args", "")),
                            kwargs=str(req.get("kwargs", "")),
                            eta=item.get("eta"),
                        )
                    )
            return tasks
        except Exception as e:
            logger.debug(f"Error getting scheduled tasks: {e}")
            return []

    def get_reserved_tasks(self) -> List[TaskInfo]:
        """Fetch reserved (prefetched by worker) tasks."""
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            reserved_data = inspect.reserved() or {}
            tasks: List[TaskInfo] = []
            for worker_name, task_list in reserved_data.items():
                for t in task_list:
                    tasks.append(
                        TaskInfo(
                            task_id=t.get("id", ""),
                            name=t.get("name", "unknown"),
                            worker=worker_name,
                            state="RESERVED",
                            args=str(t.get("args", "")),
                            kwargs=str(t.get("kwargs", "")),
                        )
                    )
            return tasks
        except Exception as e:
            logger.debug(f"Error getting reserved tasks: {e}")
            return []

    def get_revoked_tasks(self) -> Dict[str, List[str]]:
        """Fetch list of revoked task IDs."""
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            return inspect.revoked() or {}
        except Exception as e:
            logger.debug(f"Error getting revoked tasks: {e}")
            return {}

    def get_queues(self) -> List[QueueInfo]:
        """Get queue consumers and direct broker backlog depths."""
        queue_map: Dict[str, QueueInfo] = {}

        # 1. From worker active_queues
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            active_queues = inspect.active_queues() or {}
            for worker_name, q_list in active_queues.items():
                for q in q_list:
                    q_name = q.get("name", "celery")
                    if q_name not in queue_map:
                        queue_map[q_name] = QueueInfo(name=q_name, consumer_count=0, workers=[])
                    queue_map[q_name].consumer_count += 1
                    queue_map[q_name].workers.append(worker_name)
        except Exception as e:
            logger.debug(f"Error fetching worker queues: {e}")

        # 2. Discover additional queues from broker
        broker_discovered = self.broker_inspector.discover_queues()
        for q in broker_discovered:
            if q not in queue_map:
                queue_map[q] = QueueInfo(name=q, consumer_count=0, workers=[])

        # Always ensure 'celery' default queue is checked if empty
        if not queue_map:
            queue_map["celery"] = QueueInfo(name="celery", consumer_count=0, workers=[])

        # 3. Query broker for backlog message depths
        depths = self.broker_inspector.get_queues_depths(list(queue_map.keys()))
        for q_name, depth in depths.items():
            if q_name in queue_map:
                queue_map[q_name].pending_messages = depth

        return list(queue_map.values())

    def query_task(self, task_id: str) -> Dict[str, Any]:
        """Query task state using inspect and result backend if available."""
        result: Dict[str, Any] = {"task_id": task_id, "found_in_workers": []}
        try:
            inspect = self.app.control.inspect(timeout=self.timeout)
            queried = inspect.query_task(task_id) or {}
            for worker, tasks in queried.items():
                if tasks:
                    result["found_in_workers"].append({"worker": worker, "info": tasks})
        except Exception as e:
            result["query_error"] = str(e)

        # Check backend if configured
        try:
            async_res = self.app.AsyncResult(task_id)
            result["state"] = async_res.state
            if async_res.ready():
                result["ready"] = True
                result["successful"] = async_res.successful()
                result["result"] = str(async_res.result)
            else:
                result["ready"] = False
        except Exception:
            pass

        return result

    def enable_events(self) -> None:
        """Tell workers to broadcast real-time task events."""
        try:
            self.app.control.enable_events()
        except Exception as e:
            logger.debug(f"Could not enable events: {e}")

    def disable_events(self) -> None:
        """Disable worker event broadcasting."""
        try:
            self.app.control.disable_events()
        except Exception as e:
            logger.debug(f"Could not disable events: {e}")

    def revoke_task(self, task_id: str, terminate: bool = False, signal: str = "SIGTERM") -> Dict[str, Any]:
        """Revoke and optionally terminate a running task."""
        try:
            return self.app.control.revoke(task_id, terminate=terminate, signal=signal, reply=True) or {}
        except Exception as e:
            logger.error(f"Error revoking task {task_id}: {e}")
            return {"error": str(e)}

    def rate_limit(self, task_name: str, rate_limit: str) -> Dict[str, Any]:
        """Change rate limit for a task type at runtime."""
        try:
            return self.app.control.rate_limit(task_name, rate_limit, reply=True) or {}
        except Exception as e:
            logger.error(f"Error setting rate limit: {e}")
            return {"error": str(e)}
