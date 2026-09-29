"""Rich table renderers for Clirey CLI commands."""

from __future__ import annotations

import datetime
from typing import List, Optional
from rich import box
from rich.table import Table
from clirey.core.models import EventRecord, QueueInfo, TaskInfo, WorkerInfo
from clirey.ui.console import (
    format_duration,
    format_event_badge,
    format_loadavg,
    format_state_badge,
    format_uptime,
    format_worker_status,
)


def create_workers_table(workers: List[WorkerInfo], title: Optional[str] = None) -> Table:
    """Build a rich table representing worker nodes."""
    table = Table(
        title=title or "Active Celery Workers",
        box=box.ROUNDED,
        header_style="bold bright_cyan",
        title_style="bold bright_white",
        expand=True,
    )
    table.add_column("Worker Node", style="bold white", no_wrap=True)
    table.add_column("Status", justify="center")
    table.add_column("Active", justify="right", style="cyan")
    table.add_column("Processed", justify="right", style="green")
    table.add_column("Concurrency", justify="center")
    table.add_column("Pool", style="dim")
    table.add_column("Load Avg (1, 5, 15m)", justify="center")
    table.add_column("Uptime", justify="right")
    table.add_column("Queues", style="yellow")

    if not workers:
        table.add_row("[dim]No workers responding[/]", "", "", "", "", "", "", "", "")
        return table

    for w in workers:
        queues_str = ", ".join(w.queues) if w.queues else "default"
        table.add_row(
            w.name,
            format_worker_status(w.status),
            str(w.active_tasks_count),
            f"{w.total_processed_count:,}",
            str(w.concurrency),
            w.pool_type,
            format_loadavg(w.loadavg),
            format_uptime(w.uptime_seconds),
            queues_str,
        )
    return table


def create_tasks_table(tasks: List[TaskInfo], title: Optional[str] = None) -> Table:
    """Build a rich table for active / scheduled / reserved tasks."""
    table = Table(
        title=title or "Running & Tracked Tasks",
        box=box.ROUNDED,
        header_style="bold bright_cyan",
        title_style="bold bright_white",
        expand=True,
    )
    table.add_column("Task ID", style="bold dim white", no_wrap=True)
    table.add_column("Task Name", style="bold yellow")
    table.add_column("State", justify="center")
    table.add_column("Worker", style="cyan", no_wrap=True)
    table.add_column("Duration", justify="right", style="green")
    table.add_column("Args / Details", style="dim", overflow="ellipsis")

    if not tasks:
        table.add_row("[dim]No tasks active[/]", "", "", "", "", "")
        return table

    for t in tasks:
        args_display = ""
        if t.args and t.args != "()" and t.args != "[]":
            args_display += f"args={t.args} "
        if t.kwargs and t.kwargs != "{}":
            args_display += f"kwargs={t.kwargs}"
        if t.exception:
            args_display = f"[red]Error: {t.exception}[/]"
        elif t.eta:
            args_display = f"ETA: {t.eta}"

        table.add_row(
            t.task_id[:13] + "..." if len(t.task_id) > 16 else t.task_id,
            t.name,
            format_state_badge(t.state),
            t.worker or "-",
            t.duration_display,
            args_display.strip() or "-",
        )
    return table


def create_queues_table(queues: List[QueueInfo], title: Optional[str] = None) -> Table:
    """Build a rich table for queue lengths and backlog."""
    table = Table(
        title=title or "Broker Queues & Backlog",
        box=box.ROUNDED,
        header_style="bold bright_cyan",
        title_style="bold bright_white",
        expand=True,
    )
    table.add_column("Queue Name", style="bold white")
    table.add_column("Active Consumers", justify="right", style="cyan")
    table.add_column("Pending Messages (Backlog)", justify="right", style="bold magenta")
    table.add_column("Workers Subscribed", style="dim")

    if not queues:
        table.add_row("[dim]No queue information found[/]", "", "", "")
        return table

    for q in queues:
        pending_str = f"{q.pending_messages:,}" if q.pending_messages is not None else "[dim]N/A[/]"
        workers_str = ", ".join(q.workers) if q.workers else "[dim]None[/]"
        table.add_row(
            q.name,
            str(q.consumer_count),
            pending_str,
            workers_str,
        )
    return table


def create_events_table(events: List[EventRecord], title: Optional[str] = None) -> Table:
    """Build a rich table representing recent captured events."""
    table = Table(
        title=title or "Real-time Event Stream",
        box=box.ROUNDED,
        header_style="bold bright_cyan",
        title_style="bold bright_white",
        expand=True,
    )
    table.add_column("Time", style="dim", no_wrap=True)
    table.add_column("Event", justify="center")
    table.add_column("Task Name", style="bold yellow")
    table.add_column("Task ID", style="dim", no_wrap=True)
    table.add_column("Worker", style="cyan", no_wrap=True)
    table.add_column("Duration / Info", style="green")

    if not events:
        table.add_row("[dim]No events captured yet[/]", "", "", "", "", "")
        return table

    for ev in events:
        t_str = datetime.datetime.fromtimestamp(ev.timestamp).strftime("%H:%M:%S")
        tid = (ev.task_id[:10] + "...") if ev.task_id and len(ev.task_id) > 13 else (ev.task_id or "-")
        info = ""
        if ev.runtime is not None:
            info = format_duration(ev.runtime)
        elif ev.exception:
            info = f"[red]{ev.exception[:40]}[/]"
        table.add_row(
            t_str,
            format_event_badge(ev.event_type),
            ev.task_name or "-",
            tid,
            ev.worker or "-",
            info or "-",
        )
    return table
