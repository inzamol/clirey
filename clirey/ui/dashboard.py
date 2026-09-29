"""Live interactive TUI dashboard for Celery monitoring."""

from __future__ import annotations

import datetime
import time
from typing import Optional

from rich import box
from rich.align import Align
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.text import Text

from clirey.config import mask_broker_url
from clirey.core.client import CeleryClient
from clirey.core.events import EventMonitor
from clirey.ui.console import console
from clirey.ui.tables import (
    create_events_table,
    create_queues_table,
    create_tasks_table,
    create_workers_table,
)


class Dashboard:
    """Full-featured Live Terminal User Interface for Celery."""

    def __init__(
        self,
        broker_url: str,
        backend_url: Optional[str] = None,
        refresh_rate: float = 1.5,
    ):
        self.broker_url = broker_url
        self.masked_url = mask_broker_url(broker_url)
        self.refresh_rate = refresh_rate
        self.client = CeleryClient(broker_url, backend_url=backend_url)
        self.event_monitor = EventMonitor(self.client)

    def _make_header(self, workers, active_tasks, overview) -> Panel:
        """Construct top stats banner."""
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        online_count = sum(1 for w in workers if w.is_online)
        total_workers = len(workers)
        active_count = len(active_tasks)

        grid = Text()
        grid.append(" CLIREY ", style="bold black on bright_cyan")
        grid.append(f"  Broker: {self.masked_url}", style="bold white")

        grid.append("  │  ")
        grid.append(
            f"Workers: {online_count}/{total_workers} Online", style="bold green" if online_count > 0 else "bold red"
        )
        grid.append("  │  ")
        grid.append(f"Active Tasks: {active_count}", style="bold cyan")
        grid.append("  │  ")
        grid.append(f"Throughput: {overview.events_per_second:.1f} ev/s", style="bold magenta")
        grid.append("  │  ")
        grid.append(f"Processed: {overview.total_processed_tasks:,}", style="bold green")
        grid.append("  │  ")
        grid.append(
            f"Failed: {overview.failed_tasks_count:,}", style="bold red" if overview.failed_tasks_count > 0 else "dim"
        )
        grid.append("  │  ")
        grid.append(f"{now_str}", style="dim")

        return Panel(
            Align.center(grid),
            box=box.HORIZONTALS,
            style="bold bright_white",
            padding=(0, 1),
        )

    def _make_footer(self) -> Panel:
        """Construct bottom keyboard controls and status hint."""
        text = Text()
        text.append("Press ", style="dim")
        text.append("Ctrl+C", style="bold bright_yellow")
        text.append(" to exit  │  ", style="dim")
        text.append("Auto-refresh: ", style="dim")
        text.append(f"{self.refresh_rate:.1f}s", style="bold cyan")
        text.append("  │  Direct broker event streaming active", style="dim green")

        return Panel(
            Align.center(text),
            box=box.SIMPLE,
            style="dim",
            padding=(0, 1),
        )

    def build_layout(self, workers, active_tasks, queues, events, overview) -> Layout:
        """Build the full grid layout."""
        layout = Layout()

        # Split into header, body, footer
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=3),
        )

        # Split body into top half (workers + queues) and bottom half (active tasks + events)
        layout["body"].split(
            Layout(name="top_half", ratio=3),
            Layout(name="bottom_half", ratio=4),
        )

        # Top half: workers (left, 60%) + queues (right, 40%)
        layout["body"]["top_half"].split_row(
            Layout(name="workers", ratio=3),
            Layout(name="queues", ratio=2),
        )

        # Bottom half: active tasks (top) + recent events (bottom)
        layout["body"]["bottom_half"].split(
            Layout(name="tasks", ratio=2),
            Layout(name="events", ratio=2),
        )

        # Populate panels
        layout["header"].update(self._make_header(workers, active_tasks, overview))
        layout["footer"].update(self._make_footer())

        workers_table = create_workers_table(workers, title=f"Workers ({len(workers)})")
        layout["body"]["top_half"]["workers"].update(Panel(workers_table, box=box.ROUNDED, border_style="cyan"))

        queues_table = create_queues_table(queues, title=f"Queues ({len(queues)})")
        layout["body"]["top_half"]["queues"].update(Panel(queues_table, box=box.ROUNDED, border_style="yellow"))

        tasks_table = create_tasks_table(active_tasks, title=f"Currently Running Tasks ({len(active_tasks)})")
        layout["body"]["bottom_half"]["tasks"].update(Panel(tasks_table, box=box.ROUNDED, border_style="green"))

        events_table = create_events_table(events, title="Live Event Feed (Recent)")
        layout["body"]["bottom_half"]["events"].update(Panel(events_table, box=box.ROUNDED, border_style="magenta"))

        return layout

    def run(self) -> None:
        """Start the interactive live dashboard loop."""
        console.print("[bold cyan]Connecting to Celery broker and starting event listener...[/]")
        self.event_monitor.start()

        # Cache inspection data to avoid slow network latency blocking UI every frame
        workers_cache = []
        active_tasks_cache = []
        queues_cache = []
        last_inspect_time = 0.0
        inspect_interval = max(2.0, self.refresh_rate)

        try:
            with Live(console=console, screen=True, refresh_per_second=4) as live:
                while True:
                    now = time.time()

                    # Periodic full inspection
                    if (now - last_inspect_time) >= inspect_interval:
                        try:
                            workers_cache = self.client.get_workers()
                            active_tasks_cache = self.client.get_active_tasks()
                            queues_cache = self.client.get_queues()
                        except Exception:
                            pass
                        last_inspect_time = now

                    # Combine with real-time events state
                    event_tasks = self.event_monitor.get_active_tasks()
                    # Merge active tasks
                    seen_ids = {t.task_id for t in active_tasks_cache}
                    for et in event_tasks:
                        if et.task_id not in seen_ids:
                            active_tasks_cache.append(et)

                    recent_events = self.event_monitor.get_recent_events(count=15)
                    overview = self.event_monitor.get_cluster_overview()

                    layout = self.build_layout(
                        workers_cache,
                        active_tasks_cache,
                        queues_cache,
                        recent_events,
                        overview,
                    )
                    live.update(layout)
                    time.sleep(self.refresh_rate)
        except KeyboardInterrupt:
            pass
        finally:
            self.event_monitor.stop()
            console.print("[bold yellow]Clirey monitor stopped.[/]")
