import sys
from typing import Optional

# Ensure UTF-8 output streams on Windows
if sys.platform.startswith("win"):
    try:
        if sys.stdout and hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if sys.stderr and hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import time

import typer
from rich.panel import Panel

from clirey.config import ClireyConfig, mask_broker_url
from clirey.core.client import CeleryClient
from clirey.core.events import EventMonitor
from clirey.core.validation import (
    test_broker_connectivity,
    validate_broker_url,
    validate_task_id,
)
from clirey.mock.simulator import MockDashboard
from clirey.ui.console import console, err_console, format_event_badge
from clirey.ui.dashboard import Dashboard
from clirey.ui.tables import (
    create_queues_table,
    create_tasks_table,
    create_workers_table,
)

app = typer.Typer(
    name="clirey",
    help="Clirey: Modern CLI & Live Terminal Monitor for Celery with just a Broker URL.",
    add_completion=False,
)


def get_resolved_broker(broker: Optional[str]) -> str:
    """Resolve and validate broker URL."""
    resolved = ClireyConfig.resolve_broker_url(broker)
    if not resolved:
        err_console.print(
            "[bold red]Error:[/] No Celery broker URL specified!\n"
            "Provide it via [bold yellow]--broker <url>[/] or set the [bold yellow]CELERY_BROKER_URL[/] environment variable."
        )
        raise typer.Exit(code=1)

    is_valid, msg = validate_broker_url(resolved)
    if not is_valid:
        err_console.print(f"[bold red]Invalid Broker URL:[/] {msg}")
        raise typer.Exit(code=1)

    return resolved


@app.command(name="validate")
@app.command(name="check")
def cmd_validate(
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL to check"),
    timeout: float = typer.Option(3.0, "--timeout", "-t", help="Connection timeout in seconds"),
):
    """Run preflight diagnostics and connectivity verification on the Celery broker."""
    raw_broker = ClireyConfig.resolve_broker_url(broker)
    masked = mask_broker_url(raw_broker)

    console.print(f"\n[bold bright_cyan]Preflight Diagnostics for Broker:[/] {masked}\n")

    # Step 1: URL Syntax & Scheme Check
    is_valid, url_msg = validate_broker_url(raw_broker)
    if not is_valid:
        console.print(f"  [bold red][FAIL][/] [bold red]URL Syntax:[/] {url_msg}")
        raise typer.Exit(code=1)
    console.print(f"  [bold green][OK][/] [bold green]URL Syntax:[/] {url_msg}")

    # Step 2: Protocol Connectivity Test
    with console.status("[bold cyan]Testing broker connectivity...[/]", spinner="dots"):
        is_connected, conn_msg, details = test_broker_connectivity(raw_broker, timeout=timeout)

    if not is_connected:
        console.print(f"  [bold red][FAIL][/] [bold red]Broker Reachability:[/] {conn_msg}")
        if details.get("host") and details.get("port"):
            console.print(f"     [dim]Target: {details['host']}:{details['port']} ({details['scheme']})[/]")
        raise typer.Exit(code=1)

    latency_str = f"({details['latency_ms']:.1f}ms)" if details.get("latency_ms") else ""
    console.print(f"  [bold green][OK][/] [bold green]Broker Reachability:[/] Connected successfully {latency_str}")

    # Step 3: Worker Ping Inspection
    client = CeleryClient(broker_url=raw_broker, timeout=timeout)
    with console.status("[bold cyan]Scanning for active workers...[/]", spinner="dots"):
        t_ping = time.perf_counter()
        pings = client.ping()
        ping_latency = (time.perf_counter() - t_ping) * 1000

    if pings:
        worker_count = len(pings)
        console.print(f"  [bold green][OK][/] [bold green]Worker Cluster:[/] Found {worker_count} active worker(s) ({ping_latency:.1f}ms)")
        for w_name in pings:
            console.print(f"     - [cyan]{w_name}[/]")
    else:
        console.print("  [bold yellow][WARN][/] [bold yellow]Worker Cluster:[/] Broker is reachable, but 0 workers responded (workers may be stopped or idle).")

    # Step 4: Queue Discovery
    queues = client.get_queues()
    queue_names = [q.name for q in queues]
    console.print(f"  [bold green][OK][/] [bold green]Broker Queues:[/] {len(queues)} queue(s) detected: {', '.join(queue_names)}")

    console.print("\n[bold green]All preflight checks passed.[/] Ready to monitor with [bold cyan]clirey top[/].\n")




@app.command(name="top")
@app.command(name="monitor")
@app.command(name="live")
def cmd_top(
    broker: Optional[str] = typer.Option(
        None,
        "--broker",
        "-b",
        help="Celery broker URL (e.g., redis://localhost:6379/0 or amqp://guest:guest@localhost:5672//)",
    ),
    backend: Optional[str] = typer.Option(
        None,
        "--backend",
        help="Optional Celery result backend URL",
    ),
    refresh: float = typer.Option(
        1.5,
        "--refresh",
        "-r",
        help="Dashboard refresh interval in seconds",
    ),
):
    """Launch the real-time interactive TUI dashboard."""
    broker_url = get_resolved_broker(broker)
    dashboard = Dashboard(broker_url=broker_url, backend_url=backend, refresh_rate=refresh)
    dashboard.run()


@app.command(name="demo")
def cmd_demo(
    refresh: float = typer.Option(
        1.0,
        "--refresh",
        "-r",
        help="Demo refresh interval in seconds",
    ),
):
    """Run an interactive live demo simulator without needing a live Celery broker."""
    demo = MockDashboard(refresh_rate=refresh)
    demo.run()


@app.command(name="workers")
@app.command(name="status")
def cmd_workers(
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL"),
    timeout: float = typer.Option(3.0, "--timeout", "-t", help="Timeout for worker inspection in seconds"),
):
    """List all connected Celery workers, pool concurrency, load, and stats."""
    broker_url = get_resolved_broker(broker)
    console.print(f"[dim]Connecting to broker: {mask_broker_url(broker_url)}[/]")

    with console.status("[bold cyan]Inspecting Celery workers...[/]", spinner="dots"):
        client = CeleryClient(broker_url=broker_url, timeout=timeout)
        workers = client.get_workers()

    if not workers:
        console.print(
            Panel(
                "[bold yellow]No Celery workers responded within timeout.[/]\n\n"
                "• Check if workers are running and connected to this broker.\n"
                "• Verify that the broker URL is reachable.",
                title="[bold red]No Workers Detected[/]",
                border_style="red",
            )
        )
        return

    table = create_workers_table(workers)
    console.print(table)


@app.command(name="tasks")
def cmd_tasks(
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL"),
    filter_type: str = typer.Option(
        "all",
        "--type",
        "-t",
        help="Task type to display: all, active, scheduled, reserved",
    ),
    timeout: float = typer.Option(3.0, "--timeout", help="Timeout in seconds"),
):
    """List currently active, scheduled, or reserved tasks across the cluster."""
    broker_url = get_resolved_broker(broker)
    client = CeleryClient(broker_url=broker_url, timeout=timeout)

    with console.status("[bold cyan]Fetching tasks from workers...[/]", spinner="dots"):
        active = client.get_active_tasks() if filter_type in ("all", "active") else []
        scheduled = client.get_scheduled_tasks() if filter_type in ("all", "scheduled") else []
        reserved = client.get_reserved_tasks() if filter_type in ("all", "reserved") else []

    all_tasks = active + scheduled + reserved
    if not all_tasks:
        console.print("[yellow]No tasks currently running or queued in workers.[/]")
        return

    table = create_tasks_table(all_tasks, title=f"Celery Tasks ({len(all_tasks)} found)")
    console.print(table)


@app.command(name="queues")
def cmd_queues(
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL"),
    timeout: float = typer.Option(3.0, "--timeout", help="Timeout in seconds"),
):
    """Inspect broker queue backlog depths and active worker subscriptions."""
    broker_url = get_resolved_broker(broker)
    client = CeleryClient(broker_url=broker_url, timeout=timeout)

    with console.status("[bold cyan]Querying queues and broker backlog...[/]", spinner="dots"):
        queues = client.get_queues()

    table = create_queues_table(queues)
    console.print(table)


@app.command(name="events")
def cmd_events(
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL"),
):
    """Stream real-time Celery events to standard output."""
    broker_url = get_resolved_broker(broker)
    console.print(f"[bold cyan]Streaming live Celery events from:[/] {mask_broker_url(broker_url)}")
    console.print("[dim]Press Ctrl+C to stop streaming.\n[/]")

    client = CeleryClient(broker_url=broker_url)

    def on_event(ev):
        t_str = time.strftime("%H:%M:%S", time.localtime(ev.timestamp))
        task_name = ev.task_name or "unknown"
        tid = f"[{ev.task_id[:8]}]" if ev.task_id else ""
        dur = f"({ev.runtime:.2f}s)" if ev.runtime is not None else ""
        worker = f"@{ev.worker}" if ev.worker else ""
        badge = format_event_badge(ev.event_type)

        console.print(
            f"[dim]{t_str}[/]  {badge}  [bold yellow]{task_name}[/] [dim]{tid}[/] [cyan]{worker}[/] [green]{dur}[/]"
        )
        if ev.exception:
            console.print(f"       [red]Error: {ev.exception}[/]")

    monitor = EventMonitor(client, on_event_callback=on_event)
    monitor.start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        monitor.stop()
        console.print("\n[yellow]Stopped event stream.[/]")


@app.command(name="ping")
def cmd_ping(
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL"),
    timeout: float = typer.Option(3.0, "--timeout", help="Timeout in seconds"),
):
    """Ping active Celery workers and check response latency."""
    broker_url = get_resolved_broker(broker)
    client = CeleryClient(broker_url=broker_url, timeout=timeout)

    t0 = time.perf_counter()
    with console.status("[bold cyan]Pinging workers...[/]", spinner="dots"):
        res = client.ping()
    elapsed_ms = (time.perf_counter() - t0) * 1000

    if not res:
        console.print("[bold red][FAIL] No response from any worker.[/]")
        raise typer.Exit(code=1)

    console.print(f"[bold green][OK] Received responses in {elapsed_ms:.1f}ms:[/]")
    for worker, reply in res.items():
        console.print(f"  - [bold cyan]{worker}:[/] [green]{reply}[/]")


@app.command(name="task")
def cmd_task(
    task_id: str = typer.Argument(..., help="Celery task UUID to inspect"),
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL"),
    backend: Optional[str] = typer.Option(None, "--backend", help="Optional result backend URL"),
):
    """Query a specific task across workers and result backend."""
    is_valid, msg = validate_task_id(task_id)
    if not is_valid:
        err_console.print(f"[bold red]Invalid Task ID:[/] {msg}")
        raise typer.Exit(code=1)

    broker_url = get_resolved_broker(broker)
    client = CeleryClient(broker_url=broker_url, backend_url=backend)

    with console.status(f"[bold cyan]Querying task {task_id}...[/]", spinner="dots"):
        info = client.query_task(task_id)

    console.print(Panel(str(info), title=f"Task Query: {task_id}", border_style="cyan"))


@app.command(name="revoke")
def cmd_revoke(
    task_id: str = typer.Argument(..., help="Celery task UUID to revoke"),
    broker: Optional[str] = typer.Option(None, "--broker", "-b", help="Celery broker URL"),
    terminate: bool = typer.Option(False, "--terminate", "-t", help="Terminate currently executing task process"),
    signal: str = typer.Option(
        "SIGTERM", "--signal", "-s", help="Signal to send when terminating (e.g. SIGTERM, SIGKILL)"
    ),
):
    """Revoke or terminate a Celery task by ID."""
    is_valid, msg = validate_task_id(task_id)
    if not is_valid:
        err_console.print(f"[bold red]Invalid Task ID:[/] {msg}")
        raise typer.Exit(code=1)

    broker_url = get_resolved_broker(broker)
    client = CeleryClient(broker_url=broker_url)

    confirm = typer.confirm(f"Are you sure you want to revoke task '{task_id}' (terminate={terminate})?")
    if not confirm:
        console.print("[yellow]Aborted.[/]")
        return

    with console.status(f"[bold cyan]Revoking task {task_id}...[/]", spinner="dots"):
        res = client.revoke_task(task_id, terminate=terminate, signal=signal)

    console.print(f"[bold green][OK] Revoke broadcast sent.[/] Replies: {res}")




def main():
    """Main entrypoint for CLI execution."""
    app()


if __name__ == "__main__":
    main()
