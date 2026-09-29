"""Rich Console formatting, badges, and helper functions."""

import datetime
import io
import os
import sys
from typing import Optional
from rich.console import Console
from rich.text import Text

# Force UTF-8 on Windows consoles to prevent cp1252 charmap encode errors
if sys.platform.startswith("win"):
    try:
        if sys.stdout and hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if sys.stderr and hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

console = Console(force_terminal=True, legacy_windows=False)
err_console = Console(stderr=True, force_terminal=True, legacy_windows=False)



def format_state_badge(state: str) -> str:
    """Return colored badge for Celery task states."""
    s = (state or "UNKNOWN").upper()
    if s in ("SUCCESS", "SUCCEEDED"):
        return "[bold white on green] SUCCESS [/]"
    elif s in ("FAILURE", "FAILED"):
        return "[bold white on red] FAILURE [/]"
    elif s in ("STARTED", "RUNNING"):
        return "[bold black on cyan] STARTED [/]"
    elif s in ("RECEIVED", "PENDING"):
        return "[bold black on yellow] PENDING [/]"
    elif s in ("RETRY", "RETRYING"):
        return "[bold white on dark_orange]  RETRY  [/]"
    elif s in ("REVOKED", "CANCELLED"):
        return "[bold white on magenta] REVOKED [/]"
    elif s in ("SCHEDULED",):
        return "[bold white on blue] SCHED   [/]"
    elif s in ("RESERVED",):
        return "[bold white on purple] RESERV  [/]"
    return f"[dim]{s}[/]"


def format_event_badge(event_type: str) -> str:
    """Return colored badge for Celery event types."""
    et = (event_type or "").lower()
    if et == "task-succeeded":
        return "[bold green]● SUCCEED[/]"
    elif et == "task-failed":
        return "[bold red]✖ FAILED [/]"
    elif et == "task-started":
        return "[bold cyan]▶ STARTED[/]"
    elif et == "task-received":
        return "[bold yellow]↓ RECEIVE[/]"
    elif et == "task-retried":
        return "[bold orange3]↺ RETRY  [/]"
    elif et == "task-revoked":
        return "[bold magenta]⊘ REVOKED[/]"
    elif et == "worker-online":
        return "[bold green]▲ W-UP   [/]"
    elif et == "worker-offline":
        return "[bold red]▼ W-DOWN [/]"
    elif et == "worker-heartbeat":
        return "[dim cyan]♥ HEARTBT[/]"
    return f"[dim]{event_type}[/]"


def format_worker_status(status: str) -> str:
    """Format worker status with green/red dot."""
    if status.upper() == "ONLINE":
        return "[bold green]● ONLINE[/]"
    elif status.upper() == "OFFLINE":
        return "[bold red]○ OFFLINE[/]"
    return f"[yellow]? {status}[/]"


def format_duration(seconds: Optional[float]) -> str:
    """Format duration nicely in human readable format."""
    if seconds is None:
        return "-"
    if seconds < 1.0:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60.0:
        return f"{seconds:.2f}s"
    mins = int(seconds // 60)
    secs = seconds % 60
    if mins < 60:
        return f"{mins}m {secs:.0f}s"
    hours = int(mins // 60)
    mins = mins % 60
    return f"{hours}h {mins}m"


def format_uptime(seconds: float) -> str:
    """Format uptime in human readable string."""
    if seconds <= 0:
        return "0s"
    td = datetime.timedelta(seconds=int(seconds))
    days = td.days
    hours, rem = divmod(td.seconds, 3600)
    mins, secs = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h {mins}m"
    if hours > 0:
        return f"{hours}h {mins}m {secs}s"
    return f"{mins}m {secs}s"


def format_loadavg(loadavg: list) -> str:
    """Format OS load averages."""
    if not loadavg or len(loadavg) < 3:
        return "N/A"
    return f"{loadavg[0]:.2f}, {loadavg[1]:.2f}, {loadavg[2]:.2f}"
