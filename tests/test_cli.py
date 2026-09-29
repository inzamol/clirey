"""Tests for Typer CLI commands using CliRunner."""

from typer.testing import CliRunner
from clirey.cli import app

runner = CliRunner()


def test_cli_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Clirey" in result.output
    assert "top" in result.output
    assert "workers" in result.output
    assert "tasks" in result.output
    assert "queues" in result.output


def test_cli_workers_no_cluster():
    result = runner.invoke(app, ["workers", "--broker", "redis://127.0.0.1:9999/0", "--timeout", "0.5"])
    # Should handle gracefully with "No Celery workers responded"
    assert result.exit_code == 0
    assert "No Celery workers responded" in result.output or "No workers responding" in result.output


def test_cli_tasks_no_cluster():
    result = runner.invoke(app, ["tasks", "--broker", "redis://127.0.0.1:9999/0", "--timeout", "0.5"])
    assert result.exit_code == 0
    assert "No tasks" in result.output
