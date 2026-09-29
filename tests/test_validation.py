"""Tests for input validation and diagnostics."""

from clirey.core.validation import (
    validate_broker_url,
    validate_rate_limit,
    validate_task_id,
)


def test_validate_broker_url_valid():
    assert validate_broker_url("redis://localhost:6379/0")[0] is True
    assert validate_broker_url("rediss://:password@10.0.1.5:6379/1")[0] is True
    assert validate_broker_url("amqp://guest:guest@localhost:5672//")[0] is True
    assert validate_broker_url("sqs://")[0] is True
    assert validate_broker_url("sentinel://localhost:26379;sentinel://localhost:26380")[0] is True


def test_validate_broker_url_invalid():
    # Empty URL
    assert validate_broker_url("")[0] is False
    assert validate_broker_url(None)[0] is False

    # Missing scheme with hints
    is_valid, msg = validate_broker_url("localhost:6379")
    assert is_valid is False
    assert "redis://" in msg

    # Unsupported scheme
    is_valid, msg = validate_broker_url("ftp://localhost:21")
    assert is_valid is False
    assert "Unsupported broker scheme" in msg


def test_validate_task_id():
    assert validate_task_id("d4b2e8a1-1c2d-4e5f-8a9b-0c1d2e3f4a5b")[0] is True
    assert validate_task_id("custom_task_123")[0] is True
    assert validate_task_id("")[0] is False
    assert validate_task_id("a")[0] is False  # Too short


def test_validate_rate_limit():
    assert validate_rate_limit("10/s")[0] is True
    assert validate_rate_limit("100/m")[0] is True
    assert validate_rate_limit("5000/h")[0] is True
    assert validate_rate_limit("0")[0] is True
    assert validate_rate_limit("invalid_rate")[0] is False
