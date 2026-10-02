"""Validation and connectivity diagnostics for Celery brokers and inputs."""

from __future__ import annotations

import logging
import re
import socket
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from kombu import Connection

logger = logging.getLogger(__name__)

SUPPORTED_SCHEMES = {
    "redis": "Redis",
    "rediss": "Redis (SSL/TLS)",
    "sentinel": "Redis Sentinel",
    "amqp": "RabbitMQ / AMQP",
    "pyamqp": "RabbitMQ / pyamqp",
    "amqps": "RabbitMQ (SSL/TLS)",
    "sqs": "Amazon SQS",
    "rpc": "RabbitMQ RPC",
    "memory": "In-Memory Broker",
    "filesystem": "Filesystem Broker",
    "mongodb": "MongoDB",
    "couchdb": "CouchDB",
    "zookeeper": "Apache ZooKeeper",
}


class ValidationError(Exception):
    """Raised when validation fails."""


def validate_broker_url(url: Optional[str]) -> Tuple[bool, str]:
    """Validate format and scheme of a broker URL.

    Returns:
        (is_valid, error_or_success_message)
    """
    if not url or not url.strip():
        return False, "Broker URL is empty. Provide a valid Celery broker URL (e.g. redis://localhost:6379/0)."

    raw_url = url.strip()

    # Check for missing scheme (common user mistake: "localhost:6379")
    if "://" not in raw_url:
        if "6379" in raw_url or "redis" in raw_url.lower():
            return (
                False,
                f"Missing URL scheme in '{raw_url}'. Did you mean 'redis://{raw_url}/0'?",
            )
        if "5672" in raw_url or "rabbit" in raw_url.lower():
            return (
                False,
                f"Missing URL scheme in '{raw_url}'. Did you mean 'amqp://guest:guest@{raw_url}//'?",
            )
        return (
            False,
            f"Invalid broker URL '{raw_url}': missing scheme (e.g., redis://, amqp://, sqs://).",
        )

    try:
        parsed = urlparse(raw_url)
        scheme = parsed.scheme.lower()
    except Exception as e:
        return False, f"Malformed broker URL '{raw_url}': {e}"

    if not scheme:
        return False, f"Missing scheme in broker URL: '{raw_url}'"

    if scheme not in SUPPORTED_SCHEMES:
        supported = ", ".join(sorted(SUPPORTED_SCHEMES.keys()))
        return (
            False,
            f"Unsupported broker scheme '{scheme}'. Supported schemes: {supported}",
        )

    # Validate hostname / netloc for network-based brokers
    if scheme in {"redis", "rediss", "amqp", "pyamqp", "amqps", "sentinel"}:
        if not parsed.hostname and not parsed.netloc:
            return False, f"Broker URL '{raw_url}' is missing hostname/port."

    return True, f"Valid {SUPPORTED_SCHEMES.get(scheme, scheme.upper())} broker URL format."


def validate_task_id(task_id: Optional[str]) -> Tuple[bool, str]:
    """Validate Celery task ID / UUID string."""
    if not task_id or not task_id.strip():
        return False, "Task ID cannot be empty."

    tid = task_id.strip()

    # Standard UUID format check (8-4-4-4-12) or generic string
    uuid_pattern = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
    if uuid_pattern.match(tid):
        return True, "Valid UUID task ID."

    if len(tid) < 3 or len(tid) > 128:
        return False, f"Task ID length ({len(tid)}) is out of reasonable range (3-128 chars)."

    return True, "Valid custom task ID format."


def validate_rate_limit(rate_limit: Optional[str]) -> Tuple[bool, str]:
    """Validate Celery rate limit string (e.g. '100/m', '10/s', '1000/h', '0')."""
    if not rate_limit or not rate_limit.strip():
        return False, "Rate limit cannot be empty."

    rl = rate_limit.strip().lower()
    if rl in ("0", "none", "unlimited"):
        return True, "Rate limit disabled / unlimited."

    pattern = re.compile(r"^\d+/(s|m|h|d|second|minute|hour|day)$")
    if not pattern.match(rl):
        return False, f"Invalid rate limit format '{rate_limit}'. Expected format like '100/m', '10/s', '500/h'."

    return True, f"Valid rate limit '{rate_limit}'."


def test_broker_connectivity(broker_url: str, timeout: float = 3.0) -> Tuple[bool, str, Dict[str, Any]]:
    """Perform preflight network and protocol connectivity test to the broker.

    Returns:
        (is_connected, message, details_dict)
    """
    is_valid, err_msg = validate_broker_url(broker_url)
    if not is_valid:
        return False, err_msg, {"status": "invalid_url"}

    parsed = urlparse(broker_url)
    scheme = parsed.scheme.lower()
    host = parsed.hostname or "localhost"
    port = parsed.port

    # Default ports
    if not port:
        if scheme in ("redis", "rediss"):
            port = 6379
        elif scheme in ("amqp", "pyamqp"):
            port = 5672
        elif scheme == "amqps":
            port = 5671

    details: Dict[str, Any] = {
        "scheme": scheme,
        "host": host,
        "port": port,
        "latency_ms": None,
    }

    # 1. TCP Socket pre-check (fast fail on unreachable host/port)
    if port:
        t_sock_start = time.perf_counter()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                pass
            sock_latency = (time.perf_counter() - t_sock_start) * 1000
            details["tcp_socket_latency_ms"] = round(sock_latency, 2)
        except socket.timeout:
            return (
                False,
                f"Connection timed out reaching {host}:{port} (timeout={timeout}s). Check network/firewall.",
                details,
            )
        except ConnectionRefusedError:
            return (
                False,
                f"Connection refused at {host}:{port}. Verify that the broker service is running and listening on this port.",
                details,
            )
        except socket.gaierror as e:
            return (
                False,
                f"Could not resolve host '{host}': {e}. Check DNS or hostname spelling.",
                details,
            )
        except Exception as e:
            logger.debug(f"TCP check warning: {e}")

    # 2. Protocol handshake check via kombu connection
    t_start = time.perf_counter()
    try:
        with Connection(broker_url, connect_timeout=timeout) as conn:
            conn.connect()
            elapsed_ms = (time.perf_counter() - t_start) * 1000
            details["latency_ms"] = round(elapsed_ms, 2)
            details["transport"] = conn.transport.driver_type if hasattr(conn.transport, "driver_type") else scheme
            return (
                True,
                f"Successfully connected to broker ({details['transport']}) in {elapsed_ms:.1f}ms.",
                details,
            )
    except Exception as e:
        err_str = str(e)
        if "Authentication" in err_str or "auth" in err_str.lower() or "NOAUTH" in err_str or "WRONGPASS" in err_str:
            return (
                False,
                f"Authentication failed for broker at {host}:{port}. Check password/username credentials.",
                details,
            )
        return (
            False,
            f"Broker protocol handshake failed at {host}:{port}: {err_str}",
            details,
        )
