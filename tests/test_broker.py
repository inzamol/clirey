"""Tests for BrokerInspector prefix handling and queue discovery."""

from unittest.mock import MagicMock, patch

from clirey.core.broker import BrokerInspector


def test_broker_inspector_key_prefix():
    inspector = BrokerInspector("redis://localhost:6379/0", key_prefix="tenant1_")
    assert inspector.key_prefix == "tenant1_"
    assert inspector.is_redis() is True


def test_broker_inspector_discover_queues_with_prefix():
    inspector = BrokerInspector("redis://localhost:6379/0", key_prefix="tenant1_")
    mock_redis = MagicMock()
    # Mock scanning returns prefixed keys including internal celery keys and queues
    mock_redis.scan_iter.return_value = [
        b"tenant1__kombu.binding.celery",
        b"tenant1_celery.pidbox",
        b"tenant1_celeryev.test",
        b"tenant1_high_priority",
        b"tenant1_emails",
    ]
    mock_pipeline = MagicMock()
    mock_pipeline.execute.return_value = ["list", "list"]
    mock_redis.pipeline.return_value = mock_pipeline

    with patch("redis.from_url", return_value=mock_redis):
        queues = inspector.discover_queues()
        assert "high_priority" in queues
        assert "emails" in queues
        assert "_kombu.binding.celery" not in queues
        assert "celery.pidbox" not in queues


def test_broker_inspector_auto_detect_key_prefix():
    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = [
        b"custom_prefix__kombu.binding.celery.pidbox",
        b"custom_prefix__kombu.binding.celery",
        b"custom_prefix_celeryev.node1",
    ]
    with patch("redis.from_url", return_value=mock_redis):
        detected = BrokerInspector.auto_detect_key_prefix("redis://localhost:6379/0")
        assert detected == "custom_prefix_"
