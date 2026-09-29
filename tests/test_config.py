"""Tests for configuration and broker URL masking."""

from clirey.config import ClireyConfig, mask_broker_url


def test_mask_broker_url_redis():
    url = "redis://:secret_pw@192.168.1.50:6379/2"
    masked = mask_broker_url(url)
    assert ":****@" in masked
    assert "secret_pw" not in masked
    assert "192.168.1.50" in masked


def test_mask_broker_url_amqp():
    url = "amqp://admin:supersecret@rabbit.production.local:5672/vhost"
    masked = mask_broker_url(url)
    assert ":****@" in masked
    assert "supersecret" not in masked
    assert "admin" in masked


def test_resolve_broker_url_explicit():
    url = "redis://custom-host:6379/1"
    resolved = ClireyConfig.resolve_broker_url(url)
    assert resolved == url


def test_resolve_broker_url_env(monkeypatch):
    monkeypatch.setenv("CELERY_BROKER_URL", "amqp://test:test@rabbitmq:5672//")
    resolved = ClireyConfig.resolve_broker_url(None)
    assert resolved == "amqp://test:test@rabbitmq:5672//"
