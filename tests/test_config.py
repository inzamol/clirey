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


def test_resolve_key_prefix_explicit():
    assert ClireyConfig.resolve_key_prefix("my_prefix_") == "my_prefix_"


def test_resolve_key_prefix_env(monkeypatch):
    monkeypatch.setenv("CELERY_GLOBAL_KEYPREFIX", "prod_celery_")
    assert ClireyConfig.resolve_key_prefix(None) == "prod_celery_"


def test_resolve_key_prefix_json_env(monkeypatch):
    monkeypatch.delenv("CELERY_GLOBAL_KEYPREFIX", raising=False)
    monkeypatch.setenv("CELERY_BROKER_TRANSPORT_OPTIONS", '{"global_keyprefix": "env_prefix_"}')
    assert ClireyConfig.resolve_key_prefix(None) == "env_prefix_"

