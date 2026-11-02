from __future__ import annotations

import pytest

from vulcano_telemetry.config import Settings
from vulcano_telemetry.exceptions import ConfigurationError


def test_uses_documented_defaults_without_loading_runtime_resources() -> None:
    settings = Settings.from_env(environ={})

    assert settings.queue_name == "vulcano-events-queue"
    assert settings.dlq_name == "vulcano-events-dlq"
    assert settings.visibility_timeout_seconds == 30
    assert settings.max_receive_count == 5
    assert settings.receiver_wait_time_seconds == 10
    assert settings.sqs_endpoint_url == "http://localhost:4566"
    assert settings.log_rotation_bytes == 2 * 1024 * 1024
    assert settings.log_backup_count == 3


def test_requires_a_non_placeholder_secret_only_for_secret_commands() -> None:
    with pytest.raises(ConfigurationError):
        Settings.from_env(environ={}, require_secret=True)
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            environ={"VULCANO_WEBHOOK_SECRET": "replace-with-a-local-secret"},
            require_secret=True,
        )

    settings = Settings.from_env(
        environ={"VULCANO_WEBHOOK_SECRET": "locally-configured-secret"},
        require_secret=True,
    )
    assert settings.webhook_secret == "locally-configured-secret"


def test_validates_required_destination_and_related_settings() -> None:
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            environ={"VULCANO_DESTINATION_URL": "ftp://collector.example/webhook"},
            require_destination=True,
        )
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            environ={
                "VULCANO_QUEUE_NAME": "same-queue",
                "VULCANO_DLQ_NAME": "same-queue",
            }
        )

    settings = Settings.from_env(
        environ={"VULCANO_DESTINATION_URL": "https://collector.example/webhook"},
        require_destination=True,
    )
    assert settings.destination_url == "https://collector.example/webhook"
