"""Validated environment configuration, loaded only when a CLI command runs.

Keeping environment parsing here avoids duplicating defaults and lets argparse
render help without reading secrets or initializing AWS clients.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from .exceptions import ConfigurationError
from .sanitizer import (
    parse_dlq_name,
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)

LOG_ROTATION_BYTES = 2 * 1024 * 1024
LOG_BACKUP_COUNT = 3


def _read_integer(
    environment: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    raw_value = environment.get(name, str(default))
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as err:
        raise ConfigurationError(f"{name} must be an integer") from err
    if str(raw_value) != str(value):
        raise ConfigurationError(f"{name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        limit = f"{minimum} through {maximum}" if maximum is not None else f"at least {minimum}"
        raise ConfigurationError(f"{name} must be {limit}")
    return value


@dataclass(frozen=True)
class Settings:
    """Settings for one runtime process; all defaults match the project docs."""

    webhook_secret: str | None = None
    sqs_endpoint_url: str | None = "http://localhost:4566"
    aws_region: str = "us-east-1"
    queue_name: str = "vulcano-events-queue"
    dlq_name: str = "vulcano-events-dlq"
    visibility_timeout_seconds: int = 30
    max_receive_count: int = 5
    receiver_wait_time_seconds: int = 10
    destination_url: str = "http://localhost:8080/webhook"
    tolerance_seconds: int = 300
    log_path: str = "logs/vulcano-telemetry.jsonl"
    log_rotation_bytes: int = LOG_ROTATION_BYTES
    log_backup_count: int = LOG_BACKUP_COUNT

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        require_secret: bool = False,
        require_destination: bool = False,
    ) -> Settings:
        """Load and validate configuration without creating clients or handlers."""
        environment = os.environ if environ is None else environ
        secret = environment.get("VULCANO_WEBHOOK_SECRET")
        if require_secret:
            try:
                secret = parse_secret_key(secret) if secret is not None else None
            except ValueError as err:
                raise ConfigurationError(str(err)) from err
            if secret is None:
                raise ConfigurationError("VULCANO_WEBHOOK_SECRET must be configured")
        elif secret is not None and (not secret or secret.isspace()):
            raise ConfigurationError("VULCANO_WEBHOOK_SECRET cannot be empty when provided")

        endpoint = environment.get("VULCANO_SQS_ENDPOINT_URL", "http://localhost:4566")
        if endpoint:
            try:
                endpoint = parse_webhook_url(endpoint)
            except ValueError as err:
                raise ConfigurationError(f"Invalid VULCANO_SQS_ENDPOINT_URL: {err}") from err
        else:
            endpoint = None

        region = environment.get("AWS_DEFAULT_REGION", "us-east-1")
        if not region or region != region.strip():
            raise ConfigurationError("AWS_DEFAULT_REGION must be non-empty")

        try:
            queue_name = parse_queue_name(
                environment.get("VULCANO_QUEUE_NAME", "vulcano-events-queue")
            )
            dlq_name = parse_dlq_name(
                environment.get("VULCANO_DLQ_NAME", "vulcano-events-dlq")
            )
        except ValueError as err:
            raise ConfigurationError(f"Invalid SQS queue configuration: {err}") from err
        if queue_name == dlq_name:
            raise ConfigurationError("VULCANO_QUEUE_NAME and VULCANO_DLQ_NAME must differ")

        destination = environment.get(
            "VULCANO_DESTINATION_URL", "http://localhost:8080/webhook"
        )
        if require_destination:
            try:
                destination = parse_webhook_url(destination)
            except ValueError as err:
                raise ConfigurationError(f"Invalid VULCANO_DESTINATION_URL: {err}") from err

        try:
            wait_time = parse_wait_time(
                environment.get("VULCANO_RECEIVER_WAIT_TIME_SECONDS", "10")
            )
        except ValueError as err:
            raise ConfigurationError(f"Invalid VULCANO_RECEIVER_WAIT_TIME_SECONDS: {err}") from err

        return cls(
            webhook_secret=secret,
            sqs_endpoint_url=endpoint,
            aws_region=region,
            queue_name=queue_name,
            dlq_name=dlq_name,
            visibility_timeout_seconds=_read_integer(
                environment,
                "VULCANO_SQS_VISIBILITY_TIMEOUT_SECONDS",
                30,
                minimum=0,
                maximum=43_200,
            ),
            max_receive_count=_read_integer(
                environment,
                "VULCANO_SQS_MAX_RECEIVE_COUNT",
                5,
                minimum=1,
                maximum=1_000,
            ),
            receiver_wait_time_seconds=wait_time,
            destination_url=destination,
            tolerance_seconds=_read_integer(
                environment,
                "VULCANO_TOLERANCE_SEC",
                300,
                minimum=0,
            ),
            log_path=environment.get("VULCANO_LOG_PATH", "logs/vulcano-telemetry.jsonl"),
            log_rotation_bytes=LOG_ROTATION_BYTES,
            log_backup_count=LOG_BACKUP_COUNT,
        )
