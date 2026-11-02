"""Shared deterministic fixtures for offline unit tests."""

from __future__ import annotations

import pytest
import os
import uuid
from dataclasses import replace

from vulcano_telemetry.config import Settings
from vulcano_telemetry.core_sqs import CoreSQS

TEST_SECRET = "offline-test-secret"
FIXED_TIMESTAMP = 1_700_000_000


@pytest.fixture
def settings() -> Settings:
    return Settings(webhook_secret=TEST_SECRET)


@pytest.fixture
def event_body() -> bytes:
    return (
        b'{ "event_id":"evt-1", "event_type":"telemetry.created", '
        b'"occurred_at":"2026-01-02T03:04:05Z", "trace_id":"trace-1", '
        b'"payload":{"temperature":21.5} }'
    )


@pytest.fixture
def localstack_sqs_factory():
    if os.environ.get("VULCANO_RUN_LOCALSTACK_TESTS") != "1":
        pytest.skip("Set VULCANO_RUN_LOCALSTACK_TESTS=1 to run LocalStack integration tests")

    base_settings = Settings.from_env()
    instances: list[tuple[CoreSQS, Settings]] = []

    def create(*, visibility_timeout: int = 30, max_receive_count: int = 5):
        suffix = uuid.uuid4().hex[:12]
        settings = replace(
            base_settings,
            queue_name=f"vulcano-events-{suffix}-queue",
            dlq_name=f"vulcano-events-{suffix}-dlq",
            visibility_timeout_seconds=visibility_timeout,
            max_receive_count=max_receive_count,
            receiver_wait_time_seconds=1,
        )
        sqs = CoreSQS(settings)
        sqs.ensure_queues()
        instances.append((sqs, settings))
        return sqs, settings

    yield create

    for sqs, settings in instances:
        sqs.delete_queue(settings.queue_name)
        sqs.delete_queue(settings.dlq_name)
        sqs.close()
