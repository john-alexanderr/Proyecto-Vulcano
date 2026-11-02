from __future__ import annotations

import pytest

from vulcano_telemetry.event_schema import parse_event
from vulcano_telemetry.exceptions import (
    ConfigurationError,
    EventValidationError,
)
from vulcano_telemetry.sanitizer import (
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
    validate_http_url,
    validate_queue_name,
    validate_secret,
)


def test_event_validation_preserves_all_required_metadata(event_body: bytes) -> None:
    event = parse_event(event_body)

    assert event.event_id == "evt-1"
    assert event.event_type == "telemetry.created"
    assert event.occurred_at == "2026-01-02T03:04:05Z"
    assert event.trace_id == "trace-1"
    assert event.payload == {"temperature": 21.5}


@pytest.mark.parametrize(
    "body",
    [
        b"not-json",
        b"\xff",
        b"[]",
        b'{"event_id":"evt-1","event_type":"x","occurred_at":"now",'
        b'"trace_id":"trace-1"}',
        b'{"event_id":"evt-1","event_type":"x","occurred_at":"now",'
        b'"trace_id":"bad trace","payload":null}',
        b'{"event_id":"evt-1","event_type":"x","occurred_at":"now",'
        b'"trace_id":"trace-1","payload":NaN}',
    ],
)
def test_rejects_invalid_json_or_envelopes(body: bytes) -> None:
    with pytest.raises(EventValidationError):
        parse_event(body)


def test_validates_queue_names_and_standard_queue_scope() -> None:
    assert parse_queue_name("vulcano-telemetry-queue") == "vulcano-telemetry-queue"
    for invalid in ("", "vulcano-events-dlq", "events-queue", "Vulcano-events-queue"):
        with pytest.raises(ConfigurationError):
            validate_queue_name(invalid)


@pytest.mark.parametrize("value", [1, 10, 20, "3"])
def test_accepts_sqs_wait_time_range(value: int | str) -> None:
    assert 1 <= parse_wait_time(value) <= 20


@pytest.mark.parametrize("value", [0, -1, 21, "1.2", "01", True])
def test_rejects_invalid_sqs_wait_time(value: int | str | bool) -> None:
    with pytest.raises(ValueError):
        parse_wait_time(value)


def test_validates_http_destination_and_configured_secret() -> None:
    assert parse_webhook_url("https://collector.example/events") == (
        "https://collector.example/events"
    )
    assert validate_http_url("https://collector.example/events") == (
        "https://collector.example/events"
    )
    assert parse_secret_key("offline-test-secret") == "offline-test-secret"
    for invalid_url in ("/webhook", "ftp://example.test", "http://user:pass@example.test"):
        with pytest.raises(ConfigurationError):
            validate_http_url(invalid_url)
    for invalid_secret in (None, "", "short-secret", "replace-this-placeholder-secret"):
        with pytest.raises(ConfigurationError):
            validate_secret(invalid_secret)
