from __future__ import annotations

import hashlib
import hmac
import time

import pytest

from vulcano_telemetry import webhook_crypto
from vulcano_telemetry.exceptions import WebhookSignatureError, WebhookTimestampError
from vulcano_telemetry.webhook_crypto import generate_signature, verify_signature

from .conftest import FIXED_TIMESTAMP, TEST_SECRET


def test_signs_timestamp_prefix_and_original_body_bytes(
    event_body: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(webhook_crypto.time, "time", lambda: FIXED_TIMESTAMP)
    signature = generate_signature(TEST_SECRET, FIXED_TIMESTAMP, event_body)
    expected = hmac.new(
        TEST_SECRET.encode("utf-8"),
        f"{FIXED_TIMESTAMP}.".encode("ascii") + event_body,
        hashlib.sha256,
    ).hexdigest()

    assert signature == f"t={FIXED_TIMESTAMP},v1={expected}"
    assert verify_signature(TEST_SECRET, signature, event_body) is True


@pytest.mark.parametrize(
    "header",
    [None, "", "t=1700000000,v1=not-hex", "t=1700000000,v1=" + "g" * 64],
)
def test_rejects_missing_or_malformed_signatures(header: str | None, event_body: bytes) -> None:
    with pytest.raises(WebhookSignatureError):
        verify_signature(TEST_SECRET, header, event_body)


def test_rejects_non_ascii_signature_header(event_body: bytes) -> None:
    timestamp = int(time.time())
    with pytest.raises(WebhookSignatureError):
        verify_signature(TEST_SECRET, f"t={timestamp},v1=" + "a" * 63 + "é", event_body)


@pytest.mark.parametrize("timestamp", [FIXED_TIMESTAMP - 301, FIXED_TIMESTAMP + 301])
def test_rejects_old_and_future_timestamps(
    timestamp: int,
    event_body: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(webhook_crypto.time, "time", lambda: FIXED_TIMESTAMP)
    header = generate_signature(TEST_SECRET, timestamp, event_body)
    with pytest.raises(WebhookTimestampError):
        verify_signature(TEST_SECRET, header, event_body, tolerance_sec=300)


def test_rejects_a_valid_signature_for_different_raw_bytes(event_body: bytes) -> None:
    timestamp = int(time.time())
    header = generate_signature(TEST_SECRET, timestamp, event_body)
    with pytest.raises(WebhookSignatureError):
        verify_signature(TEST_SECRET, header, event_body.replace(b"21.5", b"21.50"))
