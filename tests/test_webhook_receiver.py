from __future__ import annotations

import time

from fastapi.testclient import TestClient

from vulcano_telemetry.config import Settings
from vulcano_telemetry.exceptions import QueuePublishError
from vulcano_telemetry.webhook_crypto import generate_signature
from vulcano_telemetry.webhook_receiver import create_app

from .conftest import FIXED_TIMESTAMP, TEST_SECRET


class FakeSQS:
    def __init__(self, *, fail_publish: bool = False) -> None:
        self.published: list[tuple[str, object]] = []
        self.fail_publish = fail_publish

    def send_event(self, body: str, event: object) -> str:
        if self.fail_publish:
            raise QueuePublishError("publish unavailable")
        self.published.append((body, event))
        return "message-id"


def _client(sqs: FakeSQS) -> TestClient:
    settings = Settings(webhook_secret=TEST_SECRET, tolerance_seconds=300)
    app = create_app(settings=settings, sqs=sqs)
    return TestClient(app)


def _signature(body: bytes) -> str:
    return generate_signature(TEST_SECRET, int(time.time()), body)


def test_accepts_only_after_successful_publish_and_preserves_json(event_body: bytes) -> None:
    sqs = FakeSQS()
    client = _client(sqs)
    response = client.post(
        "/webhook",
        content=event_body,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Signature": _signature(event_body),
        },
    )

    assert response.status_code == 202
    assert len(sqs.published) == 1
    assert sqs.published[0][0] == event_body.decode("utf-8")


def test_returns_401_for_missing_or_invalid_signature(event_body: bytes) -> None:
    sqs = FakeSQS()
    client = _client(sqs)

    missing = client.post("/webhook", content=event_body)
    invalid = client.post(
        "/webhook",
        content=event_body,
        headers={"X-Webhook-Signature": f"t={int(time.time())},v1=" + "0" * 64},
    )

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert sqs.published == []


def test_non_ascii_signature_header_is_a_401_not_a_server_error(event_body: bytes) -> None:
    sqs = FakeSQS()
    client = _client(sqs)

    response = client.post(
        "/webhook",
        content=event_body,
        headers=[(b"x-webhook-signature", b"\xff")],
    )

    assert response.status_code == 401
    assert sqs.published == []


def test_duplicate_signature_headers_are_rejected(event_body: bytes) -> None:
    sqs = FakeSQS()
    client = _client(sqs)
    valid = _signature(event_body).encode("ascii")

    response = client.post(
        "/webhook",
        content=event_body,
        headers=[
            (b"x-webhook-signature", valid),
            (b"x-webhook-signature", b"t=1700000000,v1=" + b"0" * 64),
        ],
    )

    assert response.status_code == 401
    assert sqs.published == []


def test_returns_401_for_stale_timestamp(
    event_body: bytes,
    monkeypatch,
) -> None:
    monkeypatch.setattr("vulcano_telemetry.webhook_crypto.time.time", lambda: FIXED_TIMESTAMP)
    sqs = FakeSQS()
    client = _client(sqs)
    stale_signature = generate_signature(TEST_SECRET, FIXED_TIMESTAMP - 301, event_body)

    response = client.post(
        "/webhook",
        content=event_body,
        headers={"X-Webhook-Signature": stale_signature},
    )

    assert response.status_code == 401
    assert sqs.published == []


def test_returns_400_after_authentication_for_invalid_json() -> None:
    sqs = FakeSQS()
    client = _client(sqs)
    body = b"{ invalid json"

    response = client.post(
        "/webhook",
        content=body,
        headers={
            "X-Webhook-Signature": _signature(body)
        },
    )

    assert response.status_code == 400
    assert sqs.published == []


def test_returns_400_for_non_utf8_body_after_authentication() -> None:
    sqs = FakeSQS()
    client = _client(sqs)
    body = b"\xff"

    response = client.post(
        "/webhook",
        content=body,
        headers={
            "X-Webhook-Signature": _signature(body)
        },
    )

    assert response.status_code == 400
    assert sqs.published == []


def test_returns_400_for_incomplete_envelope() -> None:
    sqs = FakeSQS()
    client = _client(sqs)
    body = b'{"event_id":"evt-1"}'

    response = client.post(
        "/webhook",
        content=body,
        headers={
            "X-Webhook-Signature": _signature(body)
        },
    )

    assert response.status_code == 400
    assert sqs.published == []


def test_returns_503_when_sqs_publish_fails(event_body: bytes) -> None:
    sqs = FakeSQS(fail_publish=True)
    client = _client(sqs)
    response = client.post(
        "/webhook",
        content=event_body,
        headers={
            "X-Webhook-Signature": _signature(event_body)
        },
    )

    assert response.status_code == 503
