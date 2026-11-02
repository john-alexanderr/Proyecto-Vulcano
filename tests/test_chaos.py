from __future__ import annotations

import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from vulcano_telemetry.config import Settings
from vulcano_telemetry.dispatcher import Dispatcher
from vulcano_telemetry.webhook_crypto import generate_signature
from vulcano_telemetry.webhook_receiver import create_app

from .conftest import TEST_SECRET

WRONG_SECRET = "different-offline-secret"


class RecordingSQS:
    def __init__(self) -> None:
        self.published: list[str] = []

    def send_event(self, body: str, event: object) -> str:
        self.published.append(body)
        return "message-chaos"


def _receiver_client(sqs: RecordingSQS) -> tuple[TestClient, bytes]:
    body = (
        b'{"event_id":"chaos-1","event_type":"chaos.test",'
        b'"occurred_at":"2026-01-02T03:04:05Z","trace_id":"trace-chaos",'
        b'"payload":{"value":"original"}}'
    )
    app = create_app(
        settings=Settings(webhook_secret=TEST_SECRET),
        sqs=sqs,  # type: ignore[arg-type]
    )
    return TestClient(app), body


def test_altered_signed_body_is_rejected_without_enqueue() -> None:
    sqs = RecordingSQS()
    client, signed_body = _receiver_client(sqs)
    altered_body = signed_body.replace(b"original", b"altered")
    signature = generate_signature(TEST_SECRET, int(time.time()), signed_body)

    response = client.post(
        "/webhook",
        content=altered_body,
        headers={"X-Webhook-Signature": signature},
    )

    assert response.status_code == 401
    assert sqs.published == []


def test_wrong_signing_key_is_rejected_without_enqueue() -> None:
    sqs = RecordingSQS()
    client, body = _receiver_client(sqs)
    signature = generate_signature(WRONG_SECRET, int(time.time()), body)

    response = client.post(
        "/webhook",
        content=body,
        headers={"X-Webhook-Signature": signature},
    )

    assert response.status_code == 401
    assert sqs.published == []


def test_one_hour_stale_signature_is_rejected_without_enqueue() -> None:
    sqs = RecordingSQS()
    client, body = _receiver_client(sqs)
    signature = generate_signature(TEST_SECRET, int(time.time()) - 3_600, body)

    response = client.post(
        "/webhook",
        content=body,
        headers={"X-Webhook-Signature": signature},
    )

    assert response.status_code == 401
    assert sqs.published == []


@pytest.mark.localstack
def test_poison_sqs_message_is_left_for_localstack_dlq_redrive(localstack_sqs_factory) -> None:
    sqs, settings = localstack_sqs_factory(visibility_timeout=0, max_receive_count=1)
    sqs._client.send_message(
        QueueUrl=sqs.get_queue_url(settings.queue_name),
        MessageBody='{"event_id":',
    )
    dispatcher = Dispatcher(
        sqs=sqs,
        destination_url=settings.destination_url,
        secret=TEST_SECRET,
    )

    first_receipt = sqs.receive_events(
        wait_time_seconds=1,
        max_messages=1,
    )
    assert len(first_receipt) == 1
    asyncio.run(dispatcher.process_batch(first_receipt))

    deadline = time.monotonic() + 30
    dead_letter = []
    while not dead_letter and time.monotonic() < deadline:
        dead_letter = sqs.receive_events(
            queue_name=settings.dlq_name,
            wait_time_seconds=1,
            max_messages=1,
        )
        if not dead_letter:
            poison_receipt = sqs.receive_events(
                wait_time_seconds=1,
                max_messages=1,
            )
            if poison_receipt:
                asyncio.run(dispatcher.process_batch(poison_receipt))
    assert len(dead_letter) == 1
    assert dead_letter[0].body == '{"event_id":'
