from __future__ import annotations

import time

import pytest

from vulcano_telemetry.event_schema import parse_event

pytestmark = pytest.mark.localstack


def test_localstack_queue_setup_publish_receive_delete(localstack_sqs_factory) -> None:
    sqs, settings = localstack_sqs_factory()
    body = (
        '{ "event_id":"localstack-1", "event_type":"integration.test", '
        '"occurred_at":"2026-01-02T03:04:05Z", "trace_id":"trace-localstack", '
        '"payload":{"ok":true} }'
    )

    message_id = sqs.send_event(body, parse_event(body))
    received = sqs.receive_message(wait_time_seconds=5)

    assert message_id
    assert received is not None
    assert received.body == body
    assert received.queue_name == settings.queue_name
    sqs.delete_message(received.receipt_handle)

    queue_attributes = sqs._client.get_queue_attributes(
        QueueUrl=sqs.get_queue_url(settings.queue_name),
        AttributeNames=["RedrivePolicy", "VisibilityTimeout"],
    )["Attributes"]
    redrive_policy = queue_attributes["RedrivePolicy"]
    assert settings.dlq_name in redrive_policy
    assert queue_attributes["VisibilityTimeout"] == str(settings.visibility_timeout_seconds)


def test_localstack_redrives_undeleted_receipt_to_dlq(localstack_sqs_factory) -> None:
    sqs, settings = localstack_sqs_factory(visibility_timeout=0, max_receive_count=1)
    body = (
        '{"event_id":"redrive-1","event_type":"integration.redrive",'
        '"occurred_at":"2026-01-02T03:04:05Z","trace_id":"trace-redrive",'
        '"payload":{}}'
    )

    sqs.send_event(body, parse_event(body))
    first_receipt = sqs.receive_message(wait_time_seconds=1)
    assert first_receipt is not None
    assert first_receipt.body == body
    # Deliberately do not delete: SQS visibility and RedrivePolicy own retry behavior.

    deadline = time.monotonic() + 30
    dead_letter = None
    while dead_letter is None and time.monotonic() < deadline:
        dead_letter = sqs.receive_message(
            queue_name=settings.dlq_name,
            wait_time_seconds=1,
        )
        if dead_letter is None:
            # Additional undeleted receipts trigger the configured receive threshold.
            sqs.receive_message(wait_time_seconds=1)
    assert dead_letter is not None
    assert dead_letter.body == body
