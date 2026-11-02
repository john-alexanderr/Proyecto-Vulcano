from __future__ import annotations

import asyncio

import pytest
import requests

from vulcano_telemetry.core_sqs import QueueMessage
from vulcano_telemetry.dispatcher import Dispatcher
from vulcano_telemetry.exceptions import WebhookDeliveryError
from vulcano_telemetry.webhook_crypto import verify_signature

from .conftest import TEST_SECRET


class FakeSQS:
    def __init__(self, body: str) -> None:
        self.message = QueueMessage("message-1", "receipt-1", body, "vulcano-events-queue")
        self.deleted: list[tuple[str, str | None]] = []

    def receive_events(self) -> list[QueueMessage]:
        return [self.message]

    def delete_event(self, receipt_handle: str, *, queue_name: str | None = None) -> None:
        self.deleted.append((receipt_handle, queue_name))


class FakeResponse:
    def __init__(self, status_code: int, *, fail: bool = False) -> None:
        self.status_code = status_code
        self.fail = fail

    def raise_for_status(self) -> None:
        if self.fail:
            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError("destination unavailable", response=response)


class FakeHTTPClient:
    def __init__(self, *, fail: bool = False, status_code: int | None = None) -> None:
        self.fail = fail
        self.status_code = status_code
        self.arguments: tuple[str, dict[str, str], bytes] | None = None
        self.allow_redirects: bool | None = None

    def post(
        self,
        url: str,
        *,
        data: bytes,
        headers: dict[str, str],
        timeout: tuple[float, float],
        allow_redirects: bool,
    ) -> FakeResponse:
        self.arguments = (url, headers, data)
        self.allow_redirects = allow_redirects
        status_code = (
            self.status_code
            if self.status_code is not None
            else (503 if self.fail else 202)
        )
        return FakeResponse(status_code, fail=self.fail)


def test_deletes_only_after_successful_delivery(event_body: bytes) -> None:
    sqs = FakeSQS(event_body.decode("utf-8"))
    http_client = FakeHTTPClient()
    dispatcher = Dispatcher(
        sqs=sqs,
        destination_url="https://collector.example/webhook",
        secret=TEST_SECRET,
        http_post=http_client.post,
    )

    asyncio.run(dispatcher.process_message(sqs.message))

    assert sqs.deleted == [("receipt-1", "vulcano-events-queue")]
    assert http_client.arguments is not None
    url, headers, body = http_client.arguments
    assert url == "https://collector.example/webhook"
    assert body == event_body
    assert headers["X-Trace-Id"] == "trace-1"
    assert http_client.allow_redirects is False
    assert verify_signature(TEST_SECRET, headers["X-Webhook-Signature"], body) is True


def test_keeps_message_undeleted_when_destination_fails(event_body: bytes) -> None:
    sqs = FakeSQS(event_body.decode("utf-8"))
    dispatcher = Dispatcher(
        sqs=sqs,
        destination_url="https://collector.example/webhook",
        secret=TEST_SECRET,
        http_post=FakeHTTPClient(fail=True).post,
    )

    with pytest.raises(WebhookDeliveryError):
        asyncio.run(dispatcher.process_message(sqs.message))

    assert sqs.deleted == []


def test_does_not_delete_a_redirect_response(event_body: bytes) -> None:
    sqs = FakeSQS(event_body.decode("utf-8"))
    http_client = FakeHTTPClient(status_code=302)
    dispatcher = Dispatcher(
        sqs=sqs,
        destination_url="https://collector.example/webhook",
        secret=TEST_SECRET,
        http_post=http_client.post,
    )

    with pytest.raises(WebhookDeliveryError):
        asyncio.run(dispatcher.process_message(sqs.message))

    assert http_client.allow_redirects is False
    assert sqs.deleted == []
