from __future__ import annotations

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from vulcano_telemetry.config import Settings
from vulcano_telemetry import core_sqs
from vulcano_telemetry.core_sqs import CoreSQS
from vulcano_telemetry.event_schema import parse_event
from vulcano_telemetry.exceptions import QueuePublishError, QueueReceiveError, QueueSetupError


class FakeSQSClient:
    def __init__(self) -> None:
        self.send_arguments: dict[str, object] | None = None
        self.send_error: Exception | None = None
        self.receive_error: Exception | None = None

    def get_queue_url(self, *, QueueName: str) -> dict[str, str]:
        return {"QueueUrl": f"http://localhost/queue/{QueueName}"}

    def send_message(self, **kwargs: object) -> dict[str, str]:
        self.send_arguments = kwargs
        if self.send_error is not None:
            raise self.send_error
        return {"MessageId": "message-1"}

    def receive_message(self, **kwargs: object) -> dict[str, object]:
        if self.receive_error is not None:
            raise self.receive_error
        return {"Messages": []}


def test_send_uses_unchanged_body_and_required_message_attributes(event_body: bytes) -> None:
    client = FakeSQSClient()
    sqs = CoreSQS(Settings(), client=client)
    event = parse_event(event_body)

    message_id = sqs.send_event(event_body.decode("utf-8"), event)

    assert message_id == "message-1"
    assert client.send_arguments is not None
    assert client.send_arguments["MessageBody"] == event_body.decode("utf-8")
    assert client.send_arguments["MessageAttributes"] == {
        "TraceId": {"DataType": "String", "StringValue": "trace-1"},
        "EventTopic": {"DataType": "String", "StringValue": "telemetry.created"},
        "Timestamp": {"DataType": "String", "StringValue": "2026-01-02T03:04:05Z"},
    }


@pytest.mark.parametrize(
    "error",
    [
        ClientError(
            {"Error": {"Code": "ServiceUnavailable", "Message": "offline"}},
            "SendMessage",
        ),
        BotoCoreError(error_msg="offline"),
    ],
)
def test_maps_boto_publish_errors_with_exception_chaining(
    error: Exception,
    event_body: bytes,
) -> None:
    client = FakeSQSClient()
    client.send_error = error
    sqs = CoreSQS(Settings(), client=client)

    with pytest.raises(QueuePublishError) as caught:
        sqs.send_event(event_body.decode("utf-8"), parse_event(event_body))

    assert caught.value.__cause__ is error
    assert any("SendMessage" in note for note in caught.value.__notes__)


def test_maps_boto_receive_errors_with_exception_chaining() -> None:
    client = FakeSQSClient()
    error = BotoCoreError(error_msg="offline")
    client.receive_error = error
    sqs = CoreSQS(Settings(), client=client)

    with pytest.raises(QueueReceiveError) as caught:
        sqs.receive_message()

    assert caught.value.__cause__ is error


def test_sqs_client_uses_dummy_credentials_only_for_localstack(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_boto_client(service_name: str, **kwargs: object) -> object:
        calls.append((service_name, kwargs))
        return object()

    monkeypatch.setattr(core_sqs.boto3, "client", fake_boto_client)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    core_sqs.get_sqs_client("http://localhost:4566")
    core_sqs.get_sqs_client("https://sqs.us-east-1.amazonaws.com")

    assert calls[0][0] == "sqs"
    assert calls[0][1]["aws_access_key_id"] == "test"
    assert calls[0][1]["aws_secret_access_key"] == "test"
    assert calls[1][0] == "sqs"
    assert "aws_access_key_id" not in calls[1][1]
    assert "aws_secret_access_key" not in calls[1][1]


def test_localstack_credentials_can_be_overridden_by_environment(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_boto_client(service_name: str, **kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(core_sqs.boto3, "client", fake_boto_client)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "local-override-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "local-override-secret")

    core_sqs.get_sqs_client("http://localhost:4566")

    assert captured["aws_access_key_id"] == "local-override-key"
    assert captured["aws_secret_access_key"] == "local-override-secret"


def test_maps_boto_queue_setup_errors_with_exception_chaining() -> None:
    error = ClientError(
        {"Error": {"Code": "ServiceUnavailable", "Message": "offline"}},
        "CreateQueue",
    )

    class SetupFailureClient:
        def create_queue(self, **kwargs: object) -> None:
            raise error

    sqs = CoreSQS(Settings(), client=SetupFailureClient())

    with pytest.raises(QueueSetupError) as caught:
        sqs.ensure_queues()

    assert caught.value.__cause__ is error
