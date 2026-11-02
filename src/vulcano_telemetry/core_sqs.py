"""Synchronous Boto3 SQS client, producer/consumer functions, and DLQ setup."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.exceptions import BotoCoreError, ClientError, ConnectTimeoutError, ReadTimeoutError

from .config import Settings
from .event_schema import Event
from .exceptions import (
    CorruptedMessageError,
    QueueTimeoutError,
    SQSConnectionError,
)
from .sanitizer import (
    parse_dlq_name,
    parse_queue_identifier,
    parse_queue_name,
    parse_wait_time,
)

DEFAULT_LOCALSTACK_ENDPOINT = "http://localhost:4566"
_LOCALSTACK_HOSTS = {"localhost", "localstack", "127.0.0.1", "::1"}


@dataclass(frozen=True)
class QueueMessage:
    """The SQS fields needed to validate, deliver, and acknowledge a message."""

    message_id: str
    receipt_handle: str
    body: str
    queue_name: str


def get_sqs_client(endpoint_url: str | None = DEFAULT_LOCALSTACK_ENDPOINT) -> Any:
    """Create a synchronous SQS client, using dummy credentials only on LocalStack."""
    parameters: dict[str, object] = {
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if endpoint_url is not None:
        parameters["endpoint_url"] = endpoint_url
        try:
            endpoint_host = urlsplit(endpoint_url).hostname
        except ValueError as err:
            raise ValueError("SQS endpoint URL is invalid") from err
        if endpoint_host in _LOCALSTACK_HOSTS:
            parameters["aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID") or "test"
            parameters["aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY") or "test"
    try:
        return boto3.client("sqs", **parameters)
    except (ClientError, BotoCoreError) as err:
        error = SQSConnectionError("Could not initialize the SQS client")
        error.add_note("SQS operation: create client")
        raise error from err


def _raise_sqs_error(operation: str, queue_name: str, err: Exception) -> None:
    if isinstance(err, (ReadTimeoutError, ConnectTimeoutError)):
        error: SQSConnectionError = QueueTimeoutError(f"SQS {operation} timed out")
    else:
        error = SQSConnectionError(f"SQS {operation} failed")
    error.add_note(f"SQS operation: {operation}; queue: {queue_name}")
    if isinstance(err, ClientError):
        response_error = err.response.get("Error", {})
        code = response_error.get("Code")
        if code:
            error.add_note(f"AWS error code: {code}")
    raise error from err


def send_event(
    client: Any,
    queue_url: str,
    body: str,
    event: Event,
    *,
    queue_name: str = "configured-queue",
) -> str:
    """Send the unchanged event string and assignment-required message attributes."""
    try:
        response = client.send_message(
            QueueUrl=queue_url,
            MessageBody=body,
            MessageAttributes={
                "TraceId": {"DataType": "String", "StringValue": event.trace_id},
                "EventTopic": {"DataType": "String", "StringValue": event.event_type},
                "Timestamp": {"DataType": "String", "StringValue": event.occurred_at},
            },
        )
        return str(response["MessageId"])
    except (ClientError, BotoCoreError) as err:
        _raise_sqs_error("SendMessage", queue_name, err)
    except (KeyError, TypeError) as err:
        _raise_sqs_error("SendMessage response validation", queue_name, err)


def receive_events(
    client: Any,
    queue_url: str,
    *,
    wait_time_seconds: int = 10,
    max_messages: int = 1,
    queue_name: str = "configured-queue",
) -> list[QueueMessage]:
    """Receive one SQS message by default; empty long polls return an empty list."""
    wait_time = parse_wait_time(wait_time_seconds)
    if (
        isinstance(max_messages, bool)
        or not isinstance(max_messages, int)
        or not 1 <= max_messages <= 10
    ):
        raise ValueError("max_messages must be an integer from 1 to 10")
    try:
        response = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"],
        )
    except (ClientError, BotoCoreError) as err:
        _raise_sqs_error("ReceiveMessage", queue_name, err)

    if not isinstance(response, dict):
        error = SQSConnectionError("SQS ReceiveMessage returned an invalid response")
        error.add_note(f"SQS operation: ReceiveMessage; queue: {queue_name}")
        raise error
    raw_messages = response.get("Messages", [])
    if not isinstance(raw_messages, list):
        raise CorruptedMessageError("SQS ReceiveMessage returned an invalid message list")
    messages: list[QueueMessage] = []
    for raw_message in raw_messages:
        try:
            messages.append(
                QueueMessage(
                    message_id=str(raw_message["MessageId"]),
                    receipt_handle=str(raw_message["ReceiptHandle"]),
                    body=str(raw_message["Body"]),
                    queue_name=queue_name,
                )
            )
        except (KeyError, TypeError) as err:
            error = CorruptedMessageError("SQS returned a message without required fields")
            error.add_note(f"SQS queue: {queue_name}")
            raise error from err
    return messages


def delete_event(
    client: Any,
    queue_url: str,
    receipt_handle: str,
    *,
    queue_name: str = "configured-queue",
) -> None:
    """Delete a receipt after successful delivery; failures are left for redrive."""
    try:
        client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except (ClientError, BotoCoreError) as err:
        _raise_sqs_error("DeleteMessage", queue_name, err)


class CoreSQS:
    """Thin queue adapter; boto3 calls remain synchronous and are offloaded by async roles."""

    def __init__(self, settings: Settings, *, client: Any | None = None) -> None:
        self.settings = settings
        parse_queue_name(settings.queue_name)
        parse_dlq_name(settings.dlq_name)
        parse_wait_time(settings.receiver_wait_time_seconds)
        self._client = get_sqs_client(settings.sqs_endpoint_url) if client is None else client
        self._queue_urls: dict[str, str] = {}

    def ensure_queues(self) -> None:
        """Create standard queues and configure main-queue redrive to the DLQ."""
        try:
            dead_letter_url = self._client.create_queue(
                QueueName=self.settings.dlq_name
            )["QueueUrl"]
            main_url = self._client.create_queue(
                QueueName=self.settings.queue_name
            )["QueueUrl"]
            dead_letter_arn = self._client.get_queue_attributes(
                QueueUrl=dead_letter_url,
                AttributeNames=["QueueArn"],
            )["Attributes"]["QueueArn"]
            redrive_policy = json.dumps(
                {
                    "deadLetterTargetArn": dead_letter_arn,
                    "maxReceiveCount": str(self.settings.max_receive_count),
                },
                separators=(",", ":"),
            )
            self._client.set_queue_attributes(
                QueueUrl=main_url,
                Attributes={
                    "VisibilityTimeout": str(self.settings.visibility_timeout_seconds),
                    "RedrivePolicy": redrive_policy,
                },
            )
        except (ClientError, BotoCoreError) as err:
            _raise_sqs_error("CreateQueue/SetQueueAttributes", self.settings.queue_name, err)
        except (KeyError, TypeError) as err:
            _raise_sqs_error("Queue setup response validation", self.settings.queue_name, err)
        self._queue_urls[self.settings.queue_name] = main_url
        self._queue_urls[self.settings.dlq_name] = dead_letter_url

    def get_queue_url(self, queue_name: str) -> str:
        """Resolve a configured Vulcano main or dead-letter queue URL."""
        name = parse_queue_identifier(queue_name)
        if name not in self._queue_urls:
            try:
                response = self._client.get_queue_url(QueueName=name)
                self._queue_urls[name] = response["QueueUrl"]
            except (ClientError, BotoCoreError) as err:
                _raise_sqs_error("GetQueueUrl", name, err)
            except (KeyError, TypeError) as err:
                _raise_sqs_error("GetQueueUrl response validation", name, err)
        return self._queue_urls[name]

    def send_event(self, body: str, event: Event) -> str:
        return send_event(
            self._client,
            self.get_queue_url(self.settings.queue_name),
            body,
            event,
            queue_name=self.settings.queue_name,
        )

    def receive_events(
        self,
        *,
        queue_name: str | None = None,
        wait_time_seconds: int | None = None,
        max_messages: int = 1,
    ) -> list[QueueMessage]:
        name = (
            self.settings.queue_name
            if queue_name is None
            else parse_queue_identifier(queue_name)
        )
        wait_time = (
            self.settings.receiver_wait_time_seconds
            if wait_time_seconds is None
            else parse_wait_time(wait_time_seconds)
        )
        return receive_events(
            self._client,
            self.get_queue_url(name),
            wait_time_seconds=wait_time,
            max_messages=max_messages,
            queue_name=name,
        )

    def receive_message(
        self,
        *,
        queue_name: str | None = None,
        wait_time_seconds: int | None = None,
    ) -> QueueMessage | None:
        """Compatibility single-message consumer for existing callers."""
        messages = self.receive_events(
            queue_name=queue_name,
            wait_time_seconds=wait_time_seconds,
            max_messages=1,
        )
        return messages[0] if messages else None

    def delete_event(self, receipt_handle: str, *, queue_name: str | None = None) -> None:
        name = (
            self.settings.queue_name
            if queue_name is None
            else parse_queue_identifier(queue_name)
        )
        delete_event(
            self._client,
            self.get_queue_url(name),
            receipt_handle,
            queue_name=name,
        )

    def delete_message(self, receipt_handle: str, *, queue_name: str | None = None) -> None:
        """Compatibility name for explicit receipt deletion."""
        self.delete_event(receipt_handle, queue_name=queue_name)

    def delete_queue(self, queue_name: str) -> None:
        name = parse_queue_identifier(queue_name)
        try:
            self._client.delete_queue(QueueUrl=self.get_queue_url(name))
        except (ClientError, BotoCoreError) as err:
            _raise_sqs_error("DeleteQueue", name, err)

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()


__all__ = [
    "CoreSQS",
    "QueueMessage",
    "delete_event",
    "get_sqs_client",
    "receive_events",
    "send_event",
]
