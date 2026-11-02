"""Consumidor asíncrono de SQS con entregas concurrentes acotadas y reentrega gestionada por SQS."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from typing import Any

import requests

from .core_sqs import CoreSQS, QueueMessage
from .event_schema import Event, parse_event
from .exceptions import (
    CorruptedMessageError,
    SQSConnectionError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .sanitizer import parse_secret_key, parse_webhook_url
from .webhook_crypto import generate_signature


class Dispatcher:
    """Entrega cada recibo de SQS una vez y elimina solo las entregas exitosas."""

    def __init__(
        self,
        *,
        sqs: CoreSQS,
        destination_url: str,
        secret: str,
        request_timeout: tuple[float, float] = (3.0, 10.0),
        http_post: Callable[..., Any] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._sqs = sqs
        self._http_post = requests.post if http_post is None else http_post
        self._destination_url = parse_webhook_url(destination_url)
        self._secret = parse_secret_key(secret)
        self._request_timeout = request_timeout
        self._logger = logging.getLogger(__name__) if logger is None else logger

    async def process_message(self, message: QueueMessage) -> Event:
        """Valida y reenvía los bytes sin cambios antes de confirmar el recibo."""
        event = parse_event(message.body)
        body_bytes = message.body.encode("utf-8")
        signature = generate_signature(self._secret, int(time.time()), body_bytes)
        try:
            response = await asyncio.to_thread(
                self._http_post,
                self._destination_url,
                data=body_bytes,
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-Signature": signature,
                    "X-Trace-Id": event.trace_id,
                },
                timeout=self._request_timeout,
                allow_redirects=False,
            )
            if not 200 <= response.status_code < 300:
                raise requests.HTTPError(
                    "Destination did not return a 2xx status",
                    response=response,
                )
            response.raise_for_status()
        except requests.RequestException as err:
            error = WebhookDeliveryError("Configured destination did not accept the event")
            error.add_note(
                f"MessageId: {message.message_id}; WebhookId: {event.event_id}; "
                f"TraceId: {event.trace_id}"
            )
            raise error from err

        await asyncio.to_thread(
            self._sqs.delete_event,
            message.receipt_handle,
            queue_name=message.queue_name,
        )
        self._logger.info(
            "destination_delivery_succeeded",
            extra={
                "event": "destination_delivery_succeeded",
                "TraceId": event.trace_id,
                "MessageId": message.message_id,
                "WebhookId": event.event_id,
                "destination_status": response.status_code,
            },
        )
        return event

    async def process_batch(self, messages: Sequence[QueueMessage]) -> None:
        """Procesa un lote de SQS en paralelo y clasifica los fallos de dominio agrupados."""
        if not messages:
            return
        try:
            async with asyncio.TaskGroup() as task_group:
                for message in messages:
                    task_group.create_task(self._process_message_group(message))
        except* SQSConnectionError:
            raise

    async def _process_message_group(self, message: QueueMessage) -> None:
        try:
            async with asyncio.TaskGroup() as task_group:
                task_group.create_task(self.process_message(message))
        except* WebhookSignatureError as errors:
            self._log_group("queued_signature_rejected", errors, message)
        except* WebhookTimestampError as errors:
            self._log_group("queued_timestamp_rejected", errors, message)
        except* CorruptedMessageError as errors:
            self._log_group("queued_message_rejected", errors, message)
        except* WebhookDeliveryError as errors:
            self._log_group("destination_delivery_failed", errors, message)
        except* SQSConnectionError:
            raise

    def _log_group(
        self,
        event_name: str,
        errors: BaseExceptionGroup,
        message: QueueMessage | None = None,
    ) -> None:
        context: dict[str, object] = {"event": event_name, "error": errors}
        if message is not None:
            context["MessageId"] = message.message_id
        self._logger.error(
            event_name,
            extra=context,
        )

    async def _consume(self) -> None:
        while True:
            await self.process_next()

    async def process_next(self) -> bool:
        """Recibe y despacha un lote de SQS sin bloquear el bucle de eventos."""
        messages = await asyncio.to_thread(self._sqs.receive_events)
        if not messages:
            return False
        await self.process_batch(messages)
        return True

    async def run_forever(self, *, worker_count: int = 2) -> None:
        """Supervisa la cantidad configurada de consumidores independientes de SQS."""
        if isinstance(worker_count, bool) or not isinstance(worker_count, int) or worker_count < 1:
            raise ValueError("worker_count must be a positive integer")
        async with asyncio.TaskGroup() as task_group:
            for _ in range(worker_count):
                task_group.create_task(self._consume())


__all__ = ["Dispatcher"]
