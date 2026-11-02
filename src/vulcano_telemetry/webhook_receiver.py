"""Receptor FastAPI que autentica los bytes originales de la solicitud antes de encolarla."""

from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, HTTPException, Request, Response, status

from .config import Settings
from .core_sqs import CoreSQS
from .event_schema import parse_event
from .exceptions import (
    ConfigurationError,
    CorruptedMessageError,
    SQSConnectionError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .sanitizer import parse_secret_key
from .webhook_crypto import verify_signature


def create_app(
    *,
    settings: Settings,
    sqs: CoreSQS,
    logger: logging.Logger | None = None,
) -> FastAPI:
    """Crea una aplicación ASGI inyectable sin cargar configuración,
    clientes ni conexiones de red.
    """
    try:
        secret = parse_secret_key(settings.webhook_secret)
    except ValueError as err:
        raise ConfigurationError(
            "A configured 16-character webhook secret is required"
        ) from err
    log = logging.getLogger(__name__) if logger is None else logger
    app = FastAPI(title="Vulcano Telemetry Webhook Receiver")

    @app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
    async def receive_webhook(request: Request) -> Response:
        raw_body = await request.body()
        signature_headers = request.headers.getlist("X-Webhook-Signature")
        signature_header = signature_headers[0] if len(signature_headers) == 1 else None
        header_present = bool(signature_headers)
        timestamp_errors: BaseExceptionGroup | None = None
        signature_errors: BaseExceptionGroup | None = None
        try:
            async with asyncio.TaskGroup() as task_group:
                task_group.create_task(
                    asyncio.to_thread(
                        verify_signature,
                        secret,
                        signature_header,
                        raw_body,
                        settings.tolerance_seconds,
                    )
                )
        except* WebhookTimestampError as errors:
            timestamp_errors = errors
        except* WebhookSignatureError as errors:
            signature_errors = errors

        if timestamp_errors is not None:
            log.warning(
                "webhook_signature_rejected",
                extra={
                    "event": "webhook_signature_rejected",
                    "signature_header_present": header_present,
                    "signature_valid": False,
                },
            )
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED) from timestamp_errors
        if signature_errors is not None:
            log.warning(
                "webhook_signature_rejected",
                extra={
                    "event": "webhook_signature_rejected",
                    "signature_header_present": header_present,
                    "signature_valid": False,
                },
            )
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED) from signature_errors

        log.info(
            "webhook_signature_validated",
            extra={
                "event": "webhook_signature_validated",
                "signature_header_present": True,
                "signature_valid": True,
            },
        )
        try:
            event = parse_event(raw_body)
        except CorruptedMessageError as err:
            log.warning(
                "webhook_event_rejected",
                extra={
                    "event": "webhook_event_rejected",
                    "signature_header_present": True,
                    "signature_valid": True,
                },
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST) from err

        try:
            message_id = await asyncio.to_thread(
                sqs.send_event,
                raw_body.decode("utf-8"),
                event,
            )
        except SQSConnectionError as err:
            log.error(
                "webhook_queue_publish_failed",
                extra={
                    "event": "webhook_queue_publish_failed",
                    "TraceId": event.trace_id,
                    "WebhookId": event.event_id,
                    "signature_header_present": True,
                    "signature_valid": True,
                    "error_type": type(err).__name__,
                },
            )
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from err

        log.info(
            "webhook_event_enqueued",
            extra={
                "event": "webhook_event_enqueued",
                "TraceId": event.trace_id,
                "MessageId": message_id,
                "WebhookId": event.event_id,
                "signature_header_present": True,
                "signature_valid": True,
            },
        )
        return Response(status_code=status.HTTP_202_ACCEPTED)

    return app
