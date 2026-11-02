"""Punto de entrada de la línea de comandos; importarlo no inicia recursos ni accede a la red."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence

from .config import Settings
from .core_sqs import CoreSQS
from .dispatcher import Dispatcher
from .event_schema import parse_event
from .exceptions import (
    SQSConnectionError,
    VulcanoError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .logging_engine import LoggingRuntime, configure_logging
from .webhook_receiver import create_app


def _port_number(value: str) -> int:
    try:
        port = int(value)
    except ValueError as err:
        raise argparse.ArgumentTypeError("port must be an integer from 1 to 65535") from err
    if not 1 <= port <= 65_535:
        raise argparse.ArgumentTypeError("port must be an integer from 1 to 65535")
    return port


def build_parser() -> argparse.ArgumentParser:
    """Crea el analizador sin leer la configuración ni iniciar recursos."""
    parser = argparse.ArgumentParser(
        prog="vulcano-telemetry",
        description="Run the signed webhook-to-SQS event pipeline.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    producer = commands.add_parser(
        "produce-sqs",
        help="publish one validated event from --event-json or standard input",
    )
    producer.add_argument(
        "--event-json",
        help="event JSON text (read standard input when omitted)",
    )

    receiver = commands.add_parser("start-receiver", help="run the signed POST /webhook receiver")
    receiver.add_argument("--host", default="0.0.0.0", help="HTTP bind host (default: 0.0.0.0)")
    receiver.add_argument("--port", type=_port_number, default=8000, help="HTTP port")

    commands.add_parser(
        "start-dispatcher",
        help="deliver queued events to the configured destination",
    )
    return parser


def _read_producer_body(event_json: str | None) -> bytes:
    if event_json is not None:
        return event_json.encode("utf-8")
    return sys.stdin.buffer.read()


async def _supervise_dispatcher(dispatcher: Dispatcher) -> None:
    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(dispatcher.run_forever())


def _run_dispatcher(dispatcher: Dispatcher, logger: logging.Logger) -> int:
    failed = False
    try:
        asyncio.run(_supervise_dispatcher(dispatcher))
    except* WebhookSignatureError as errors:
        logger.error(
            "dispatcher_signature_error_group",
            extra={"event": "dispatcher_signature_error_group", "error": errors},
        )
        failed = True
    except* WebhookTimestampError as errors:
        logger.error(
            "dispatcher_timestamp_error_group",
            extra={"event": "dispatcher_timestamp_error_group", "error": errors},
        )
        failed = True
    except* SQSConnectionError as errors:
        logger.error(
            "dispatcher_sqs_error_group",
            extra={"event": "dispatcher_sqs_error_group", "error": errors},
        )
        failed = True
    except* WebhookDeliveryError as errors:
        logger.error(
            "dispatcher_delivery_error_group",
            extra={"event": "dispatcher_delivery_error_group", "error": errors},
        )
        failed = True
    return 1 if failed else 0


def _run_command(args: argparse.Namespace) -> int:
    needs_secret = args.command in {"start-receiver", "start-dispatcher"}
    needs_destination = args.command == "start-dispatcher"
    settings = Settings.from_env(
        require_secret=needs_secret,
        require_destination=needs_destination,
    )
    logging_runtime: LoggingRuntime | None = None
    sqs: CoreSQS | None = None
    logger = logging.getLogger("vulcano_telemetry")
    try:
        logging_runtime = configure_logging(
            settings.log_path,
            max_bytes=settings.log_rotation_bytes,
            backup_count=settings.log_backup_count,
            logger=logger,
        )
        sqs = CoreSQS(settings)
        sqs.ensure_queues()

        if args.command == "produce-sqs":
            raw_body = _read_producer_body(args.event_json)
            event = parse_event(raw_body)
            body = raw_body.decode("utf-8")
            message_id = sqs.send_event(body, event)
            logger.info(
                "event_published",
                extra={
                    "event": "event_published",
                    "TraceId": event.trace_id,
                    "MessageId": message_id,
                    "WebhookId": event.event_id,
                },
            )
            print(f"Published SQS message {message_id}")
            return 0

        if args.command == "start-receiver":
            app = create_app(settings=settings, sqs=sqs, logger=logger)
            import uvicorn

            uvicorn.run(app, host=args.host, port=args.port, log_config=None)
            return 0

        dispatcher = Dispatcher(
            sqs=sqs,
            destination_url=settings.destination_url,
            secret=settings.webhook_secret or "",
            logger=logger,
        )
        return _run_dispatcher(dispatcher, logger)
    finally:
        try:
            if sqs is not None:
                sqs.close()
        finally:
            if logging_runtime is not None:
                logging_runtime.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Analiza primero para que la ayuda sea segura e inicia solo el entorno seleccionado."""
    args = build_parser().parse_args(argv)
    try:
        return _run_command(args)
    except VulcanoError as err:
        print(f"vulcano-telemetry: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
