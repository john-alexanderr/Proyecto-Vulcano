from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
from pathlib import Path

from vulcano_telemetry.exceptions import WebhookDeliveryError, WebhookSignatureError
from vulcano_telemetry.logging_engine import GzipRotatingFileHandler, configure_logging


def test_forensic_json_has_utc_context_and_safe_nested_exception_data(tmp_path: Path) -> None:
    log_path = tmp_path / "forensic.jsonl"
    logger = logging.getLogger(f"forensic-{tmp_path.name}")
    runtime = configure_logging(
        str(log_path),
        max_bytes=1,
        backup_count=3,
        logger=logger,
    )

    signature_error = WebhookSignatureError("usable-signature-must-not-appear")
    signature_error.add_note("header-presence=recorded")
    root_group = ExceptionGroup(
        "raw-request-body-must-not-appear",
        [signature_error, ValueError("private-secret-must-not-appear")],
    )
    root_group.add_note("SQS operation: ReceiveMessage; queue: vulcano-events-queue")
    delivery_error = WebhookDeliveryError("destination failure")
    delivery_error.add_note("TraceId: trace-forensic")
    delivery_error.__cause__ = root_group

    try:
        logger.error(
            "delivery receipt failed",
            extra={
                "event": "delivery_receipt_failed",
                "TraceId": "trace-forensic",
                "MessageId": "message-forensic",
                "WebhookId": "webhook-forensic",
                "signature_header_present": True,
                "signature_valid": False,
                "error": delivery_error,
                "signature_value": "usable-signature-must-not-appear",
                "secret": "private-secret-must-not-appear",
                "request_body": "raw-request-body-must-not-appear",
            },
        )
        logger.info("force gzip rotation", extra={"event": "rotation_triggered"})
    finally:
        runtime.close()

    archive_path = tmp_path / "forensic.jsonl.1.gz"
    with gzip.open(archive_path, "rt", encoding="utf-8") as archive:
        serialized = archive.readline()
    record = json.loads(serialized)

    timestamp = dt.datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
    assert timestamp.tzinfo == dt.timezone.utc
    assert record["TraceId"] == "trace-forensic"
    assert record["MessageId"] == "message-forensic"
    assert record["WebhookId"] == "webhook-forensic"
    assert record["signature_header_present"] is True
    assert record["signature_valid"] is False
    assert record["process"] > 0
    assert record["threadName"]
    cause = record["error"]["cause"]
    assert cause["type"] == "ExceptionGroup"
    assert cause["notes"] == [
        "SQS operation: ReceiveMessage; queue: vulcano-events-queue"
    ]
    assert cause["exceptions"][0]["type"] == "WebhookSignatureError"
    assert cause["exceptions"][0]["notes"] == ["header-presence=recorded"]
    assert "usable-signature-must-not-appear" not in serialized
    assert "private-secret-must-not-appear" not in serialized
    assert "raw-request-body-must-not-appear" not in serialized


def test_logging_rotation_defaults_match_rubric(tmp_path: Path) -> None:
    handler = GzipRotatingFileHandler(str(tmp_path / "defaults.jsonl"))
    try:
        assert handler.maxBytes == 2 * 1024 * 1024
        assert handler.backupCount == 3
    finally:
        handler.close()
