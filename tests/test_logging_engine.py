from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path

from vulcano_telemetry.logging_engine import configure_logging


def test_queued_json_logging_schema_and_gzip_rotation(tmp_path: Path) -> None:
    log_path = tmp_path / "runtime.jsonl"
    logger = logging.getLogger(f"vulcano-test-{tmp_path.name}")
    runtime = configure_logging(
        str(log_path),
        max_bytes=1,
        backup_count=2,
        logger=logger,
    )
    try:
        logger.info(
            "signature checked",
            extra={
                "event": "signature_checked",
                "TraceId": "trace-1",
                "signature_header_present": True,
                "signature_valid": True,
                "signature_value": "must-not-be-logged",
                "secret": "must-not-be-logged",
                "request_body": "must-not-be-logged",
            },
        )
        logger.info("second record", extra={"event": "test_record"})
    finally:
        runtime.close()

    archive_path = tmp_path / "runtime.jsonl.1.gz"
    assert archive_path.exists()
    assert not list(tmp_path.glob("*.tmp"))
    with gzip.open(archive_path, "rt", encoding="utf-8") as archive:
        archived_record = json.loads(archive.readline())
    with log_path.open(encoding="utf-8") as active_log:
        active_record = json.loads(active_log.readline())

    assert archived_record["event"] == "signature_checked"
    assert archived_record["TraceId"] == "trace-1"
    assert archived_record["signature_header_present"] is True
    assert archived_record["signature_valid"] is True
    assert "signature_value" not in archived_record
    assert "secret" not in archived_record
    assert "request_body" not in archived_record
    assert active_record["event"] == "test_record"
    assert {"timestamp", "level", "event", "message"} <= active_record.keys()
    assert {"process", "threadName"} <= active_record.keys()
    assert active_record["timestamp"].endswith("Z")
