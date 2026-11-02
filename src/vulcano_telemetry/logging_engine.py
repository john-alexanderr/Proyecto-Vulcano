"""Nonblocking JSON logging with recursive safe exception and gzip serialization."""

from __future__ import annotations

import copy
import gzip
import json
import logging
import logging.handlers
import os
import queue
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import LOG_BACKUP_COUNT, LOG_ROTATION_BYTES
from .exceptions import LoggingError

_EXTRA_FIELDS = {
    "TraceId": ("TraceId", "trace_id"),
    "MessageId": ("MessageId", "message_id"),
    "WebhookId": ("WebhookId", "webhook_id", "event_id"),
    "signature_header_present": ("signature_header_present",),
    "signature_valid": ("signature_valid",),
    "destination_status": ("destination_status",),
    "queue_name": ("queue_name",),
    "error_type": ("error_type",),
    "error": ("error",),
}


def _serialize_exception(
    error: BaseException,
    seen: frozenset[int] = frozenset(),
) -> dict[str, Any]:
    """Serialize exception relationships without exception messages or tracebacks."""
    identity = id(error)
    if identity in seen:
        return {"type": type(error).__name__, "cycle": True}
    path = seen | {identity}
    details: dict[str, Any] = {"type": type(error).__name__}
    notes = getattr(error, "__notes__", None)
    if notes:
        details["notes"] = [note for note in notes if isinstance(note, str)]
    if isinstance(error, BaseExceptionGroup):
        details["exceptions"] = [_serialize_exception(child, path) for child in error.exceptions]
    cause = error.__cause__
    context = error.__context__
    if cause is not None:
        details["cause"] = _serialize_exception(cause, path)
    elif context is not None and not error.__suppress_context__:
        details["context"] = _serialize_exception(context, path)
    return details


class AsyncJSONFormatter(logging.Formatter):
    """Format UTC JSON with correlation fields and safe exception structures."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc)
        document: dict[str, Any] = {
            "timestamp": timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "event": getattr(record, "event", record.name),
            "message": record.getMessage(),
            "TraceId": None,
            "MessageId": None,
            "WebhookId": None,
            "process": record.process,
            "threadName": record.threadName,
        }
        for output_name, candidates in _EXTRA_FIELDS.items():
            for candidate in candidates:
                value = getattr(record, candidate, None)
                if value is None:
                    continue
                if output_name == "error" and isinstance(value, BaseException):
                    document[output_name] = _serialize_exception(value)
                elif isinstance(value, (str, int, float, bool)):
                    document[output_name] = value
                break
        if record.exc_info and isinstance(record.exc_info[1], BaseException):
            document["exception"] = _serialize_exception(record.exc_info[1])
        return json.dumps(document, ensure_ascii=False, separators=(",", ":"))


class AsyncQueueHandler(logging.handlers.QueueHandler):
    """Enqueue a safe record copy without formatting exceptions on caller threads."""

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        prepared = copy.copy(record)
        prepared.message = record.getMessage()
        prepared.msg = prepared.message
        prepared.args = None
        prepared.exc_text = None
        return prepared


class GzipRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Rotate at the assignment's 2 MiB limit and atomically publish gzip archives."""

    def __init__(
        self,
        filename: str,
        *,
        max_bytes: int = LOG_ROTATION_BYTES,
        backup_count: int = LOG_BACKUP_COUNT,
    ) -> None:
        super().__init__(
            filename,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
            delay=False,
        )
        self.namer = self._archive_name
        self.rotator = self._compress_archive

    @staticmethod
    def _archive_name(filename: str) -> str:
        return f"{filename}.gz"

    @staticmethod
    def _compress_archive(source: str, destination: str) -> None:
        destination_path = Path(destination)
        descriptor, temporary_path = tempfile.mkstemp(
            prefix=f".{destination_path.name}.",
            suffix=".tmp",
            dir=destination_path.parent,
        )
        try:
            with os.fdopen(descriptor, "wb") as archive_file:
                with open(source, "rb") as source_file:
                    with gzip.GzipFile(fileobj=archive_file, mode="wb", mtime=0) as compressed:
                        shutil.copyfileobj(source_file, compressed)
            os.replace(temporary_path, destination)
        finally:
            if os.path.exists(temporary_path):
                os.unlink(temporary_path)
        os.unlink(source)


class LoggingRuntime:
    """Own a QueueHandler/QueueListener pair and stop it deterministically."""

    def __init__(
        self,
        logger: logging.Logger,
        queue_handler: AsyncQueueHandler,
        listener: logging.handlers.QueueListener,
        file_handler: GzipRotatingFileHandler,
        previous_level: int,
    ) -> None:
        self._logger = logger
        self._queue_handler = queue_handler
        self._listener = listener
        self._file_handler = file_handler
        self._previous_level = previous_level
        self._closed = False

    def close(self) -> None:
        """Drain queued records, stop the listener, and close the rotated file."""
        if self._closed:
            return
        self._logger.removeHandler(self._queue_handler)
        try:
            self._listener.stop()
        finally:
            try:
                self._file_handler.close()
            finally:
                self._logger.setLevel(self._previous_level)
                self._closed = True


def configure_logging(
    log_path: str,
    *,
    max_bytes: int = LOG_ROTATION_BYTES,
    backup_count: int = LOG_BACKUP_COUNT,
    logger: logging.Logger | None = None,
) -> LoggingRuntime:
    """Start queue-backed JSON logging; file I/O runs only on QueueListener."""
    active_logger = logging.getLogger() if logger is None else logger
    path = Path(log_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = GzipRotatingFileHandler(
            str(path),
            max_bytes=max_bytes,
            backup_count=backup_count,
        )
    except OSError as err:
        raise LoggingError("Could not initialize the JSON log file") from err

    file_handler.setFormatter(AsyncJSONFormatter())
    record_queue: queue.Queue[logging.LogRecord] = queue.Queue()
    queue_handler = AsyncQueueHandler(record_queue)
    listener = logging.handlers.QueueListener(
        record_queue,
        file_handler,
        respect_handler_level=True,
    )
    previous_level = active_logger.level
    active_logger.setLevel(logging.INFO)
    active_logger.addHandler(queue_handler)
    try:
        listener.start()
    except RuntimeError as err:
        active_logger.removeHandler(queue_handler)
        file_handler.close()
        active_logger.setLevel(previous_level)
        raise LoggingError("Could not start the JSON log listener") from err
    return LoggingRuntime(
        active_logger,
        queue_handler,
        listener,
        file_handler,
        previous_level,
    )


__all__ = [
    "AsyncJSONFormatter",
    "AsyncQueueHandler",
    "GzipRotatingFileHandler",
    "LoggingRuntime",
    "configure_logging",
]
