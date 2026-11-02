"""Validación del sobre JSON del evento sin reescribir el cuerpo original."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from .exceptions import CorruptedMessageError

_SAFE_TRACE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass(frozen=True)
class Event:
    """Metadatos validados para los atributos de SQS y la correlación."""

    event_id: str
    event_type: str
    occurred_at: str
    trace_id: str
    payload: Any


def _reject_nonfinite(value: str) -> None:
    raise ValueError(f"Non-finite JSON number is not allowed: {value}")


def _validate_text_field(envelope: dict[str, Any], name: str, max_length: int) -> str:
    value = envelope.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise CorruptedMessageError(f"Event field {name!r} must be a non-empty string")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise CorruptedMessageError(f"Event field {name!r} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as err:
        raise CorruptedMessageError(f"Event field {name!r} is not valid Unicode text") from err
    return value


def parse_event(raw_body: bytes | str) -> Event:
    """Valida JSON UTF-8 y los campos requeridos sin modificar `raw_body`."""
    try:
        text = raw_body.decode("utf-8") if isinstance(raw_body, bytes) else raw_body
        if not isinstance(text, str):
            raise TypeError("Event body must be bytes or text")
        envelope = json.loads(text, parse_constant=_reject_nonfinite)
    except (TypeError, ValueError, RecursionError) as err:
        raise CorruptedMessageError("Request body must be valid UTF-8 JSON") from err

    if not isinstance(envelope, dict):
        raise CorruptedMessageError("Event body must be a JSON object")
    missing = {"event_id", "event_type", "occurred_at", "trace_id", "payload"} - envelope.keys()
    if missing:
        missing_fields = ", ".join(sorted(missing))
        raise CorruptedMessageError(f"Event envelope is missing required fields: {missing_fields}")

    event_id = _validate_text_field(envelope, "event_id", 256)
    event_type = _validate_text_field(envelope, "event_type", 256)
    occurred_at = _validate_text_field(envelope, "occurred_at", 128)
    trace_id = _validate_text_field(envelope, "trace_id", 128)
    if _SAFE_TRACE_ID.fullmatch(trace_id) is None:
        raise CorruptedMessageError("Event field 'trace_id' must use safe ASCII characters")

    return Event(
        event_id=event_id,
        event_type=event_type,
        occurred_at=occurred_at,
        trace_id=trace_id,
        payload=envelope["payload"],
    )
