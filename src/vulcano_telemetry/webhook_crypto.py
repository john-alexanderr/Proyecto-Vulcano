"""Firma HMAC-SHA256 con marca temporal sobre los bytes originales del webhook."""

from __future__ import annotations

import hashlib
import hmac
import re
import time

from .exceptions import WebhookSignatureError, WebhookTimestampError
from .sanitizer import parse_secret_key

_SIGNATURE_HEADER = re.compile(r"^t=([0-9]{1,12}),v1=([0-9A-Fa-f]{64})$")


def _validated_secret(secret: str) -> bytes:
    try:
        return parse_secret_key(secret).encode("utf-8")
    except ValueError as err:
        error = WebhookSignatureError("Configured webhook secret is invalid")
        error.add_note("VULCANO_WEBHOOK_SECRET must contain at least 16 characters")
        raise error from err


def generate_signature(secret: str, timestamp: int, payload_bytes: bytes) -> str:
    """Devuelve `t=<timestamp>,v1=<hex>` para los bytes exactos del contenido."""
    key = _validated_secret(secret)
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, int)
        or not 0 <= timestamp <= 999_999_999_999
    ):
        raise ValueError("timestamp must be an integer with at most 12 non-negative digits")
    if not isinstance(payload_bytes, bytes):
        raise TypeError("payload_bytes must be bytes")
    timestamp_text = str(timestamp)
    signed_bytes = f"{timestamp_text}.".encode("utf-8") + payload_bytes
    digest = hmac.new(key, signed_bytes, hashlib.sha256).hexdigest()
    return f"t={timestamp_text},v1={digest}"


def verify_signature(
    secret: str,
    header_value: str,
    payload_bytes: bytes,
    tolerance_sec: int = 300,
) -> bool:
    """Verifica una firma ASCII con marca temporal y devuelve verdadero solo si es válida.

    Las firmas mal formadas o que no coinciden generan `WebhookSignatureError`;
    las marcas temporales fuera de la tolerancia inclusiva generan
    `WebhookTimestampError`.
    """
    if not isinstance(header_value, str):
        raise WebhookSignatureError("Webhook signature header is missing")
    try:
        header_value.encode("ascii")
    except UnicodeEncodeError as err:
        error = WebhookSignatureError("Webhook signature header must contain ASCII only")
        error.add_note("Header name: X-Webhook-Signature")
        raise error from err

    match = _SIGNATURE_HEADER.fullmatch(header_value)
    if match is None:
        error = WebhookSignatureError("Webhook signature header is malformed")
        error.add_note("Expected format: t=<timestamp>,v1=<64 hexadecimal characters>")
        raise error
    timestamp_text, supplied_digest = match.groups()
    timestamp = int(timestamp_text)
    if isinstance(tolerance_sec, bool) or not isinstance(tolerance_sec, int) or tolerance_sec < 0:
        raise ValueError("tolerance_sec must be a non-negative integer")
    if abs(time.time() - timestamp) > tolerance_sec:
        error = WebhookTimestampError("Webhook signature timestamp is outside the accepted window")
        error.add_note(f"Allowed timestamp tolerance: {tolerance_sec} seconds")
        raise error

    key = _validated_secret(secret)
    if not isinstance(payload_bytes, bytes):
        raise TypeError("payload_bytes must be bytes")
    expected_digest = hmac.new(
        key,
        f"{timestamp_text}.".encode("utf-8") + payload_bytes,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(
        expected_digest.encode("ascii"),
        supplied_digest.lower().encode("ascii"),
    ):
        error = WebhookSignatureError("Webhook signature does not match the request body")
        error.add_note("The raw request body is used for signature validation")
        raise error
    return True
__all__ = ["generate_signature", "verify_signature"]
