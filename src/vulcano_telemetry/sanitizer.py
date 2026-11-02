"""Validación de entradas compartida por la configuración y el procesamiento de eventos."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from .exceptions import ConfigurationError

_QUEUE_NAME = re.compile(r"^vulcano-[a-z0-9-]+-queue$")
_DLQ_NAME = re.compile(r"^vulcano-[a-z0-9-]+-dlq$")


def parse_queue_name(value: str) -> str:
    """Valida la convención de nombres de cola estándar de Vulcano definida por la consigna."""
    if not isinstance(value, str) or _QUEUE_NAME.fullmatch(value) is None:
        raise ValueError("queue name must match ^vulcano-[a-z0-9-]+-queue$")
    return value


def parse_dlq_name(value: str) -> str:
    """Valida la convención de nombres documentada para la cola de mensajes fallidos."""
    if not isinstance(value, str) or _DLQ_NAME.fullmatch(value) is None:
        raise ValueError("dead-letter queue name must match ^vulcano-[a-z0-9-]+-dlq$")
    return value


def parse_queue_identifier(value: str) -> str:
    """Valida el nombre configurado de la cola principal o de la cola de mensajes fallidos."""
    try:
        return parse_queue_name(value)
    except ValueError:
        return parse_dlq_name(value)


def parse_wait_time(value: int | str | bool) -> int:
    """Devuelve una duración de consulta larga de SQS entre 1 y 20 segundos."""
    if isinstance(value, bool):
        raise ValueError("WaitTimeSeconds must be an integer from 1 to 20")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as err:
        raise ValueError("WaitTimeSeconds must be an integer from 1 to 20") from err
    if str(value) != str(parsed) or not 1 <= parsed <= 20:
        raise ValueError("WaitTimeSeconds must be an integer from 1 to 20")
    return parsed


def parse_webhook_url(value: str) -> str:
    """Exige una URL HTTP(S) absoluta válida, con host y sin credenciales."""
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("webhook URL must be an absolute HTTP(S) URL")
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError as err:
        raise ValueError("webhook URL must be a valid HTTP(S) URL") from err
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise ValueError("webhook URL must be an absolute HTTP(S) URL without credentials")
    return value


def parse_secret_key(value: str) -> str:
    """Exige una clave de firma de al menos 16 caracteres y no vacía."""
    if not isinstance(value, str) or len(value) < 16 or value.isspace():
        raise ValueError("webhook secret must contain at least 16 characters")
    normalized = value.strip().casefold()
    if (
        normalized in {"your-secret-key-here", "replace-me-with-a-secret"}
        or normalized.startswith(
            ("replace-", "replace_", "change-", "change_", "your-", "your_")
        )
        or "placeholder" in normalized
        or (normalized.startswith("<") and normalized.endswith(">"))
    ):
        raise ValueError("replace the webhook-secret placeholder before starting this command")
    return value


def validate_queue_name(value: str, *, setting_name: str = "queue name") -> str:
    """Validador compatible que genera el error de configuración de la aplicación."""
    try:
        return parse_queue_name(value)
    except ValueError as err:
        raise ConfigurationError(f"Invalid {setting_name}: {err}") from err


def validate_wait_time(value: int | str | bool) -> int:
    """Validador compatible para los valores de consulta larga del proyecto."""
    try:
        return parse_wait_time(value)
    except ValueError as err:
        raise ConfigurationError(str(err)) from err


def validate_http_url(value: str, *, setting_name: str = "URL") -> str:
    """Validador compatible para los destinos HTTP configurados."""
    try:
        return parse_webhook_url(value)
    except ValueError as err:
        raise ConfigurationError(f"Invalid {setting_name}: {err}") from err


def validate_secret(value: str | None) -> str:
    """Validador compatible para las claves HMAC configuradas."""
    try:
        if value is None:
            raise ValueError("webhook secret must contain at least 16 characters")
        return parse_secret_key(value)
    except ValueError as err:
        raise ConfigurationError(str(err)) from err


__all__ = [
    "parse_dlq_name",
    "parse_queue_identifier",
    "parse_queue_name",
    "parse_secret_key",
    "parse_wait_time",
    "parse_webhook_url",
    "validate_http_url",
    "validate_queue_name",
    "validate_secret",
    "validate_wait_time",
]
