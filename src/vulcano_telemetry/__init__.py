"""API pública del flujo firmado de eventos webhook de Vulcano hacia SQS."""

from .core_sqs import (
    CoreSQS,
    QueueMessage,
    delete_event,
    get_sqs_client,
    receive_events,
    send_event,
)
from .config import Settings
from .dispatcher import Dispatcher
from .event_schema import Event, parse_event
from .exceptions import (
    ConfigurationError,
    CorruptedMessageError,
    QueueTimeoutError,
    SQSConnectionError,
    VulcanoError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .sanitizer import (
    parse_dlq_name,
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)
from .webhook_crypto import generate_signature, verify_signature
from .webhook_receiver import create_app

__all__ = [
    "CoreSQS",
    "ConfigurationError",
    "CorruptedMessageError",
    "Dispatcher",
    "Event",
    "QueueMessage",
    "QueueTimeoutError",
    "SQSConnectionError",
    "Settings",
    "VulcanoError",
    "WebhookDeliveryError",
    "WebhookSignatureError",
    "WebhookTimestampError",
    "delete_event",
    "create_app",
    "generate_signature",
    "get_sqs_client",
    "parse_event",
    "parse_dlq_name",
    "parse_queue_name",
    "parse_secret_key",
    "parse_wait_time",
    "parse_webhook_url",
    "receive_events",
    "send_event",
    "verify_signature",
]
