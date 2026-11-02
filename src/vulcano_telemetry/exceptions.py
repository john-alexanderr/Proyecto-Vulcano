"""Typed errors raised by the Vulcano telemetry pipeline."""


class VulcanoError(Exception):
    """Base class for expected application errors."""


class ConfigurationError(VulcanoError):
    """Raised when runtime configuration is invalid or incomplete."""


class CorruptedMessageError(VulcanoError):
    """Raised when an SQS message does not contain a valid event envelope."""


EventValidationError = CorruptedMessageError


class WebhookSignatureError(VulcanoError):
    """Raised when a signature is missing, malformed, or does not match."""


class WebhookTimestampError(VulcanoError):
    """Raised when a signed timestamp is stale or too far in the future."""


class SQSConnectionError(VulcanoError):
    """Raised when an SQS request fails or its response is unusable."""


class QueueTimeoutError(SQSConnectionError):
    """Raised when a synchronous SQS operation times out."""


class WebhookDeliveryError(VulcanoError):
    """Raised when a configured destination rejects or cannot receive an event."""


SignatureValidationError = WebhookSignatureError
StaleTimestampError = WebhookTimestampError
QueueError = SQSConnectionError
QueueSetupError = SQSConnectionError
QueuePublishError = SQSConnectionError
QueueReceiveError = SQSConnectionError
QueueDeleteError = SQSConnectionError
DeliveryError = WebhookDeliveryError


class LoggingError(VulcanoError):
    """Raised when asynchronous logging cannot be initialized."""
