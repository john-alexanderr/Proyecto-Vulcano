from __future__ import annotations

from vulcano_telemetry.exceptions import (
    ConfigurationError,
    CorruptedMessageError,
    DeliveryError,
    EventValidationError,
    LoggingError,
    QueueDeleteError,
    QueuePublishError,
    QueueReceiveError,
    QueueSetupError,
    QueueTimeoutError,
    SQSConnectionError,
    SignatureValidationError,
    StaleTimestampError,
    VulcanoError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)


def test_domain_errors_all_inherit_from_exception() -> None:
    domain_errors = (
        ConfigurationError,
        DeliveryError,
        EventValidationError,
        CorruptedMessageError,
        LoggingError,
        QueueDeleteError,
        QueuePublishError,
        QueueReceiveError,
        QueueSetupError,
        QueueTimeoutError,
        SQSConnectionError,
        SignatureValidationError,
        StaleTimestampError,
        WebhookDeliveryError,
        WebhookSignatureError,
        WebhookTimestampError,
    )

    assert issubclass(VulcanoError, Exception)
    assert all(issubclass(error, Exception) for error in domain_errors)
