"""Errores tipados generados por el flujo de telemetría de Vulcano."""


class VulcanoError(Exception):
    """Clase base para los errores esperados de la aplicación."""


class ConfigurationError(VulcanoError):
    """Se genera cuando la configuración de ejecución es inválida o incompleta."""


class CorruptedMessageError(VulcanoError):
    """Se genera cuando un mensaje de SQS no contiene un sobre de evento válido."""


EventValidationError = CorruptedMessageError


class WebhookSignatureError(VulcanoError):
    """Se genera cuando falta una firma, está mal formada o no coincide."""


class WebhookTimestampError(VulcanoError):
    """Se genera cuando la marca temporal firmada está vencida o demasiado adelantada."""


class SQSConnectionError(VulcanoError):
    """Se genera cuando falla una solicitud a SQS o su respuesta no se puede usar."""


class QueueTimeoutError(SQSConnectionError):
    """Se genera cuando vence el tiempo de espera de una operación síncrona de SQS."""


class WebhookDeliveryError(VulcanoError):
    """Se genera cuando el destino configurado rechaza un evento o no puede recibirlo."""


SignatureValidationError = WebhookSignatureError
StaleTimestampError = WebhookTimestampError
QueueError = SQSConnectionError
QueueSetupError = SQSConnectionError
QueuePublishError = SQSConnectionError
QueueReceiveError = SQSConnectionError
QueueDeleteError = SQSConnectionError
DeliveryError = WebhookDeliveryError


class LoggingError(VulcanoError):
    """Se genera cuando no se puede inicializar el registro asíncrono."""
