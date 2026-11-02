# TP-2: Vulcano Telemetry Event Pipeline Requirements

This document records the practical assignment contract for the local webhook-to-SQS pipeline. It deliberately excludes unrelated cloud-provider scans and application-level retry or idempotency systems.

> **Scope status:** The implementation follows the current university rubric. Project metadata targets Python 3.14; no Python 3.14 verification result is claimed.

## 1. Quick path and flow

The system accepts a signed event, preserves its request bytes through SQS, and delivers those same bytes to a configured destination.

```text
Producer --POST /webhook--> Receiver --SendMessage--> vulcano-events-queue
                                |                         |
                     HMAC + timestamp check        Worker receives 1
                                                          |
                                               signed unchanged body
                                                          v
                                            configured destination

Delivery failure: do not delete message -> visibility expires -> SQS redelivers
                  -> RedrivePolicy moves repeated failures to vulcano-events-dlq
```

SQS Standard redelivery and its DLQ are the retry mechanism. Standard queues are at-least-once, so duplicate deliveries are possible; the pipeline makes no exactly-once claim and does not add an application retry loop or idempotency store. A future destination-side deduplication window keyed by `event_id` could mitigate duplicates, but it is not implemented and would change destination behavior.

## 2. Event and signature contract

### Event envelope

The request body is a JSON event envelope containing at least:

| Field | Purpose |
|---|---|
| `event_id` | Event identity for correlation. It is not an exactly-once or deduplication guarantee. |
| `event_type` | Event kind. |
| `occurred_at` | Event occurrence timestamp. |
| `trace_id` | Correlation across receiver, queue, worker, and destination. |
| `payload` | Event-specific data. |

The receiver may parse the envelope for validation and logging, but it must not reserialize it before enqueueing. The worker validates the queued JSON and forwards the unchanged UTF-8 body.

### HMAC-SHA256

- Header: `X-Webhook-Signature: t=<timestamp>,v1=<hex>`.
- Compute the signature over exactly these bytes:
  ```python
  f"{timestamp}.".encode() + raw_body
  ```
- Validate the timestamp against the configured tolerance and compare the supplied digest with the expected HMAC-SHA256 digest.
- Validate the supplied digest as ASCII hexadecimal before comparing it with `hmac.compare_digest()`.
- The worker signs the exact body bytes it sends to the configured destination, using the same timestamped signing format, and propagates `trace_id` in `X-Trace-Id`.
- Public API: `generate_signature(secret: str, timestamp: int, payload_bytes: bytes) -> str` and `verify_signature(secret: str, header_value: str, payload_bytes: bytes, tolerance_sec: int = 300) -> bool`.
- The handout does not specify a numeric timestamp tolerance. The project default is 300 seconds (`VULCANO_TOLERANCE_SEC`); this is not a course-mandated value.
- Malformed, non-ASCII, or mismatched signature values raise `WebhookSignatureError`; stale or future timestamps raise `WebhookTimestampError`.

## 3. HTTP contract selected for the project

These response codes are the project contract selected to satisfy the assignment's requirement to decide and test HTTP behavior:

| Condition | Response |
|---|---|
| Missing or invalid signature, or stale timestamp | `401 Unauthorized`; do not enqueue the request. |
| Invalid JSON or missing required event-envelope fields | `400 Bad Request`; do not enqueue the request. |
| SQS `SendMessage` succeeds | `202 Accepted`; respond only after successful enqueue. |
| SQS publish fails | `503 Service Unavailable`; never return `202` for a failed publish. |

The signature check uses the original request bytes, not parsed or normalized JSON. Tests must assert that invalid or stale requests do not reach `SendMessage`.

## 4. SQS delivery and local defaults

| Setting | Project default | Behavior |
|---|---:|---|
| Main queue | `vulcano-events-queue` (Standard) | Receives the original event body. |
| Dead-letter queue | `vulcano-events-dlq` | Target configured with the main queue's SQS `RedrivePolicy`. |
| `VisibilityTimeout` | 30 seconds | A failed, undeleted message becomes visible for redelivery when this timeout expires. |
| `maxReceiveCount` | 5 | SQS redrives repeatedly received failures to the DLQ according to `RedrivePolicy`. |
| Worker long-poll `WaitTimeSeconds` | 10 seconds | Configurable from 1 through 20; each `ReceiveMessage` call requests one event. |

The handout does not prescribe numeric queue values. These are explicitly project defaults for local development and tests, not assignment requirements. The bootstrap script reads queue names, visibility timeout, and max receive count from environment variables and configures the main queue's visibility and redrive attributes. Each dispatcher consumer calls `ReceiveMessage` for one event; `asyncio.TaskGroup` supervises two independent pollers by default.

On delivery success, delete the SQS message. On delivery failure, do not delete it: SQS visibility timeout expiry causes redelivery, and the configured redrive policy eventually sends it to the DLQ. Do not add a separate dispatcher retry loop.

## 5. Package and component responsibilities

The import package is `src/vulcano_telemetry/`; `src/app_operator.py` is the required top-level CLI entry point. Keep the CLI import-safe and defer clients, network connections, and logging startup until a command runs. The project targets Python 3.14; `except*` and `TaskGroup` are supported by Python 3.11+, but the project does not claim that 3.14 has been tested.

| File | Responsibility |
|---|---|
| `__init__.py` | Export the public API through an explicit `__all__`. |
| `exceptions.py` | Define `VulcanoError(Exception)`, `SQSConnectionError`, `QueueTimeoutError`, `CorruptedMessageError`, `WebhookSignatureError`, `WebhookTimestampError`, and `WebhookDeliveryError`. |
| `config.py` | Centralize validated environment parsing and defaults. Loading is deferred until after argument parsing so CLI help remains side-effect-free. |
| `sanitizer.py` | Expose `parse_queue_name` (`^vulcano-[a-z0-9-]+-queue$`), `parse_wait_time` (1–20), `parse_webhook_url` (HTTP(S) with host), and `parse_secret_key` (at least 16 characters). These parsers validate config/CLI input; secrets are never accepted as CLI arguments. The DLQ has its separate `-dlq` parser to preserve its documented name. |
| `event_schema.py` | Validate UTF-8 JSON and the required event envelope without changing the source body; enforce a bounded safe `trace_id`. |
| `core_sqs.py` | Expose `get_sqs_client(endpoint_url)`, `send_event`, `receive_events`, and explicit `delete_event`. Use synchronous Boto3, the default LocalStack endpoint, `us-east-1`, LocalStack-only `test`/`test` credentials with environment overrides, message attributes, and SQS `RedrivePolicy`. Catch `ClientError` and `BotoCoreError`, chain causes, and attach safe operation context with `add_note()`. |
| `webhook_crypto.py` | Expose the rubric's exact `generate_signature` and `verify_signature` APIs; sign exact raw bytes, parse strictly, enforce timestamp tolerance, and compare in constant time. |
| `webhook_receiver.py` | Receive the HTTP request, validate its raw body and signature, publish to SQS, and apply the selected HTTP contract. |
| `dispatcher.py` | Run concurrent asynchronous SQS pollers, use `asyncio.to_thread` for Boto3 and `TaskGroup` for task supervision, preserve body bytes, and delete only after a successful destination response. |
| `logging_engine.py` | Provide UTC JSON `AsyncJSONFormatter`, nonblocking queue logging, `TraceId`/`MessageId`/`WebhookId`/process/thread fields, safe recursive exception/cause/notes serialization, and 2 MiB/3-backup atomic gzip rotation. |
| `app_operator.py` and `src/app_operator.py` | Expose `produce-sqs`, `start-receiver`, and `start-dispatcher`; top-level entry point delegates to the package CLI, which supervises async dispatch and handles typed exception groups. |
| `tests/` | Hold offline unit/failure/forensic tests and opt-in LocalStack integration/chaos tests. |
| `requirements.txt` | Pin boto3, botocore, FastAPI, Uvicorn, requests, pytest, and supporting test/development dependencies. |

Boto3 is synchronous, not an async client. The async receiver and dispatcher use `asyncio.to_thread` for Boto3 operations. Typed `except*` handlers classify `WebhookSignatureError`, `WebhookTimestampError`, `SQSConnectionError`, and `WebhookDeliveryError` raised within task groups.

## 6. Destination and security

- Configure the destination outside the SQS message (for example, through `VULCANO_DESTINATION_URL`).
- For per-client destinations, include a client identifier and resolve it using an allowlist or trusted configuration. Never accept an arbitrary, unvalidated URL from a message as the delivery target.
- Keep `VULCANO_WEBHOOK_SECRET` and AWS credentials in environment variables. `test`/`test` are dummy credentials permitted only for a LocalStack endpoint; the environment can override them. Remote endpoints use Boto3's normal credential chain, and no real secrets are placed in source, logs, or command-line arguments.
- Record whether `X-Webhook-Signature` was present and whether validation succeeded. Never log the usable signature value, secret, or raw body.
- Validate configuration before starting a command. `--help` must import without network connections or unexpected logs.
- Catch only expected error types. Do not use bare `except`, catch `BaseException`, or use `except: pass`; tests must not swallow arbitrary errors.
- Always stop/close `QueueListener` in a `finally` path. Do not put `return`, `break`, or `continue` in `finally`.

## 7. Logging and rotation

Use `QueueHandler` to keep request/worker threads from waiting on file formatting and I/O, and a `QueueListener` to write JSON records. Each record uses a UTC ISO-8601 timestamp and includes the required fields from this project schema:

`timestamp`, `level`, `event`, `message`, `process`, and `threadName` are required. Add `TraceId`, `MessageId`, `WebhookId`, `signature_header_present`, and `signature_valid` when applicable; signature status fields record presence and validation outcome only.

Context fields may be absent when they do not apply. Serialize exception type, nested `ExceptionGroup` members, causes, and `add_note()` details recursively, but omit exception messages and tracebacks to avoid leaking raw bodies, credentials, or usable signatures.

Write the active log to `VULCANO_LOG_PATH`; rotate at exactly 2 MiB and keep three gzip-compressed backups. Compress each archive to a temporary file in the same directory, then rename the completed gzip file atomically so readers never see a partial archive.

## 8. Configuration defaults

`.env.example` documents the local settings. The numeric values below are project defaults, not course-mandated values.

| Setting | Default | Notes |
|---|---|---|
| `VULCANO_WEBHOOK_SECRET` | Explicit placeholder only | Replace locally; the application requires a configured secret of at least 16 characters. |
| `VULCANO_SQS_ENDPOINT_URL` | `http://localhost:4566` | LocalStack endpoint. |
| `VULCANO_QUEUE_NAME` | `vulcano-events-queue` | Main queue. |
| `VULCANO_DLQ_NAME` | `vulcano-events-dlq` | Dead-letter queue. |
| `VULCANO_SQS_VISIBILITY_TIMEOUT_SECONDS` | `30` | Main queue visibility timeout. |
| `VULCANO_SQS_MAX_RECEIVE_COUNT` | `5` | Main queue redrive threshold. |
| `VULCANO_RECEIVER_WAIT_TIME_SECONDS` | `10` | Long-poll wait; worker receives one message per call. |
| `VULCANO_DESTINATION_URL` | `http://localhost:8080/webhook` | Local destination example; configure outside the message. |
| `VULCANO_TOLERANCE_SEC` | `300` | Timestamp tolerance. |
| `VULCANO_LOG_PATH` | `logs/vulcano-telemetry.jsonl` | Active JSON log. |
| Log rotation | 2 MiB | Fixed rubric value. |
| Gzip backup count | `3` | Fixed rubric value. |
| Destination HTTP timeout | connect `3` s / read `10` s | Explicit per-request timeout; a timeout is one failed receipt, not an internal retry. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Environment override or `test`, `test` | Dummy defaults apply only to loopback LocalStack endpoints; remote SQS uses Boto3's credential chain. |
| `AWS_DEFAULT_REGION` | `us-east-1` | LocalStack region. |

## 9. Test plan and acceptance

Offline tests should cover:

| Test area | Required evidence |
|---|---|
| CLI/configuration | Queue, wait-time, URL, and configured-secret validation; `--help` has no network connection or unexpected log output. |
| Signature | Valid, invalid, missing, and stale timestamps; signing uses the exact raw request bytes. |
| Chaos | Altered body and wrong key return `401`; a one-hour-old signature returns `401`. |
| Request validation | Invalid JSON or an incomplete event envelope returns `400` and never enqueues a message. |
| Rejection | Invalid signature and stale-timestamp requests never enqueue a message. |
| HTTP/SQS | `202` only after successful publish and `503` on publish failure. |
| Logging | JSON schema and UTC timestamp validation, signature-status fields without signature contents, gzip archive decompression, and atomic archive naming. |

`tests/test_forensic_log.py` verifies UTC fields, process/thread context, TraceId/MessageId/WebhookId, signature presence only, recursive exception notes, and gzip decompression. LocalStack integration tests cover queue creation, publish/receive/delete, redrive configuration, and a poison receipt reaching the DLQ. They are marked `localstack` and run only when `VULCANO_RUN_LOCALSTACK_TESTS=1`. If opted in, they must fail visibly when required operations fail; they must not catch-and-skip arbitrary exceptions. They use isolated queues and test-specific settings to keep redrive practical, without changing or misrepresenting the documented local defaults. The `<100 ms` figure is a measurement target for a defined test and environment, not a universal latency guarantee.

## 10. Explicit exclusions

- No SQLite idempotency store or exactly-once claim.
- No application-level dispatcher retry loop; failed messages are left undeleted for SQS redelivery and DLQ redrive.
- Future improvement only: the destination may deduplicate on `event_id` within an agreed retention window to mitigate SQS Standard's at-least-once duplicates; this implementation does not store idempotency state.
- No unrelated Azure/GCP/provider-scan (`scan_all_providers`) diagram or requirements.
