# TP-2: Vulcano Telemetry Event Pipeline

A local webhook receiver validates HMAC-SHA256 signatures over the original request bytes, queues events in SQS, and concurrent dispatcher workers forward and sign unchanged bodies for one configured destination. JSON logs use a nonblocking queue and rotate into gzip-compressed files.

> **Runtime status:** The receiver, SQS adapter, dispatcher, signature handling, structured logging, CLI, offline tests, and opt-in LocalStack integration tests implement the current rubric. The project targets Python 3.14; this does not claim a Python 3.14 test result.

## Quick path

1. Copy `.env.example` to `.env` and replace the webhook-secret placeholder locally. Keep secrets and AWS credentials in environment variables; never put a secret in a CLI argument or commit `.env`.
2. Export the local settings into the shell:
   ```bash
   set -a
   . ./.env
   set +a
   ```
3. Start LocalStack separately at the configured endpoint, then create the queues and redrive policy:
   ```bash
   bash scripts/localstack_bootstrap.sh
   ```
4. Install the pinned dependencies:
   ```bash
   python3.14 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```
5. In separate terminals, start the dispatcher and receiver:
   ```bash
   PYTHONPATH=src .venv/bin/python -m app_operator start-dispatcher
   PYTHONPATH=src .venv/bin/python -m app_operator start-receiver
   ```

The receiver listens on `0.0.0.0:8000` by default. The destination must be running at `VULCANO_DESTINATION_URL` (the local example is `http://localhost:8080/webhook`). Boto3 calls run in worker threads; two independent dispatcher pollers are supervised with `asyncio.TaskGroup`, and each `ReceiveMessage` call requests one event. Each receipt gets one HTTP attempt; failed messages remain undeleted for SQS redelivery and eventual DLQ handling. The rubric targets Python 3.14; if the local interpreter is 3.12, treat that as an unverified local compatibility runtime, not a 3.14 verification result.

To enqueue a validated event directly for a dispatcher demo, use `produce-sqs` with JSON or standard input:

```bash
PYTHONPATH=src .venv/bin/python -m app_operator produce-sqs \
  --event-json '{"event_id":"evt-1","event_type":"telemetry.created","occurred_at":"2026-01-02T03:04:05Z","trace_id":"trace-1","payload":{"temperature":21.5}}'
```

This command publishes directly to SQS and does not exercise receiver signature validation. Webhook senders must send `X-Webhook-Signature: t=<timestamp>,v1=<hex>` using HMAC-SHA256 over `f"{timestamp}.".encode() + raw_body`.

## Assignment contract

| Area | Contract |
|---|---|
| Ingress | `POST /webhook`; validate `X-Webhook-Signature` against the original raw body and its timestamp before enqueueing. |
| Event | JSON envelope with at least `event_id`, `event_type`, `occurred_at`, `trace_id`, and `payload`. |
| Queue | Standard `vulcano-events-queue`, with `RedrivePolicy` configured to `vulcano-events-dlq`; message attributes carry `TraceId`, `EventTopic`, and `Timestamp`. |
| Delivery | One destination is configured outside the message. The worker forwards the unchanged body, signs those exact bytes, adds `X-Trace-Id`, then deletes only after a successful HTTP response. |
| Retry | SQS Standard is at-least-once. SQS visibility-based redelivery and DLQ redrive only; duplicates are possible and there is no exactly-once claim. |
| Logs | Nonblocking JSON logging with `QueueHandler`/`QueueListener`, UTC timestamps, `TraceId`, `MessageId`, `WebhookId`, process/thread context, and 2 MiB / 3-backup gzip rotation using atomic rename. |

The dispatcher timeout is 3 seconds to connect and 10 seconds to read. A future destination-side deduplication window keyed by `event_id` could mitigate Standard SQS duplicates; that changes destination behavior and is not implemented here. See [the detailed requirements](docs/requirements.md) for signature bytes, queue settings, component responsibilities, security rules, and the test plan.

## Verification

Run offline tests without LocalStack:

```bash
.venv/bin/pytest -m "not localstack"
```

LocalStack integration tests are explicitly opt-in. Start LocalStack, export the example environment (including its dummy `test` credentials), then run:

```bash
VULCANO_RUN_LOCALSTACK_TESTS=1 .venv/bin/pytest -m localstack
```

The offline `test_chaos.py` covers altered-body and wrong-key 401 responses plus a one-hour stale timestamp. `test_forensic_log.py` covers UTC context, safe exception groups, and gzip archives. When opted in, an unavailable LocalStack endpoint is a test failure rather than a skipped integration run; the tests use isolated queues and cover poison-message DLQ redrive.

## Package layout

```text
src/vulcano_telemetry/
  app_operator.py
  config.py
  core_sqs.py
  dispatcher.py
  event_schema.py
  exceptions.py
  logging_engine.py
  sanitizer.py
  webhook_crypto.py
  webhook_receiver.py
src/app_operator.py  # top-level executable entry point
tests/
scripts/localstack_bootstrap.sh
docs/requirements.md
requirements.txt
```

`config.py` centralizes validated environment parsing so each command shares defaults while `--help` remains safe: parsing help does not load configuration, create clients, or start logging. The required top-level CLI delegates to `vulcano_telemetry.app_operator`. Metadata targets Python 3.14; the local runtime may differ.

## Configuration

LocalStack uses environment-provided credentials, falling back to dummy `test` / `test` values only for a loopback LocalStack endpoint. Those values are never passed to remote AWS endpoints. Queue defaults are a 30-second visibility timeout, `maxReceiveCount` 5, and a 10-second long-poll wait. Logging rotation is fixed by the rubric at 2 MiB with 3 gzip backups. See [the requirements](docs/requirements.md) for the validated environment contract.

Do not place a destination URL supplied by an event directly into a message and trust it. If destinations vary by client, put a client identifier in the event and resolve it through an allowlist or trusted configuration.
