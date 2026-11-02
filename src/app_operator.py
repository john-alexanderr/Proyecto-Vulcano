"""Top-level executable wrapper for the Vulcano telemetry CLI."""

from vulcano_telemetry.app_operator import main

__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
