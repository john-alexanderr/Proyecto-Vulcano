"""Punto de entrada ejecutable para la CLI de telemetría de Vulcano."""

from vulcano_telemetry.app_operator import main

__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
