from __future__ import annotations

import pytest

import app_operator as top_level_app_operator
from vulcano_telemetry import app_operator


def test_help_does_not_load_settings_or_start_runtime(monkeypatch, capsys) -> None:
    def unexpected(*args, **kwargs):
        raise AssertionError("--help must not initialize runtime resources")

    monkeypatch.setattr(app_operator.Settings, "from_env", unexpected)
    monkeypatch.setattr(app_operator, "configure_logging", unexpected)
    monkeypatch.setattr(app_operator, "CoreSQS", unexpected)

    with pytest.raises(SystemExit) as result:
        app_operator.main(["--help"])

    assert result.value.code == 0
    output = capsys.readouterr()
    assert "produce-sqs" in output.out
    assert "start-receiver" in output.out
    assert "start-dispatcher" in output.out
    assert output.err == ""


def test_package_does_not_shadow_common_standard_library_modules() -> None:
    module_names = {
        "asyncio",
        "http",
        "json",
        "logging",
        "queue",
        "time",
        "typing",
    }
    package_names = {
        "app_operator",
        "config",
        "core_sqs",
        "dispatcher",
        "event_schema",
        "exceptions",
        "logging_engine",
        "sanitizer",
        "webhook_crypto",
        "webhook_receiver",
    }

    assert package_names.isdisjoint(module_names)


def test_required_top_level_entrypoint_delegates_to_package_cli() -> None:
    assert top_level_app_operator.main is app_operator.main
