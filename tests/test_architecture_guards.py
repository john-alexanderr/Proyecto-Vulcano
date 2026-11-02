from __future__ import annotations

import ast
from pathlib import Path


def test_runtime_avoids_broad_exception_catches_and_finally_control_flow() -> None:
    source_root = Path(__file__).parents[1] / "src" / "vulcano_telemetry"
    source_paths = [*source_root.glob("*.py"), *source_root.parent.glob("*.py")]
    for source_path in source_paths:
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler):
                assert node.type is not None, f"bare except in {source_path.name}"
                assert not (
                    isinstance(node.type, ast.Name) and node.type.id == "BaseException"
                ), f"BaseException caught in {source_path.name}"
            if isinstance(node, (ast.Try, ast.TryStar)):
                forbidden = (ast.Return, ast.Break, ast.Continue)
                assert not any(
                    isinstance(child, forbidden)
                    for statement in node.finalbody
                    for child in ast.walk(statement)
                ), f"control flow in finally in {source_path.name}"
