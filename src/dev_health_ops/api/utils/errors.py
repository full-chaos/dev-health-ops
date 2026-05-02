from __future__ import annotations

from typing import Any


def error_detail(
    message: str,
    errors: list[str] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    result: dict[str, Any] = {"message": message}
    if errors:
        result["errors"] = errors
    result.update(kwargs)
    return result
