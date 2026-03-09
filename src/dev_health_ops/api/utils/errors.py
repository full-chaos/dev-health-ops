from __future__ import annotations


def error_detail(message: str, errors: list[str] | None = None, **kwargs) -> dict:
    result = {"message": message}
    if errors:
        result["errors"] = errors
    result.update(kwargs)
    return result
