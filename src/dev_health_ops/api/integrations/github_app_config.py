from __future__ import annotations

import os
from pathlib import Path


def github_app_slug() -> str | None:
    return os.getenv("GITHUB_APP_SLUG")


def github_app_id() -> str | None:
    return os.getenv("GITHUB_APP_ID")


def github_app_private_key() -> str | None:
    inline_key = os.getenv("GITHUB_APP_PRIVATE_KEY")
    if inline_key:
        return inline_key
    key_path = os.getenv("GITHUB_APP_PRIVATE_KEY_PATH")
    if key_path:
        return Path(key_path).read_text(encoding="utf-8")
    return None


def github_app_client_id() -> str | None:
    return os.getenv("GITHUB_APP_CLIENT_ID")


def github_app_client_secret() -> str | None:
    return os.getenv("GITHUB_APP_CLIENT_SECRET")
