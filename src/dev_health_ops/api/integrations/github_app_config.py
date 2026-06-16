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
        # Accept a single-line PEM with escaped newlines (how .env files,
        # Docker env, and most secret managers store multi-line values). A
        # real multi-line PEM contains no literal "\n", so this is a no-op
        # for keys that already have real newlines.
        return inline_key.replace("\\n", "\n")
    key_path = os.getenv("GITHUB_APP_PRIVATE_KEY_PATH")
    if key_path:
        return Path(key_path).read_text(encoding="utf-8")
    return None


def github_app_client_id() -> str | None:
    return os.getenv("GITHUB_APP_CLIENT_ID")


def github_app_client_secret() -> str | None:
    return os.getenv("GITHUB_APP_CLIENT_SECRET")


def github_app_callback_url() -> str | None:
    return os.getenv("GITHUB_APP_CALLBACK_URL")
