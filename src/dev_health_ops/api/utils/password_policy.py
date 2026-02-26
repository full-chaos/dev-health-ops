from __future__ import annotations

from functools import lru_cache
from pathlib import Path

MIN_PASSWORD_LENGTH = 12
MAX_PASSWORD_LENGTH = 128


@lru_cache(maxsize=1)
def _load_common_passwords() -> set[str]:
    passwords_path = (
        Path(__file__).resolve().parents[2] / "data" / "common_passwords.txt"
    )
    if not passwords_path.exists():
        return set()

    with passwords_path.open("r", encoding="utf-8") as password_file:
        return {
            line.strip().lower()
            for line in password_file
            if line.strip() and not line.strip().startswith("#")
        }


def validate_password(password: str) -> list[str]:
    violations: list[str] = []

    password_length = len(password)
    if password_length < MIN_PASSWORD_LENGTH:
        violations.append(
            f"Password must be at least {MIN_PASSWORD_LENGTH} characters long"
        )
    if password_length > MAX_PASSWORD_LENGTH:
        violations.append(
            f"Password must be no more than {MAX_PASSWORD_LENGTH} characters long"
        )

    if not any(character.isalpha() for character in password):
        violations.append("Password must include at least one letter")
    if not any(character.isdigit() for character in password):
        violations.append("Password must include at least one number")

    if password.lower() in _load_common_passwords():
        violations.append("Password is too common")

    return violations
