from __future__ import annotations

from pathlib import Path

SQL_ROOT = Path(__file__).resolve().parent.parent / "sql"


def load_sql(relative_path: str) -> str:
    path = SQL_ROOT / relative_path
    return path.read_text(encoding="utf-8")
