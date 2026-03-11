from __future__ import annotations

from datetime import date, timedelta


def chunk_date_range(
    since: date, before: date, chunk_days: int = 7
) -> list[tuple[date, date]]:
    if chunk_days <= 0:
        raise ValueError("chunk_days must be greater than 0")
    if since > before:
        raise ValueError("since must be before or equal to before")

    chunks: list[tuple[date, date]] = []
    cursor = since
    delta = timedelta(days=chunk_days - 1)
    one_day = timedelta(days=1)

    while cursor <= before:
        chunk_end = min(cursor + delta, before)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + one_day

    return chunks
