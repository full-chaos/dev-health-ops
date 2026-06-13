"""Bounded, defensive reading of untrusted ZIP archives (CHAOS-2370).

CI test/coverage reports arrive as ZIP artifacts downloaded from GitHub Actions
and GitLab. ZIPs from external sources are untrusted input and must be handled
defensively against:

- **Zip-slip / path traversal** — entry names like ``../../etc/passwd`` or
  absolute paths. We never extract to disk; we read entries in memory and reject
  dangerous names outright.
- **Decompression bombs** — a tiny archive that inflates to gigabytes. We cap
  per-entry uncompressed size, total uncompressed size, entry count, and the
  per-entry compression ratio.

The reader yields only entries whose names match a caller-supplied predicate
(e.g. ``*.xml``), so we never inflate files we don't care about.
"""

from __future__ import annotations

import io
import logging
import zipfile
from collections.abc import Callable, Iterator

logger = logging.getLogger(__name__)

# Conservative defaults; callers may override per archive.
DEFAULT_MAX_ENTRIES = 2_000
DEFAULT_MAX_FILE_BYTES = 64 * 1024 * 1024  # 64 MiB per entry (uncompressed)
DEFAULT_MAX_TOTAL_BYTES = 256 * 1024 * 1024  # 256 MiB total (uncompressed)
DEFAULT_MAX_COMPRESSION_RATIO = 200  # uncompressed / compressed per entry


def _is_safe_member_name(name: str) -> bool:
    """Reject absolute paths and parent-directory traversal in entry names."""
    if not name or name.endswith("/"):
        return False  # directory entry — nothing to read
    if name.startswith("/") or name.startswith("\\"):
        return False
    # Normalize separators and reject any ``..`` path component.
    parts = name.replace("\\", "/").split("/")
    if any(part == ".." for part in parts):
        return False
    # Windows drive-absolute (e.g. ``C:\\``).
    if len(name) >= 2 and name[1] == ":":
        return False
    return True


def iter_zip_members(
    data: bytes,
    *,
    name_filter: Callable[[str], bool],
    max_entries: int = DEFAULT_MAX_ENTRIES,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
) -> Iterator[tuple[str, bytes]]:
    """Yield ``(name, content_bytes)`` for matching, in-bounds ZIP members.

    Entries are skipped (with a warning) rather than raising when they fail a
    safety check, so one hostile entry doesn't abort ingestion of the rest.
    Raises ``zipfile.BadZipFile`` only when ``data`` is not a valid ZIP — the
    caller should treat that as "no reports in this artifact".
    """
    total_uncompressed = 0
    processed = 0
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        infos = archive.infolist()
        if len(infos) > max_entries:
            logger.warning(
                "Archive has %d entries (>%d cap); processing first %d only",
                len(infos),
                max_entries,
                max_entries,
            )
        for info in infos[:max_entries]:
            name = info.filename
            if not _is_safe_member_name(name):
                logger.warning("Skipping unsafe archive member name: %r", name)
                continue
            if not name_filter(name):
                continue
            declared = info.file_size
            if declared > max_file_bytes:
                logger.warning(
                    "Skipping oversized archive member %r (%d bytes > %d cap)",
                    name,
                    declared,
                    max_file_bytes,
                )
                continue
            compressed = info.compress_size or 1
            if declared and declared / compressed > max_compression_ratio:
                logger.warning(
                    "Skipping archive member %r: compression ratio %.0f exceeds %d",
                    name,
                    declared / compressed,
                    max_compression_ratio,
                )
                continue
            if total_uncompressed + declared > max_total_bytes:
                logger.warning(
                    "Archive total uncompressed size cap (%d) reached; stopping",
                    max_total_bytes,
                )
                break
            # Read with an explicit byte cap as a second line of defense in case
            # the declared size in the header understated the real content.
            with archive.open(info) as member:
                content = member.read(max_file_bytes + 1)
            if len(content) > max_file_bytes:
                logger.warning(
                    "Skipping archive member %r: actual size exceeds %d cap",
                    name,
                    max_file_bytes,
                )
                continue
            total_uncompressed += len(content)
            processed += 1
            yield name, content
    logger.debug(
        "Read %d member(s) from archive (%d bytes)", processed, total_uncompressed
    )
