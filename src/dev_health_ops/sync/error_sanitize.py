"""Redaction for legacy free-form error-text DB columns (CHAOS-2766).

Live verification of CHAOS-2758 (evidence comment on CHAOS-2742) demonstrated
that a provider exception whose message embeds an ``Authorization`` header
(``403 rate limited -- Authorization: Bearer ghp_FAKE...``) persisted
VERBATIM into the pre-existing ``sync_run_units.error`` column and its
siblings (``sync_runs.error``, ``sync_run_reference_discovery.error``,
``sync_dispatch_outbox.last_error``) -- every one of these is populated from
``str(exc)`` (or an f-string embedding it) at a worker boundary that never
controls what a provider client library puts in an exception message.

This is deliberately a **redaction pass**, not the allow-list used for the
durable rate-limit observation store's ``reason`` column
(``_normalized_rate_limit_reason`` in ``workers/sync_units.py``, CHAOS-2758).
That column is a closed, normalized enum with no diagnostic-text mandate, so
allow-listing a fixed vocabulary is correct there. The columns this module
guards are free-form operator-facing diagnostics -- the whole point of
persisting them is to help operators debug a failed sync without re-running
it, so blanket-replacing the text (or falling back to a category-only string)
would defeat their purpose. Instead this strips/masks the specific
credential-shaped substrings that must never reach the database and leaves
everything else intact.

See ``docs/providers/rate-limit-policy.md`` (the "Legacy error-text columns"
section) for the full design rationale and the callers wired up to this
helper.
"""

from __future__ import annotations

import re

#: Substituted for any credential-shaped substring this module recognizes.
#: Tests assert this marker is present (and the original secret is not)
#: rather than hardcoding the redaction text inline everywhere.
REDACTION_MARKER = "[REDACTED]"

#: Default cap on persisted error text. These are `Text` columns (no DB-level
#: limit), but an unbounded provider response body or traceback fragment is a
#: bloat risk in its own right, independent of the credential-leak risk this
#: module primarily targets. Individual call sites may pass a tighter
#: ``max_length`` (e.g. ``sync/dispatch_outbox.py`` keeps its pre-existing,
#: tighter 2000-char cap for `sync_dispatch_outbox.last_error`).
DEFAULT_MAX_ERROR_TEXT_LENGTH = 4000

_TRUNCATION_SUFFIX = "...[truncated]"

# Ordered so header-shaped matches consume their whole "<Scheme> <credential>"
# pair before the narrower bare-token patterns below get a chance to leave a
# dangling fragment. Every pattern is deliberately conservative about how
# much of the surrounding text it consumes -- wide enough to catch the
# credential, narrow enough not to eat an entire diagnostic message.
_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Authorization / Proxy-Authorization header, any scheme, e.g.
    # "Authorization: Bearer ghp_xxx" or "authorization=Basic xxx". Consumes
    # the header name plus up to two following tokens (scheme + credential).
    re.compile(r"(?i)\b(authorization|proxy-authorization)\s*[:=]\s*\S+(?:\s+\S+)?"),
    # Bearer scheme with no leading header name, e.g. "used Bearer ghp_xxx".
    re.compile(r"(?i)\bbearer\s+\S+"),
    # HTTP Basic auth base64 blob, with or without a leading header name.
    re.compile(r"(?i)\bbasic\s+[a-z0-9+/=]{8,}\b"),
    # Provider personal-access-token / bot-token prefixes are
    # self-identifying -- the prefix alone is enough to know it's a secret,
    # so redact wherever it appears, header or not.
    re.compile(r"(?i)\bghp_[a-z0-9]{20,}\b"),
    re.compile(r"(?i)\bgho_[a-z0-9]{20,}\b"),
    re.compile(r"(?i)\bghu_[a-z0-9]{20,}\b"),
    re.compile(r"(?i)\bghs_[a-z0-9]{20,}\b"),
    re.compile(r"(?i)\bghr_[a-z0-9]{20,}\b"),
    re.compile(r"(?i)\bgithub_pat_[a-z0-9_]{20,}\b"),
    re.compile(r"(?i)\bglpat-[a-z0-9_-]{20,}\b"),
    re.compile(r"(?i)\bxox[baprs]-[a-z0-9-]{10,}\b"),
    # token=/private_token=/api_key=/access_token=/secret=-style query
    # params or key/value diagnostic pairs. Kept last so a PAT-prefixed
    # value under one of these keys is already redacted by a more specific
    # pattern above; this just catches the (redundant but harmless) leftover
    # "key=" wrapper and any non-prefixed secret value.
    re.compile(
        r"(?i)\b(private_token|access_token|api_key|apikey|client_secret|"
        r"secret|token)\s*[:=]\s*\S+"
    ),
    # Credential embedded in a URL's userinfo component (scheme://user:pass@
    # or scheme://:pass@), e.g. a broker/result-backend/database connection
    # string surfacing in a Celery enqueue exception or a DB driver error
    # (redis://:password@host:6379/0, amqp://user:pass@host,
    # postgres://user:pass@host/db). Redacts the scheme+userinfo span
    # entirely rather than trying to preserve the scheme, matching this
    # module's existing behavior of consuming the whole credential-bearing
    # span (see the Authorization-header pattern above) -- the host/path
    # after "@" is left intact for diagnostics. Requires "://" immediately
    # before the userinfo so an ordinary "contact admin@example.com" mention
    # is never touched.
    re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s/@]+@"),
)


def sanitize_error_text(
    value: BaseException | str | None,
    *,
    max_length: int | None = DEFAULT_MAX_ERROR_TEXT_LENGTH,
) -> str | None:
    """Return ``value`` with credential-shaped substrings redacted.

    ``value`` may be the exception itself (preferred -- the exception class
    name is preserved as a diagnostic prefix, e.g. ``"RateLimitException: 403
    rate limited -- [REDACTED]"``, so an empty-message exception like a bare
    ``raise SomeError()`` still persists something useful) or an
    already-stringified message (no class-name prefix is synthesized, since
    there is no exception object to name).

    Redaction runs before truncation so a length cap can never split a
    credential in half and leave a partial value exposed.
    """
    if value is None:
        return None
    if isinstance(value, BaseException):
        message = str(value).strip()
        class_name = type(value).__name__
        text = f"{class_name}: {message}" if message else class_name
    else:
        text = str(value)
    if not text:
        return text
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(REDACTION_MARKER, text)
    if max_length is not None and len(text) > max_length:
        if max_length > len(_TRUNCATION_SUFFIX):
            text = text[: max_length - len(_TRUNCATION_SUFFIX)] + _TRUNCATION_SUFFIX
        else:
            text = text[:max_length]
    return text
