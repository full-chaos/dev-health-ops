"""GitLab instance-identity normalization (CHAOS-2801).

A GitLab ``project_id`` is only unique within one GitLab *instance*, so
work-item unit scoping discriminates same-id rows by the instance they were
discovered from. This module is the SINGLE normalizer for that discriminator
— used at BOTH the persist path (``processors/gitlab.py`` repo write sites,
which stamp ``settings.gitlab_instance_url``) and the comparison site
(``metrics/job_work_items.py`` numeric-id scoping). Never introduce a second
copy: a divergence between what is persisted and what is compared silently
re-opens the cross-instance collision (codex HIGH on PR #1143) or, worse,
false-mismatches every row of a healthy integration and trips the
CHAOS-2737 fail-closed path org-wide (codex MED on PR #1148).

Stdlib-only on purpose so it is importable from processors, metrics, and
tests without any dependency cycle.
"""

from __future__ import annotations

from urllib.parse import urlsplit

# Default ports are stripped so equivalent spellings of the same endpoint
# ("https://host" vs "https://host:443") never false-mismatch — a harmless
# credential formatting change must not flip an integration's every row to
# mismatch-reject (codex MED, PR #1148). Non-default ports are preserved:
# two GitLab instances CAN legitimately run on different ports of one host.
_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_gitlab_instance(url: object) -> str | None:
    """Normalize a GitLab base/instance URL to a comparable discriminator.

    Returns ``scheme://host[:port]`` with:

    * scheme + host lowercased,
    * userinfo and path/query/fragment ignored (``https://HOST/api/v4/`` and
      ``https://host`` compare equal),
    * scheme defaulting to ``https`` when absent (``gitlab.example.com``),
    * the port stripped when it is the scheme's default (http:80,
      https:443) and preserved otherwise.

    Returns ``None`` when ``url`` is not a non-blank string, has no
    parseable host, or carries a malformed port — callers treat ``None`` as
    "discriminator unknown/absent", never as a distinct instance.
    """
    if not isinstance(url, str) or not url.strip():
        return None
    candidate = url.strip()
    if "://" not in candidate:
        candidate = f"//{candidate}"
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return None
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return None
    scheme = (parsed.scheme or "https").strip().lower()
    try:
        port = parsed.port
    except ValueError:
        # Malformed port (e.g. "https://host:notaport") — unknown, not a
        # distinct instance.
        return None
    if port is not None and port != _DEFAULT_PORTS.get(scheme):
        return f"{scheme}://{host}:{port}"
    return f"{scheme}://{host}"
