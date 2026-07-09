"""Server-side identity derivation for external-ingest records (CHAOS-2698).

Two deterministic ID spaces this module owns, per master-spec CC4/CC7:

- ``derive_repo_uuid`` — the ClickHouse ``repos.id`` UUID. MUST match what
  native sync derives for the "same" repo (``get_repo_uuid_from_repo``,
  ``models/git.py:72``) so a repo dedupes identically whether it arrived via
  ``fullchaos_sync`` or ``customer_push`` — this is the one-active-owner
  handoff's data-layer linchpin (brief-2698-sinks.md D2).
- ``derive_work_item_id`` — the namespaced ``work_items.work_item_id`` string
  every downstream query already assumes (``jira:ABC-123``,
  ``gh:owner/repo#123``, ``ghpr:owner/repo#123``, ``gitlab:group/project#456``,
  ``gitlab:group/project!456``, ``linear:CHAOS-123``). Verified against
  ``providers/github/normalize.py:108``, ``providers/gitlab/normalize.py:57,216``,
  ``providers/jira/normalize.py:271``, ``providers/linear/normalize.py:213``
  (brief-2698-sinks.md D3). Customers send provider-native keys
  (``WorkItemV1.external_key``) — never the namespaced ID (master-spec CC7).

Neither function normalizes case beyond what the underlying primitive already
does: ``get_repo_uuid_from_repo`` lower-cases its seed (so repo UUIDs are
case-insensitive), but ``derive_work_item_id`` does NOT lower-case ``instance``
or ``external_key`` — this matches native sync's ``work_item_id`` construction
exactly (it never lower-cases either), which means two pushes that disagree
only in ``repositoryExternalId`` casing produce the same repo UUID but
different ``work_item_id`` strings. That is a pre-existing native-sync
behavior, not something this module introduces or fixes.
"""

from __future__ import annotations

import uuid

from dev_health_ops.models.git import get_repo_uuid_from_repo

__all__ = ["derive_repo_uuid", "derive_work_item_id"]


def derive_repo_uuid(system: str, instance: str, external_id: str) -> uuid.UUID:
    """Derive the ClickHouse ``repos.id`` UUID for a pushed repository.

    ``system in {"github", "gitlab"}``: seed is ``external_id`` (the provider
    full name, e.g. ``owner/repo`` / ``group/subgroup/project`` — must equal
    ``instance`` per master-spec CC6), fed unchanged into
    ``get_repo_uuid_from_repo`` exactly as native sync does.

    ``system == "custom"``: no real-provider namespace to align with, so the
    seed is ``f"custom:{instance}:{external_id}"`` (brief-2698-sinks.md D2) —
    a distinct namespace with no collision risk against real-provider UUIDs.
    """
    seed = external_id if system != "custom" else f"custom:{instance}:{external_id}"
    return get_repo_uuid_from_repo(seed)


def derive_work_item_id(
    system: str,
    instance: str | None,
    external_key: str,
    work_item_type: str | None = None,
) -> str:
    """Derive the namespaced ``work_items.work_item_id`` string.

    ``instance`` is the repo full name (``owner/repo`` / ``group/project``)
    for ``system in {"github", "gitlab"}`` — ignored for jira/linear, where
    the provider-native key alone (``external_key``) is already globally
    unique within the org's Jira/Linear namespace.

    ``work_item_type`` disambiguates the issue vs. pr/merge_request
    namespace for github/gitlab (``"pr"`` -> ``ghpr:``, ``"merge_request"``
    -> ``gitlab:...!...``); any other value (including ``None``, the
    schema's documented default) is treated as a plain issue.
    """
    if system == "jira":
        return f"jira:{external_key}"
    if system == "linear":
        return f"linear:{external_key}"
    if system == "github":
        repo = instance or ""
        if work_item_type == "pr":
            return f"ghpr:{repo}#{external_key}"
        return f"gh:{repo}#{external_key}"
    if system == "gitlab":
        repo = instance or ""
        if work_item_type == "merge_request":
            return f"gitlab:{repo}!{external_key}"
        return f"gitlab:{repo}#{external_key}"
    # system == "custom": CC6 excludes the work_item family for custom
    # sources in v1, but ids.py stays provider-neutral / defensive here
    # rather than raising, matching D2's custom repo-UUID fallback shape.
    return f"custom:{instance}:{external_key}"
