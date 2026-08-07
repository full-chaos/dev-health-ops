"""Enable the CHAOS-3292 ``ask_dev_wave_3_1`` preflight for a corpus org.

WHY THIS EXISTS (CHAOS-3219 Phase 2 exit, live-diagnosed 2026-08-06):
``prepare_ask_dev_acceptance.py`` deliberately does NOT enable
``ask_dev_wave_3_1`` -- the shared positive-control oracle scenario is meant
to exercise the pre-3292 legacy loop, and a test
(``tests/acceptance/test_ask_dev_not_found_smoke.py``) pins that prepare
stays wave-3.1-free. Every Wave-3.1 smoke therefore turns the flag on FOR
ITSELF (``smoke_ask_dev_not_found.py``, ``smoke_ask_dev_stack3_intents.py``,
``smoke_ask_dev_unrelated_evidence.py``, ``smoke_ask_dev_team_attribution.py``
all carry their own ``_WAVE_3_1_FEATURE_KEY`` block).

The Wave 4 corpus runner never learned to do that. Consequence, measured
rather than reasoned about: ``_wave_3_1_enabled`` fails CLOSED to the legacy
path (``production_runtime.py``), so ``preflight = None``, the orchestrator
skips its whole preflight block, and every one of the 91 armed cases ran
against the pre-3292 loop -- zero ``dev_run_intents``, zero
``dev_run_subject_sets``, zero ``dev_run_resolutions``, zero
``dev_run_stage_diagnostics``, an empty answer frame whose ``direct_answer``
is "No matching subject was found for this question.", and the CHAOS-3289
backstop terminating every run ``insufficient_evidence``. Exit run #3's 55
failures were an artifact of that one missing setup step, not of the product.

This mirrors the smokes' existing enablement sequence (GET flags -> GET
overrides -> POST or PATCH) rather than inventing a second interpretation of
what "enabled" means. It RAISES instead of recording into a
``ScenarioRecorder``, because for the corpus this is setup: a run that could
not enable the flag must fail before it measures anything, never proceed and
silently produce another 91 legacy-path receipts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

WAVE_3_1_FEATURE_KEY = "ask_dev_wave_3_1"

_REASON = "CHAOS-3219 Wave 4 corpus acceptance run"


class FeatureEnablementError(RuntimeError):
    """Raised when ``ask_dev_wave_3_1`` could not be enabled for an org."""


def _override_expiry_is_past(item: Any) -> bool:
    """Whether this override row carries an ``expires_at`` already in the past.

    Codex adversarial review round 2 (MEDIUM, confirmed against
    ``licensing/feature_decisions.py``, which reads ``expires_at`` into
    ``FeatureOverrideSnapshot``): production stops honouring an expired
    override, so an ``is_enabled: true`` row that has expired grants nothing
    at runtime. A read-back that inspects only ``is_enabled`` is satisfied by
    exactly the shape production denies -- the same false-certification class
    as the global kill switch, and it would put the corpus back on the legacy
    path with every receipt certifying otherwise.

    An unparseable value is treated as EXPIRED (fail closed): the corpus must
    not proceed on a grant it cannot confirm.
    """

    raw = item.get("expires_at") if isinstance(item, dict) else None
    if raw in (None, ""):
        return False
    if not isinstance(raw, str):
        return True
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed <= datetime.now(UTC)


def _override_is_effectively_enabled(item: Any) -> bool:
    """``is_enabled`` AND not expired -- i.e. what production actually grants."""

    return (
        isinstance(item, dict)
        and item.get("is_enabled") is True
        and not _override_expiry_is_past(item)
    )


def _require_globally_enabled_flag(api: Any) -> dict[str, Any]:
    """The FeatureFlag catalog row, or raise -- registered AND globally on.

    Codex adversarial review (HIGH, confirmed): the first version of this
    module checked only that the flag was REGISTERED, then treated an enabled
    ORG OVERRIDE as proof the corpus would exercise the Wave 3.1 preflight.
    Production does not make that decision. ``evaluate_org_feature_async``
    ALSO fails closed when the FeatureFlag row itself is globally disabled, so
    a global kill-switch left ``_wave_3_1_enabled`` False while this setup and
    the per-receipt ``measured_wave_3_1_preflight_path`` check both passed on
    override membership alone -- reproducing, inside the fix for it, the exact
    false-confidence class this module exists to eliminate.

    ``prepare_ask_dev_acceptance.py`` already guards this ("required feature
    flags are globally disabled"); mirroring the SMOKES' override sequence had
    silently dropped it, because the smokes run after prepare has already
    checked. The corpus has no such upstream, so it checks here.
    """

    flags = api.request("GET", "/api/v1/admin/feature-flags")
    if not isinstance(flags, list):
        raise FeatureEnablementError(
            f"GET /api/v1/admin/feature-flags returned {type(flags).__name__}, "
            "expected a list"
        )
    flag = next(
        (
            item
            for item in flags
            if isinstance(item, dict) and item.get("key") == WAVE_3_1_FEATURE_KEY
        ),
        None,
    )
    if flag is None:
        raise FeatureEnablementError(
            f"feature flag {WAVE_3_1_FEATURE_KEY} is not registered on this "
            "stack -- the corpus cannot measure the CHAOS-3292 preflight path"
        )
    if flag.get("is_enabled") is not True:
        raise FeatureEnablementError(
            f"feature flag {WAVE_3_1_FEATURE_KEY} is globally disabled "
            f"({flag!r}) -- an org override cannot re-enable it, production's "
            "evaluate_org_feature_async still resolves it OFF, and the corpus "
            "would measure the pre-3292 legacy path while every receipt "
            "claimed otherwise"
        )
    return flag


def ensure_wave_3_1_enabled(api: Any, *, org_id: str) -> bool:
    """VERIFY-FIRST: confirm ``ask_dev_wave_3_1`` is ON for ``org_id``, and
    write an override ONLY if one is missing or disabled. Returns whether a
    write was performed.

    Verify-first, not enable-then-verify (team-lead ruling, CHAOS-3219 Phase 2
    exit). ``org_feature_overrides`` is a table WORLD_DIGEST hashes, so an
    unconditional write on every run drifted the live world away from the
    pinned digest -- a second armed run against the same stack then failed
    verification and needed a full restore, undoing re-runnability that
    ``principal_sessions`` had deliberately won back.

    The world now SEEDS this flag for its own orgs (``world._ENTITLEMENT_FIELDS``),
    so against a clean world this function performs **zero writes** and the
    digest still verifies after a full armed run. The write path is retained
    and still exercised for any org the world does not seed -- it must not
    become dead code that silently stops working, and a non-world org is a
    legitimate future caller.

    ``api`` must be an ADMIN-authenticated client: the feature-override
    endpoints require superuser admin access, which is exactly why the
    corpus runner passes its ``acceptance_api`` admin session here and not a
    per-case principal session.
    """

    flag = _require_globally_enabled_flag(api)

    override_path = f"/api/v1/admin/orgs/{org_id}/feature-overrides"
    overrides = api.request("GET", override_path)
    if not isinstance(overrides, list):
        raise FeatureEnablementError(
            f"GET {override_path} returned {type(overrides).__name__}, expected a list"
        )
    existing = next(
        (
            item
            for item in overrides
            if isinstance(item, dict)
            and item.get("feature_key") == WAVE_3_1_FEATURE_KEY
        ),
        None,
    )

    if existing is None:
        created = api.request(
            "POST",
            override_path,
            {
                "feature_id": flag["id"],
                "is_enabled": True,
                "reason": _REASON,
            },
        )
        if not _override_is_effectively_enabled(created):
            raise FeatureEnablementError(
                f"failed to enable {WAVE_3_1_FEATURE_KEY} for org {org_id}: {created!r}"
            )
        return True

    if _override_is_effectively_enabled(existing):
        # Already on AND unexpired -- the seeded-world path. Zero writes, so
        # WORLD_DIGEST still verifies after this run.
        return False

    override_id = existing.get("id")
    if not isinstance(override_id, str):
        raise FeatureEnablementError(
            f"{WAVE_3_1_FEATURE_KEY} override for org {org_id} has no usable id: "
            f"{existing!r}"
        )
    # `expires_at: None` is sent explicitly: flipping only `is_enabled` on an
    # expired row leaves the expiry intact, so production still denies it and
    # the run proceeds on a grant that does not exist.
    updated = api.request(
        "PATCH",
        f"{override_path}/{override_id}",
        {"is_enabled": True, "reason": _REASON, "expires_at": None},
    )
    if not _override_is_effectively_enabled(updated):
        raise FeatureEnablementError(
            f"failed to enable {WAVE_3_1_FEATURE_KEY} for org {org_id}: {updated!r}"
        )
    return True


def verify_wave_3_1_enabled(api: Any, *, org_id: str) -> None:
    """Read the override BACK and confirm it is on.

    Separate from :func:`enable_wave_3_1` on purpose: the write returning a
    200 with ``is_enabled: True`` is the server echoing the request body, not
    proof the org actually resolves the flag as enabled on its next read. A
    corpus run whose entire validity rests on this flag verifies it rather
    than trusting the write -- the failure this exists to prevent already
    happened once, silently, for three exit runs.

    Re-checks the GLOBAL flag row too (Codex HIGH), not just the org override:
    an enabled override on a globally-disabled flag still resolves OFF in
    production, and verification that cannot see that certifies a legacy-path
    run as a Wave 3.1 run.
    """

    _require_globally_enabled_flag(api)
    override_path = f"/api/v1/admin/orgs/{org_id}/feature-overrides"
    overrides = api.request("GET", override_path)
    if not isinstance(overrides, list):
        raise FeatureEnablementError(
            f"GET {override_path} returned {type(overrides).__name__}, expected a list"
        )
    match = next(
        (
            item
            for item in overrides
            if isinstance(item, dict)
            and item.get("feature_key") == WAVE_3_1_FEATURE_KEY
        ),
        None,
    )
    if match is not None and _override_expiry_is_past(match):
        raise FeatureEnablementError(
            f"{WAVE_3_1_FEATURE_KEY} override for org {org_id} is EXPIRED "
            f"(expires_at={match.get('expires_at')!r}) -- production stops "
            "honouring an expired override, so this would measure the "
            "pre-3292 legacy path while claiming the Wave 3.1 path"
        )
    if match is None or match.get("is_enabled") is not True:
        raise FeatureEnablementError(
            f"{WAVE_3_1_FEATURE_KEY} read back as NOT enabled for org "
            f"{org_id} after enablement: {match!r} -- refusing to run the "
            "corpus against the pre-3292 legacy path"
        )
