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

from typing import Any

WAVE_3_1_FEATURE_KEY = "ask_dev_wave_3_1"

_REASON = "CHAOS-3219 Wave 4 corpus acceptance run"


class FeatureEnablementError(RuntimeError):
    """Raised when ``ask_dev_wave_3_1`` could not be enabled for an org."""


def enable_wave_3_1(api: Any, *, org_id: str) -> None:
    """Ensure ``ask_dev_wave_3_1`` is ON for ``org_id``. Idempotent.

    ``api`` must be an ADMIN-authenticated client: the feature-override
    endpoints require superuser admin access, which is exactly why the
    corpus runner passes its ``acceptance_api`` admin session here and not a
    per-case principal session.
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
        if not (isinstance(created, dict) and created.get("is_enabled") is True):
            raise FeatureEnablementError(
                f"failed to enable {WAVE_3_1_FEATURE_KEY} for org {org_id}: {created!r}"
            )
        return

    if existing.get("is_enabled") is True:
        return

    override_id = existing.get("id")
    if not isinstance(override_id, str):
        raise FeatureEnablementError(
            f"{WAVE_3_1_FEATURE_KEY} override for org {org_id} has no usable id: "
            f"{existing!r}"
        )
    updated = api.request(
        "PATCH",
        f"{override_path}/{override_id}",
        {"is_enabled": True, "reason": _REASON},
    )
    if not (isinstance(updated, dict) and updated.get("is_enabled") is True):
        raise FeatureEnablementError(
            f"failed to enable {WAVE_3_1_FEATURE_KEY} for org {org_id}: {updated!r}"
        )


def verify_wave_3_1_enabled(api: Any, *, org_id: str) -> None:
    """Read the override BACK and confirm it is on.

    Separate from :func:`enable_wave_3_1` on purpose: the write returning a
    200 with ``is_enabled: True`` is the server echoing the request body, not
    proof the org actually resolves the flag as enabled on its next read. A
    corpus run whose entire validity rests on this flag verifies it rather
    than trusting the write -- the failure this exists to prevent already
    happened once, silently, for three exit runs.
    """

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
    if match is None or match.get("is_enabled") is not True:
        raise FeatureEnablementError(
            f"{WAVE_3_1_FEATURE_KEY} read back as NOT enabled for org "
            f"{org_id} after enablement: {match!r} -- refusing to run the "
            "corpus against the pre-3292 legacy path"
        )
