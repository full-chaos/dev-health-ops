"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: WORLD_DIGEST-pinned oracle guard
(ruling D2 -- expected values are computed from the world generator and
pinned by digest; a receipt whose world digest does not match the pinned
one must FAIL, never silently be trusted).

Thin wrapper over ``dev_health_ops.fixtures.world`` rather than a
reimplementation: ``compute_world_digest``/``read_pinned_digest``/
``_diff_components`` are the exact functions ``fixtures world
--verify-digest`` itself uses (see ``world.py``'s own ``_verify_digest``,
which this module's :func:`verify_world_digest` mirrors) -- importing them
means Lane 2a's guard and the CLI's own verification can never quietly
diverge on what "drift" means. This module only adds a plain,
``argparse``-free function signature a pytest fixture can call, and splits
the async compute-and-compare step from a pure, synchronously-testable
assertion step.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dev_health_ops.fixtures.world import (
    _diff_components,
    compute_world_digest,
    default_digest_path,
    load_world_manifest,
    read_pinned_digest,
)

__all__ = [
    "WorldDigestMismatchError",
    "WorldDigestVerification",
    "require_world_digest_match",
    "verify_world_digest",
]


class WorldDigestMismatchError(Exception):
    """The live WORLD_DIGEST does not match the pinned one."""


@dataclass(frozen=True, slots=True)
class WorldDigestVerification:
    pinned_digest: str
    live_digest: str
    matched: bool
    drifted_components: tuple[str, ...]


async def verify_world_digest(
    manifest_path: str | Path,
    *,
    sink: str,
    postgres_uri: str,
    digest_path: str | Path | None = None,
) -> WorldDigestVerification:
    """Compute the live digest and compare it to the pinned one.

    Needs a live, reachable ClickHouse/Postgres scratch database -- this is
    the one function in this module that cannot be unit tested without
    infra. :func:`require_world_digest_match` (pure) is what a unit test
    exercises against a fabricated :class:`WorldDigestVerification`.
    """

    manifest = load_world_manifest(manifest_path)
    path = (
        Path(digest_path) if digest_path is not None else default_digest_path(manifest)
    )
    pinned = read_pinned_digest(path)
    live = await compute_world_digest(manifest, sink=sink, postgres_uri=postgres_uri)
    matched = live["digest"] == pinned["digest"]
    drifted = (
        ()
        if matched
        else tuple(
            _diff_components(pinned.get("components", {}), live.get("components", {}))
        )
    )
    return WorldDigestVerification(
        pinned_digest=pinned["digest"],
        live_digest=live["digest"],
        matched=matched,
        drifted_components=drifted,
    )


def require_world_digest_match(
    verification: WorldDigestVerification, *, digest_path: str | Path
) -> None:
    """Raise :class:`WorldDigestMismatchError` on a mismatch; no-op on a match.

    Call this once, at corpus-run session start, before any case executes:
    a receipt minted against a world state that has drifted from the pinned
    fixture (ruling D2) must never be trusted, regardless of whether the
    individual case assertions happen to still pass by coincidence.
    """

    if verification.matched:
        return
    raise WorldDigestMismatchError(
        "WORLD_DIGEST verification FAILED against "
        f"{digest_path}: pinned={verification.pinned_digest} "
        f"live={verification.live_digest}. Drifted component(s): "
        f"{list(verification.drifted_components)}. A wave4_case_result.v1 "
        "receipt computed against a world state that does not match the "
        "frozen fixture must never be trusted (ruling D2) -- regenerate and "
        "re-pin WORLD_DIGEST (or fix what drifted), never suppress this "
        "check."
    )
