"""Per-provider sync queue routing (CHAOS-2299).

Live incidents are diagnosed per-queue (``LLEN sync.linear`` answers
"is Linear stuck?" with one number), and per-provider queues allow targeted
purges without nuking every provider's in-flight syncs.

Routing is gated by the ``PROVIDER_SYNC_QUEUES_ENABLED`` env flag (default
OFF) so producers and consumers can move independently. Two-phase rollout:

1. Deploy workers whose ``-Q`` lists include the ``sync.<provider>`` queues
   (consumers first — they also still consume the shared ``sync`` queue).
2. Flip ``PROVIDER_SYNC_QUEUES_ENABLED=true`` on producers (API, beat,
   workers) to start routing to the per-provider queues.

Flipping the flag before step 1 would strand messages on queues nothing
consumes. The flag is read at call time (not import time) so it can be
toggled without restart-ordering pain.

CHAOS-2517 adds cost-class sub-queues (light/medium/heavy) gated by
``SYNC_COST_CLASS_QUEUES`` env flag (default OFF). Same two-phase rollout
applies: add the new queues to worker -Q lists first, then flip the flag.

NOTE: CHAOS-2284 (SyncDispatchPolicy — designed, not built) will absorb this
routing decision into a single dispatch policy object. Keep this a free
function so that migration is a move, not a refactor.
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Provider-level queue constants (CHAOS-2299)
# ---------------------------------------------------------------------------

# Providers with a dedicated sync queue. Must stay in lockstep with the
# ``sync.<provider>`` entries in workers.config.task_queues and the -Q lists
# in compose.yml (guarded by tests/test_compose_config.py).
SYNC_QUEUE_PROVIDERS = frozenset({"github", "gitlab", "linear", "jira", "launchdarkly"})

# Fallback queue: unknown providers, any messages already in flight on the
# legacy shared queue at deploy time, and ALL providers while the
# PROVIDER_SYNC_QUEUES_ENABLED flag is off.
DEFAULT_SYNC_QUEUE = "sync"

# ---------------------------------------------------------------------------
# Cost-class queue constants (CHAOS-2517)
# ---------------------------------------------------------------------------

# Cost-class queue names per (provider, cost_class) pair.
# Only providers that have cost-class queues declared here are eligible;
# anything else falls back to the provider queue or the shared sync queue.
# Must stay in lockstep with workers.config.task_queues.
SYNC_COST_CLASS_QUEUES: dict[tuple[str, str], str] = {
    ("github", "light"): "sync.github.light",
    ("github", "medium"): "sync.github.medium",
    ("github", "heavy"): "sync.github.heavy",
    ("gitlab", "light"): "sync.gitlab.light",
    ("gitlab", "medium"): "sync.gitlab.medium",
    ("gitlab", "heavy"): "sync.gitlab.heavy",
    ("jira", "medium"): "sync.jira.medium",
    ("linear", "medium"): "sync.linear.medium",
}

# All cost-class queue names as a frozenset for O(1) membership checks.
SYNC_COST_CLASS_QUEUE_NAMES: frozenset[str] = frozenset(SYNC_COST_CLASS_QUEUES.values())


# ---------------------------------------------------------------------------
# Feature-flag readers (call-time, not import-time)
# ---------------------------------------------------------------------------


def _provider_sync_queues_enabled() -> bool:
    """Read the rollout flag at call time so ops/tests can flip it live."""
    return os.getenv("PROVIDER_SYNC_QUEUES_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _cost_class_queues_enabled() -> bool:
    """Read the cost-class queue rollout flag at call time.

    Requires ``PROVIDER_SYNC_QUEUES_ENABLED`` to also be on; cost-class queues
    are a sub-tier of provider queues.
    """
    return os.getenv("SYNC_COST_CLASS_QUEUES", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }


# ---------------------------------------------------------------------------
# Routing helpers
# ---------------------------------------------------------------------------


def cost_class_queue_for_provider(provider: str, cost_class: str) -> str | None:
    """Return the cost-class queue name for a (provider, cost_class) pair.

    Returns ``None`` when no cost-class queue is defined for the pair (caller
    should fall back to the provider queue or the shared sync queue).
    """
    normalized = (provider or "").strip().lower()
    return SYNC_COST_CLASS_QUEUES.get((normalized, cost_class))


def sync_queue_for_provider(provider: str) -> str:
    """Return the Celery queue name for a provider's sync tasks.

    With ``PROVIDER_SYNC_QUEUES_ENABLED`` unset/falsy (the default) every
    provider routes to the shared ``sync`` queue — safe for deployments whose
    workers have not yet expanded their ``-Q`` lists. With the flag enabled,
    known providers get ``sync.<provider>``; anything else falls back to the
    shared ``sync`` queue, which workers keep consuming for safety.

    Compatibility wrapper: CHAOS-2517 dispatch policy calls :func:`route` in
    ``sync.dispatch_policy`` instead. This function is kept for callers that
    have not yet migrated.
    """
    if not _provider_sync_queues_enabled():
        return DEFAULT_SYNC_QUEUE
    normalized = (provider or "").strip().lower()
    if normalized in SYNC_QUEUE_PROVIDERS:
        return f"sync.{normalized}"
    return DEFAULT_SYNC_QUEUE
