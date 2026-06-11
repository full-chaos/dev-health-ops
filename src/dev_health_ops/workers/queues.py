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

NOTE: CHAOS-2284 (SyncDispatchPolicy — designed, not built) will absorb this
routing decision into a single dispatch policy object. Keep this a free
function so that migration is a move, not a refactor.
"""

from __future__ import annotations

import os

# Providers with a dedicated sync queue. Must stay in lockstep with the
# ``sync.<provider>`` entries in workers.config.task_queues and the -Q lists
# in compose.yml (guarded by tests/test_compose_config.py).
SYNC_QUEUE_PROVIDERS = frozenset({"github", "gitlab", "linear", "jira", "launchdarkly"})

# Fallback queue: unknown providers, any messages already in flight on the
# legacy shared queue at deploy time, and ALL providers while the
# PROVIDER_SYNC_QUEUES_ENABLED flag is off.
DEFAULT_SYNC_QUEUE = "sync"


def _provider_sync_queues_enabled() -> bool:
    """Read the rollout flag at call time so ops/tests can flip it live."""
    return os.getenv("PROVIDER_SYNC_QUEUES_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def sync_queue_for_provider(provider: str) -> str:
    """Return the Celery queue name for a provider's sync tasks.

    With ``PROVIDER_SYNC_QUEUES_ENABLED`` unset/falsy (the default) every
    provider routes to the shared ``sync`` queue — safe for deployments whose
    workers have not yet expanded their ``-Q`` lists. With the flag enabled,
    known providers get ``sync.<provider>``; anything else falls back to the
    shared ``sync`` queue, which workers keep consuming for safety.
    """
    if not _provider_sync_queues_enabled():
        return DEFAULT_SYNC_QUEUE
    normalized = (provider or "").strip().lower()
    if normalized in SYNC_QUEUE_PROVIDERS:
        return f"sync.{normalized}"
    return DEFAULT_SYNC_QUEUE
