"""Per-provider sync queue routing (CHAOS-2299).

Live incidents are diagnosed per-queue (``LLEN sync.linear`` answers
"is Linear stuck?" with one number), and per-provider queues allow targeted
purges without nuking every provider's in-flight syncs.

NOTE: CHAOS-2284 (SyncDispatchPolicy — designed, not built) will absorb this
routing decision into a single dispatch policy object. Keep this a free
function so that migration is a move, not a refactor.
"""

from __future__ import annotations

# Providers with a dedicated sync queue. Must stay in lockstep with the
# ``sync.<provider>`` entries in workers.config.task_queues and the -Q lists
# in compose.yml (guarded by tests/test_compose_config.py).
SYNC_QUEUE_PROVIDERS = frozenset({"github", "gitlab", "linear", "jira", "launchdarkly"})

# Fallback queue: unknown providers and any messages already in flight on the
# legacy shared queue at deploy time.
DEFAULT_SYNC_QUEUE = "sync"


def sync_queue_for_provider(provider: str) -> str:
    """Return the Celery queue name for a provider's sync tasks.

    Known providers get ``sync.<provider>``; anything else falls back to the
    shared ``sync`` queue, which workers keep consuming for safety.
    """
    normalized = (provider or "").strip().lower()
    if normalized in SYNC_QUEUE_PROVIDERS:
        return f"sync.{normalized}"
    return DEFAULT_SYNC_QUEUE
