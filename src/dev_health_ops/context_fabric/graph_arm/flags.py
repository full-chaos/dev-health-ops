"""CHAOS-3617: the arm's flags and trial-datastore configuration.

Two flags, and they are **independent** because the issue requires it:

* :func:`graph_projection_enabled` — may the arm *write* to the trial store;
* :func:`graph_read_enabled` — may an investigation *read* from it.

Independent, not nested, so a trial can project without any read path being
reachable (the safe order to bring the arm up), and so switching reads off
does not stop the projection and thereby reset the watermark. Both default
OFF: the repo's convention is ``os.getenv(...) == "1"``, which makes "unset"
and "anything else" both mean off, and there is no configuration that turns
either on implicitly.

``ASK_DEV_*`` is deliberately *not* the prefix. These flags gate a shadow
trial that never touches the Ask Dev answer path, and naming them
``ASK_DEV_GRAPH_*`` would invite exactly the coupling the corrective plan
forbids.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

__all__ = [
    "GRAPH_PROJECTION_FLAG",
    "GRAPH_READ_FLAG",
    "REQUIRE_LIVE_FLAG",
    "TRIAL_STORE_URI_VAR",
    "TrialStoreConfig",
    "graph_projection_enabled",
    "graph_read_enabled",
    "live_store_required",
    "trial_store_config",
]

GRAPH_PROJECTION_FLAG = "CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED"
GRAPH_READ_FLAG = "CONTEXT_FABRIC_GRAPH_READ_ENABLED"
TRIAL_STORE_URI_VAR = "CONTEXT_FABRIC_GRAPH_STORE_URI"
TRIAL_STORE_PASSWORD_VAR = "CONTEXT_FABRIC_GRAPH_STORE_PASSWORD"

#: When set to ``1``, a test that would otherwise skip because no live trial
#: store is reachable must FAIL instead. Exists because a skipped
#: verification reads as coverage: the reproduction procedure sets this, and
#: ``test_chaos_3617_live_gate.py`` fails loudly if the store is absent.
REQUIRE_LIVE_FLAG = "CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE"


def _enabled(name: str) -> bool:
    return os.getenv(name) == "1"


def graph_projection_enabled() -> bool:
    """Whether the arm may write to the trial graph store. Default off."""

    return _enabled(GRAPH_PROJECTION_FLAG)


def graph_read_enabled() -> bool:
    """Whether an investigation may read from the trial graph store.

    Independent of :func:`graph_projection_enabled` on purpose — see the
    module docstring.
    """

    return _enabled(GRAPH_READ_FLAG)


def live_store_required() -> bool:
    """Whether an unreachable trial store must fail rather than skip."""

    return _enabled(REQUIRE_LIVE_FLAG)


@dataclass(frozen=True, slots=True)
class TrialStoreConfig:
    """Where the isolated trial datastore lives.

    ``uri`` is a FalkorDB endpoint (``falkor://host:port``). There is no
    default host and no default port that would reach a real service: an
    unset :data:`TRIAL_STORE_URI_VAR` yields ``None`` from
    :func:`trial_store_config` and every caller treats that as "no trial
    store configured", so a misconfigured environment cannot silently
    project into whatever happens to be listening on 6379.
    """

    uri: str
    password: str | None = None

    @property
    def host(self) -> str:
        return self._parts()[0]

    @property
    def port(self) -> int:
        return self._parts()[1]

    def _parts(self) -> tuple[str, int]:
        raw = self.uri
        for prefix in ("falkor://", "redis://"):
            if raw.startswith(prefix):
                raw = raw[len(prefix) :]
                break
        else:
            raise ValueError(
                f"trial store uri {self.uri!r} must start with falkor:// or "
                "redis://; an unschemed value is ambiguous enough to point at "
                "the wrong service"
            )
        raw = raw.split("/", 1)[0]
        host, _, port = raw.partition(":")
        if not host:
            raise ValueError(f"trial store uri {self.uri!r} names no host")
        if not port:
            raise ValueError(
                f"trial store uri {self.uri!r} names no port; the trial store "
                "runs on its own isolated port and must be stated explicitly"
            )
        return host, int(port)


def trial_store_config() -> TrialStoreConfig | None:
    """The configured trial store, or ``None`` when none is configured."""

    uri = os.getenv(TRIAL_STORE_URI_VAR)
    if not uri:
        return None
    return TrialStoreConfig(uri=uri, password=os.getenv(TRIAL_STORE_PASSWORD_VAR))
