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
    "GRAPH_CONNECT_TIMEOUT_VAR",
    "GRAPH_MAX_CONNECTIONS_VAR",
    "GRAPH_PROJECTION_FLAG",
    "GRAPH_READ_FLAG",
    "GRAPH_READ_TIMEOUT_VAR",
    "GRAPH_SOCKET_TIMEOUT_VAR",
    "GRAPH_WRITE_TIMEOUT_VAR",
    "REQUIRE_LIVE_FLAG",
    "TRIAL_STORE_URI_VAR",
    "GraphDeadlines",
    "TrialStoreConfig",
    "graph_deadlines",
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


#: CHAOS-3631. ``GraphArmStore`` used to construct its FalkorDB client with no
#: connect, socket or operation deadline -- redis-py's documented "block
#: forever" defaults. A backend that accepts a TCP connection but never
#: answers (a firewall that drops rather than refuses, a wedged server, an
#: exhausted connection slot) then wedged the calling coroutine, and every
#: caller downstream of it, indefinitely. These five env vars are the whole
#: fix's configuration surface: connect/socket bound the transport;
#: read/write bound each store operation end to end (see
#: ``store._await_with_deadline``), split in two because they scale
#: differently -- a read is a fixed handful of round trips no matter how big
#: the partition is, while a write is proportional to batch size and, with a
#: semantic embedder, makes one real network call per node and edge (measured
#: against ``CloudEmbedder``: 200-500ms each), so a batch of a few hundred
#: records can legitimately take well over a minute. A single shared bound
#: either times out a legitimate large write or leaves a hung read waiting
#: far longer than a metadata probe should. ``max_connections`` bounds how
#: many sockets one process opens against one backend, so a run of hangs
#: cannot also exhaust file descriptors.
GRAPH_CONNECT_TIMEOUT_VAR = "CONTEXT_FABRIC_GRAPH_CONNECT_TIMEOUT_S"
GRAPH_SOCKET_TIMEOUT_VAR = "CONTEXT_FABRIC_GRAPH_SOCKET_TIMEOUT_S"
GRAPH_READ_TIMEOUT_VAR = "CONTEXT_FABRIC_GRAPH_READ_TIMEOUT_S"
GRAPH_WRITE_TIMEOUT_VAR = "CONTEXT_FABRIC_GRAPH_WRITE_TIMEOUT_S"
GRAPH_MAX_CONNECTIONS_VAR = "CONTEXT_FABRIC_GRAPH_MAX_CONNECTIONS"

#: Conservative shipped defaults. Wide enough that a healthy local or
#: in-cluster FalkorDB never trips them (the live suite's own operations
#: complete in milliseconds; a real semantic-embedded write of hundreds of
#: records measured well under two minutes), tight enough that a wedged
#: backend cannot hold a caller indefinitely.
DEFAULT_GRAPH_CONNECT_TIMEOUT_S = 5.0
DEFAULT_GRAPH_SOCKET_TIMEOUT_S = 10.0
DEFAULT_GRAPH_READ_TIMEOUT_S = 15.0
DEFAULT_GRAPH_WRITE_TIMEOUT_S = 120.0
DEFAULT_GRAPH_MAX_CONNECTIONS = 10


@dataclass(frozen=True, slots=True)
class GraphDeadlines:
    """The bounds every live graph-store operation must complete within.

    ``connect_timeout_s`` and ``socket_timeout_s`` are passed straight through
    to ``falkordb.asyncio.FalkorDB`` (redis-py's ``socket_connect_timeout`` /
    ``socket_timeout``), so a TCP handshake or a single socket read/write
    that never completes fails at the transport layer. ``read_timeout_s`` and
    ``write_timeout_s`` are enforced by ``store._await_with_deadline``
    wrapping each store operation in ``asyncio.wait_for`` -- a second, coarser
    bound over the WHOLE operation (which may be many socket round trips)
    that also catches a request stuck anywhere above the socket, which a
    socket-level timeout alone would not.
    """

    connect_timeout_s: float = DEFAULT_GRAPH_CONNECT_TIMEOUT_S
    socket_timeout_s: float = DEFAULT_GRAPH_SOCKET_TIMEOUT_S
    read_timeout_s: float = DEFAULT_GRAPH_READ_TIMEOUT_S
    write_timeout_s: float = DEFAULT_GRAPH_WRITE_TIMEOUT_S
    max_connections: int = DEFAULT_GRAPH_MAX_CONNECTIONS

    def __post_init__(self) -> None:
        for name, value in (
            ("connect_timeout_s", self.connect_timeout_s),
            ("socket_timeout_s", self.socket_timeout_s),
            ("read_timeout_s", self.read_timeout_s),
            ("write_timeout_s", self.write_timeout_s),
        ):
            if not value > 0:
                raise ValueError(
                    f"GraphDeadlines.{name} must be a positive number of "
                    f"seconds, got {value!r}; a zero or negative deadline "
                    "would refuse every operation, not bound one"
                )
        if self.max_connections <= 0:
            raise ValueError(
                "GraphDeadlines.max_connections must be a positive integer, "
                f"got {self.max_connections!r}"
            )


def _positive_float(var: str, default: float) -> float:
    raw = os.getenv(var)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{var}={raw!r} is not a number") from exc
    return value


def _positive_int(var: str, default: int) -> int:
    raw = os.getenv(var)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{var}={raw!r} is not an integer") from exc
    return value


def graph_deadlines() -> GraphDeadlines:
    """The configured deadlines, read fresh from the environment.

    Not cached: a test (or an operator via a config reload) that changes one
    of these variables between calls must see the change, and the values are
    read once per store construction / module-level probe, never per query.
    """

    return GraphDeadlines(
        connect_timeout_s=_positive_float(
            GRAPH_CONNECT_TIMEOUT_VAR, DEFAULT_GRAPH_CONNECT_TIMEOUT_S
        ),
        socket_timeout_s=_positive_float(
            GRAPH_SOCKET_TIMEOUT_VAR, DEFAULT_GRAPH_SOCKET_TIMEOUT_S
        ),
        read_timeout_s=_positive_float(
            GRAPH_READ_TIMEOUT_VAR, DEFAULT_GRAPH_READ_TIMEOUT_S
        ),
        write_timeout_s=_positive_float(
            GRAPH_WRITE_TIMEOUT_VAR, DEFAULT_GRAPH_WRITE_TIMEOUT_S
        ),
        max_connections=_positive_int(
            GRAPH_MAX_CONNECTIONS_VAR, DEFAULT_GRAPH_MAX_CONNECTIONS
        ),
    )
