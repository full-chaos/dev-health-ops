"""What a trial artifact must be bound to before anyone may cite it.

CHAOS-3619 requires artifacts bound to "exact commits/images/configuration,
Graphiti/backend versions, projection generation and corpus hashes". This
module produces that binding, and every value in it is **read from the thing
itself** rather than typed in.

That is the whole design rule here. A hand-maintained version block is a
second source of truth for facts that already exist, and its first drift is
silent: the artifact keeps claiming graphiti-core 0.29.3 while the run used
something else, and nothing fails. So the commit comes from ``git``, the
dependency versions come from the installed distributions' own metadata, the
corpus hash comes from the corpus's committed manifest, and the arm versions
come from the modules that stamp them onto packets.

The one thing that cannot be derived is whether the tree was clean. It is
recorded rather than enforced: a dirty-tree run is a legitimate exploratory
run and refusing it would push people to record nothing at all. What must
never happen is a dirty-tree run being cited as reproducible, so the flag is
part of the binding and the report prints it.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import asdict, dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

__all__ = [
    "TRIAL_ARTIFACT_SCHEMA_VERSION",
    "TrialBinding",
    "collect_binding",
]

#: The artifact's own shape version. Distinct from the packet's schema
#: version and from the shadow record's: a consumer that parses this file
#: needs to know which layout it is reading, and an unversioned artifact
#: read by a later tool is a silent drift surface.
TRIAL_ARTIFACT_SCHEMA_VERSION = "chaos_3619_trial_results.v1"

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

#: The corpus's committed manifest. It already carries a sha256 per exported
#: file, so hashing the manifest binds every one of them transitively.
_CORPUS_MANIFEST = (
    _REPOSITORY_ROOT
    / "contracts"
    / "ask-dev-investigation-corpus"
    / "v1"
    / "manifest.json"
)

#: The frozen CHAOS-3615 contract manifest. Bound for the same reason: a
#: packet scored against one contract revision is not comparable with a
#: packet scored against another.
_CONTRACT_MANIFEST = (
    _REPOSITORY_ROOT / "contracts" / "ask-dev-investigation" / "v1" / "manifest.json"
)

#: Distributions whose exact version changes what a graph run means.
_BOUND_DISTRIBUTIONS = ("graphiti-core", "falkordb", "redis", "pydantic")


def _git(*args: str) -> str:
    """One git query, or an explicit marker. Never a silent empty string."""

    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=_REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:  # pragma: no cover
        return f"<git unavailable: {type(exc).__name__}>"
    if completed.returncode != 0:
        return f"<git failed: {completed.stderr.strip()[:120]}>"
    return completed.stdout.strip()


def _sha256_of(path: Path) -> str:
    """A file's digest, or a marker naming the file that was missing.

    Deliberately not an empty string and deliberately not a raised
    exception. An absent corpus manifest must be visible in the artifact --
    "this run was not bound to a corpus" is exactly the kind of fact a
    reader needs and a blank field hides.
    """

    if not path.exists():
        return f"<missing: {path.relative_to(_REPOSITORY_ROOT)}>"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _distribution_version(name: str) -> str:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "<not installed>"


@dataclass(frozen=True, slots=True)
class TrialBinding:
    """Everything a later reader needs to know what produced a result set."""

    schema_version: str
    commit: str
    tree_clean: bool
    branch: str
    corpus_version: str
    corpus_manifest_sha256: str
    contract_manifest_sha256: str
    packet_schema_version: str
    shadow_record_schema_version: str
    native_arm_id: str
    native_projection_version: str
    graph_arm_id: str
    graph_projection_version: str
    graph_query_version: str
    graph_ranking_version: str
    graph_embedder_model_id: str
    graph_attachment_encoding: str
    trial_store_backend: str
    dependency_versions: dict[str, str] = field(default_factory=dict)
    per_case_timeout_seconds: float = 0.0
    notes: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def collect_binding(
    *,
    per_case_timeout_seconds: float,
    trial_store_backend: str,
    graph_embedder_model_id: str,
    notes: tuple[str, ...] = (),
) -> TrialBinding:
    """Read the binding off the running system.

    ``trial_store_backend`` and ``graph_embedder_model_id`` are arguments
    rather than derived because they are properties of the RUN -- which
    server it wrote to, which embedder wrote the vectors -- and the run is
    the only thing that knows. Every other field is derived here so it
    cannot be mistyped.
    """

    from dev_health_ops.api.dev.investigation_corpus import world
    from dev_health_ops.api.dev.investigation_shadow import (
        INVESTIGATION_SHADOW_RECORD_SCHEMA_VERSION,
    )
    from dev_health_ops.context_fabric.graph_arm import backend as graph_backend
    from dev_health_ops.context_fabric.graph_arm.packet_builder import (
        ARM_ID as GRAPH_ARM_ID,
    )
    from dev_health_ops.context_fabric.graph_arm.packet_builder import RANKING_VERSION
    from dev_health_ops.context_fabric.graph_arm.projection import (
        PROJECTION_VERSION as GRAPH_PROJECTION_VERSION,
    )
    from dev_health_ops.context_fabric.graph_arm.readback import QUERY_VERSION
    from dev_health_ops.context_fabric.native_arm.projection import (
        NATIVE_ARM_ID,
        NATIVE_PROJECTION_VERSION,
    )

    status = _git("status", "--porcelain")
    return TrialBinding(
        schema_version=TRIAL_ARTIFACT_SCHEMA_VERSION,
        commit=_git("rev-parse", "HEAD"),
        # An error marker is not "clean". ``status`` returning a git-failure
        # string would otherwise be truthy in the wrong direction if this
        # were written as ``not status``.
        tree_clean=status == "",
        branch=_git("rev-parse", "--abbrev-ref", "HEAD"),
        corpus_version=world.CORPUS_VERSION,
        corpus_manifest_sha256=_sha256_of(_CORPUS_MANIFEST),
        contract_manifest_sha256=_sha256_of(_CONTRACT_MANIFEST),
        packet_schema_version="ask_dev_investigation_packet.v1",
        shadow_record_schema_version=INVESTIGATION_SHADOW_RECORD_SCHEMA_VERSION,
        native_arm_id=NATIVE_ARM_ID,
        native_projection_version=NATIVE_PROJECTION_VERSION,
        graph_arm_id=GRAPH_ARM_ID,
        graph_projection_version=GRAPH_PROJECTION_VERSION,
        graph_query_version=QUERY_VERSION,
        graph_ranking_version=RANKING_VERSION,
        graph_embedder_model_id=graph_embedder_model_id,
        graph_attachment_encoding=graph_backend.ATTACHMENT_ENCODING,
        trial_store_backend=trial_store_backend,
        dependency_versions={
            name: _distribution_version(name) for name in _BOUND_DISTRIBUTIONS
        },
        per_case_timeout_seconds=per_case_timeout_seconds,
        notes=notes,
    )


def binding_digest(binding: TrialBinding) -> str:
    """A stable digest of one binding, for citing a run in one token."""

    return hashlib.sha256(
        json.dumps(binding.to_json(), sort_keys=True).encode("utf-8")
    ).hexdigest()
