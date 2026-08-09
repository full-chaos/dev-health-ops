"""CHAOS-3617: the Graphiti binding. Lazy, structured-only, no prose.

Graphiti is a **reference implementation under evaluation, not an approved
dependency**. Nothing here is imported at module-import time: every entry
point calls :func:`require_graphiti`, which imports on demand and raises a
message naming the optional extra if it is absent. That is what lets the
default production dependency set stay free of it while the arm still runs
end to end when the extra is installed.

Two properties of this file carry most of the arm's integrity.

**The ``fact`` string is a triple rendering, never a sentence.**
``graphiti_core.edges.EntityEdge`` requires a ``fact: str`` and its intended
value is natural language — that field is precisely where "convert the
structured record into prose so the extraction adapter is happy" would
happen, and the issue forbids exactly that. So :func:`triple_fact` renders
``"<source_canonical_id> <relationship> <target_canonical_id>"`` and nothing
else: three tokens, none of them written by anyone, all three recoverable by
:func:`parse_triple_fact`. ``test_chaos_3617_no_prose.py`` reconstructs every
edge's fact from its record and asserts equality, so an adapter cannot add a
clause without failing.

**Structured ingestion makes no model call at all.** ``add_episode`` and
``add_triplet`` both invoke the LLM — the first for extraction, the second
for edge resolution. Structured records go through
``utils.bulk_utils.add_nodes_and_edges_bulk``, which is a direct driver
write, with :class:`DeterministicEmbedder` supplying embeddings from a hash.
That makes structured projection reproducible, offline and free.

**The honest limitation of that embedder**, stated here because it would
otherwise be a silent measurement error: a hash embedding carries **no
semantic similarity whatsoever**. Nearest-neighbour search over it is
meaningless. :attr:`DeterministicEmbedder.semantic` is ``False`` and any
candidate-search path must consult that flag before claiming a semantic
match signal — exact, alias, acronym, previous-name and provider-identifier
matching are all exact lookups and are unaffected.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from dev_health_ops.api.dev.investigation_contract import RelationshipType

from .projection import GraphEdge, GraphNode, GraphProjection

if TYPE_CHECKING:  # pragma: no cover - typing only
    from graphiti_core.edges import EntityEdge
    from graphiti_core.nodes import EntityNode

__all__ = [
    "EMBEDDING_MODEL_VAR",
    "GRAPHITI_EXTRA",
    "SEMANTIC_MECHANISMS",
    "CloudEmbedder",
    "MatchMechanism",
    "TELEMETRY_ENV_VAR",
    "TRIPLE_FACT_PATTERN",
    "DeterministicEmbedder",
    "EmbeddingBackend",
    "GraphitiUnavailableError",
    "entity_node_label",
    "graphiti_module",
    "graphiti_version",
    "parse_triple_fact",
    "require_graphiti",
    "to_graphiti_edges",
    "to_graphiti_nodes",
    "triple_fact",
]

#: The optional dependency extra that installs Graphiti. Named in the error
#: message so the failure is actionable rather than an ImportError traceback.
GRAPHITI_EXTRA = "context-graph-trial"

#: A canonical triple rendering: three whitespace-separated tokens.
TRIPLE_FACT_PATTERN = re.compile(
    r"^(?P<source>\S+) (?P<relationship>[a-z_]+) (?P<target>\S+)$"
)

#: Dimension of the deterministic embedding. Matches Graphiti's own default
#: ``EMBEDDING_DIM`` so a store written with this embedder and later read
#: with a real one does not fail on a dimension mismatch — it will return
#: nonsense, which is a measurement question, not a crash.
DETERMINISTIC_EMBEDDING_DIM = 1024


#: Where the cloud embedder reads its model from. The key follows the repo's
#: existing convention (``llm/credentials.py``): ``LLM_API_KEY`` first, then
#: ``OPENAI_API_KEY``.
EMBEDDING_MODEL_VAR = "CONTEXT_FABRIC_GRAPH_EMBEDDING_MODEL"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"


class MatchMechanism(StrEnum):
    """*How* a subject match was produced, as opposed to what it matched.

    The wire's ``SubjectMatchSignal`` says what matched — an alias, an
    acronym, a fuzzy label. It cannot say how, and the distinction is the one
    that decides whether a claim is honest: ``FUZZY_LABEL`` produced by
    Levenshtein over stored labels is a real lexical match that needs no
    model, while the same signal produced by nearest-neighbour search over
    embeddings is a semantic claim that means nothing at all when the
    embedder is a hash.

    So the mechanism is tracked inside the arm and checked at emission. It
    never reaches the packet: the contract is frozen and ``extra="forbid"``,
    and this is an arm-internal integrity concern rather than something a
    consumer should branch on.
    """

    EXACT_LOOKUP = "exact_lookup"
    ALIAS_LOOKUP = "alias_lookup"
    LEXICAL_FUZZY = "lexical_fuzzy"
    EMBEDDING_SIMILARITY = "embedding_similarity"
    MODEL_INFERENCE = "model_inference"


#: Mechanisms that are meaningless unless the active embedder actually
#: carries semantics. Emitting a match produced by one of these under
#: :class:`DeterministicEmbedder` would be a confident, arbitrary ordering
#: that no downstream scorer could distinguish from a real one.
SEMANTIC_MECHANISMS: frozenset[MatchMechanism] = frozenset(
    {MatchMechanism.EMBEDDING_SIMILARITY, MatchMechanism.MODEL_INFERENCE}
)


class GraphitiUnavailableError(RuntimeError):
    """Graphiti is not installed, and the caller needed it."""


#: Graphiti's telemetry env var. Upstream default is **enabled**: it posts
#: anonymous usage events to PostHog on ``Graphiti.__init__``
#: (``graphiti_core/telemetry/telemetry.py``).
TELEMETRY_ENV_VAR = "GRAPHITI_TELEMETRY_ENABLED"


def require_graphiti() -> Any:
    """Import and return ``graphiti_core``, or raise an actionable error.

    Forces telemetry **off** before importing, unconditionally. Graphiti
    ships with usage reporting on by default, and a trial that ingests one
    organization's project, team and repository structure has no business
    opening an outbound connection to a third-party analytics host. The
    override is not "set if unset": an environment that already said
    ``true`` is overridden too, and this arm deliberately provides no way to
    turn it back on. Pinned by
    ``test_chaos_3617_backend.py::test_telemetry_is_forced_off``.
    """

    return graphiti_module()


def graphiti_module(dotted: str = "") -> Any:
    """Import ``graphiti_core`` or one of its submodules, on demand.

    Submodules are imported by name rather than reached as attributes:
    ``graphiti_core`` does not import ``graphiti_core.driver.falkordb_driver``
    for you, so ``require_graphiti().driver.falkordb_driver`` raises
    ``AttributeError`` -- an error that reads like a version incompatibility
    and is nothing of the sort.

    Telemetry is forced off here, before any import: see
    :func:`require_graphiti`.
    """

    import importlib

    os.environ[TELEMETRY_ENV_VAR] = "false"
    name = f"graphiti_core.{dotted}" if dotted else "graphiti_core"
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:  # pragma: no cover - env dependent
        if exc.name is not None and not exc.name.startswith("graphiti_core"):
            raise
        raise GraphitiUnavailableError(
            "graphiti-core is not installed. It is a reference implementation "
            "under evaluation, not an approved dependency, so it lives in the "
            f"optional extra '{GRAPHITI_EXTRA}': install with "
            f"`uv sync --extra {GRAPHITI_EXTRA}`."
        ) from exc


def graphiti_version() -> str:
    """The installed graphiti-core version, for trial artifacts.

    Read from installed distribution metadata rather than a module
    attribute, because the exact dependency version has to appear in the
    recorded run and a hand-maintained constant would drift.
    """

    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("graphiti-core")
    except PackageNotFoundError as exc:  # pragma: no cover - env dependent
        raise GraphitiUnavailableError(
            "graphiti-core is not installed, so no version can be recorded "
            "in the trial artifact"
        ) from exc


@runtime_checkable
class EmbeddingBackend(Protocol):
    """An embedder, plus the one fact a consumer must not have to guess.

    ``semantic`` is the whole point of having a protocol here rather than
    using ``EmbedderClient`` directly: a caller that ranks candidates by
    vector similarity has to be able to ask whether the vectors mean
    anything, and a bare ``EmbedderClient`` cannot answer.
    """

    @property
    def model_id(self) -> str:
        """The exact model this embedder speaks for, for trial artifacts."""

    @property
    def semantic(self) -> bool:
        """Whether the vectors carry meaning. See the class docstring."""

    async def create(self, input_data: Any) -> list[float]:
        """Embed one input."""


@dataclass(frozen=True, slots=True)
class DeterministicEmbedder:
    """A reproducible, offline, **non-semantic** embedding.

    Derives a unit vector from BLAKE2b over the input text. Same text always
    yields the same vector; different texts yield unrelated vectors with no
    relationship to meaning. It exists so structured projection can be run
    with no API key, no network and byte-identical results across runs — not
    so that anything can be searched by similarity.
    """

    dimension: int = DETERMINISTIC_EMBEDDING_DIM

    @property
    def model_id(self) -> str:
        return f"deterministic_blake2b.v1.d{self.dimension}"

    @property
    def semantic(self) -> bool:
        return False

    async def create(self, input_data: Any) -> list[float]:
        return self.embed(_as_text(input_data))

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        return [self.embed(text) for text in input_data_list]

    def embed(self, text: str) -> list[float]:
        needed = self.dimension * 4
        material = bytearray()
        counter = 0
        while len(material) < needed:
            digest = hashlib.blake2b(
                f"{counter}\0{text}".encode(), digest_size=64
            ).digest()
            material.extend(digest)
            counter += 1
        values = [
            int.from_bytes(material[index * 4 : index * 4 + 4], "big") / 2**32 - 0.5
            for index in range(self.dimension)
        ]
        norm = math.sqrt(sum(value * value for value in values)) or 1.0
        return [value / norm for value in values]


def _as_text(input_data: Any) -> str:
    if isinstance(input_data, str):
        return input_data
    if isinstance(input_data, Iterable):
        return "\0".join(str(item) for item in input_data)
    return str(input_data)


@dataclass(frozen=True, slots=True)
class CloudEmbedder:
    """A real, semantic embedder: Graphiti's OpenAI client, wrapped.

    Exists so that the retrieval capabilities under test measure Graphiti's
    actual value-add rather than a bare graph store —
    :class:`DeterministicEmbedder` produces vectors with no semantic content,
    so similarity search over it is meaningless by construction.

    Two properties this wrapper adds over using ``OpenAIEmbedder`` directly:

    * ``semantic`` is ``True``, which is what
      ``packet_builder`` consults before letting an embedding-derived match
      reach a packet;
    * ``model_id`` names the exact model, and that string is folded into the
      emitted ``projection_version`` — because a store embedded with one
      model and a store embedded with another are not the same projection,
      and comparing runs across them would be comparing different things.

    Cost is real and is bounded before any call is made: see
    ``store.GraphArmStore.write_projection``, which checks the embedding
    budget against the node+edge count it already knows.
    """

    model: str = DEFAULT_EMBEDDING_MODEL
    api_key: str | None = None

    @property
    def model_id(self) -> str:
        return f"openai_{self.model.replace('-', '_').replace('.', '_')}"

    @property
    def semantic(self) -> bool:
        return True

    @classmethod
    def from_environment(cls) -> CloudEmbedder:
        """Build from the repo's existing LLM credential convention.

        Raises rather than falling back to a non-semantic embedder: silently
        degrading would produce a run that *looks* like a semantic one and
        scores like noise, which is the worst of both.
        """

        key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not key:
            raise RuntimeError(
                "no LLM_API_KEY/OPENAI_API_KEY is set, so no semantic embedder "
                "can be built. Refusing to fall back to DeterministicEmbedder: "
                "a run that silently used hash vectors would look like a "
                "semantic run and score like noise"
            )
        return cls(
            model=os.getenv(EMBEDDING_MODEL_VAR, DEFAULT_EMBEDDING_MODEL), api_key=key
        )

    def _client(self) -> Any:
        module = graphiti_module("embedder.openai")
        return module.OpenAIEmbedder(
            config=module.OpenAIEmbedderConfig(
                embedding_model=self.model, api_key=self.api_key
            )
        )

    async def create(self, input_data: Any) -> list[float]:
        return await self._client().create(input_data=input_data)

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        return await self._client().create_batch(input_data_list)


def embedder_projection_suffix(embedder: EmbeddingBackend) -> str:
    """The embedder's contribution to ``projection_version``.

    Folded into the version rather than carried in its own field because the
    frozen packet contract has no slot for it and ``extra="forbid"`` — and
    because it belongs there anyway: two stores embedded with different
    models are different projections, and a version that called them the same
    would make two incomparable runs look comparable.
    """

    token = re.sub(r"[^a-z0-9_]+", "_", embedder.model_id.lower()).strip("_")
    if not token or not token[0].isalpha():
        token = f"e_{token}"
    return token


def triple_fact(edge: GraphEdge) -> str:
    """The canonical triple rendering of an edge. Not a sentence.

    Three tokens: the source canonical id, the frozen relationship token,
    the target canonical id. No article, no verb conjugation, no
    capitalisation, nothing an adapter chose. Canonical ids never contain
    whitespace (``OpaqueID``'s pattern forbids it), so the rendering round
    trips through :func:`parse_triple_fact` exactly.
    """

    for canonical_id in (edge.source_canonical_id, edge.target_canonical_id):
        if not canonical_id or any(char.isspace() for char in canonical_id):
            raise ValueError(
                f"canonical id {canonical_id!r} contains whitespace; the "
                "triple rendering would not round trip and the stored fact "
                "would become ambiguous"
            )
    return (
        f"{edge.source_canonical_id} "
        f"{edge.relationship.value} "
        f"{edge.target_canonical_id}"
    )


def parse_triple_fact(fact: str) -> tuple[str, RelationshipType, str]:
    """Recover ``(source_id, relationship, target_id)`` from a stored fact.

    Raises on anything that is not exactly a triple rendering, which is how
    a stored fact containing prose is detected on read rather than being
    quietly presented as evidence.
    """

    match = TRIPLE_FACT_PATTERN.fullmatch(fact)
    if match is None:
        raise ValueError(
            f"stored fact {fact!r} is not a canonical triple rendering; the "
            "graph arm never writes prose facts, so this record did not come "
            "from structured projection"
        )
    return (
        match.group("source"),
        RelationshipType(match.group("relationship")),
        match.group("target"),
    )


def entity_node_label(node: GraphNode) -> str:
    """The Graphiti node label for a projected node.

    Entity kinds get their own label so a traversal can be constrained by
    kind; observations get ``Observation`` plus their kind, which keeps them
    trivially excludable from any query that walks relationships.
    """

    if node.entity_kind is not None:
        return f"CF{node.entity_kind.value.title().replace('_', '')}"
    assert node.observation_kind is not None
    return f"CFObs{node.observation_kind.value.title().replace('_', '')}"


def to_graphiti_nodes(
    projection: GraphProjection, embedder: EmbeddingBackend
) -> list[EntityNode]:
    """Projected nodes as Graphiti ``EntityNode``s. No summaries, ever.

    ``summary`` is left empty on purpose. It is Graphiti's slot for a
    model-written regional description, and filling it from a structured
    record is the prose conversion this arm must not perform.
    ``test_chaos_3617_no_prose.py`` asserts every structured node's summary
    is ``""``.
    """

    entity_node_cls = graphiti_module("nodes").EntityNode
    nodes: list[EntityNode] = []
    for node in projection.nodes:
        attributes: dict[str, Any] = {
            "cf_canonical_id": node.canonical_id,
            "cf_org_id": node.org_id,
            "cf_source_class": node.source_class.value,
            "cf_observed_at": node.observed_at.isoformat(),
            "cf_is_entity": node.is_entity,
        }
        if node.entity_kind is not None:
            attributes["cf_entity_kind"] = node.entity_kind.value
        if node.observation_kind is not None:
            attributes["cf_observation_kind"] = node.observation_kind.value
        if node.repository_ids:
            attributes["cf_repository_ids"] = ",".join(sorted(node.repository_ids))
        if node.valid_from is not None:
            attributes["cf_valid_from"] = node.valid_from.isoformat()
        if node.valid_to is not None:
            attributes["cf_valid_to"] = node.valid_to.isoformat()
        for alias in node.aliases:
            attributes.setdefault(f"cf_alias_{alias.kind.value}", "")
            existing = str(attributes[f"cf_alias_{alias.kind.value}"])
            joined = f"{existing}\x1f{alias.value}" if existing else alias.value
            attributes[f"cf_alias_{alias.kind.value}"] = joined
        for key, value in node.attributes.items():
            attributes[f"cf_attr_{key}"] = value
        nodes.append(
            entity_node_cls(
                uuid=node.uuid,
                name=node.display_label,
                group_id=node.partition,
                labels=[entity_node_label(node)],
                created_at=node.observed_at,
                summary="",
                attributes=attributes,
                name_embedding=embedder.embed(node.canonical_id)
                if isinstance(embedder, DeterministicEmbedder)
                else None,
            )
        )
    return nodes


def to_graphiti_edges(
    projection: GraphProjection, embedder: EmbeddingBackend
) -> list[EntityEdge]:
    """Projected edges as Graphiti ``EntityEdge``s with triple facts."""

    entity_edge_cls = graphiti_module("edges").EntityEdge
    edges: list[EntityEdge] = []
    for edge in projection.edges:
        fact = triple_fact(edge)
        attributes: dict[str, Any] = {
            "cf_org_id": edge.org_id,
            "cf_relationship": edge.relationship.value,
            "cf_source_canonical_id": edge.source_canonical_id,
            "cf_source_kind": edge.source_kind.value,
            "cf_target_canonical_id": edge.target_canonical_id,
            "cf_target_kind": edge.target_kind.value,
            "cf_source_class": edge.source_class.value,
        }
        if edge.contributor_count is not None:
            attributes["cf_contributor_count"] = edge.contributor_count
        if edge.observation_ids:
            attributes["cf_observation_ids"] = ",".join(sorted(edge.observation_ids))
        edges.append(
            entity_edge_cls(
                uuid=edge.uuid,
                group_id=edge.partition,
                source_node_uuid=edge.source_uuid,
                target_node_uuid=edge.target_uuid,
                name=edge.relationship.value,
                fact=fact,
                created_at=edge.observed_at,
                valid_at=edge.valid_from,
                invalid_at=edge.valid_to,
                reference_time=edge.observed_at,
                attributes=attributes,
                fact_embedding=embedder.embed(fact)
                if isinstance(embedder, DeterministicEmbedder)
                else None,
            )
        )
    return edges
