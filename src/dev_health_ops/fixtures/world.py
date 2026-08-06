"""``dev-hops fixtures world`` -- CHAOS-3219 versioned deterministic fixture
world (``ask-dev-world.v1``).

Loads ``tests/acceptance/world/ask-dev-world.v1/{world,subjects,sources}.json``,
seeds a multi-org Ask Dev world by REUSING the existing single-repo fixture
machinery (``fixtures.runner.run_fixtures_generation``, called once per named
repo -- ``demo_repo_name`` already returns a caller's exact ``--repo-name``
unchanged whenever ``repo_count <= 1``, so no changes to ``runner.py``'s
generation loop were needed), then layers world-specific realizations
(PROJECT catalog rows, per-source sync-health matrix, conflicting-evidence
CI runs, retention-aged conversations) on top, and finally computes/verifies
a ``WORLD_DIGEST`` content hash of the generated state.

Determinism (CHAOS-3392 lesson): every identifier is ``uuid.uuid5`` derived
from ``master_seed``/id-seed strings, and every relative timestamp is
derived from ``world.json``'s pinned ``now`` -- this module never calls
``datetime.now()``/``utcnow()`` itself. ``run_fixtures_generation`` (reused,
not owned by this module) DOES call ``datetime.now(timezone.utc)`` at dozens
of sites across ``fixtures/generators/*.py``, ``metrics/job_daily.py`` and
``work_graph/builder.py`` to anchor its own day-window loops -- rewriting
all of that shared, widely-used generation code to accept an injectable
clock was judged out of this lane's safe blast radius (it would touch
every existing caller of ``fixtures generate``, not just this new
subcommand). Instead, ``_frozen_clock`` (below) freezes ``datetime.now()``
for the DURATION of each ``run_fixtures_generation`` call by swapping the
``datetime`` name each of those modules imported at call time -- a
call-site-local, fully-reverted patch that only ``fixtures world`` ever
activates; ``fixtures generate`` and every other caller is untouched. This
was verified empirically against a live scratch database rather than
assumed correct from the source read alone -- both because the module list
below must stay in sync with wherever ``run_fixtures_generation`` gains a
new ``datetime.now()`` call site in the future, and because a monkeypatch
covering the wrong scope would fail exactly the kind of silent-drift
category CHAOS-3392 itself was. CORRECTED 2026-08-05 (CHAOS-3432): a prior
version of this sentence claimed "two full generations -> identical
WORLD_DIGEST" as an already-proven, closed fact. That is only true for
SINGLE-generation pinning (this digest correctly detects drift within one
generated database, live-verified repeatedly this round via HIGH-3/HIGH-5
mutation tests). Cross-generation reproducibility -- two INDEPENDENT
``fixtures world`` runs producing an IDENTICAL digest -- is NOT proven; see
``world.json``'s ``cross_generation_digest_status`` field and
``_frozen_clock``'s own docstring below for the full, current evidence
trail.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import logging
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from dev_health_ops.fixtures import world_verify
from dev_health_ops.fixtures.generators import conflicts as conflicts_gen
from dev_health_ops.fixtures.generators import projects as projects_gen
from dev_health_ops.fixtures.generators import retention_conversations as conv_gen
from dev_health_ops.fixtures.generators import source_health
from dev_health_ops.fixtures.runner import (
    _MISSING_TABLE_MARKERS,
    run_fixtures_generation,
)
from dev_health_ops.storage import run_with_store

WORLD_SCHEMA_VERSION = "ask_dev_world.v1"

#: Every module ``run_fixtures_generation`` (transitively) calls that binds
#: ``datetime.now(timezone.utc)`` to its own module-level ``datetime`` name
#: (``from datetime import datetime, timezone``) to anchor a day-window loop
#: or a ``last_synced``/``computed_at`` stamp. Enumerated by grepping
#: ``fixtures/generators/*.py``, ``fixtures/runner.py``,
#: ``metrics/job_daily.py`` and ``work_graph/builder.py`` for
#: ``datetime.now(`` call sites, then confirmed empirically (see
#: ``_frozen_clock``'s docstring) -- NOT a claim derived from reading alone.
#:
#: HIGH-4 correction (2026-08-05): that enumeration missed two modules, both
#: one hop outside the directories actually grepped:
#:   - ``dev_health_ops.storage.clickhouse`` -- ``store.insert_git_commit_
#:     data`` and 30+ sibling methods each default ``last_synced``/
#:     ``synced_at`` to ``datetime.now(timezone.utc)`` whenever the caller
#:     doesn't supply an explicit value, which the base generator's commit/
#:     PR/review/deployment/work_item/ci_pipeline_run records never do.
#:   - ``dev_health_ops.models.operational`` -- ``CanonicalOperationalEntity``
#:     (the base class ``OperationalIncident`` and friends inherit) defaults
#:     ``observed_at``/``last_synced`` via ``field(default_factory=_utcnow)``,
#:     and ``_utcnow()`` reads that module's OWN ``datetime`` name -- hit by
#:     the standard per-repo generator's ``write_operational_batch`` call.
#: Both are real production behavior for an actual sync (correctly stamping
#: "when did we really observe this"), so the fix does not change either
#: module's default -- each is patched INTO frozen-clock scope only for the
#: duration of ``fixtures world``'s own generation call, exactly like every
#: other module here. Found live: the first two-clean-generation
#: identical-digest run after HIGH-3 widened the digest to include these
#: same watermark columns (a prior digest scope that excluded them could
#: never have caught this -- the nondeterminism was always there, just
#: unobserved). Confirmed via direct row comparison between two scratch
#: databases: every column matched except the watermark columns, which
#: showed each run's own real wall-clock generation time.
#:
#: Second pass (same day, same technique, one more table found): after the
#: two modules above closed git_commits/deployments/git_pull_requests/
#: git_pull_request_reviews/work_items/operational_incidents, a re-run still
#: drifted on ``ci_pipeline_runs`` -- ``insert_testops_pipeline_runs`` (the
#: extended-row path ``fixtures generate``'s ClickHouse branch actually uses
#: for this table, per ``runner.py``'s own "MV fires exactly once" comment)
#: is attached to ``ClickHouseStore`` from a SEPARATE module,
#: ``storage.mixins.testops_cicd`` -- its own ``datetime`` name, unaffected
#: by patching ``storage.clickhouse``. ``work_unit_investments`` also still
#: drifted, but NOT on its ``computed_at`` (already excluded by
#: ``_VOLATILE_COLUMNS`` -- this one really was just a wall-clock reformat
#: with no digest effect): direct row comparison showed every column
#: identical except ``categorization_run_id``, a `uuid.uuid4().hex` invoice
#: number with no wall-clock component at all (``work_graph.investment.
#: materialize``'s ``run_id = config.run_id or uuid.uuid4().hex``, never
#: supplied by ``materialize_fixture_investments``) -- clock-freezing
#: cannot fix a pure-randomness source; see ``_VOLATILE_COLUMNS``'s own
#: entry for that half of the fix. ``work_graph.investment.materialize`` is
#: still added here too, for its OWN wall-clock ``computed_at`` fallback --
#: currently harmless to the digest (excluded), but freezing it removes a
#: latent trap for any future column that stops being excluded, the same
#: way HIGH-3 just did to four other tables.
_CLOCK_PATCHED_MODULES: tuple[str, ...] = (
    "dev_health_ops.fixtures.generator",
    "dev_health_ops.fixtures.runner",
    "dev_health_ops.fixtures.generators.ai_governance",
    "dev_health_ops.fixtures.generators.ai_workflow",
    "dev_health_ops.fixtures.generators.commits",
    "dev_health_ops.fixtures.generators.incidents",
    "dev_health_ops.fixtures.generators.interactions",
    "dev_health_ops.fixtures.generators.investments",
    "dev_health_ops.fixtures.generators.pipelines",
    "dev_health_ops.fixtures.generators.prs",
    "dev_health_ops.fixtures.generators.product_telemetry",
    "dev_health_ops.fixtures.generators.teams",
    "dev_health_ops.fixtures.generators.work_items",
    "dev_health_ops.metrics.job_daily",
    "dev_health_ops.models.operational",
    "dev_health_ops.storage.clickhouse",
    "dev_health_ops.storage.mixins.testops_cicd",
    "dev_health_ops.work_graph.builder",
    "dev_health_ops.work_graph.investment.materialize",
)


class _FrozenDateTime(datetime):
    """A ``datetime`` subclass whose ``now()``/``utcnow()`` return a single
    pinned instant, for swapping in as the ``datetime`` name inside another
    module for the duration of :func:`_frozen_clock`."""

    _frozen_now: datetime

    @classmethod
    def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
        if tz is not None:
            return cls._frozen_now.astimezone(tz)
        return cls._frozen_now.replace(tzinfo=None)

    @classmethod
    def utcnow(cls) -> datetime:  # type: ignore[override]  # noqa: D102
        return cls._frozen_now.replace(tzinfo=None)


@contextmanager
def _frozen_clock(pinned_now: datetime):
    """Freeze ``datetime.now()`` to ``pinned_now`` for every module in
    :data:`_CLOCK_PATCHED_MODULES`, for the lifetime of the ``with`` block.

    Call-site-local and fully reverted in a ``finally`` even on exception --
    only ``fixtures world``'s own call to ``run_fixtures_generation`` is
    ever wrapped in this; ``fixtures generate`` and every other entry point
    into the same generator modules runs with the real, unpatched
    ``datetime`` exactly as before.

    KNOWN RESIDUAL GAP -- CHAOS-3432, declared-blocked, disclosed not
    hidden (see ``world.json``'s ``cross_generation_digest_status`` field,
    which every ``fixtures world`` run logs a WARNING against). CORRECTED
    2026-08-05 twice same day -- earlier versions of this paragraph
    described theories later disproven by direct evidence; see git history
    if the prior wording is needed.

    This clock-freeze work (plus the HIGH-3/HIGH-4 module-enumeration pass
    the same day -- ``storage.clickhouse``, ``models.operational``,
    ``storage.mixins.testops_cicd``, ``work_graph.investment.materialize``,
    wrapping this postprocess handler itself, and excluding
    ``categorization_run_id`` from the digest) closed every WATERMARK-
    COLUMN and RANDOM-IDENTIFIER source of drift found live, each
    individually confirmed via a live two-generation rerun: real wall-clock
    defaults in four separate modules the original module-list enumeration
    missed (all one hop outside the directories it grepped), a
    postprocess handler that ran entirely outside any frozen-clock scope,
    and a per-run-random (not wall-clock) categorization run id that
    needed digest exclusion rather than freezing. ``teams`` (the one write
    that deliberately keeps real wall-clock time to win a
    ReplacingMergeTree race) was confirmed excluded/safe across every one
    of these reruns, not merely assumed.

    What remains open, root-caused down to a leading hypothesis but NOT
    confirmed: an isolated, one-repo-at-a-time row-count divergence
    (``ci_pipeline_runs``/``deployments``/``work_graph_pr_commit``/
    ``work_unit_investments``) that affects a DIFFERENT single repo each
    run, while every other repo -- including the elevated-volume
    ``probe/source-truncated-workgraph`` -- is exactly identical. Ruled
    out by direct evidence, not assumption: (1) the mock LLM provider
    (``llm/providers/mock.py``) has zero ``random`` usage; (2) a "global
    random state desyncs whichever repo runs next" theory, contradicted by
    every repo generated AFTER the affected one (fixed sort order) being
    byte-identical across runs; (3) ``PYTHONHASHSEED=0`` pinned identically
    across both processes -- did NOT converge the digest, and the drifted
    table/repo set was DIFFERENT and LARGER than the un-pinned run,
    evidence against a hash-seed-driven mechanism; (4) an
    org-settings-driven LLM-concurrency override silently defeating
    ``llm_concurrency=1`` -- checked live against the actual scratch
    Postgres, ``resolve_llm_org_settings_concurrency`` returns ``None`` as
    expected; (5) duplicate ``work_graph_edges`` rows causing a
    non-deterministic ``argMax(..., last_synced)`` tie-break in
    ``fetch_work_graph_edges`` (whose own docstring already documents an
    ``ORDER BY`` guard against exactly this class) -- checked live, zero
    duplicate edge identities exist for the affected org. LEADING, NOT YET
    CONFIRMED hypothesis: concurrent ClickHouse batch insertion via
    ``fixtures/runner.py``'s ``_insert_batches`` (``asyncio.Semaphore`` +
    ``asyncio.gather``, ``MAX_WORKERS`` defaulting to 4, used for every
    ClickHouse table insert in the generation path) -- the discriminating
    experiment (rerun both generations with ``MAX_WORKERS=1``, forcing
    every batch fully serial) was in flight when this investigation was
    stopped by explicit user direction (time/token budget), not by a
    negative result. Tracked as CHAOS-3432, not hidden.
    """

    frozen = type("_FrozenDateTime", (_FrozenDateTime,), {"_frozen_now": pinned_now})
    originals: dict[str, Any] = {}
    for module_name in _CLOCK_PATCHED_MODULES:
        module = importlib.import_module(module_name)
        if hasattr(module, "datetime"):
            originals[module_name] = module.datetime
            setattr(module, "datetime", frozen)  # noqa: B010
    try:
        yield
    finally:
        for module_name, original in originals.items():
            module = importlib.import_module(module_name)
            setattr(module, "datetime", original)  # noqa: B010


#: Same fixture namespace every generator in this package derives
#: deterministic ids from.
FIXTURE_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

#: Column names excluded from WORLD_DIGEST hashing across every table.
#: Two categories, both non-seed-derived by construction:
#: 1. wall-clock artifacts of insertion/sync time (the ``_frozen_clock``
#:    fix pins these deterministically during generation too, but excluding
#:    them here is also correct defense-in-depth for any caller of
#:    ``compute_world_digest`` that runs outside a frozen-clock window,
#:    e.g. ``--verify-digest`` against a database another process wrote to).
#: 2. ``feature_id`` -- ``org_feature_overrides.feature_id`` references
#:    ``feature_flags.id``, a row Alembic migrations 0067/0070 seed with
#:    ``uuid.uuid4()`` (verified empirically: the SAME migration run against
#:    two independently-migrated scratch databases produced two DIFFERENT
#:    ``feature_flags.id`` values for the SAME ``key='ask_dev'`` row). That
#:    id is environment-specific migration-application entropy, never
#:    fixture-world content -- excluding it here does not weaken the guard,
#:    since ``org_feature_overrides.is_enabled``/``reason``/``org_id`` (the
#:    columns that actually encode this world's content) remain hashed.
_VOLATILE_COLUMNS = frozenset(
    {
        "last_synced",
        "updated_at",
        "created_at",
        "computed_at",
        "synced_at",
        "started_at",
        "ended_at",
        "queued_at",
        "finished_at",
        "last_sync_at",
        "feature_id",
        # HIGH-4 (2026-08-05): work_unit_investments.categorization_run_id
        # is `config.run_id or uuid.uuid4().hex` (work_graph.investment.
        # materialize) -- a fresh random hex string every run, with no
        # wall-clock component at all, so no amount of clock-freezing can
        # make it reproduce across two generations. Same category as
        # `feature_id` above: a real per-run identifier that legitimately
        # varies, not a content field. Found live via direct row
        # comparison between two scratch databases: every OTHER column on
        # this table matched exactly.
        "categorization_run_id",
    }
)

#: Codex HIGH-3 (2026-08-05): the blanket exclusion above is correct for
#: MOST columns on MOST tables (genuine wall-clock insertion artifacts),
#: but for the specific tables whose watermark column IS what sources.json
#: claims a current/stale/measured-zero state FROM, blanket-excluding it
#: means a regression in aging/zeroing (e.g. `age_source_rows` silently
#: no-op'ing, or `write_and_verify_measured_zero_metric`'s delete step
#: regressing) would leave the digest GREEN even though the very thing the
#: state claims about is now false. Per-table, per-field JUSTIFIED
#: overrides: these columns are KEPT (not excluded) for exactly these
#: tables, because with the CHAOS-3392/_frozen_clock fix + explicit
#: deterministic aging (source_health.age_source_rows/zero_out_source) +
#: the HIGH-1 measured-zero postcondition, every one of these values is
#: now fully seed-and-pinned_now-derived, not wall-clock-derived -- so
#: including them does not reintroduce cross-run digest instability, it
#: only makes the digest ABLE to catch a regression that un-does that
#: determinism.
_WATERMARK_COLUMNS_TO_KEEP_BY_TABLE: dict[str, frozenset[str]] = {
    "git_commits": frozenset({"last_synced"}),
    "git_pull_requests": frozenset({"last_synced"}),
    "git_pull_request_reviews": frozenset({"last_synced"}),
    "deployments": frozenset({"last_synced"}),
    "work_items": frozenset({"last_synced"}),
    "ci_pipeline_runs": frozenset({"last_synced"}),
    "dora_metrics_daily": frozenset({"computed_at"}),
}


def _volatile_columns_for_table(table: str) -> frozenset[str]:
    """The exclusion set for ``table``: the global default, MINUS whatever
    watermark column(s) that specific table's entry in
    :data:`_WATERMARK_COLUMNS_TO_KEEP_BY_TABLE` says to keep (i.e. hash).
    """

    keep = _WATERMARK_COLUMNS_TO_KEEP_BY_TABLE.get(table, frozenset())
    return _VOLATILE_COLUMNS - keep


#: ClickHouse tables the digest covers, scoped by ``org_id``. Codex HIGH-3
#: (2026-08-05): expanded from the original 7 (repos/teams/projects/
#: work_items/git_pull_requests/ci_pipeline_runs/deployments) to also
#: cover git_commits, git_pull_request_reviews, dora_metrics_daily (the
#: measured-zero/DORA table), work_unit_investments (investment
#: categorization), and the two work-graph fast-path tables -- every
#: table any subjects.json/sources.json claim in this world actually
#: consumes, not an arbitrary subset. All 12 confirmed (empirically, via
#: `DESCRIBE TABLE` against a live scratch database -- not assumed from
#: a migration-file grep, which missed several ADD COLUMN sites) to carry
#: an `org_id` column, so the existing per-org WHERE clause covers them
#: uniformly.
_CLICKHOUSE_DIGEST_TABLES: tuple[str, ...] = (
    "repos",
    "teams",
    "projects",
    "work_items",
    "git_commits",
    "git_pull_requests",
    "git_pull_request_reviews",
    "ci_pipeline_runs",
    "deployments",
    "operational_incidents",
    "dora_metrics_daily",
    "work_unit_investments",
    "work_graph_issue_pr",
    "work_graph_pr_commit",
)

#: Postgres tables the digest covers. ``org_scoped=True`` filters by
#: ``org_id``; ``id_seed_scoped=True`` filters ``id IN (<world's own ids>)``
#: (needed for ``users``, which carries no ``org_id`` column of its own).
_POSTGRES_DIGEST_TABLES: tuple[tuple[str, str], ...] = (
    ("organizations", "id"),
    ("users", "id"),
    ("memberships", "org_id"),
    ("sync_configurations", "org_id"),
    ("org_retention_policies", "org_id"),
    ("org_feature_overrides", "org_id"),
    ("dev_conversations", "org_id"),
    ("dev_runs", "org_id"),
    ("dev_run_subject_sets", "org_id"),
)


class WorldManifestError(ValueError):
    """The world/subjects/sources manifest failed shape or cross-reference
    validation -- fail loud before any generation happens."""


class WorldDigestDriftError(RuntimeError):
    """``--verify-digest`` found the live database no longer matches the
    pinned ``WORLD_DIGEST`` file."""


class UnsafeSinkError(ValueError):
    """The target database does not look like a disposable scratch database.

    Raised at the very top of :func:`run_fixtures_world`, BEFORE any store,
    session, or client is constructed -- ``fixtures world`` issues
    destructive mutations (``ALTER TABLE ... DELETE``, the delete-and-
    reinsert in ``source_health.age_source_rows``, ``DROP``/``CREATE
    DATABASE`` in the operator's own setup) that must never reach a shared
    dev or production database. Codex adversarial review (CRITICAL,
    2026-08-05): ``--sink``/``--postgres-uri`` previously accepted ANY
    ClickHouse/Postgres URI including the shared dev ``default``/
    ``devhealth`` databases with no check at all -- this generalizes
    ``ci/local_validate.sh``'s own hardcoded "``CLICKHOUSE_URI`` must never
    default to ``/default``" rule (the CHAOS-2604-era safety contract) into
    an entrypoint guard every ``fixtures world`` invocation goes through,
    not just the one CI script that happened to remember it.
    """


#: Database names that are ALWAYS unsafe for ``fixtures world``, regardless
#: of whether they happen to contain the scratch marker below --
#: defense-in-depth on top of the positive allowlist-by-convention check.
#: Sourced from the actual shared dev stack this repo's compose files
#: provision (``ops/compose.yml``'s ``POSTGRES_DB=devhealth``; ClickHouse's
#: always-present ``default`` database) plus Postgres' own reserved
#: administrative/template databases (a fixture run against ``template1``
#: would corrupt the template every future ``CREATE DATABASE`` clones from).
_HARD_DENIED_DATABASE_NAMES = frozenset(
    {"default", "devhealth", "postgres", "template0", "template1", ""}
)

#: A database name must contain at least one of these tokens
#: (case-insensitive) to be accepted as an explicit, disposable scratch
#: target. Deliberately a POSITIVE allowlist-by-convention, not a denylist
#: of "known bad" names: a denylist silently fails to protect against a
#: real, currently-unknown database a future compose/migration change
#: introduces, while a positive marker requirement fails closed against
#: anything not explicitly opted in. Both entries are existing,
#: already-in-use conventions, not invented for this guard: every scratch
#: database this lane's own live verification runs used
#: (``chaos3219_world_scratch``, ``..._scratch_b``, ...) contains
#: ``"scratch"``; ``ci/local_validate.sh``'s own scratch db
#: (``ci_local_validate_<hash>``) does not contain the literal substring
#: ``"scratch"`` but does contain ``"ci_local_validate"``, so that prefix
#: is accepted too rather than forcing a naming-convention mismatch
#: against a pattern this repo already ships.
_SCRATCH_MARKERS = ("scratch", "ci_local_validate")


def _looks_like_scratch_name(lowered_db_name: str) -> bool:
    return any(marker in lowered_db_name for marker in _SCRATCH_MARKERS)


def _database_name_from_uri(uri: str) -> str:
    """The path component of a ClickHouse/Postgres URI, e.g.
    ``clickhouse://ch:ch@host:8123/foo?x=1`` -> ``"foo"``. An omitted path
    (``clickhouse://ch:ch@host:8123``) returns ``""`` -- clickhouse-connect
    treats a missing path as the ``default`` database, so an empty name
    must be treated exactly as unsafe as the literal string ``"default"``
    (see ``_HARD_DENIED_DATABASE_NAMES``, which includes ``""``).
    """

    return urlparse(uri).path.lstrip("/")


def _require_scratch_database(uri: str | None, *, kind: str) -> None:
    """Fail loudly, before any connection, unless ``uri`` names an explicit
    scratch database. ``kind`` is ``"clickhouse"`` or ``"postgres"``, used
    only for the error message.
    """

    if not uri:
        raise UnsafeSinkError(
            f"fixtures world: no {kind} URI provided -- refusing to guess "
            "a target database."
        )
    db_name = _database_name_from_uri(uri)
    lowered = db_name.lower()
    if lowered in _HARD_DENIED_DATABASE_NAMES:
        raise UnsafeSinkError(
            f"fixtures world: {kind} database {db_name!r} is a known shared "
            "dev/system database, not a disposable scratch target. "
            "fixtures world issues destructive mutations and must never "
            "run against it. Point --sink/--postgres-uri at a dedicated "
            f"scratch database instead (name must contain one of {_SCRATCH_MARKERS!r})."
        )
    if not _looks_like_scratch_name(lowered):
        raise UnsafeSinkError(
            f"fixtures world: {kind} database {db_name!r} does not look "
            f"like a scratch database (name must contain one of {_SCRATCH_MARKERS!r}) "
            "-- refusing to run destructive fixture generation against an "
            "unidentified target. Create a dedicated scratch database "
            f"(e.g. {db_name}_scratch) and point --sink/--postgres-uri at it."
        )


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class WorldManifest:
    manifest_path: Path
    world: dict[str, Any]
    subjects: dict[str, Any]
    sources: dict[str, Any]

    @property
    def master_seed(self) -> int:
        return int(self.world["master_seed"])

    @property
    def pinned_now(self) -> datetime:
        return _parse_dt(self.world["pinned_now"])

    @property
    def orgs(self) -> list[dict[str, Any]]:
        return list(self.world["orgs"])

    def org(self, alias: str) -> dict[str, Any]:
        for org in self.orgs:
            if org["alias"] == alias:
                return org
        raise WorldManifestError(f"world.json has no org with alias={alias!r}")

    def org_id(self, alias: str) -> uuid.UUID:
        return derive_id(self.master_seed, self.org(alias)["id_seed"])

    def user_id(self, alias: str) -> uuid.UUID:
        for user in self.world["users"]:
            if user["alias"] == alias:
                return derive_id(self.master_seed, user["id_seed"])
        raise WorldManifestError(f"world.json has no user with alias={alias!r}")


def derive_id(master_seed: int, id_seed: str) -> uuid.UUID:
    """The one place a world/subjects/sources id_seed string becomes a UUID.

    Namespaced by ``master_seed`` too (not only the id_seed string) so a
    future ``ask-dev-world.v2`` sharing an id_seed string never collides
    with v1's ids.
    """

    return uuid.uuid5(FIXTURE_NAMESPACE, f"{master_seed}:{id_seed}")


def derive_repo_seed(master_seed: int, org_alias: str, repo_full_name: str) -> int:
    """Deterministic per-(org, repo) seed for ``SyntheticDataGenerator``."""

    digest = hashlib.sha256(
        f"{master_seed}:{org_alias}:{repo_full_name}".encode()
    ).hexdigest()
    return int(digest[:8], 16)


def load_world_manifest(manifest_path: str | Path) -> WorldManifest:
    path = Path(manifest_path)
    world = json.loads(path.read_text())
    subjects = json.loads((path.parent / "subjects.json").read_text())
    sources = json.loads((path.parent / "sources.json").read_text())
    manifest = WorldManifest(
        manifest_path=path, world=world, subjects=subjects, sources=sources
    )
    validate_world_manifest(manifest)
    return manifest


def validate_world_manifest(manifest: WorldManifest) -> None:
    """Shape + cross-reference validation. Raises :class:`WorldManifestError`.

    Deliberately checked BEFORE any DB connection is opened, matching the
    project's "fail loud before doing work" convention (``_ensure_org_
    unpolluted`` et al.).
    """

    for label, doc in (
        ("world.json", manifest.world),
        ("subjects.json", manifest.subjects),
        ("sources.json", manifest.sources),
    ):
        version = doc.get("schema_version")
        if version != WORLD_SCHEMA_VERSION:
            raise WorldManifestError(
                f"{label} schema_version={version!r}, expected {WORLD_SCHEMA_VERSION!r}"
            )

    org_aliases = {org["alias"] for org in manifest.world.get("orgs", [])}
    if not org_aliases:
        raise WorldManifestError("world.json declares zero orgs")

    for subject in manifest.subjects.get("subjects", []):
        alias = subject.get("org_alias")
        if alias is not None and alias not in org_aliases:
            raise WorldManifestError(
                f"subjects.json subject {subject.get('id')!r} references "
                f"unknown org_alias={alias!r}"
            )
        asked_from = subject.get("asked_from_org_alias")
        if asked_from is not None and asked_from not in org_aliases:
            raise WorldManifestError(
                f"subjects.json subject {subject.get('id')!r} references "
                f"unknown asked_from_org_alias={asked_from!r}"
            )

    required_classes = set(
        manifest.subjects.get("class_coverage_check", {}).get("required_classes", [])
    )
    present_classes = {s["class"] for s in manifest.subjects.get("subjects", [])}
    missing_classes = required_classes - present_classes
    if missing_classes:
        raise WorldManifestError(
            "subjects.json has zero realizing rows for required class(es): "
            f"{sorted(missing_classes)}"
        )

    for entry in manifest.sources.get("matrix", []):
        alias = (entry.get("realized_by") or {}).get("org_alias")
        if alias is not None and alias not in org_aliases:
            raise WorldManifestError(
                f"sources.json state {entry.get('state')!r} references "
                f"unknown org_alias={alias!r}"
            )

    required_states = set(
        manifest.sources.get("state_coverage_check", {}).get("required_states", [])
    )
    present_states = {m["state"] for m in manifest.sources.get("matrix", [])}
    missing_states = required_states - present_states
    if missing_states:
        raise WorldManifestError(
            "sources.json has zero realizing rows for required state(s): "
            f"{sorted(missing_states)}"
        )

    # Typed per-entry schema (Codex HIGH-2, 2026-08-05): the checks above
    # only confirm every REQUIRED class/state string appears somewhere --
    # a bare {"class": "deleted"} row with no realization fields at all
    # passed every check above. world_verify's per-class/per-state schema
    # closes that: every entry's OWN class/state now dictates a concrete
    # required-field shape, not just "the class name is spelled right".
    for subject in manifest.subjects.get("subjects", []):
        try:
            world_verify.validate_subject_entry_schema(subject)
        except world_verify.WorldSchemaError as exc:
            raise WorldManifestError(f"subjects.json: {exc}") from exc
    for entry in manifest.sources.get("matrix", []):
        try:
            world_verify.validate_source_entry_schema(entry)
        except world_verify.WorldSchemaError as exc:
            raise WorldManifestError(f"sources.json: {exc}") from exc

    # CHAOS-3432 (2026-08-05): the whole-world analogue of sources.json's
    # DECLARED_BLOCKED_STATUS / subjects.json's REALIZED_UNVERIFIED_LIVE_STATUS
    # -- a claim about the WORLD as a whole (cross-generation digest
    # reproducibility), not a single manifest entry, so it lives at
    # world.json's top level instead of inside subjects/sources. Same
    # discipline: typed, ticketed, schema-checked -- never a silent claim.
    cross_gen_status = manifest.world.get("cross_generation_digest_status")
    if cross_gen_status is not None:
        if (
            not isinstance(cross_gen_status, dict)
            or cross_gen_status.get("status") != "declared-blocked"
        ):
            raise WorldManifestError(
                "world.json 'cross_generation_digest_status', if present, must "
                "be an object with status='declared-blocked' -- got "
                f"{cross_gen_status!r}"
            )
        if not cross_gen_status.get("blocked_by"):
            raise WorldManifestError(
                "world.json 'cross_generation_digest_status' has "
                "status='declared-blocked' but no non-empty 'blocked_by' "
                "ticket reference -- an unproven cross-run claim must name "
                "what is tracking it."
            )


# ---------------------------------------------------------------------------
# Repo roster derivation -- collected FROM subjects.json/sources.json rather
# than hand-duplicated here, so the manifest stays the single source of
# truth for "which named repos exist" (no risk of world.py silently
# generating a repo subjects.json/sources.json no longer references, or vice
# versa).
# ---------------------------------------------------------------------------


def collect_repo_roster(manifest: WorldManifest) -> dict[str, set[str]]:
    """``{org_alias: {repo_full_name, ...}}`` scanned out of subjects/sources."""

    roster: dict[str, set[str]] = {
        alias: set() for alias in (org["alias"] for org in manifest.orgs)
    }

    def _add(alias: str | None, name: str | None) -> None:
        if alias and name:
            roster.setdefault(alias, set()).add(name)

    for subject in manifest.subjects.get("subjects", []):
        alias = subject.get("org_alias")
        _add(alias, subject.get("repo_full_name"))
        for candidate in subject.get("candidates") or []:
            _add(alias, candidate)
        for member in subject.get("members") or []:
            _add(alias, member)

    for entry in manifest.sources.get("matrix", []):
        realized_by = entry.get("realized_by") or {}
        _add(realized_by.get("org_alias"), realized_by.get("repo_full_name"))

    return roster


#: Repos needing a non-default generation profile (extra volume for the
#: truncated-workgraph probe). Keyed by repo_full_name; every other repo
#: uses world.json's ``generation`` block verbatim.
_VOLUME_OVERRIDES: dict[str, dict[str, int]] = {
    "probe/source-truncated-workgraph": {
        "days": 21,
        "commits_per_day": 8,
        "pr_count": 40,
    },
}

#: Repos realizing NO_DATA/measured-zero for specific source classes
#: (deployments/work_items/ci_runs) via POST-HOC deletion
#: (``source_health.zero_out_source``, run after generation) rather than at
#: generation time -- ``generate_commits``/``generate_work_items`` etc. do
#: not tolerate a zero-volume profile (``random.randint(1, 0)`` raises), and
#: the real DataHealthState signal these probes need to demonstrate is a
#: *specific source's* absent watermark, not a wholly-empty repo. These two
#: repos therefore generate with the ordinary default profile and are zeroed
#: for exactly the sources sources.json names afterward -- see
#: ``_run_clickhouse_postprocess``.
NO_DATA_PROBE_REPOS = frozenset({"probe/source-no-data", "probe/source-measured-zero"})


def _generation_namespace(
    manifest: WorldManifest,
    *,
    org_alias: str,
    org_id: uuid.UUID,
    repo_full_name: str,
    sink: str,
    allow_mixed_org: bool,
) -> argparse.Namespace:
    gen_cfg = manifest.world.get("generation", {})
    overrides = _VOLUME_OVERRIDES.get(repo_full_name, {})
    return argparse.Namespace(
        sink=sink,
        db_type=None,
        repo_name=repo_full_name,
        repo_count=1,
        days=overrides.get("days", gen_cfg.get("days_of_history", 10)),
        commits_per_day=overrides.get(
            "commits_per_day", gen_cfg.get("commits_per_day", 3)
        ),
        pr_count=overrides.get("pr_count", gen_cfg.get("pr_count", 6)),
        seed=derive_repo_seed(manifest.master_seed, org_alias, repo_full_name),
        provider="synthetic",
        with_work_graph=bool(gen_cfg.get("with_work_graph", True)),
        with_metrics=bool(gen_cfg.get("with_metrics", True)),
        team_count=gen_cfg.get("team_count", 3),
        skip_coherence_validation=False,
        allow_mixed_org=allow_mixed_org,
        org=str(org_id),
        # Codex HIGH-4 (2026-08-05): serializes LLM-categorization
        # completion order inside materialize_fixture_investments, intended
        # to help two otherwise-identical `fixtures world` generations
        # reproduce an identical WORLD_DIGEST -- see runner.py's call site
        # and work_graph.runner.materialize_fixture_investments's own
        # docstring. `fixtures generate` never sets this attribute, so it
        # keeps the function's unchanged default (5) -- this is scoped to
        # the world path only. NOT sufficient on its own: a residual,
        # narrowed-but-unconfirmed source of cross-generation drift remains
        # open under CHAOS-3432 (see _frozen_clock's docstring) even with
        # this set to 1 -- do not read this line as proof the drift is
        # closed.
        llm_concurrency=1,
    )


# ---------------------------------------------------------------------------
# Postgres: auth roster, entitlements, retention policies, sync config,
# retention-aged conversations.
# ---------------------------------------------------------------------------


def _build_auth_fixture(manifest: WorldManifest) -> dict[str, Any]:
    """``organizations``/``users``/``memberships``/``licenses`` for the
    entire world -- shaped exactly like ``fixtures.runner._seed_auth_data``
    expects (that function is reused verbatim, not reimplemented)."""

    from dev_health_ops.licensing.generator import generate_test_license
    from dev_health_ops.licensing.types import LicenseTier
    from dev_health_ops.models.licensing import OrgLicense
    from dev_health_ops.models.users import Membership, Organization, User

    organizations = []
    users = []
    memberships = []
    licenses = []

    for org in manifest.orgs:
        org_id = manifest.org_id(org["alias"])
        organizations.append(
            Organization(
                id=org_id,
                slug=org["slug"],
                name=org["name"],
                is_active=True,
                tier="enterprise",
            )
        )
        license = OrgLicense(
            org_id=org_id,
            license_key=generate_test_license(org_id=str(org_id)),
            tier=LicenseTier.ENTERPRISE.value,
            managed_by="manual",
        )
        license.id = derive_id(manifest.master_seed, f"license:{org['alias']}")
        licenses.append(license)

    for user in manifest.world["users"]:
        user_id = manifest.user_id(user["alias"])
        org_id = manifest.org_id(user["org_alias"])
        users.append(
            User(
                id=user_id,
                email=user["email"],
                username=user["username"],
                password_hash=None,
                full_name=user["full_name"],
                auth_provider="local",
                is_active=True,
                is_verified=True,
                is_superuser=bool(user.get("is_superuser", False)),
            )
        )
        memberships.append(
            Membership(
                id=derive_id(manifest.master_seed, f"membership:{user['alias']}"),
                user_id=user_id,
                org_id=org_id,
                role=user.get("membership_role", "member"),
            )
        )

    return {
        "organizations": organizations,
        "users": users,
        "memberships": memberships,
        "licenses": licenses,
    }


async def _seed_entitlements(session: Any, manifest: WorldManifest) -> None:
    """``org_feature_overrides`` for ``ask_dev``/``ask_dev_contextual_entrypoints``.

    Requires the ``FeatureFlag`` catalog rows Alembic migrations 0067/0070
    seed to already exist -- fails loud (not silently skipped) if they do
    not, since a world generated without them cannot realize the
    disabled-entitlement org at all.
    """

    from sqlalchemy import select

    from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride

    flag_rows = (
        (
            await session.execute(
                select(FeatureFlag).where(
                    FeatureFlag.key.in_(["ask_dev", "ask_dev_contextual_entrypoints"])
                )
            )
        )
        .scalars()
        .all()
    )
    flags_by_key = {flag.key: flag for flag in flag_rows}
    missing = {"ask_dev", "ask_dev_contextual_entrypoints"} - set(flags_by_key)
    if missing:
        raise WorldManifestError(
            f"Postgres is missing FeatureFlag catalog row(s) {sorted(missing)} -- "
            "run `dev-hops migrate postgres` (Alembic 0067/0070 seed these) "
            "before `fixtures world`."
        )

    entitlement_fields = {
        "ask_dev": "ask_dev_entitlement",
        "ask_dev_contextual_entrypoints": "ask_dev_contextual_entrypoints_entitlement",
    }
    for org in manifest.orgs:
        org_id = manifest.org_id(org["alias"])
        for key, field in entitlement_fields.items():
            is_enabled = org.get(field, "enabled") == "enabled"
            override = OrgFeatureOverride(
                org_id=org_id,
                feature_id=flags_by_key[key].id,
                is_enabled=is_enabled,
                reason="ask-dev-world.v1 fixture",
            )
            override.id = derive_id(
                manifest.master_seed, f"feature-override:{org['alias']}:{key}"
            )
            await session.merge(override)


async def _seed_retention_and_sync(session: Any, manifest: WorldManifest) -> None:
    from dev_health_ops.models.retention import OrgRetentionPolicy

    for org in manifest.orgs:
        org_id = manifest.org_id(org["alias"])
        for idx, policy in enumerate(org.get("retention_policies", [])):
            row = OrgRetentionPolicy(
                id=derive_id(
                    manifest.master_seed,
                    f"retention:{org['alias']}:{policy['resource_type']}:{idx}",
                ),
                org_id=org_id,
                resource_type=policy["resource_type"],
                retention_days=policy["retention_days"],
                is_active=True,
            )
            await session.merge(row)

        sync_configs = source_health.build_sync_configurations_for_org(
            str(org_id),
            {name: cfg["state"] for name, cfg in org.get("sync_providers", {}).items()},
            as_of=manifest.pinned_now,
        )
        for config in sync_configs:
            config.id = derive_id(
                manifest.master_seed, f"sync-config:{org['alias']}:{config.provider}"
            )
            await session.merge(config)


async def _seed_conversations(session: Any, manifest: WorldManifest) -> None:
    primary_alias = "primary"
    primary_org_id = manifest.org_id(primary_alias)
    ordinary_user_id = manifest.user_id(f"{primary_alias}.ordinary")

    bundles = []

    # Retention-aged conversations: one already past its policy window, one
    # freshly created, per retention policy world.json declares for the
    # primary org.
    for org in manifest.orgs:
        org_id = manifest.org_id(org["alias"])
        owner_alias = next(
            (
                u["alias"]
                for u in manifest.world["users"]
                if u["org_alias"] == org["alias"]
            ),
            None,
        )
        if owner_alias is None:
            continue
        owner_id = manifest.user_id(owner_alias)
        for policy in org.get("retention_policies", []):
            retention_days = policy["retention_days"]
            aged_days = retention_days + 5 if retention_days > 0 else 1
            bundles.append(
                conv_gen.build_retention_aged_conversation(
                    org_id=org_id,
                    user_id=owner_id,
                    id_seed=f"retention-aged:{org['alias']}:{retention_days}",
                    retention_days=retention_days,
                    age_days=aged_days,
                    pinned_now=manifest.pinned_now,
                    title=f"retention probe ({retention_days}-day, aged)",
                )
            )
            bundles.append(
                conv_gen.build_retention_aged_conversation(
                    org_id=org_id,
                    user_id=owner_id,
                    id_seed=f"retention-fresh:{org['alias']}:{retention_days}",
                    retention_days=retention_days,
                    age_days=0,
                    pinned_now=manifest.pinned_now,
                    title=f"retention probe ({retention_days}-day, fresh)",
                )
            )

    bundles.append(
        conv_gen.build_stale_context_conversation(
            org_id=primary_org_id,
            user_id=ordinary_user_id,
            id_seed="stale-context",
            repo_full_name="rotated/service",
            pinned_now=manifest.pinned_now,
        )
    )
    bundles.append(
        conv_gen.build_validation_packet(
            org_id=primary_org_id,
            user_id=ordinary_user_id,
            id_seed="validation-packet",
            pinned_now=manifest.pinned_now,
        )
    )

    for bundle in bundles:
        await session.merge(bundle.conversation)
        await session.flush()
        for run in bundle.runs:
            await session.merge(run)
        await session.flush()
        for subject_set in bundle.subject_sets:
            await session.merge(subject_set)
    await session.commit()


async def _run_postgres_phase(postgres_uri: str, manifest: WorldManifest) -> None:
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(postgres_uri, pool_pre_ping=True)
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    try:
        async with session_factory() as session:
            from dev_health_ops.fixtures.runner import _seed_auth_data

            await _seed_auth_data(
                session, _build_auth_fixture(manifest), overwrite_real_users=True
            )
            await _seed_entitlements(session, manifest)
            await _seed_retention_and_sync(session, manifest)
            await session.commit()
        async with session_factory() as session:
            await _seed_conversations(session, manifest)
    finally:
        await engine.dispose()


class MeasuredZeroWriteError(RuntimeError):
    """A measured-zero metric write did not verifiably land as claimed.

    Codex adversarial review (HIGH-1, 2026-08-05): the original code
    guarded the write behind ``hasattr(store, "write_dora_metrics")`` --
    ``store`` here is a ``ClickHouseStore`` (from ``run_with_store``),
    which has no such method (it lives on ``ClickHouseMetricsSink``
    instead), so the guard was ALWAYS false and the whole block silently
    no-op'd. ``probe/source-measured-zero`` kept whatever random,
    almost-certainly-nonzero ``deployment_frequency`` value
    ``run_fixtures_generation``'s own ``with_metrics`` pass had already
    written, while ``sources.json`` claimed 0.0 -- a measurement that
    never happened, reading as coverage. Per this repo's own verification
    rule ("a measurement that did not happen must FAIL, loudly"), the
    write is now followed by a live read-back assertion; this exception is
    what "loudly" means here -- raised, not logged-and-continued.
    """


async def write_and_verify_measured_zero_metric(
    client: Any,
    *,
    sink_dsn: str,
    repo_id: uuid.UUID,
    org_id: str,
    metric_name: str,
    day: Any,
    computed_at: datetime,
) -> None:
    """Force ``metric_name`` to read exactly ``0.0`` for ``repo_id``, then
    prove it by reading the live table back -- not merely asserting the
    write call returned without raising.

    ``dora_metrics_daily`` is a plain ``MergeTree`` (migration ``023b``),
    NOT a ``ReplacingMergeTree`` -- inserting a new row for the same
    ``(repo_id, day, metric_name)`` key does not replace an existing row,
    it adds a second one alongside it. Existing rows for this
    ``(repo_id, metric_name)`` are therefore deleted first (synchronous
    ``ALTER TABLE ... DELETE``, mirroring
    ``source_health.zero_out_source``'s same-table-family pattern) so the
    post-write read-back is unambiguous: exactly one row, and it is 0.0.
    """

    from dev_health_ops.metrics.schemas import DORAMetricsRecord
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    await asyncio.to_thread(
        client.command,
        "ALTER TABLE dora_metrics_daily DELETE WHERE repo_id = {repo_id:UUID} "
        "AND metric_name = {metric_name:String} SETTINGS mutations_sync = 1",
        parameters={"repo_id": str(repo_id), "metric_name": metric_name},
    )

    sink_obj = ClickHouseMetricsSink(sink_dsn, client=client)
    sink_obj.write_dora_metrics(
        [
            DORAMetricsRecord(
                repo_id=repo_id,
                day=day,
                metric_name=metric_name,
                value=0.0,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )

    result = await asyncio.to_thread(
        client.query,
        # No `FINAL` here -- Codex HIGH-3 (2026-08-05), found live: this
        # function's own docstring above already establishes
        # dora_metrics_daily is a plain MergeTree, which does not support
        # `FINAL` at all (`Code: 181 ILLEGAL_FINAL`). The prior version of
        # this query used it anyway -- harmless against a stub client (which
        # never parses SQL), fatal the first time this path actually ran
        # against a live ClickHouse. Correctness here does not depend on
        # `FINAL` regardless: the delete-then-single-insert above already
        # guarantees at most one row for this key.
        "SELECT value FROM dora_metrics_daily "
        "WHERE repo_id = {repo_id:UUID} AND metric_name = {metric_name:String} "
        "ORDER BY day DESC",
        parameters={"repo_id": str(repo_id), "metric_name": metric_name},
    )
    rows = list(result.result_rows)
    if len(rows) != 1:
        raise MeasuredZeroWriteError(
            f"world: measured-zero postcondition failed for repo_id={repo_id} "
            f"metric_name={metric_name!r}: expected exactly 1 row after the "
            f"delete+write, found {len(rows)}. The pre-existing "
            "run_fixtures_generation-written row(s) were not cleanly "
            "replaced -- refusing to claim measured-zero is realized."
        )
    (value,) = rows[0]
    if value != 0.0:
        raise MeasuredZeroWriteError(
            f"world: measured-zero postcondition failed for repo_id={repo_id} "
            f"metric_name={metric_name!r}: live read-back value={value!r}, "
            "expected exactly 0.0. The write did not land as claimed."
        )
    logging.info(
        "world: verified measured-zero %s=0.0 for repo_id=%s (live read-back)",
        metric_name,
        repo_id,
    )


# ---------------------------------------------------------------------------
# ClickHouse post-processing: projects, source-state aging/zeroing, conflicts.
# ---------------------------------------------------------------------------


async def _run_clickhouse_postprocess(sink: str, manifest: WorldManifest) -> None:
    async def _handler(store: Any) -> None:
        client = store.client
        pinned_now = manifest.pinned_now
        stale_ts = pinned_now - timedelta(hours=96)

        # Explicit team-kind subjects (e.g. the literal-parenthetical-alias
        # "Platform Reliability (Ground Control)" team) are NOT part of the
        # curated demo-team roster `run_fixtures_generation` seeds per repo
        # (that roster is fixed-size and drawn from demo_identity.DEMO_TEAMS)
        # -- insert them directly so alias_matching has a real `teams` row
        # whose `name` carries the parenthetical to split on.
        # WORLD_DIGEST reproducibility: run_fixtures_generation's OWN
        # per-repo team upsert re-derives each org's curated team roster
        # (demo_identity.DEMO_TEAMS) fresh on EVERY repo call within that
        # org, each with an independently-seeded `members` list -- since
        # `teams` is a ReplacingMergeTree(updated_at), whichever repo call
        # happens to write last wins, and that ordering is a real-execution
        # timing race, not something the master seed determines. Verified
        # empirically against two live scratch generations (see the Lane 1a
        # report). Rewriting the ENTIRE curated + alias team roster here,
        # once, with a fixed (empty) `members` list, makes this postprocess
        # write the deterministic FINAL content -- but only if it actually
        # WINS the ReplacingMergeTree(updated_at) race against every
        # per-repo call's own version. models.teams.Team.__init__ defaults
        # `updated_at` to REAL `datetime.now(timezone.utc)` when not passed
        # (models/teams.py is intentionally not in _CLOCK_PATCHED_MODULES --
        # patching a model file's shared `datetime` name risks unrelated
        # side effects), so using world.json's pinned_now here does NOT
        # reliably win: pinned_now is a fixed 2026 date, but a per-repo
        # call's real-wall-clock default can easily be LATER than it,
        # letting the racy version win anyway (confirmed empirically: teams
        # was the only table still drifting after the frozen-clock fix,
        # even with this postprocess write already in place with
        # `updated_at=pinned_now`). Real wall-clock time is the deliberate,
        # narrow exception here -- this write always runs LAST (after every
        # repo generation call), so `datetime.now()` at this exact point is
        # guaranteed later than any competing version, and `updated_at`
        # is excluded from WORLD_DIGEST hashing (_VOLATILE_COLUMNS) either
        # way, so using real time here has no effect on digest content.
        from dev_health_ops.fixtures.demo_identity import demo_team_identity

        canonicalized_at = datetime.now(timezone.utc)
        team_rows: list[dict[str, Any]] = []
        gen_cfg = manifest.world.get("generation", {})
        team_count = int(gen_cfg.get("team_count", 3))
        for org in manifest.orgs:
            org_id = str(manifest.org_id(org["alias"]))
            for index in range(team_count):
                curated = demo_team_identity(index)
                if curated is None:
                    continue
                team_id, team_name = curated
                team_rows.append(
                    {
                        "id": team_id,
                        "name": team_name,
                        "org_id": org_id,
                        "members": [],
                        "is_active": 1,
                        "updated_at": canonicalized_at,
                    }
                )
        for subject in manifest.subjects.get("subjects", []):
            if (
                subject.get("entity_kind") != "team"
                or "team_display_name" not in subject
            ):
                continue
            team_rows.append(
                {
                    "id": subject["team_id"],
                    "name": subject["team_display_name"],
                    "org_id": str(manifest.org_id(subject["org_alias"])),
                    "members": [],
                    "is_active": 1,
                    "updated_at": canonicalized_at,
                }
            )
        if team_rows and hasattr(store, "insert_teams"):
            await store.insert_teams(team_rows)
            logging.info(
                "world: inserted %d canonical team row(s) (curated + alias)",
                len(team_rows),
            )

        project_records = []
        for subject in manifest.subjects.get("subjects", []):
            if subject.get("entity_kind") != "project":
                continue
            org_id = str(manifest.org_id(subject["org_alias"]))
            repo_full_name = subject.get("repo_full_name") or subject["project_id"]
            display_name = subject.get("project_display_name") or subject.get(
                "team_display_name"
            )
            record = projects_gen.build_project_record(
                org_id=org_id,
                repo_full_name=repo_full_name,
                display_name=display_name,
                is_active=True,
                as_of=pinned_now - timedelta(days=1),
            )
            project_records.append(record)
            if subject["id"] == "subject.deleted.legacy-billing":
                project_records.append(
                    projects_gen.build_retired_project_version(
                        record, retired_as_of=pinned_now - timedelta(hours=2)
                    )
                )

        # Every repo-backed subject/probe also gets a matching active
        # projects row so PROJECT-scope resolution has a catalog entity to
        # find, even when subjects.json didn't ask for one explicitly.
        roster = collect_repo_roster(manifest)
        already_covered = {record.id for record in project_records}
        for org_alias, repo_names in roster.items():
            org_id = str(manifest.org_id(org_alias))
            for repo_full_name in sorted(repo_names):
                if repo_full_name in already_covered:
                    continue
                project_records.append(
                    projects_gen.build_project_record(
                        org_id=org_id,
                        repo_full_name=repo_full_name,
                        as_of=pinned_now - timedelta(days=1),
                    )
                )
                already_covered.add(repo_full_name)

        await projects_gen.insert_projects(client, project_records)
        logging.info("world: inserted %d projects rows", len(project_records))

        primary_org_id = str(manifest.org_id("primary"))
        stale_repo_id = str(uuid.uuid5(FIXTURE_NAMESPACE, "probe/source-stale"))
        for source in (
            "commits",
            "pull_requests",
            "reviews",
            "deployments",
            "work_items",
        ):
            try:
                await source_health.age_source_rows(
                    client,
                    org_id=primary_org_id,
                    repo_id=stale_repo_id,
                    source=source,
                    stale_watermark=stale_ts,
                )
            except Exception as exc:  # noqa: BLE001
                if not any(marker in str(exc) for marker in _MISSING_TABLE_MARKERS):
                    raise
                logging.warning(
                    "world: skipped aging %s (table missing): %s", source, exc
                )

        no_data_repo_id = str(uuid.uuid5(FIXTURE_NAMESPACE, "probe/source-no-data"))
        measured_zero_repo_id = str(
            uuid.uuid5(FIXTURE_NAMESPACE, "probe/source-measured-zero")
        )
        for repo_id in (no_data_repo_id, measured_zero_repo_id):
            for source in ("deployments", "work_items", "ci_runs"):
                try:
                    await source_health.zero_out_source(
                        client, org_id=primary_org_id, repo_id=repo_id, source=source
                    )
                except Exception as exc:  # noqa: BLE001
                    if not any(marker in str(exc) for marker in _MISSING_TABLE_MARKERS):
                        raise
                    logging.warning(
                        "world: skipped zeroing %s for %s (table missing): %s",
                        source,
                        repo_id,
                        exc,
                    )

        # Codex HIGH-2 live-verification correction (2026-08-05): sources.json
        # claims primary org's "incidents" source reads UNCONFIGURED org-wide
        # (no pagerduty/opsgenie/incident SyncConfiguration row -- primary's
        # own world.json sync_providers deliberately omits them). That claim
        # is about CONFIGURATION, but NativeDataHealthReader's own logic
        # (data_health_service.py: "if watermark is not None: configured =
        # True") makes "configured" partly DATA-driven too -- any REAL row
        # in operational_incidents overrides an absent SyncConfiguration.
        # The standard per-repo generator (runner.py's
        # ``write_operational_batch`` call) writes SOME synthetic incidents
        # for most repos unconditionally, unrelated to this source-health
        # claim's concept -- so without this, ANY other primary-org repo's
        # incidents would make the org-wide claim false, caught live the
        # first time verify_sources_against_production_data_health actually
        # ran end to end (it observed COMPLETE/NO_DATA instead of
        # UNCONFIGURED). No other sources.json/subjects.json entry claims
        # real incidents data for primary org (checked: only the
        # sibling-org "unauthorized" entry mentions "incidents", and that
        # path never reaches operational_incidents at all -- it short-
        # circuits on the cross-tenant scope guard) -- explicitly enforcing
        # emptiness here is safe and makes the claim's actual intent
        # ("no incidents integration is configured for primary") hold
        # regardless of what individual repos' synthetic incident volume
        # happened to produce.
        try:
            await source_health.zero_out_source(
                client,
                org_id=primary_org_id,
                repo_id=primary_org_id,
                source="incidents",
            )
        except Exception as exc:  # noqa: BLE001
            if not any(marker in str(exc) for marker in _MISSING_TABLE_MARKERS):
                raise
            logging.warning(
                "world: skipped zeroing incidents for primary org (table missing): %s",
                exc,
            )

        await write_and_verify_measured_zero_metric(
            client,
            sink_dsn=sink,
            repo_id=uuid.UUID(measured_zero_repo_id),
            org_id=primary_org_id,
            metric_name="deployment_frequency",
            day=(pinned_now - timedelta(days=1)).date(),
            computed_at=pinned_now,
        )

        conflicting_repo_id = uuid.uuid5(
            FIXTURE_NAMESPACE, "probe/source-conflicting-ci"
        )
        pair = conflicts_gen.build_conflicting_ci_runs(
            repo_id=conflicting_repo_id,
            org_id=primary_org_id,
            as_of=pinned_now - timedelta(hours=6),
            seed_label="ci",
        )
        if hasattr(store, "insert_testops_pipeline_runs"):
            rows = conflicts_gen.to_clickhouse_extended_rows(
                pair, team_id=None, service_id="api-gateway"
            )
            await store.insert_testops_pipeline_runs(rows)
        elif hasattr(store, "insert_ci_pipeline_runs"):
            await store.insert_ci_pipeline_runs(
                conflicts_gen.to_postgres_ci_pipeline_runs(pair)
            )
        logging.info(
            "world: inserted conflicting CI run pair for probe/source-conflicting-ci"
        )

    # HIGH-4 (2026-08-05): _frozen_clock previously wrapped ONLY the
    # per-repo `run_fixtures_generation` calls, never this postprocess
    # handler -- by the time it runs, every patched module's `datetime`
    # name has already been reverted (the per-repo `with _frozen_clock`
    # block has exited once, for the LAST repo). This handler's own
    # conflicting-ci-run insert goes through `store.insert_testops_
    # pipeline_runs` (storage.mixins.testops_cicd, already patched for the
    # per-repo path) -- unpatched here, it fell back to real wall-clock
    # `last_synced`, found live via direct row comparison between two
    # scratch databases (every column on the conflicting-ci rows matched
    # except last_synced). Wrapping the whole handler is deliberately safe
    # for the teams-canonicalization write just above, which intentionally
    # keeps using REAL wall-clock time to win a ReplacingMergeTree race
    # (see its own comment): that write calls `datetime.now()` via this
    # module's (`fixtures.world`) OWN name, and `fixtures.world` itself is
    # -- deliberately -- never in `_CLOCK_PATCHED_MODULES`, so it is
    # unaffected by this wrap regardless of scope.
    with _frozen_clock(manifest.pinned_now):
        await run_with_store(sink, "clickhouse", _handler, org_id=None)


# ---------------------------------------------------------------------------
# WORLD_DIGEST
# ---------------------------------------------------------------------------


def _row_content_key(
    column_names: list[str], row: tuple[Any, ...], *, volatile: frozenset[str]
) -> str:
    """``volatile`` is the CALLER-resolved exclusion set for the specific
    table this row came from (see ``_volatile_columns_for_table``) -- Codex
    HIGH-3 (2026-08-05): this used to be the single global
    ``_VOLATILE_COLUMNS`` constant, unconditionally, for every table. That
    meant a table whose watermark column IS the thing a claimed state (e.g.
    "stale", "measured-zero") is ABOUT could have that exact column silently
    hashed out of the digest, so a regression that broke the aging/zeroing
    step would leave the digest bit-for-bit identical and green. Passing the
    per-table set explicitly (no default) makes every call site say what it
    means to include, rather than inheriting a blanket exclusion.
    """
    parts = [
        f"{name}={value!r}"
        for name, value in zip(column_names, row, strict=True)
        if name not in volatile
    ]
    parts.sort()
    return "|".join(parts)


#: Codex HIGH-3 (2026-08-05), found live (a stub client cannot catch this --
#: it never parses the SQL): ``FINAL`` is only legal against merge-tree
#: engines that actually version/dedup rows (ReplacingMergeTree,
#: CollapsingMergeTree, ...). ``dora_metrics_daily`` is a PLAIN
#: ``MergeTree`` (see ``023b_dora_metrics.sql`` -- no dedup, deliberately,
#: since a repo/day/metric_name can be recomputed and every computation is
#: a fact worth keeping) -- appending it to ``_CLICKHOUSE_DIGEST_TABLES``
#: without this exception made ``compute_world_digest`` raise
#: ``Code: 181 ILLEGAL_FINAL`` for every table in the same org loop, not
#: just for ``dora_metrics_daily`` itself, which the live mutation tests
#: below caught immediately on the very first run against a real
#: ClickHouse. Every other table in ``_CLICKHOUSE_DIGEST_TABLES`` is a
#: ReplacingMergeTree (confirmed per-table against each table's own
#: ``CREATE TABLE`` in ``src/dev_health_ops/migrations/clickhouse/``).
_PLAIN_MERGETREE_TABLES = frozenset({"dora_metrics_daily"})


async def _clickhouse_table_digest(
    client: Any, table: str, org_id: str
) -> dict[str, Any]:
    final_clause = "" if table in _PLAIN_MERGETREE_TABLES else " FINAL"
    try:
        result = await asyncio.to_thread(
            client.query,
            f"SELECT * FROM {table}{final_clause} WHERE org_id = {{org_id:String}}",
            parameters={"org_id": org_id},
        )
    except Exception as exc:  # noqa: BLE001
        if any(marker in str(exc) for marker in _MISSING_TABLE_MARKERS):
            return {"row_count": 0, "content_hash": hashlib.sha256(b"").hexdigest()}
        raise
    column_names = list(result.column_names)
    volatile = _volatile_columns_for_table(table)
    row_keys = sorted(
        _row_content_key(column_names, row, volatile=volatile)
        for row in result.result_rows
    )
    content_hash = hashlib.sha256("\n".join(row_keys).encode()).hexdigest()
    return {"row_count": len(row_keys), "content_hash": content_hash}


async def _postgres_table_digest(
    session: Any, table: str, org_column: str, org_id: str
) -> dict[str, Any]:
    from sqlalchemy import text

    result = await session.execute(
        text(f"SELECT * FROM {table} WHERE {org_column} = :org_id"),  # noqa: S608
        {"org_id": org_id},
    )
    column_names = list(result.keys())
    rows = result.fetchall()
    volatile = _volatile_columns_for_table(table)
    row_keys = sorted(
        _row_content_key(column_names, tuple(row), volatile=volatile) for row in rows
    )
    content_hash = hashlib.sha256("\n".join(row_keys).encode()).hexdigest()
    return {"row_count": len(row_keys), "content_hash": content_hash}


async def compute_world_digest(
    manifest: WorldManifest, *, sink: str, postgres_uri: str
) -> dict[str, Any]:
    """The full content-digest breakdown: per (store, table, org) hash, plus
    one top-level ``digest`` sha256 over the whole canonical breakdown."""

    components: dict[str, Any] = {"clickhouse": {}, "postgres": {}}

    async def _ch_handler(store: Any) -> None:
        client = store.client
        for table in _CLICKHOUSE_DIGEST_TABLES:
            components["clickhouse"].setdefault(table, {})
            for org in manifest.orgs:
                org_id = str(manifest.org_id(org["alias"]))
                components["clickhouse"][table][
                    org["alias"]
                ] = await _clickhouse_table_digest(client, table, org_id)

    await run_with_store(sink, "clickhouse", _ch_handler, org_id=None)

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(postgres_uri, pool_pre_ping=True)
    try:
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as session:
            for table, org_column in _POSTGRES_DIGEST_TABLES:
                components["postgres"].setdefault(table, {})
                for org in manifest.orgs:
                    org_id = str(manifest.org_id(org["alias"]))
                    if table == "users":
                        continue  # aggregated once below (no org_id column)
                    components["postgres"][table][
                        org["alias"]
                    ] = await _postgres_table_digest(session, table, org_column, org_id)
            user_ids = [
                str(manifest.user_id(u["alias"])) for u in manifest.world["users"]
            ]
            from sqlalchemy import text

            result = await session.execute(
                text("SELECT * FROM users WHERE id::text = ANY(:ids)"),
                {"ids": user_ids},
            )
            column_names = list(result.keys())
            rows = result.fetchall()
            users_volatile = _volatile_columns_for_table("users")
            row_keys = sorted(
                _row_content_key(column_names, tuple(row), volatile=users_volatile)
                for row in rows
            )
            components["postgres"]["users"] = {
                "world": {
                    "row_count": len(row_keys),
                    "content_hash": hashlib.sha256(
                        "\n".join(row_keys).encode()
                    ).hexdigest(),
                }
            }
    finally:
        await engine.dispose()

    canonical = json.dumps(components, sort_keys=True)
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    return {
        "schema_version": WORLD_SCHEMA_VERSION,
        "master_seed": manifest.master_seed,
        "digest": digest,
        "components": components,
    }


def default_digest_path(manifest: WorldManifest) -> Path:
    return manifest.manifest_path.parent / "WORLD_DIGEST"


def write_digest(digest_doc: dict[str, Any], path: Path) -> None:
    path.write_text(json.dumps(digest_doc, indent=2, sort_keys=True) + "\n")


def read_pinned_digest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise WorldDigestDriftError(
            f"WORLD_DIGEST not found at {path} -- run `dev-hops fixtures world` "
            "(without --verify-digest) at least once to generate a pinned digest."
        )
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _resolve_postgres_uri(ns: argparse.Namespace) -> str:
    uri = (
        getattr(ns, "postgres_uri", None)
        or os.getenv("POSTGRES_URI")
        or os.getenv("DATABASE_URI")
    )
    if not uri:
        raise WorldManifestError(
            "fixtures world requires a Postgres URI (--postgres-uri or "
            "POSTGRES_URI/DATABASE_URI) -- the world's org/user/entitlement/"
            "retention/conversation rows all live in the semantic DB."
        )
    return uri


async def _run_production_verification(
    sink: str, postgres_uri: str, manifest: WorldManifest
) -> None:
    """Codex HIGH-2 fix: after generation, re-derive what a real request
    would observe through the ACTUAL production code paths and raise
    loudly on the first claim that is not actually realized -- see
    ``world_verify``'s module docstring for exactly what is (and is not)
    covered here and why.
    """

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from dev_health_ops.api.dev.work_graph_neighbors_service import MAX_NEIGHBORS

    engine = create_async_engine(postgres_uri, pool_pre_ping=True)
    try:
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )

        async def _handler(store: Any) -> None:
            client = store.client
            async with session_factory() as session:
                subjects_verified = (
                    await world_verify.verify_subjects_against_production(
                        client=client, manifest=manifest
                    )
                )
                sources_verified = (
                    await world_verify.verify_sources_against_production_data_health(
                        client=client, session=session, manifest=manifest
                    )
                )
            primary_org_id = str(manifest.org_id("primary"))
            conflicting_repo_id = str(
                uuid.uuid5(FIXTURE_NAMESPACE, "probe/source-conflicting-ci")
            )
            await world_verify.verify_conflicting_ci_runs(
                client, org_id=primary_org_id, repo_id=conflicting_repo_id
            )
            truncated_entry = next(
                (
                    entry
                    for entry in manifest.sources["matrix"]
                    if entry.get("state") == "truncated"
                ),
                None,
            )
            if truncated_entry is not None and world_verify.is_declared_blocked(
                truncated_entry
            ):
                # 2026-08-05: the truncated state cannot currently be
                # produced by any generator volume knob (see
                # world_verify.DECLARED_BLOCKED_STATUS's own docstring for
                # the full history) -- declared-blocked, not claimed, not
                # silently dropped. Loud, not silent: this line is the
                # trace that the check was intentionally skipped, not
                # forgotten.
                logging.warning(
                    "world: SKIPPING verify_truncated_work_graph -- "
                    "sources.json state='truncated' is declared-blocked "
                    "(blocked_by=%s). This claim is NOT verified and NOT "
                    "counted as realized.",
                    truncated_entry.get("blocked_by"),
                )
            else:
                await world_verify.verify_truncated_work_graph(
                    client, org_id=primary_org_id, max_neighbors=MAX_NEIGHBORS
                )
            logging.info(
                "world: production-path verification OK -- %d subject claim(s), "
                "%d source-state claim(s), plus conflicting/truncated raw checks",
                len(subjects_verified),
                len(sources_verified),
            )

        await run_with_store(sink, "clickhouse", _handler, org_id=None)
    finally:
        await engine.dispose()


async def _generate_world(ns: argparse.Namespace, manifest: WorldManifest) -> int:
    postgres_uri = _resolve_postgres_uri(ns)

    await _run_postgres_phase(postgres_uri, manifest)
    logging.info(
        "world: seeded orgs/users/entitlements/retention/sync-config/conversations"
    )

    roster = collect_repo_roster(manifest)
    total_repos = sum(len(repos) for repos in roster.values())
    generated = 0
    for org_alias, repo_names in roster.items():
        org_id = manifest.org_id(org_alias)
        for repo_full_name in sorted(repo_names):
            repo_ns = _generation_namespace(
                manifest,
                org_alias=org_alias,
                org_id=org_id,
                repo_full_name=repo_full_name,
                sink=ns.sink,
                allow_mixed_org=getattr(ns, "allow_mixed_org", False),
            )
            with _frozen_clock(manifest.pinned_now):
                rc = await run_fixtures_generation(repo_ns)
            if rc != 0:
                logging.error(
                    "world: fixtures generate failed for org=%s repo=%s (rc=%d)",
                    org_alias,
                    repo_full_name,
                    rc,
                )
                return rc
            generated += 1
            logging.info(
                "world: generated repo %d/%d (%s / %s)",
                generated,
                total_repos,
                org_alias,
                repo_full_name,
            )

    await _run_clickhouse_postprocess(ns.sink, manifest)

    await _run_production_verification(ns.sink, postgres_uri, manifest)

    digest_doc = await compute_world_digest(
        manifest, sink=ns.sink, postgres_uri=postgres_uri
    )
    digest_path = (
        Path(ns.digest_path)
        if getattr(ns, "digest_path", None)
        else default_digest_path(manifest)
    )
    write_digest(digest_doc, digest_path)
    logging.info(
        "world: wrote WORLD_DIGEST=%s to %s", digest_doc["digest"], digest_path
    )
    cross_gen_status = manifest.world.get("cross_generation_digest_status")
    if isinstance(cross_gen_status, dict) and cross_gen_status.get("status") == (
        "declared-blocked"
    ):
        # CHAOS-3432: loud, not silent, on every single generation -- this
        # digest is a valid, verifiable PIN for THIS database (--verify-digest
        # against it works), but cross-generation reproducibility (would a
        # SECOND independent run produce the SAME digest) is not proven.
        logging.warning(
            "world: WORLD_DIGEST cross-generation reproducibility is "
            "DECLARED-BLOCKED (blocked_by=%s) -- this digest is a valid pin "
            "for THIS database only; do not assume a second independent "
            "generation would reproduce it.",
            cross_gen_status.get("blocked_by"),
        )
    return 0


async def _verify_digest(ns: argparse.Namespace, manifest: WorldManifest) -> int:
    postgres_uri = _resolve_postgres_uri(ns)
    digest_path = (
        Path(ns.digest_path)
        if getattr(ns, "digest_path", None)
        else default_digest_path(manifest)
    )
    pinned = read_pinned_digest(digest_path)
    live = await compute_world_digest(manifest, sink=ns.sink, postgres_uri=postgres_uri)
    if live["digest"] != pinned["digest"]:
        drifted_components = _diff_components(
            pinned.get("components", {}), live.get("components", {})
        )
        raise WorldDigestDriftError(
            "WORLD_DIGEST verification FAILED: live database no longer matches "
            f"the pinned digest at {digest_path}. pinned={pinned['digest']} "
            f"live={live['digest']}. Drifted component(s): {drifted_components}"
        )
    logging.info("world: WORLD_DIGEST verified OK (%s)", live["digest"])
    return 0


def _diff_components(pinned: dict[str, Any], live: dict[str, Any]) -> list[str]:
    drifted: list[str] = []
    for store in set(pinned) | set(live):
        pinned_store = pinned.get(store, {})
        live_store = live.get(store, {})
        for table in set(pinned_store) | set(live_store):
            if pinned_store.get(table) != live_store.get(table):
                drifted.append(f"{store}.{table}")
    return sorted(drifted)


async def run_fixtures_world(ns: argparse.Namespace) -> int:
    # Entrypoint scratch-DB guard (Codex CRITICAL, 2026-08-05): checked
    # FIRST, before manifest loading and before any store/session/client is
    # constructed. Both target databases must be an explicit, disposable
    # scratch database -- see UnsafeSinkError's docstring.
    try:
        postgres_uri = _resolve_postgres_uri(ns)
        _require_scratch_database(getattr(ns, "sink", None), kind="clickhouse")
        _require_scratch_database(postgres_uri, kind="postgres")
    except (WorldManifestError, UnsafeSinkError) as exc:
        logging.error("world: refusing to run: %s", exc)
        return 1

    try:
        manifest = load_world_manifest(ns.manifest)
    except (WorldManifestError, FileNotFoundError, json.JSONDecodeError) as exc:
        logging.error("world: manifest validation failed: %s", exc)
        return 1

    if getattr(ns, "verify_digest", False):
        try:
            return await _verify_digest(ns, manifest)
        except WorldDigestDriftError as exc:
            logging.error("%s", exc)
            return 1

    try:
        return await _generate_world(ns, manifest)
    except (WorldManifestError, world_verify.WorldVerificationError) as exc:
        logging.error("world: %s", exc)
        return 1
