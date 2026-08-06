"""Ambient-environment isolation for the test suite (CHAOS-3402).

Why this exists
---------------
CI runners start each job from a bare environment and set exactly the handful of
variables listed in ``.github/workflows/test.yml``. A developer machine does not:
``ops/.env`` is loaded by direnv in every ops shell, so ``pytest`` inherits ~65
real configuration values. Tests whose intent is "this variable is ABSENT" then
see a real-looking-but-wrong value and take a different code path. Fifteen tests
across five files pass in CI and fail locally purely for that reason, with four
independent root causes:

* ``GITHUB_APP_PRIVATE_KEY_PATH`` (a RELATIVE path) makes the credential
  resolver try to read a GitHub App key instead of reporting "no credentials".
* ``AUTH_AUTO_CREATE_ORG_ON_REGISTER=false`` makes ``/auth/register`` return
  ``org_id: null`` by design, so tests asserting auto-created orgs fail.
* ``LOG_LEVEL=debug`` un-quiets ``aiosqlite``, which echoes raw SQL with bind
  parameters, so log-sanitization tests that sweep every captured record
  rediscover the secret they planted.
* ``LICENSE_PRIVATE_KEY`` (a real 64-byte key where a 32-byte seed is required)
  reaches a CLI subprocess through ``os.environ.copy()``.

The fix is one class-level guard rather than fifteen per-test patches: before
collection, remove every variable the application reads that no CI lane or gate
script provides. What remains is the CI-equivalent environment, so a local run
and a CI run see the same inputs.

Deriving the list
-----------------
``SCRUB_ENV_NAMES`` is the union of

1. every environment variable name read by ``src/dev_health_ops/**`` through
   ``os.getenv`` / ``os.environ.get`` / ``os.environ[...]`` (string literals and
   module-level string constants), and
2. every name declared in the committed ``.env.example``,

minus :data:`KEEP_ENV_NAMES`. The list is checked in rather than recomputed on
every ``pytest`` start (the scan costs ~0.9s per xdist worker) and
``tests/test_env_isolation_contract.py`` re-derives it and fails loudly on
drift. To regenerate after adding a new ``os.getenv`` call::

    python -c "import sys; sys.path.insert(0,'tests'); \
        import _env_isolation as e; print(e.render_scrub_literal())"
"""

from __future__ import annotations

import ast
import os
import re
from collections.abc import Iterable, Mapping, MutableMapping
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src" / "dev_health_ops"
ENV_EXAMPLE = REPO_ROOT / ".env.example"

#: Opt-out hatch for debugging: a comma-separated list of names to leave alone
#: for one run (e.g. ``DEV_HEALTH_TEST_ENV_ALLOW=LOG_LEVEL pytest ...``). The
#: exemption is echoed into the pytest header so a run that took it never reads
#: as a clean run.
ALLOW_ENV = "DEV_HEALTH_TEST_ENV_ALLOW"

# ---------------------------------------------------------------------------
# Keep list — every entry is a variable some CI lane, ci/ gate script, or test
# opt-in gate SETS deliberately. Scrubbing any of these would break the lane
# that supplies it, so ambient exposure here is accepted and CI-equivalent.
# ---------------------------------------------------------------------------
KEEP_ENV_NAMES: frozenset[str] = frozenset(
    {
        # --- analytics / semantic DBs, supplied by CI and by ci/local_validate.sh
        # test.yml "Run parallel unit test contract" sets all four; local_validate
        # exports CLICKHOUSE_URI at an isolated scratch db for the clickhouse-marked
        # tier and the live argMax proof. Scrubbing these makes the gate's live
        # stage and 36 CH-reading test modules silently lose their target.
        # (DATABASE_URL, the legacy alias, is NOT here: no lane sets it, and it
        # feeds the same resolve_credentials_sync branch DATABASE_URI does.)
        "CLICKHOUSE_URI",
        "DATABASE_URI",
        "POSTGRES_URI",
        "SECONDARY_DATABASE_URI",
        # --- opt-in live-Postgres tiers. test.yml's second pytest step sets these;
        # six test modules read them from the ambient environment to decide whether
        # to run. Scrubbing would silently skip an entire tier.
        "DEV_HEALTH_POSTGRES_TEST_URI",
        "DEV_HEALTH_TEST_POSTGRES_ADMIN_URI",
        "METRIC_BRIDGE_POSTGRES_TEST_URI",
        "POSTGRES_SYNC_SCHEDULER_TEST_URL",
        "WORKER_OUTBOX_POSTGRES_TEST_URI",
        "EXPLAIN_CLICKHOUSE_URI",
        # --- other opt-in tiers read from ambient by tests themselves.
        "ASK_DEV_LIVE_OPENAI_BASE_URL",
        "ASK_DEV_LIVE_OPENAI_MODEL",
        "GITHUB_PRIVATE_REPO",
        "LIVE_E2E_BASE_URL",
        "PAGERDUTY_API_TOKEN",
        "PAGERDUTY_LIVE_SMOKE",
        "PAGERDUTY_REGION",
        "SKIP_INTEGRATION_TESTS",
        "TEST_ORG_ID",
        # --- provider tokens exported by ci/run_tests.sh for the integration tier
        # (GH_TOKEN -> GITHUB_TOKEN, GL_TOKEN -> GITLAB_TOKEN) and read from ambient
        # by tests/test_connectors_integration.py and friends.
        "GITHUB_TOKEN",
        "GITLAB_TOKEN",
        "GITLAB_URL",
        # --- set by a CI lane or by ci/local_validate.sh for the process under test.
        "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER",
        "DISABLE_DOTENV",
        "ENVIRONMENT",
        "JWT_SECRET_KEY",
        "OPERATIONAL_ORDERING_CONTRACT",
        "ORG_ID",
        # OTEL off in CI and in the gate: with it on, the exporter retries to
        # localhost:4317 on every span, flooding logs and slowing the suite.
        "OTEL_ENABLED",
        # --- owned by pytest itself.
        "PYTEST_CURRENT_TEST",
        "CI",
        "GITHUB_ACTIONS",
    }
)

# ---------------------------------------------------------------------------
# Conditional keeps — scrubbed by default, retained only when the lane that
# needs them announces itself. Maps the variable to the sentinel that proves
# the lane is running.
# ---------------------------------------------------------------------------
CONDITIONAL_KEEP_ENV_NAMES: dict[str, str] = {
    # REDIS_URL is a *pollutant* in the unit tier and a *requirement* in exactly
    # one CI lane, so it can be neither plainly scrubbed nor plainly kept.
    #
    # Pollutant: ops/.env points it at the live shared valkey container, whose
    # rate-limit state survives between tests in an xdist worker. With it set,
    # `pytest tests/test_linear_provider.py` fails
    # TestLinearClientRetry::test_429_backoff_grows_exponentially (assert 5 == 4);
    # with it unset, 65/65 pass. The whole file, not the single test -- running
    # that test alone passes either way, which is how CHAOS-3402 came to exclude
    # it as a "pure concurrency flake". No unit lane supplies it: `test.yml`,
    # `ci/run_tests.sh` and `ci/local_validate.sh` mention neither REDIS_URL nor
    # VALKEY.
    #
    # Requirement: ci/run_live_backend_e2e.sh:399 exports it, and :406 runs
    # tests/test_external_ingest_customer_push_live.py -- "the only module in
    # this repo that needs all three live services at once", which reads
    # REDIS_URL at import (:91) and skipifs the WHOLE MODULE without it (:96).
    # An unconditional scrub would turn that lane's coverage into a silent skip.
    #
    # LIVE_E2E_BASE_URL is that lane's own sentinel, exported at
    # run_live_backend_e2e.sh:405 -- one line after REDIS_URL, same subshell.
    "REDIS_URL": "LIVE_E2E_BASE_URL",
}

# ---------------------------------------------------------------------------
# Scrub list — derived; see module docstring. Regenerate with
# ``render_scrub_literal()`` when src/ starts reading a new variable.
# ---------------------------------------------------------------------------
SCRUB_ENV_NAMES: frozenset[str] = frozenset(
    {
        "ALLOWED_CHECKOUT_DOMAINS",
        "ALLOW_STALE_FEATURE_BUNDLES",
        "ANTHROPIC_API_KEY",
        "APP_BASE_URL",
        "APP_ENV",
        "ASK_DEV_ACCEPTANCE_OPENAI_API_KEY",
        "ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL",
        "ASK_DEV_ACCEPTANCE_OPENAI_PORT",
        "ASK_DEV_LIVE_ACCEPTANCE",
        "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD",
        "ASK_DEV_QUA_SHADOW_ENABLED",
        "ASK_DEV_SCRIPTED_PROVIDER_ROLE",
        "ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR",
        "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX",
        "ASK_DEV_QUA_SHADOW_ENABLED",
        "ATLASSIAN_API_TOKEN",
        "ATLASSIAN_CLIENT_ENABLED",
        "ATLASSIAN_CLIENT_ID",
        "ATLASSIAN_CLIENT_SECRET",
        "ATLASSIAN_CLOUD_ID",
        "ATLASSIAN_COOKIES_JSON",
        "ATLASSIAN_EMAIL",
        "ATLASSIAN_GQL_BASE_URL",
        "ATLASSIAN_GQL_ENABLED",
        "ATLASSIAN_GQL_EXPERIMENTAL_APIS",
        "ATLASSIAN_JIRA_BASE_URL",
        "ATLASSIAN_JIRA_SPRINT_IDS_FIELD",
        "ATLASSIAN_JIRA_STORY_POINTS_FIELD",
        "ATLASSIAN_OAUTH_ACCESS_TOKEN",
        "ATLASSIAN_OAUTH_GQL_ENDPOINT",
        "ATLASSIAN_OAUTH_REFRESH_TOKEN",
        "AUTH_AUTO_CREATE_ORG_ON_REGISTER",
        "AUTO_RUN_MIGRATIONS",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_TENANT_ID",
        "BUGSINK_BASE_URL",
        "BUGSINK_CREATE_SUPERUSER",
        "BUGSINK_SECRET_KEY",
        "BUSINESS_HOURS_END",
        "BUSINESS_HOURS_START",
        "BUSINESS_TIMEZONE",
        "BYO_LLM_MAX_BUDGET_MICRO_USD",
        "CELERY_BROKER_URL",
        "CELERY_RESULT_BACKEND",
        "COMMIT_STATS_MAX_COMMITS",
        "CORS_ALLOWED_ORIGINS",
        "DASHSCOPE_API_KEY",
        "DASHSCOPE_BASE_URL",
        "DATABASE_URL",
        "DEV_HEALTH_ALLOW_PLACEHOLDER_CLICKHOUSE_URI",
        "DEV_HEALTH_SINK",
        "EMAIL_API_KEY",
        "EMAIL_FROM_ADDRESS",
        "EMAIL_PROVIDER",
        "ENV",
        "EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES",
        "EXTERNAL_INGEST_MAX_BODY_BYTES",
        "EXTERNAL_INGEST_MAX_RECORDS",
        "EXTERNAL_INGEST_STATUS_RETENTION_DAYS",
        "FULLCHAOS_API_TOKEN",
        "FULLCHAOS_API_URL",
        "FULLCHAOS_INGEST_TOKEN",
        "FULLCHAOS_ORG_ID",
        "GEMINI_API_KEY",
        "GEMINI_BASE_URL",
        "GEMINI_MODEL",
        "GITHUB_APP_CALLBACK_URL",
        "GITHUB_APP_CLIENT_ID",
        "GITHUB_APP_CLIENT_SECRET",
        "GITHUB_APP_ID",
        "GITHUB_APP_INSTALLATION_ID",
        "GITHUB_APP_PRIVATE_KEY",
        "GITHUB_APP_PRIVATE_KEY_PATH",
        "GITHUB_APP_SLUG",
        "GITHUB_BASE_URL",
        "GITHUB_HTTP_BACKOFF_FACTOR",
        "GITHUB_HTTP_BACKOFF_MAX",
        "GITHUB_HTTP_MAX_RETRIES",
        "GITHUB_HTTP_TIMEOUT_SECONDS",
        "GITHUB_LINEAR_LINKBACK_BOTS",
        "GITHUB_PROJECTS_V2",
        "GITHUB_URL",
        "GITHUB_WEBHOOK_SECRET",
        "GITLAB_NOTES_LIMIT",
        "GITLAB_WEBHOOK_TOKEN",
        "GRAPHQL_AUTH_REQUIRED",
        "GRAPHQL_MAX_QUERY_BYTES",
        "HIDE_MIGRATED_CHILD_CONFIGS",
        "IDENTITY_MAPPING_PATH",
        "IMPERSONATION_TTL_MINUTES",
        "INGEST_API_KEYS",
        "INGEST_ASYNC_INSERT",
        "INGEST_ASYNC_INSERT_TIMEOUT_MS",
        "INGEST_SIGNING_SECRET",
        "INSTANCE_ID",
        "INVESTMENT_LLM_BATCH_MIN_ITEMS",
        "INVESTMENT_LLM_BATCH_MODE",
        "INVESTMENT_LLM_BATCH_POLL_INTERVAL_SECONDS",
        "INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS",
        "INVESTMENT_LLM_CONCURRENCY",
        "INVESTMENT_MATERIALIZE_CHUNK_SIZE",
        "INVESTMENT_MAX_COMPONENT_NODES",
        "JIRA_API_TOKEN",
        "JIRA_BASE_URL",
        "JIRA_COMMENTS_LIMIT",
        "JIRA_EMAIL",
        "JIRA_EPIC_LINK_FIELD",
        "JIRA_FETCH_ALL",
        "JIRA_FETCH_BOARD_SPRINTS",
        "JIRA_FETCH_COMMENTS",
        "JIRA_FETCH_WORKLOGS",
        "JIRA_JQL",
        "JIRA_OPS_PROJECT_TYPES",
        "JIRA_PROJECT_KEYS",
        "JIRA_SPRINT_FIELD",
        "JIRA_STORY_POINTS_FIELD",
        "JIRA_USE_PROVIDER",
        "JIRA_WEBHOOK_SECRET",
        "JWT_AUDIENCE",
        "JWT_ISSUER",
        "LICENSE_KEY",
        "LICENSE_PRIVATE_KEY",
        "LICENSE_PUBLIC_KEY",
        "LICENSE_SECRET_KEY",
        "LINEAR_API_KEY",
        "LINEAR_BACKFILL_MAX_WINDOW_DAYS",
        "LINEAR_COMMENTS_LIMIT",
        "LINEAR_TRUSTED_SCM_HOSTS",
        "LLM_API_KEY",
        "LLM_BASE_URL",
        "LLM_MODEL",
        "LLM_PROVIDER",
        "LMSTUDIO_BASE_URL",
        "LMSTUDIO_MODEL",
        "LOCAL_LLM_API_KEY",
        "LOCAL_LLM_BASE_URL",
        "LOCAL_LLM_MODEL",
        "LOG_JSON",
        "LOG_LEVEL",
        "MIGRATION_DATABASE_URI",
        "MIGRATION_DATABASE_URI_FILE",
        "OLLAMA_BASE_URL",
        "OLLAMA_MODEL",
        "OPENAI_API_KEY",
        "OTEL_ENVIRONMENT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_METRIC_EXPORT_INTERVAL",
        "OTEL_SAMPLE_RATE",
        "OTEL_SERVICE_NAME",
        "PAGERDUTY_SUBDOMAIN",
        "PAGERDUTY_WEBHOOK_TRANSPORT",
        "PAGER_DUTY_CLIENT_ID",
        "PAGER_DUTY_REDIRECT_URI",
        "PAGER_DUTY_SECRET",
        "PGBOUNCER_TRANSACTION_MODE",
        "POSTGRES_CONNECT_TIMEOUT_SECONDS",
        "PROVIDER_SYNC_QUEUES_ENABLED",
        "QWEN_API_KEY",
        "QWEN_LOCAL_MODEL",
        "QWEN_MODEL",
        # Scrubbed by default; see CONDITIONAL_KEEP_ENV_NAMES for the one lane
        # that gets it back.
        "REDIS_URL",
        "REPO_PATH",
        "REPO_UUID",
        "RESEND_API_KEY",
        "RIVER_COORDINATOR_DATABASE_PASSWORD",
        "RIVER_COORDINATOR_DATABASE_ROLE",
        "RIVER_DOMAIN_DATABASE_PASSWORD",
        "RIVER_DOMAIN_DATABASE_ROLE",
        "RIVER_QUEUE_DATABASE_PASSWORD",
        "RIVER_QUEUE_DATABASE_ROLE",
        "SENTRY_DSN",
        "SENTRY_ENVIRONMENT",
        "SENTRY_PROFILES_RATE",
        "SENTRY_SEND_PII",
        "SENTRY_TRACES_RATE",
        "SERVICE_NAME",
        "SERVICE_VERSION",
        "SETTINGS_ENCRYPTION_KEY",
        "SETTINGS_ENCRYPTION_SALT",
        "SMTP_HOST",
        "SMTP_PASSWORD",
        "SMTP_PORT",
        "SMTP_USERNAME",
        "SMTP_USE_TLS",
        "SOCIAL_GITHUB_CLIENT_ID",
        "SOCIAL_GITHUB_CLIENT_SECRET",
        "SOCIAL_GITLAB_CLIENT_ID",
        "SOCIAL_GITLAB_CLIENT_SECRET",
        "SOCIAL_GOOGLE_CLIENT_ID",
        "SOCIAL_GOOGLE_CLIENT_SECRET",
        "STATUS_MAPPING_PATH",
        "STRIPE_PRICE_ID_ENTERPRISE",
        "STRIPE_PRICE_ID_TEAM",
        "STRIPE_SECRET_KEY",
        "STRIPE_WEBHOOK_SECRET",
        "SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS",
        "SYNC_BUDGET_BUCKET_LIMITS",
        "SYNC_BUDGET_DEFAULT_LIMIT",
        "SYNC_BUDGET_DEFERRAL_JITTER_SECONDS",
        "SYNC_BUDGET_DEFERRAL_SECONDS",
        "SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS",
        "SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT",
        "SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS",
        "SYNC_COST_CLASS_QUEUES",
        "SYNC_DISPATCH_REDISPATCH_COUNTDOWN",
        "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS",
        "SYNC_OUTBOX_CLAIM_TIMEOUT_SECONDS",
        "SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS",
        "SYNC_REFERENCE_DISCOVERY_LEASE_SECONDS",
        "SYNC_REFERENCE_DISCOVERY_MAX_ATTEMPTS",
        "SYNC_REFERENCE_DISCOVERY_MAX_LIFETIME_SECONDS",
        "SYNC_REFERENCE_DISCOVERY_READBACK_SECONDS",
        "SYNC_RUN_AUTH_STRICT",
        "SYNC_RUN_MAX_UNITS",
        "SYNC_UNIT_CONCURRENCY_PER_BUCKET",
        "SYNC_UNIT_DISPATCH_STALE_SECONDS",
        "SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES",
        "SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS",
        "SYNC_UNIT_MAX_LIFETIME_SECONDS",
        "SYNC_UNIT_RUNNING_LEASE_SECONDS",
        "SYNC_UNIT_RUNNING_STALE_SECONDS",
        "SYNC_WATERMARK_OVERLAP",
        "TEAMS_API_TIMEOUT",
        "TEAMS_USE_BETA_API",
        "TEAM_MAPPING_PATH",
        "TELEMETRY_ENDPOINT",
        "TRIAL_DAYS",
        "TRUSTED_PROXIES",
        "VALIDATE_LOADER_OUTPUT",
        "WORKER_GITHUB_REPO_METADATA_ENABLED",
        "WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED",
        "WORKER_OPERATIONAL_BRIDGE_TOKEN",
    }
)


# ---------------------------------------------------------------------------
# Derivation (used by the drift guard, not on the hot path)
# ---------------------------------------------------------------------------

_ENV_READERS = {"getenv", "get", "pop"}
_ENV_TARGETS = {"os", "os.environ", "environ"}
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level ``NAME = "STRING"`` bindings.

    ``api/auth/config.py`` reads ``os.getenv(AUTH_AUTO_CREATE_ORG_ENV)``; without
    resolving constants the scan misses the very variable that broke eight tests.
    """
    consts: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        if not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                consts[target.id] = node.value.value
    return consts


def _literal_or_const(node: ast.expr, consts: Mapping[str, str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return consts.get(node.id)
    return None


def discover_src_env_names(src_root: Path | None = None) -> set[str]:
    """Every env var name ``src/dev_health_ops`` reads."""
    root = src_root or SRC_ROOT
    names: set[str] = set()
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - defensive
            continue
        consts = _module_string_constants(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr in _ENV_READERS
                    and node.args
                    and ast.unparse(func.value) in _ENV_TARGETS
                ):
                    name = _literal_or_const(node.args[0], consts)
                    if name and _ENV_NAME_RE.match(name):
                        names.add(name)
            elif isinstance(node, ast.Subscript) and isinstance(
                node.value, ast.Attribute
            ):
                if ast.unparse(node.value) == "os.environ":
                    name = _literal_or_const(node.slice, consts)
                    if name and _ENV_NAME_RE.match(name):
                        names.add(name)
    return names


def discover_env_example_names(path: Path | None = None) -> set[str]:
    """Every name declared (set or commented-out) in ``.env.example``."""
    target = path or ENV_EXAMPLE
    pattern = re.compile(r"^\s*#?\s*([A-Z][A-Z0-9_]*)=")
    return {
        match.group(1)
        for line in target.read_text(encoding="utf-8").splitlines()
        if (match := pattern.match(line))
    }


def derive_scrub_names() -> set[str]:
    """Recompute :data:`SCRUB_ENV_NAMES` from the tree."""
    return (discover_src_env_names() | discover_env_example_names()) - KEEP_ENV_NAMES


def render_scrub_literal() -> str:
    """Print a paste-ready ``SCRUB_ENV_NAMES`` body (see module docstring)."""
    return "\n".join(f'        "{name}",' for name in sorted(derive_scrub_names()))


# ---------------------------------------------------------------------------
# The scrub itself
# ---------------------------------------------------------------------------


def exempted_names(environ: Mapping[str, str] | None = None) -> frozenset[str]:
    """Names the caller asked to keep for this run via :data:`ALLOW_ENV`."""
    source = os.environ if environ is None else environ
    raw = source.get(ALLOW_ENV, "")
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def lane_conditional_keeps(
    environ: Mapping[str, str] | None = None,
) -> frozenset[str]:
    """Conditional keeps whose lane sentinel is present in ``environ``.

    Reported separately from :func:`exempted_names` in the pytest header: "a
    lane needs this" and "a human asked for this" are different claims, and a
    run that silently conflated them could not be audited afterwards.
    """
    source = os.environ if environ is None else environ
    return frozenset(
        name
        for name, sentinel in CONDITIONAL_KEEP_ENV_NAMES.items()
        if sentinel in source
    )


def scrub_ambient_env(
    environ: MutableMapping[str, str],
    names: Iterable[str] = SCRUB_ENV_NAMES,
    exempt: Iterable[str] = (),
) -> list[str]:
    """Delete ``names`` from ``environ``; return the names actually removed.

    Returning the removed names (rather than a count or nothing) is deliberate:
    the caller reports them in the pytest header, so a run that started from a
    polluted shell says so out loud instead of looking like a clean run.
    """
    exempt_set = set(exempt)
    removed = []
    for name in sorted(names):
        if name in exempt_set:
            continue
        if environ.pop(name, None) is not None:
            removed.append(name)
    return removed
