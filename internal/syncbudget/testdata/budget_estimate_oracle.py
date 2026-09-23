#!/usr/bin/env python3
"""Live-Python oracle for the in-process dispatch budget estimator (CHAOS-6243).

The Go sync dispatcher used to call the Python api's
/api/internal/worker-sync/dispatch-budget-estimate; internal/syncbudget now
does that work in Go. This script is the Python half of the differential
check. It GENERATES every case (so both sides read the same input bytes),
runs the REAL, unmodified production functions on each, and prints one JSON
document:

- ``estimate``: dev_health_ops.sync.budget.estimate_provider_budget over a
  real SyncTaskContext, for a matrix of provider x dataset x credential
  mapping x processor flags x window x environment. The result is each
  estimate's BudgetEstimate.to_dict() -- the whole record, not chosen
  fields -- or the exception class name.
- ``fingerprint``: dev_health_ops.credentials.fingerprint.credential_fingerprint
  (the run-auth witness the bootstrap compares under SYNC_RUN_AUTH_STRICT).
- ``env_credentials``: dev_health_ops.workers.task_utils._resolve_env_credentials.
- ``credential_mapping``: task_utils._credential_mapping over a row whose
  ciphertext THIS script encrypted with core.encryption.encrypt_value, so
  the Go side decrypts real production ciphertext.

Credential mappings, processor flags and dataset options travel as JSON
TEXT: Python json.loads()s them exactly as the bootstrap does and the Go
side decodes the same bytes.

Imports run with stdout redirected (instrumentation may print), as in
internal/syncdispatchruntime/testdata/dispatch_admission_oracle.py.
"""

from __future__ import annotations

import contextlib
import itertools
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

ORG = "00000000-0000-4000-8000-000000000001"
INTEGRATION = "00000000-0000-4000-8000-000000000002"
ROW_UUID = "00000000-0000-4000-8000-000000000003"

# Every variable any estimator, _resolve_env_credentials or the bootstrap
# reads. Cleared before each case so an ambient value cannot leak in.
ENV_NAMES = (
    "JIRA_FETCH_WORKLOGS",
    "ATLASSIAN_GQL_ENABLED",
    "ATLASSIAN_JIRA_BASE_URL",
    "JIRA_BASE_URL",
    "GITHUB_TOKEN",
    "GITHUB_URL",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY_PATH",
    "GITHUB_APP_INSTALLATION_ID",
    "GITLAB_TOKEN",
    "GITLAB_URL",
    "JIRA_API_TOKEN",
    "JIRA_EMAIL",
    "LINEAR_API_KEY",
    "ATLASSIAN_API_TOKEN",
    "ATLASSIAN_EMAIL",
    "ATLASSIAN_CLOUD_ID",
    "LAUNCHDARKLY_API_KEY",
    "TELEMETRY_API_KEY",
    "PAGER_DUTY_CLIENT_ID",
    "PAGER_DUTY_SECRET",
    "PAGERDUTY_API_TOKEN",
    "PAGERDUTY_SUBDOMAIN",
    "PAGERDUTY_REGION",
)

PROVIDERS = ("github", "gitlab", "jira", "linear", "pagerduty", "launchdarkly")

# Every DatasetKey value, plus one no estimator knows.
DATASETS = (
    "repo-metadata",
    "commits",
    "commit-stats",
    "files",
    "blame",
    "prs",
    "pr-reviews",
    "pr-comments",
    "cicd",
    "tests",
    "deployments",
    "incidents",
    "security",
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
    "feature-flags",
    "services",
    "business-services",
    "escalation-policies",
    "schedules",
    "on-calls",
    "users",
    "teams",
    "incident-alerts",
    "incident-log-entries",
    "incident-notes",
    "nope",
)

BASE = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
WINDOWS = (
    (None, None),
    (BASE, None),
    (BASE, BASE + timedelta(hours=12)),
    (BASE, BASE + timedelta(days=1)),
    (BASE, BASE + timedelta(days=7, hours=23)),
    (BASE, BASE - timedelta(days=2, hours=12)),
    (BASE, BASE + timedelta(days=90)),
)

FLAGS = (
    "{}",
    '{"sync_prs": true}',
    '{"sync_prs": false}',
    '{"sync_prs": 0, "fetch_worklogs": 1, "gql_enabled": "x"}',
    '{"jira_fetch_worklogs": [], "atlassian_gql_enabled": {"a": 1}}',
    "null",
)

# One ordinary mapping per provider, for the dataset x window x flags sweep.
BASE_CREDENTIALS = {
    "github": '{"token": "ghp_x"}',
    "gitlab": '{"token": "glpat", "base_url": "https://gitlab.example.com"}',
    "jira": '{"email": "a@b.c", "api_token": "t", "base_url": "https://acme.atlassian.net"}',
    "linear": '{"api_key": "lin"}',
    "pagerduty": '{"subdomain": "acme", "region": "eu"}',
    "launchdarkly": '{"api_key": "ld", "project_key": "p"}',
}

# Mapping shapes every provider sees: non-mappings, empty, and the value
# and URL shapes whose Python str()/json.dumps()/urlparse() behaviour a
# port can get wrong.
SHARED_CREDENTIALS = (
    "[]",
    '"text"',
    "null",
    "5",
    "{}",
    '{"token": ""}',
    '{"token": 12345, "base_url": "https://GitHub.Example.COM:8443/api/v3"}',
    '{"token": {"b": 2, "a": [1, "it\'s", "say \\"hi\\""]}, "baseUrl": "https://h.example"}',
    '{"base_url": "", "baseUrl": "https://second.example/x"}',
    '{"base_url": null, "baseUrl": null}',
    '{"base_url": "", "baseUrl": ""}',
    '{"base_url": "gitlab.example.com"}',
    '{"base_url": "//bare.example/path"}',
    '{"base_url": "  \\t https://sp\\nace.example/ "}',
    '{"base_url": "HTTP://Upper.Example"}',
    '{"base_url": "https://u:p@user.example:99/"}',
    '{"base_url": "https://[::1]:8080/"}',
    '{"base_url": "https://[fe80::1%eTh0]/"}',
    '{"base_url": "https://[::1/"}',
    '{"base_url": "https://[1.2.3.4]/"}',
    '{"base_url": "https://[v1.fe]/"}',
    '{"base_url": "https://[vz.x]/"}',
    '{"base_url": "https://host\\u2100.example/"}',
    '{"base_url": "https://exämple.example/"}',
    '{"base_url": "https://ΟΔΟΣ.example/"}',
    '{"base_url": "https://İstanbul.example/"}',
    '{"base_url": "mailto:x@y"}',
    '{"base_url": "1https://digit.example"}',
    '{"base_url": 123}',
    '{"base_url": ["https://list.example"]}',
    '{"base_url": true}',
    '{"app_id": 12, "installation_id": "34", "user_id": 1.5, "group_id": 1e16, "project_id": 123456789012345678901234567890}',
    '{"app_id": {"z": null, "é": "ü"}, "username": "\\u00e9\\ud83d\\ude00", "project_key": "k", "environment": false}',
    '{"organization_id": "o", "workspace_id": null, "team_id": 0}',
    '{"email": "e", "cloud_id": "c", "cloudId": "C", "client_id": "i", "clientId": "I"}',
    '{"api_token": "a", "apiToken": 1, "access_token": [], "accessToken": [0], "refresh_token": "r", "refreshToken": false}',
    '{"private_token": "pt", "access_token": "at"}',
    '{"jira_base_url": "http://jira.example//", "jiraBaseUrl": "x"}',
    '{"jiraBaseUrl": "  jira.example/  "}',
    '{"gitlab_url": "https://gl.example", "url": "https://u.example", "base_url": "https://b.example"}',
    '{"url": "https://u.example", "base_url": "https://b.example"}',
    '{"region": "EU", "subdomain": 7}',
    '{"region": null, "subdomain": null}',
    '{"region": "eu"}',
    '{"dup": 1, "dup": 2, "token": "first", "token": "second"}',
    '{"token": "t", "base_url": "https://x.example", "x": NaN}',
)

OPTIONS = (
    "{}",
    "null",
    '{"enrichment_cap": 0}',
    '{"enrichment_cap": 1}',
    '{"enrichment_cap": 100}',
    '{"enrichment_cap": 101}',
    '{"enrichment_cap": -5}',
    '{"enrichment_cap": 1.5}',
    '{"enrichment_cap": true}',
    '{"enrichment_cap": "7"}',
    '{"enrichment_cap": null}',
    '{"enrichment_cap": 4611686018427387904}',
    '{"enrichment_cap": 100000000000000000000}',
)

ENVIRONMENTS: tuple[dict[str, str], ...] = (
    {},
    {"JIRA_FETCH_WORKLOGS": " TRUE ", "ATLASSIAN_GQL_ENABLED": "on"},
    {"JIRA_FETCH_WORKLOGS": "0", "ATLASSIAN_GQL_ENABLED": "yes "},
    {
        "ATLASSIAN_JIRA_BASE_URL": "env.atlassian.example",
        "JIRA_BASE_URL": "https://other.example",
    },
    {"JIRA_BASE_URL": "http://only-jira.example/"},
    {"ATLASSIAN_JIRA_BASE_URL": "", "JIRA_BASE_URL": ""},
)

CREDENTIAL_IDS = (None, "", ROW_UUID)


def _set_env(env: dict[str, str]) -> None:
    for name in ENV_NAMES:
        os.environ.pop(name, None)
    os.environ.update(env)


def _estimate_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []

    def add(
        provider,
        dataset,
        credentials,
        flags="{}",
        window=(None, None),
        options="{}",
        env=None,
        credential_id=ROW_UUID,
    ):
        cases.append(
            {
                "provider": provider,
                "dataset_key": dataset,
                "credential_id": credential_id,
                "credentials": credentials,
                "processor_flags": flags,
                "window_start": window[0].isoformat() if window[0] else None,
                "window_end": window[1].isoformat() if window[1] else None,
                "dataset_options": options,
                "env": env or {},
            }
        )

    for provider in PROVIDERS:
        for dataset, window, flags in itertools.product(DATASETS, WINDOWS, FLAGS):
            add(provider, dataset, BASE_CREDENTIALS[provider], flags, window)
        probe = {
            "github": "prs",
            "gitlab": "work-items",
            "jira": "work-items",
            "linear": "work-items",
            "pagerduty": "incidents",
            "launchdarkly": "feature-flags",
        }[provider]
        for credentials, credential_id in itertools.product(
            SHARED_CREDENTIALS, CREDENTIAL_IDS
        ):
            add(provider, probe, credentials, credential_id=credential_id)
    for provider_spelling in ("GitHub", "PAGERDUTY", "Jira", "bitbucket", ""):
        add(provider_spelling, "work-items", '{"token": "t"}')
    for dataset, options in itertools.product(
        (
            "incident-alerts",
            "incident-log-entries",
            "incident-notes",
            "incidents",
            "custom-thing",
        ),
        OPTIONS,
    ):
        add("pagerduty", dataset, BASE_CREDENTIALS["pagerduty"], options=options)
    for dataset, env, flags, credentials in itertools.product(
        ("work-items", "work-item-comments", "incidents"),
        ENVIRONMENTS,
        FLAGS[:4],
        (BASE_CREDENTIALS["jira"], "{}", '{"email": "e"}'),
    ):
        add(
            "jira",
            dataset,
            credentials,
            flags,
            (BASE, BASE + timedelta(days=3)),
            env=env,
        )
    return cases


def _parse(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _run_estimate(
    case: dict[str, Any], estimate_provider_budget, SyncTaskContext
) -> dict[str, Any]:
    _set_env(case["env"])
    try:
        flags_raw = json.loads(case["processor_flags"])
        options_raw = json.loads(case["dataset_options"])
        context = SyncTaskContext(
            unit_id="u",
            sync_run_id="r",
            org_id=ORG,
            integration_id=INTEGRATION,
            source_id="s",
            source_external_id="e",
            provider=case["provider"],
            dataset_key=case["dataset_key"],
            cost_class="light",
            mode="full",
            window_start=_parse(case["window_start"]),
            window_end=_parse(case["window_end"]),
            # SyncTaskBootstrap.load's own normalisation of both columns.
            processor_flags={str(k): bool(v) for k, v in dict(flags_raw or {}).items()},
            credential_id=case["credential_id"],
            decrypted_credentials=json.loads(case["credentials"]),
            db_url="",
            dataset_options=dict(options_raw or {}),
        )
        estimates = estimate_provider_budget(context)
        return {"estimates": [estimate.to_dict() for estimate in estimates]}
    except Exception as exc:  # noqa: BLE001 -- the class IS the result
        return {"error": type(exc).__name__}


def _fingerprint_cases() -> list[dict[str, Any]]:
    return [
        {"credentials": credentials, "credential_id": credential_id}
        for credentials, credential_id in itertools.product(
            SHARED_CREDENTIALS, CREDENTIAL_IDS
        )
    ]


def _env_credential_cases() -> list[dict[str, Any]]:
    everything = {name: f"v-{name.lower()}" for name in ENV_NAMES}
    partial = {
        "GITHUB_TOKEN": "t",
        "GITHUB_URL": "",
        "GITLAB_URL": "u",
        "PAGERDUTY_REGION": "eu",
    }
    return [
        {"provider": provider, "env": env}
        for provider, env in itertools.product(
            PROVIDERS + ("atlassian", "telemetry", "GitHub", "unknown"),
            ({}, everything, partial),
        )
    ]


MAPPING_CASES = (
    (
        '{"token": "t", "base_url": "https://secret.example"}',
        '{"base_url": "https://config.example", "url": "u"}',
    ),
    ('{"token": "t"}', "null"),
    ('{"token": "t"}', "{}"),
    ('{"token": "t"}', "[1, 2]"),
    ('["not", "a", "mapping"]', "{}"),
    ('["not", "a", "mapping"]', '{"base_url": "x"}'),
    ('"text"', '{"k": 1}'),
    (None, '{"base_url": "https://config-only.example"}'),
    (None, "null"),
    ('{"n": 1.0, "big": 123456789012345678901234567890, "é": "ü"}', '{"z": 1, "n": 2}'),
)


# PagerDuty descriptors whose hydration outcome is decided before any
# network or database call: the mode, PAGER_DUTY_CLIENT_ID and the presence
# of the keys hydrate_pagerduty_credentials indexes. Cases that would reach
# the token store or the token exchange are pinned by the Go integration
# tests instead.
HYDRATION_CASES: tuple[tuple[str, dict[str, str]], ...] = (
    ('{"auth_mode": "api_token", "api_token": "t", "subdomain": "s"}', {}),
    ('{"subdomain": "s"}', {}),
    ('{"auth_mode": null}', {}),
    ('{"auth_mode": "OAUTH", "subdomain": "s"}', {}),
    (
        '{"auth_mode": "oauth", "oauth_credential_name": "n", "oauth_binding_id": "b"}',
        {},
    ),
    (
        '{"auth_mode": "oauth", "oauth_credential_name": "n", "oauth_binding_id": "b"}',
        {"PAGER_DUTY_CLIENT_ID": ""},
    ),
    (
        '{"auth_mode": "oauth", "oauth_binding_id": "b"}',
        {"PAGER_DUTY_CLIENT_ID": "app"},
    ),
    (
        '{"auth_mode": "oauth", "oauth_credential_name": "n"}',
        {"PAGER_DUTY_CLIENT_ID": "app"},
    ),
    (
        '{"auth_mode": "client_credentials", "client_id": "i", "client_secret": "s", "subdomain": "d"}',
        {},
    ),
    (
        '{"auth_mode": "client_credentials", "client_secret": "s", "subdomain": "d", "region": "us"}',
        {},
    ),
    (
        '{"auth_mode": "client_credentials", "client_id": "i", "subdomain": "d", "region": "us"}',
        {},
    ),
    (
        '{"auth_mode": "client_credentials", "client_id": "i", "client_secret": "s", "region": "us"}',
        {},
    ),
)


def main() -> int:
    with open(os.devnull, "w") as devnull, contextlib.redirect_stdout(devnull):
        from dev_health_ops.core.encryption import encrypt_value
        from dev_health_ops.credentials.fingerprint import credential_fingerprint
        from dev_health_ops.providers.pagerduty.sync_auth import (
            hydrate_pagerduty_credentials,
        )
        from dev_health_ops.sync.budget import estimate_provider_budget
        from dev_health_ops.workers.sync_bootstrap import SyncTaskContext
        from dev_health_ops.workers.task_utils import (
            _credential_mapping,
            _resolve_env_credentials,
        )

    estimate = []
    for case in _estimate_cases():
        estimate.append(
            {
                "input": case,
                "python": _run_estimate(
                    case, estimate_provider_budget, SyncTaskContext
                ),
            }
        )

    fingerprint = []
    for case in _fingerprint_cases():
        try:
            result = {
                "fingerprint": credential_fingerprint(
                    json.loads(case["credentials"]),
                    credential_id=case["credential_id"],
                    integration_id=INTEGRATION,
                )
            }
        except Exception as exc:  # noqa: BLE001
            result = {"error": type(exc).__name__}
        fingerprint.append({"input": case, "python": result})

    env_credentials = []
    for case in _env_credential_cases():
        _set_env(case["env"])
        env_credentials.append(
            {"input": case, "python": _resolve_env_credentials(case["provider"])}
        )

    pagerduty_hydration = []
    for descriptor, env in HYDRATION_CASES:
        _set_env(env)
        os.environ.pop("PAGER_DUTY_CLIENT_ID", None)
        os.environ.update(env)
        try:
            hydrate_pagerduty_credentials(json.loads(descriptor), org_id=ORG)
            outcome = "ok"
        except Exception as exc:  # noqa: BLE001
            outcome = type(exc).__name__
        pagerduty_hydration.append(
            {"input": {"descriptor": descriptor, "env": env}, "python": outcome}
        )

    _set_env({})
    os.environ["SETTINGS_ENCRYPTION_KEY"] = "oracle-only-settings-key"
    os.environ.pop("SETTINGS_ENCRYPTION_SALT", None)
    credential_mapping = []
    for plaintext, config in MAPPING_CASES:
        ciphertext = encrypt_value(plaintext) if plaintext is not None else None
        row = SimpleNamespace(
            credentials_encrypted=ciphertext, config=json.loads(config)
        )
        try:
            mapping = _credential_mapping(row)
            # repr() keeps key order and value types; the Go side renders
            # its mapping the same way.
            result = {"repr": repr(mapping)}
        except Exception as exc:  # noqa: BLE001
            result = {"error": type(exc).__name__}
        credential_mapping.append(
            {
                "input": {"ciphertext": ciphertext, "config": config},
                "python": result,
            }
        )

    json.dump(
        {
            "settings_encryption_key": os.environ["SETTINGS_ENCRYPTION_KEY"],
            "estimate": estimate,
            "fingerprint": fingerprint,
            "env_credentials": env_credentials,
            "credential_mapping": credential_mapping,
            "pagerduty_hydration": pagerduty_hydration,
        },
        sys.stdout,
        ensure_ascii=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
