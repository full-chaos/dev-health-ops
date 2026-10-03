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

from cryptography.fernet import Fernet

# Fernet seals with a random IV and the clock, so a recording would differ
# from run to run. The IV is a counter and the time a constant: the token is
# still the production Fernet format, made by the real encrypt_value.
_PIN_COUNTER = itertools.count(1)


def _pinned_encrypt(self: Fernet, data: bytes) -> bytes:
    return self._encrypt_from_parts(
        data, 1700000000, next(_PIN_COUNTER).to_bytes(16, "big")
    )


setattr(Fernet, "encrypt", _pinned_encrypt)

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
    # dict() of an iterable of pairs, as SyncTaskBootstrap.load applies it.
    '[["sync_prs", 1]]',
    '[[1, true], [true, false], ["1", 1], [1.0, 0]]',
    '["ab", {"sync_prs": 1, "x": 2}]',
    "[[null, 1], [0.5, 1], [false, 1]]",
    '"text"',
    '[["only-one"]]',
    "[[[1], 2]]",
    "5",
    "true",
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
    '{"token": "t", "app_id": Infinity, "installation_id": -Infinity, "user_id": NaN}',
    '{"token": "\\ud83d\\ude00 \\ud800 lone", "base_url": " https://x.example/ "}',
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
    '[["enrichment_cap", 250]]',
    '[["enrichment_cap", 7], [1, 2], ["enrichment_cap", 350]]',
    '[["enrichment_cap"]]',
    '[{"a": 1}]',
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

# Windows the main sweep lacks: end without start, and negative spans.
EXTRA_WINDOWS = (
    (None, BASE),
    (None, BASE + timedelta(days=3)),
    (BASE, BASE - timedelta(hours=1)),
    (BASE, BASE - timedelta(days=1)),
    (BASE, BASE - timedelta(days=2)),
    (BASE, BASE - timedelta(days=2, seconds=1)),
    (BASE, BASE - timedelta(days=3, hours=23)),
)

JIRA_SECRET_KEYS = (
    "api_token",
    "apiToken",
    "access_token",
    "accessToken",
    "refresh_token",
    "refreshToken",
)

HOSTLESS_JIRA_CREDENTIALS = (
    '{"base_url": "https:///x"}',
    '{"base_url": "https://:80/"}',
    '{"base_url": "http://"}',
    '{"base_url": "///"}',
    '{"base_url": "//"}',
    '{"jira_base_url": "https://@/"}',
)

HOSTLESS_JIRA_ENVIRONMENTS: tuple[dict[str, str], ...] = (
    {"ATLASSIAN_JIRA_BASE_URL": "https://"},
    {"JIRA_BASE_URL": "http://:8080"},
    {"ATLASSIAN_JIRA_BASE_URL": "///", "JIRA_BASE_URL": "https://jira.example"},
)

BIG_CAP_OPTIONS = (
    '{"enrichment_cap": -100000000000000000000}',
    '{"enrichment_cap": -4611686018427387904}',
    '{"enrichment_cap": -9223372036854775809}',
    '{"enrichment_cap": 9223372036854775807}',
    '{"enrichment_cap": 9223372036854775808}',
    '{"enrichment_cap": 1000000000000000000000000000000}',
)


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
    # CHAOS-8386 rows: a window with only an end, and negative windows on
    # and off a whole-day boundary, for every provider and dataset.
    for provider, dataset, window in itertools.product(
        PROVIDERS, DATASETS, EXTRA_WINDOWS
    ):
        add(provider, dataset, BASE_CREDENTIALS[provider], "{}", window)
    # A lone surrogate in each secret the Jira estimator hashes, and in the
    # PagerDuty subdomain: Python's strict UTF-8 encode raises.
    for key in JIRA_SECRET_KEYS:
        add("jira", "work-items", '{"%s": "\\ud800"}' % key)
        add("jira", "work-items", '{"email": "e", "%s": "ok", "api_token": "\\ud800"}' % key)
    add("pagerduty", "incidents", '{"subdomain": "\\ud800", "region": "eu"}')
    add("pagerduty", "incidents", '{"subdomain": "\\ud83d\\ude00"}')
    # A base URL with no hostname, in a credential and in the environment.
    for provider in ("jira",):
        for credentials in HOSTLESS_JIRA_CREDENTIALS:
            add(provider, "work-items", credentials)
    for env in HOSTLESS_JIRA_ENVIRONMENTS:
        add("jira", "work-items", "{}", env=env)
        add("jira", "work-items", "[]", env=env)
    # An enrichment_cap beyond int64 on the negative side falls back to the
    # default, the positive side is computed.
    for dataset, options in itertools.product(
        ("incident-alerts", "incident-log-entries", "incident-notes"),
        BIG_CAP_OPTIONS,
    ):
        add("pagerduty", dataset, BASE_CREDENTIALS["pagerduty"], options=options)
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
    ] + _extra_fingerprint_cases()


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


EMPTY_CIPHERTEXT = object()
GARBAGE_CIPHERTEXT = object()
# A config column that is SQL NULL (None), not the JSON text "null".
SQL_NULL = object()

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
    # An empty ciphertext is falsy: decrypted is {}.
    (EMPTY_CIPHERTEXT, '{"base_url": "https://empty.example"}'),
    (EMPTY_CIPHERTEXT, "null"),
    # Ciphertext that does not decrypt, and plaintext that is not JSON.
    (GARBAGE_CIPHERTEXT, "{}"),
    (GARBAGE_CIPHERTEXT, '{"base_url": "https://x.example"}'),
    ("not json", "{}"),
    ("", "{}"),
    ('{"token": "t"}', SQL_NULL),
    (None, SQL_NULL),
    ('["not", "a", "mapping"]', '{"k": 1}'),
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


# Secrets whose UTF-8 holds a byte pair that looks like (but is not) the
# encoding of a lone surrogate (ED A0..BF): the strict encode accepts them.
SURROGATE_LOOKALIKE_SECRETS = (
    "\\u00e0",
    "\\u00e9",
    "\\ud7ff",
    "\\ud55c",
    "\\ue000",
    "\\ud83d\\ude00",
    "a\\u00e0",
    "\\u00e0a",
    "\\ud800",
    "a\\ud800",
    "\\udbff",
    "\\udfff",
    "\\ud800\\ud800",
)


def _extra_fingerprint_cases() -> list[dict[str, Any]]:
    cases = []
    for secret in SURROGATE_LOOKALIKE_SECRETS:
        for key in ("token", "api_token", "client_secret"):
            cases.append(
                {"credentials": '{"%s": "%s"}' % (key, secret), "credential_id": ROW_UUID}
            )
    return cases


# JSON documents the Go decoder, repr and dumps are compared on: json.loads,
# repr(), json.dumps(sort_keys=True, default=str, separators=(",", ":")),
# dict(value or {}) and the processor_flags comprehension, each on the same
# text. Raw lone surrogates stay out of the texts (the document is JSON, and
# only their \u escapes are listed): a text travels through JSON itself.
JSON_CURATED = (
    "",
    " ",
    "null",
    " null ",
    "nul",
    "nullx",
    "true",
    "false",
    "True",
    "NaN",
    "Infinity",
    "-Infinity",
    "+Infinity",
    "-NaN",
    "Inf",
    "0",
    "-0",
    "-",
    "--1",
    "01",
    "-01",
    "0.",
    "0.5",
    ".5",
    "1.",
    "1e",
    "1e+",
    "1E5",
    "1e-5",
    "1e400",
    "-1e400",
    "1.5e+3",
    "0e0",
    "1.0",
    "-0.0",
    "100000000000000000000",
    "-100000000000000000000",
    "9223372036854775807",
    "9223372036854775808",
    "-9223372036854775808",
    "-9223372036854775809",
    "0.1",
    "1e22",
    "1e21",
    "1e16",
    "123456789.123456789",
    "5e-324",
    "2.5e-5",
    '""',
    '"',
    '"abc',
    "\"a'b\"",
    "\"a'b\\\"c\"",
    '"\\u0041"',
    '"\\u00e9"',
    '"\\u00"',
    '"\\uZZZZ"',
    '"\\ud800"',
    '"\\ud800\\u0041"',
    '"\\udc00"',
    '"\\ud83d\\ude00"',
    '"\\ud83d\\ud83d\\ude00"',
    '"\\ude00\\ud83d"',
    '"\\ud800\\udbff"',
    '"\\x41"',
    '"\\\'"',
    '"tab\there"',
    '"nl\\nx"',
    '"\\u0000\\u001f\\u007f\\u0080\\u00a0\\u00ad\\u2028\\u2029\\ufeff\\ufffe"',
    '"\\u0301\\u200b\\u3000\\ue000\\uf8ff"',
    '"\\ufffe\\uffff\\ud7ff\\ue000\\ud83d\\ude00"',
    '"\\u00e9\\u00ff\\u0100\\u0378\\u1c80\\u2e5d"',
    '"\\ud83d\\udcc4\\ud83c\\udfff\\udb40\\udc01\\udbff\\udfff"',
    "[]",
    "[ ]",
    "[1]",
    "[1,]",
    "[,1]",
    "[1 2]",
    "[[]]",
    "[[[[[[[[[[1]]]]]]]]]]",
    "{}",
    "{ }",
    '{"a"}',
    '{"a":}',
    '{"a":1,}',
    '{,}',
    '{1: 2}',
    "{'a': 1}",
    '{"a": 1, "a": 2}',
    '{"b": 1, "a": 2, "b": 3}',
    '{"a": 1} {"b": 2}',
    "[1] x",
    "\t[1]\n",
    "\x0b[1]",
    "﻿[1]",
    " [1]",
    '["a\\u0000b"]',
    '[NaN, Infinity, -Infinity]',
    '{"a": NaN, "b": [Infinity]}',
    '[[1, 2], [1.0, 3], [true, 4], [0, 5], [false, 6], [0.0, 7], [-0.0, 8]]',
    '[[1, 2], [true, 3]]',
    '[[true, 2], [1, 3]]',
    '[[1.5, 2], [1.5, 3], [2.5, 4]]',
    '[[1e400, 1], [1e400, 2], [-1e400, 3]]',
    # Declared divergence (Go keeps two NaN keys, see the Go test).
    '[[NaN, 1], [NaN, 2]]',
    '[[null, 1], [null, 2]]',
    '[["a", 1], ["a", 2], ["b", 3]]',
    '[["a", 1], ["b"]]',
    '[["a", 1, 2]]',
    '[["a", 1], "bc", {"x": 1, "y": 2}]',
    '["ab", "cd", "a"]',
    '["", "x"]',
    '["é1", "ü2"]',
    '["\\ud83d\\ude00"]',
    '[{"k": 1, "j": 2}]',
    '[{"k": 1}]',
    '[{}]',
    '[[[1], 2]]',
    '[[{"a": 1}, 2]]',
    '[[[], 2]]',
    '[1, 2]',
    '[null]',
    '[true]',
    '[[]]',
    '[[]]',
    '"ab"',
    '"a"',
    '5',
    '0.0',
    'true',
    '{"x": [1, {"y": null}]}',
    '[["1", 1], [1, 2], [1.0, 3], [true, 4], ["True", 5]]',
    '[[10000000000000000000000, 1], [1e22, 2]]',
    '[[9007199254740993, 1], [9007199254740992.0, 2]]',
    '[[-0.0, 1], [0, 2], [false, 3]]',
    '[["\\u00e9", 1], ["e\\u0301", 2]]',
    '[["a", 1], ["A", 2]]',
    '{"sync_prs": 1, "SYNC_PRS": 0, "k": "", "l": [], "m": {}, "n": 0.0, "o": null, "p": "0"}',
    '{"1": true, "1.0": false}',
    '[[1, true], ["1", false]]',
    '[[1.5, true], ["1.5", false]]',
    '[[1e22, true], ["1e+22", false]]',
    '[[true, 1], ["True", 0]]',
    '[[null, 1], ["None", 0]]',
    '[[10000000000000000000000, 1], ["10000000000000000000000", 0]]',
    '[[0.1, 1], ["0.1", 0], [1e-07, 1], ["1e-07", 0]]',
    '[[1e16, 1], ["1e+16", 0], [123456789012345680000.0, 1], ["1.2345678901234568e+20", 0]]',
    '[[NaN, 1], ["nan", 0], [Infinity, 1], ["inf", 0], [-Infinity, 1], ["-inf", 0]]',
    '[["\\u00e9", 1], ["\\u00e9", 0]]',
)

JSON_RICH_DOCS = (
    '{"a": [1, -2.5e3, true, null, "x\\n\\u00e9\\ud83d\\ude00"], "b": {"c": false}}',
    '[0, -0, 0.5, 1E5, 1e-5, -1.5E+2, 12345678901234567890, NaN, -Infinity, 7]',
    '"\\"\\\\\\/\\b\\f\\n\\r\\t\\u0041\\u00e9\\ud83d\\ude00\\ud800\\udc00 x\\udbff"',
    '[["a", 1], ["bc", 2], [1, true], [2.5, null], {"x": 1, "y": 2}]',
    '{"k": 1, "k": 2, "j": [], "i": {}}',
)

JSON_MUTATION_CHARS = '{}[],:"\\ \t\n\x00\x1f0-+.eEnNtfIau/é\x7f'


def _json_texts() -> list[str]:
    texts: list[str] = []
    seen: set[str] = set()

    def add(text: str) -> None:
        if text not in seen:
            seen.add(text)
            texts.append(text)

    for text in JSON_CURATED:
        add(text)
    for doc in JSON_RICH_DOCS:
        add(doc)
        for index in range(len(doc) + 1):
            add(doc[:index])
        for index in range(len(doc)):
            add(doc[:index] + doc[index + 1 :])
        for index in range(len(doc)):
            for char in JSON_MUTATION_CHARS:
                add(doc[:index] + char + doc[index + 1 :])
    return texts


def _json_value_result(text: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except Exception as exc:  # noqa: BLE001
        return {"error": type(exc).__name__}
    result: dict[str, Any] = {
        "repr": repr(value),
        "dumps": json.dumps(
            value, sort_keys=True, default=str, separators=(",", ":")
        ),
    }
    try:
        result["dict"] = repr(dict(value or {}))
    except Exception as exc:  # noqa: BLE001
        result["dict_error"] = type(exc).__name__
    try:
        flags = {str(k): bool(v) for k, v in dict(value or {}).items()}
        result["flags"] = repr(sorted(flags.items()))
    except Exception as exc:  # noqa: BLE001
        result["flags_error"] = type(exc).__name__
    return result


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

    json_values = [
        {"input": text, "python": _json_value_result(text)}
        for text in _json_texts()
    ]

    _set_env({})
    os.environ["SETTINGS_ENCRYPTION_KEY"] = "oracle-only-settings-key"
    os.environ.pop("SETTINGS_ENCRYPTION_SALT", None)
    credential_mapping = []
    for plaintext, config in MAPPING_CASES:
        if plaintext is EMPTY_CIPHERTEXT:
            ciphertext = ""
        elif plaintext is GARBAGE_CIPHERTEXT:
            ciphertext = "not-a-fernet-token"
        elif plaintext is not None:
            ciphertext = encrypt_value(plaintext)
        else:
            ciphertext = None
        config_value = None if config is SQL_NULL else json.loads(config)
        row = SimpleNamespace(
            credentials_encrypted=ciphertext, config=config_value
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
                "input": {
                    "ciphertext": ciphertext,
                    "config": None if config is SQL_NULL else config,
                },
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
            "json_values": json_values,
        },
        sys.stdout,
        ensure_ascii=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
