# Numeric string-parsing inventory (`strconv.ParseFloat`/`ParseInt`/`Atoi`)

Report-only, no fixes. Requested alongside the CHAOS-4818 AST lint follow-up,
relaying lane-4441's measured finding: `strconv.ParseFloat` is **not** a
correct port of CPython's `float()` in either direction (a hex-float literal
like `"0x1p-2"` parses in Go and raises in Python; a leading/trailing space
parses in Python's `float()` and errors in Go; PEP 515 underscore grouping
happens to agree; a full-width Unicode digit string parses in Python via its
Nd-to-ASCII transform and errors in Go). The same class of gap plausibly
applies to `strconv.ParseInt`/`strconv.Atoi` versus Python's `int()`, though
that has not been independently measured the way `ParseFloat` was.

`pythonparity.ParseFloat` (CHAOS-4841, lane-4441's queue, behind #2103/#2110)
will be the correct primitive for a genuine Python-`float()`-port call site
once it lands. This inventory exists so that landing can be followed by a
targeted swap rather than another full-repo grep.

## How to read this

Every `strconv.ParseFloat`/`ParseInt`/`Atoi` call site in non-test `.go`
files is listed below (`rg -n 'strconv\.(ParseFloat|ParseInt|Atoi)\(' --type go
-g '!*_test.go'`, full repo). Each gets a first-pass classification by
package/context, **not** a verified per-site diff against the Python source
the way `generate_fma_golden.py`'s corpus was — that verification is exactly
the follow-up work this inventory is scoping, not doing.

- **PARITY?** -- the call site's package name or immediate context suggests
  it may be re-parsing a value CPython's `float()`/`int()` parses somewhere
  in `dev_health_ops`, i.e. a genuine port. Worth checking against the actual
  Python source before touching.
- **INFRA** -- parses a value with no Python counterpart to match (an HTTP
  header, a provider API's pagination cursor or numeric ID, a cron
  expression, a CLI/env config value, an internal registry key). Almost
  certainly fine as ordinary Go parsing; flagged here only for completeness,
  not because it needs `pythonparity.ParseFloat`/`ParseInt`.

This split is a heuristic, not a proof -- see lane-4441's own correction
about `strings.ToLower`/`EqualFold`/`cases.Lower` (a uniform rule "buries the
real ones"). Treat PARITY? rows as "check this first", not "this is broken".

## ParseFloat

| file:line | classification | why |
| --- | --- | --- |
| `internal/platform/tracing/tracing.go:194` | INFRA | trace sampling rate, no Python source |
| `internal/syncdispatchruntime/clickhouse_readback.go:220` | PARITY? | reads back a stored metric value; check whether the writer side is a Python-computed float |
| `internal/providerfoundation/http.go:462` | INFRA | HTTP `Retry-After`-style header |
| `internal/providerfoundation/http.go:488` | INFRA | HTTP header, same function family as :462 |
| `internal/testsupport/oraclecompare/oraclecompare.go:459` | INFRA | test-comparison tooling itself: parses the STRINGIFIED value from both sides to compare, not a business-logic port |
| `internal/testsupport/oraclecompare/oraclecompare.go:460` | INFRA | same function, Go side of the same comparison |
| `internal/jobs/workgraph/units/components.go:126` | PARITY? | `workgraph` ports Python `dev_health_ops.work_graph`; check the Python component-unit parser |
| `cmd/dev-health-worker/workgraph_issue_pr_links.go:249` | PARITY? | same work-graph family as above |
| `internal/providersync/github_tests_reports.go:1589` | INFRA | GitHub API test-report field, provider-native format, no Python parity source (providersync is the Go-native ingestion layer, not a Python port) |
| `internal/jobs/metrics/daily/testops_risk_native_clickhouse.go:376` | PARITY? | `testops_risk_native_clickhouse.go` is a direct CHAOS-4818/4824 sweep file (this PR's own siblings); check `compute_testops_risk.py` for the corresponding parse |
| `internal/providersync/gitlab_deployments_route.go:363` | INFRA | GitLab API field, provider-native |
| `internal/providersync/gitlab_tests_route.go:793` | INFRA | GitLab API field, provider-native |
| `internal/providersync/github_work_items_projects_v2.go:832` | INFRA | GitHub API field, provider-native |
| `internal/providersync/gitlab_feature_flags_route.go:417` | INFRA | GitLab API field, provider-native |
| `internal/providersync/jira_work_items_rows.go:818` | INFRA | Jira API field, provider-native |
| `internal/providersync/jira_work_items_rows.go:826` | INFRA | Jira API field, provider-native |
| `internal/providersync/status_mapping_pyyaml.go:225` | PARITY? | filename says it ports PyYAML's number grammar directly -- check against PyYAML's (not CPython `float()`'s) actual float-parsing rules, which are YAML-1.1-spec-shaped, not the same grammar lane-4441 measured |
| `internal/providersync/status_mapping_pyyaml.go:236` | PARITY? | same file/function family as :225 |

## ParseInt

| file:line | classification | why |
| --- | --- | --- |
| `internal/externalrecompute/valkey.go:307` | INFRA | internal cache-key/ticket parsing |
| `internal/streamrunner/valkey.go:365` | INFRA | internal cache-key parsing |
| `internal/streamrunner/valkey.go:368` | INFRA | internal cache-key parsing |
| `internal/providersync/jira_atlassian_route.go:584` | INFRA | Jira API field, provider-native |
| `internal/providersync/jira_atlassian_route.go:651` | INFRA | Jira API field, provider-native |
| `internal/providersync/github_tests_reports.go:1396` | INFRA | test-report coverage line number, provider-native |
| `internal/providersync/github_tests_reports.go:1402` | INFRA | test-report hit count, provider-native |
| `internal/providersync/github_tests_reports.go:1476` | INFRA | provider-native |
| `internal/providersync/status_mapping_pyyaml.go:174` | PARITY? | same PyYAML-grammar-port file as the ParseFloat rows above (binary literal) |
| `internal/providersync/status_mapping_pyyaml.go:176` | PARITY? | same file (hex literal) |
| `internal/providersync/status_mapping_pyyaml.go:180` | PARITY? | same file (octal literal) |
| `internal/providersync/status_mapping_pyyaml.go:182` | PARITY? | same file (decimal literal) |
| `internal/providersync/status_mapping_pyyaml.go:196` | PARITY? | same file (digit group) |
| `internal/providersync/native_rest.go:764` | INFRA | provider-native ID field |
| `internal/providersync/gitlab_commit_stats_route.go:252` | INFRA | GitLab API field, provider-native |
| `internal/providersync/pagerduty_incidents_route.go:1199` | INFRA | PagerDuty API field, provider-native |
| `internal/providersync/github_work_items_rest_collect.go:459` | INFRA | GitHub API field, provider-native |

## Atoi (`strconv.Atoi` is `ParseInt(s, 10, 0)`; same class)

All ~40 non-test call sites are pagination cursors (GitHub/GitLab/Jira/PagerDuty
API page numbers), HTTP headers, cron-expression fields
(`internal/scheduler/sync/cron.go`), CLI/env config
(`internal/platform/config/config.go`), job-contract registry version keys
(`internal/jobcontract/registry.go`), or budget/backfill counters
(`internal/syncdispatchruntime/*`, `internal/scheduler/sync/*`) -- **all
INFRA** by inspection, no Python `int()` counterpart to port against. Not
tabled individually; re-run the `Atoi` grep in this file's header if a
specific site needs re-checking after new code lands.

Two exceptions worth a second look, same reasoning as the ParseFloat/ParseInt
`status_mapping_pyyaml.go` rows:

| file:line | classification | why |
| --- | --- | --- |
| `internal/providersync/status_mapping_pyyaml.go:268` | PARITY? | same PyYAML-grammar-port file (scientific-notation exponent) |
| `internal/jobs/workgraph/units/constants.go:138` | PARITY? | same `workgraph` family as the ParseFloat PARITY? row above |

## Suggested next step

Once `pythonparity.ParseFloat` (CHAOS-4841) lands: pull the corresponding
Python source for each PARITY? row above (`compute_testops_risk.py` for
`testops_risk_native_clickhouse.go:376`, the relevant `work_graph` module for
the two `workgraph`/`units` rows, and PyYAML's own float/int grammar --
**not** CPython `float()`/`int()` -- for the `status_mapping_pyyaml.go` rows)
and decide per-site whether the divergence lane-4441 measured is reachable
there. This file does not do that verification; it only narrows where to
look.
