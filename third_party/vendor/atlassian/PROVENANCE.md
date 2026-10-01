# Vendored: github.com/full-chaos/atlassian (go/ directory)

Source: https://github.com/full-chaos/atlassian at cb7c3665cb90 (2026-07-16).
Copied from upstream: go/go.mod, go/atlassian/ (the Go module's library tree), LICENSE; modified ONLY by the patches listed under "Local modifications" below. Tests and tools directories are not copied.

Why vendored: the upstream go.mod declares `module atlassian`, which the Go proxy refuses when fetched as
github.com/full-chaos/atlassian/go. Ops requires it as `atlassian` and points it here with a replace directive.
Replace this directory with a normal tagged require once upstream declares its real module path.

Location: the path contains a `vendor` element on purpose: ci/check_go.sh skips `*/vendor/*` when it discovers Go
modules and when it checks formatting, so third-party code is neither vetted nor gofmt-checked as ops code. The root
module reaches it only through the replace directive in go.mod.

## Local modifications

A re-vendor must re-apply these, or drop one only when upstream carries the same change. Each patch is a
unified diff against the upstream text at the commit above, in `patches/`; the test that keeps the change from
silently regressing is `internal/atlassianteams/vendored_documents_test.go` (it reads every document of
`atlassian/graph` and `atlassian/graph/gen` and fails on the conflict below).

| patch | file | upstream text | our text | why |
|---|---|---|---|---|
| `patches/0001-alias-typed-cypher-values.patch` (CHAOS-7493) | `atlassian/graph/gen/teamwork_graph_api.go` (the five Teamwork Graph documents `TEAMWORKGRAPH_TEAMACTIVEPROJECTS`, `_TEAMUSERS`, `_USERTEAMS`, `_USERMANAGER`, `_USERDIRECTREPORTS`, and the decoders of the typed value objects) | `... on GraphStoreCypherQueryV2StringObject { value }`, `... on GraphStoreCypherQueryV2IntObject { value }`, and the Float, Boolean and Timestamp objects likewise: one response key, `value`, selected with a different scalar type under each fragment; decoders read `json:"value"` | the same fragments with aliases: `{ stringValue: value }`, `{ intValue: value }`, `{ floatValue: value }`, `{ booleanValue: value }`, `{ timestampValue: value }`; decoders read `json:"stringValue"` etc.; the decoder's untyped fallback probes `stringValue` | The Atlassian gateway refuses the upstream documents with `Validation error (FieldsConflict) : '.../columns/value/value' : returns different types 'String' and 'Int'` (GraphQL "fields can merge": one response key with different types across fragments). Found by running `dho sync teams --provider jira` against the live gateway: every members read failed, so the Atlassian Teams sync wrote nothing. |
