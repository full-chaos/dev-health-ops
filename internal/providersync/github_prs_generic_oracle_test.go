package providersync

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// This file wires the (github, prs) row-construction boundary
// (normalizeGitHubPullRequest) to the generic, declarative Python<->Go
// oracle comparator built for CHAOS-3162 (oracle_compare_test.go), backed by
// the live registration in
// testdata/oracle_pairs/github_prs_row.py ("github/prs/row").
//
// This does NOT replace github_prs_normalization_oracle_test.go's
// hand-picked-field oracle -- that one stays exactly as it is, proving
// TestGitHubPRSNormalizationMatchesLivePythonFunctions's specific
// state/timestamp claims. This file exists to prove the DIFFERENT, stronger
// property CHAOS-3162 asked for: a comparator that diffs the WHOLE row and
// fails on any UNDECLARED divergence, so a future field nobody thought to
// hand-pick is compared anyway.

// oraclePullRequestGoOnlyFields are the pullRequestRow fields the Python
// build_row side structurally cannot have an opinion about -- Go-side
// effect/tenant bookkeeping stamped after row construction, not part of
// build_git_pull_request's return value at all. Every one of these has a
// mirror entry (with its own, independent reason) in
// testdata/oracle_pairs/github_prs_row.py's excluded_fields; the two lists
// are deliberately not shared code, so a change to one does not silently
// widen the other.
var oraclePullRequestGoOnlyFields = map[string]string{
	"last_synced": "stamped by Collect from normalizedAt, not part of build_git_pull_request's inputs or outputs",
	"source_id":   "native sink always writes null; build_git_pull_request has no equivalent parameter",
	"org_id":      "stamped from claim.OrgID by normalizeGitHubPullRequest, not part of the built row's own fields conceptually",
}

// oraclePullRequestClaim and oraclePullRequestNormalizedAt are the fixed
// claim/timestamp both the production path and the buggy-substitute path
// use, so the only thing that can differ between them is the function(s)
// under test.
var oraclePullRequestClaim = Claim{Unit: Unit{
	OrgID: "org-oracle", Provider: "github", Dataset: "prs", SourceExternalID: "octo/widgets",
}}

var oraclePullRequestNormalizedAt = time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)

// decodeOraclePullRequestInput decodes one oracle case's {"raw_pr": ...,
// "repo_id": ...} input into the same (repoID, gitHubPullDetailPayload)
// shape Collect feeds normalizeGitHubPullRequest. Pure decoding, no
// business logic -- shared by both the production path and the
// buggy-substitute path below so a decode bug cannot itself become a
// divergence between what the two paths are actually testing.
func decodeOraclePullRequestInput(
	t *testing.T, input map[string]any,
) (string, gitHubPullDetailPayload) {
	t.Helper()
	rawPR, ok := input["raw_pr"]
	if !ok {
		t.Fatalf("oracle case missing raw_pr: %v", input)
	}
	repoID, ok := input["repo_id"].(string)
	if !ok || repoID == "" {
		t.Fatalf("oracle case missing repo_id: %v", input)
	}
	encoded, err := json.Marshal(rawPR)
	if err != nil {
		t.Fatalf("marshal oracle case raw_pr: %v", err)
	}
	var detail gitHubPullDetailPayload
	if err := json.Unmarshal(encoded, &detail); err != nil {
		t.Fatalf("unmarshal oracle case raw_pr into gitHubPullDetailPayload: %v", err)
	}
	return repoID, detail
}

// buildPullRequestRowForOracle is the (correct, current, production)
// Go-side row builder for the "github/prs/row" pair: decode the case the
// same way GitHubPullRequestRouteHandler.Collect does, then call the REAL,
// unmodified normalizeGitHubPullRequest directly -- not a copy of its body
// (codex finding #3, CHAOS-3162 second adversarial review: a copy is a
// second source of truth that can drift from the real thing while staying
// green; this is exactly the discipline that caught the earlier
// tab-indented-anchor mutation-plan bug, applied to this framework's own
// baseline test). Returning the concrete pullRequestRow struct (not a
// hand-picked map) is what makes this row's completeness a Go-compiler
// guarantee rather than a runtime choice (codex finding #1) -- see
// oracle_compare_test.go's typedEncode doc comment.
func buildPullRequestRowForOracle(t *testing.T, input map[string]any) pullRequestRow {
	t.Helper()
	repoID, detail := decodeOraclePullRequestInput(t, input)
	row, err := normalizeGitHubPullRequest(
		oraclePullRequestClaim, repoID, detail, oraclePullRequestNormalizedAt,
	)
	if err != nil {
		t.Fatalf("normalizeGitHubPullRequest: %v", err)
	}
	return row
}

// mustNormalizeOraclePullRequest is an INLINED COPY of
// normalizeGitHubPullRequest's body, parameterized on the state-normalizer
// and login-coercion functions so a pre-fix (buggy) variant can be
// substituted for TestGenericOracleRediscoversRowConstructionDefects.
// normalizeGitHubPullRequest itself hardcodes normalizePRState/
// gitHubPullUserLogin as free function calls (correctly -- production code
// should not carry injectable test seams it doesn't need), so there is no
// way to substitute a buggy sub-function through the real function's own
// signature; this copy exists ONLY to make that substitution possible.
// Every rediscovery test using this path is proving "the comparator
// catches a wrong VALUE that a buggy sub-function would have produced",
// NOT "this copy matches production" -- that second, different claim is
// what buildPullRequestRowForOracle's direct call proves instead, and
// nothing in this file relies on this copy for that claim.
func mustNormalizeOraclePullRequest(
	t *testing.T,
	input map[string]any,
	normalizeState func(string, *time.Time) string,
	userLogin func(json.RawMessage) string,
) pullRequestRow {
	t.Helper()
	repoID, detail := decodeOraclePullRequestInput(t, input)
	claim, normalizedAt := oraclePullRequestClaim, oraclePullRequestNormalizedAt

	if detail.Number < 1 {
		t.Fatalf("oracle case raw_pr.number must be >= 1")
	}
	createdAt := parseGitHubPullTime(detail.CreatedAt)
	mergedAt := parseGitHubPullTime(detail.MergedAt)
	closedAt := parseGitHubPullTime(detail.ClosedAt)
	resolvedCreatedAt := resolveCreatedAt(createdAt, mergedAt, closedAt, normalizedAt)
	var rawState string
	if detail.State != nil {
		rawState = *detail.State
	}
	authorName := "Unknown"
	if login := userLogin(detail.User); login != "" {
		authorName = login
	}
	row := pullRequestRow{
		RepoID: repoID, Number: detail.Number, Title: detail.Title,
		Body: detail.Body, State: normalizeState(rawState, mergedAt),
		AuthorName: authorName, AuthorEmail: nil,
		CreatedAt: resolvedCreatedAt.UTC(), MergedAt: mergedAt, ClosedAt: closedAt,
		HeadBranch: gitHubPullRefName(detail.Head), BaseBranch: gitHubPullRefName(detail.Base),
		Additions: detail.Additions, Deletions: detail.Deletions, ChangedFiles: detail.ChangedFiles,
		CommentsCount: detail.Comments, LastSynced: normalizedAt, OrgID: claim.OrgID,
	}
	if err := row.validate(claim); err != nil {
		t.Fatalf("oracle-built row failed validate(): %v", err)
	}
	return row
}

func oraclePullRequestCases() []oracleCase {
	return []oracleCase{
		{
			ID: "closed_merged_with_boolean_login",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"raw_pr": map[string]any{
					"id": 991234, "number": 42, "title": "Add widget support",
					"body": "This PR adds widget support.", "state": "closed",
					"user":          map[string]any{"login": true},
					"created_at":    "2026-07-10T09:00:00Z",
					"updated_at":    "2026-07-21T15:30:00Z",
					"merged_at":     "2026-07-21T15:30:00Z",
					"closed_at":     "2026-07-21T15:30:00Z",
					"head":          map[string]any{"ref": "feature/widgets"},
					"base":          map[string]any{"ref": "main"},
					"additions":     120,
					"deletions":     30,
					"changed_files": 5,
					"comments":      3,
				},
			},
		},
		{
			ID: "closed_with_trailing_cr",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"raw_pr": map[string]any{
					"id": 5, "number": 5, "title": "t", "body": "b", "state": "closed\r",
					"user":       map[string]any{"login": "octocat"},
					"created_at": "2026-01-01T00:00:00Z",
					"closed_at":  "2026-01-02T00:00:00Z",
				},
			},
		},
		{
			ID: "numeric_login_open_pr",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"raw_pr": map[string]any{
					"id": 6, "number": 6, "title": "n", "body": "b", "state": "open",
					"user":       map[string]any{"login": 12345},
					"created_at": "2026-02-01T00:00:00Z",
				},
			},
		},
	}
}

// TestGenericOracleMatchesLivePythonForRowConstruction is the "current code
// is clean" half of CHAOS-3162's acceptance test: the real, current,
// unmodified Go row-construction path against the real, live Python
// build_git_pull_request chain, diffed field-by-field with zero undeclared
// exclusions beyond what both oracle_pairs/github_prs_row.py and
// oraclePullRequestGoOnlyFields declare in writing.
func TestGenericOracleMatchesLivePythonForRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/prs/row", oraclePullRequestCases(),
		buildPullRequestRowForOracle, oraclePullRequestGoOnlyFields,
	)
}

// buggyNormalizePRStateStripsOnlySpaces reproduces the exact pre-M7-fix
// defect: whitespace is removed from the WHOLE string (not just the ends)
// and \r is never treated as whitespace at all, so "closed\r" -- which
// Python's raw_state.strip().lower() matches to "closed" -- falls through
// to the default branch here instead.
func buggyNormalizePRStateStripsOnlySpaces(rawState string, mergedAt *time.Time) string {
	if rawState == "" {
		return "open"
	}
	cleaned := strings.ReplaceAll(rawState, " ", "")
	switch strings.ToLower(cleaned) {
	case "merged":
		return "merged"
	case "opened", "open":
		return "open"
	case "closed":
		if mergedAt != nil {
			return "merged"
		}
		return "closed"
	default:
		return "open"
	}
}

// buggyGitHubPullUserLoginStringOnly reproduces the exact pre-M8-fix
// defect: decoding "login" directly into a Go string field. json.Unmarshal
// fails (type error) for any non-string login -- a bool or a number -- so
// this returns "" exactly where Python's str(user["login"]) would return
// "True" or "12345".
func buggyGitHubPullUserLoginStringOnly(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var user struct {
		Login string `json:"login"`
	}
	if json.Unmarshal(raw, &user) != nil {
		return ""
	}
	return user.Login
}

// buildPullRequestRowForOracleWithCorruptedMergedAt reproduces the shape of
// M3 (round 2): a row builder that gets merged_at/closed_at wrong in a way
// the OLD, hand-picked oracle (which asserted state/created_at/author_name
// but never merged_at/closed_at) would have shipped clean. Here the bug is
// swapping merged_at and closed_at -- a realistic copy-paste mistake, not a
// contrived one -- to prove the GENERIC comparator (which compares every
// field the row exposes, not a chosen subset) catches it where a narrower
// oracle demonstrably did not, twice, in this pair's actual review history.
func buildPullRequestRowForOracleWithCorruptedMergedAt(t *testing.T, input map[string]any) pullRequestRow {
	t.Helper()
	row := mustNormalizeOraclePullRequest(t, input, normalizePRState, gitHubPullUserLogin)
	row.MergedAt, row.ClosedAt = row.ClosedAt, row.MergedAt
	return row
}

// requireOracleRediscovers calls oracleDivergences directly (NOT
// compareRowsAgainstPythonOracle, whose t.Errorf calls would propagate a
// failure to this test's ancestors regardless of how the return value is
// inspected -- Go's testing package has no way to "catch" a subtest
// failure) and asserts the divergence list is non-empty: a "probe that
// finds nothing" here means the generic comparator is BLIND to the injected
// pre-fix defect -- exactly the false confidence CHAOS-3162 exists to rule
// out -- so an empty list fails this test loudly. The found divergences are
// logged (t.Log, not t.Error) purely for readability when running -v; they
// are the proof, not a failure.
func requireOracleRediscovers[T any](
	t *testing.T,
	name string,
	pairID string,
	cases []oracleCase,
	buggyBuilder func(t *testing.T, input map[string]any) T,
	goOnlyFields map[string]string,
) {
	t.Helper()
	wrapped := func(t *testing.T, input map[string]any) any { return buggyBuilder(t, input) }
	t.Run(name, func(t *testing.T) {
		t.Helper()
		divergences := oracleDivergences(t, pairID, cases, wrapped, goOnlyFields)
		if len(divergences) == 0 {
			t.Fatalf("expected the generic oracle to rediscover the injected pre-fix defect, "+
				"but it reported every case matching under pair %q -- the comparator did not "+
				"notice the divergence", pairID)
		}
		for _, message := range divergences {
			t.Logf("rediscovered: %s", message)
		}
	})
}

// TestGenericOracleRediscoversRowConstructionDefects is CHAOS-3162's
// acceptance gate for the row-construction boundary: run the SAME generic
// comparator, cases, and pair id, but substitute a pre-fix (buggy) Go row
// builder for each of the three defects this boundary can express, and
// confirm the comparator's whole-row, fail-on-undeclared-divergence design
// actually notices every one of them -- not merely that unit tests
// targeting these fields would notice, but that the GENERIC oracle itself,
// with no knowledge of these specific bugs baked in, catches them as a side
// effect of diffing everything.
func TestGenericOracleRediscoversRowConstructionDefects(t *testing.T) {
	cases := oraclePullRequestCases()

	buggyStateBuilder := func(t *testing.T, input map[string]any) pullRequestRow {
		return mustNormalizeOraclePullRequest(
			t, input, buggyNormalizePRStateStripsOnlySpaces, gitHubPullUserLogin,
		)
	}
	requireOracleRediscovers(
		t, "rediscovers pre-M7 state-normalization whitespace bug",
		"github/prs/row", cases, buggyStateBuilder, oraclePullRequestGoOnlyFields,
	)

	buggyLoginBuilder := func(t *testing.T, input map[string]any) pullRequestRow {
		return mustNormalizeOraclePullRequest(
			t, input, normalizePRState, buggyGitHubPullUserLoginStringOnly,
		)
	}
	requireOracleRediscovers(
		t, "rediscovers pre-M8 non-string login coercion bug",
		"github/prs/row", cases, buggyLoginBuilder, oraclePullRequestGoOnlyFields,
	)

	requireOracleRediscovers(
		t, "rediscovers unasserted built fields (merged_at/closed_at swap)",
		"github/prs/row", cases, buildPullRequestRowForOracleWithCorruptedMergedAt,
		oraclePullRequestGoOnlyFields,
	)
}
