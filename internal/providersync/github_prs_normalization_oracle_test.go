package providersync

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// githubPRSOracleCase mirrors one entry of
// testdata/python_github_prs_normalization_oracle.py's CASES list. The
// Go-side raw_state/created_at/merged_at/closed_at strings below MUST stay
// byte-identical to that file's CASES -- there is no shared source of truth
// between the two, so a drift here silently stops proving anything. This is
// the trade-off documented in the oracle's own docstring: keeping both
// lists in one file (rather than one generating the other) keeps the
// oracle a standalone, directly-runnable script.
type githubPRSOracleCase struct {
	id        string
	rawState  string
	createdAt string
	mergedAt  string
	closedAt  string
	wantState string
}

var githubPRSOracleCases = []githubPRSOracleCase{
	{id: "open", rawState: "open", createdAt: "2026-07-10T09:00:00Z", wantState: "open"},
	{id: "opened_alias", rawState: "opened", createdAt: "2026-07-10T09:00:00Z", wantState: "open"},
	{id: "closed_unmerged", rawState: "closed", createdAt: "2026-07-10T09:00:00Z",
		closedAt: "2026-07-15T00:00:00Z", wantState: "closed"},
	{id: "closed_merged", rawState: "closed", createdAt: "2026-07-10T09:00:00Z",
		mergedAt: "2026-07-21T15:30:00Z", closedAt: "2026-07-21T15:30:00Z", wantState: "merged"},
	{id: "merged_literal", rawState: "merged", createdAt: "2026-07-10T09:00:00Z",
		mergedAt: "2026-07-21T15:30:00Z", closedAt: "2026-07-21T15:30:00Z", wantState: "merged"},
	{id: "internal_whitespace", rawState: "clo sed", createdAt: "2026-07-10T09:00:00Z",
		wantState: "open"},
	{id: "trailing_carriage_return", rawState: "closed\r", createdAt: "2026-07-10T09:00:00Z",
		closedAt: "2026-07-15T00:00:00Z", wantState: "closed"},
	{id: "leading_trailing_whitespace", rawState: "  Closed  ", createdAt: "2026-07-10T09:00:00Z",
		mergedAt: "2026-07-21T15:30:00Z", closedAt: "2026-07-21T15:30:00Z", wantState: "merged"},
	{id: "created_at_absent_falls_back_to_merged_at", rawState: "closed",
		mergedAt: "2026-07-21T15:30:00Z", wantState: "merged"},
	{id: "created_and_merged_absent_falls_back_to_closed_at", rawState: "closed",
		closedAt: "2026-07-22T00:00:00Z", wantState: "closed"},
}

func mustParseOracleTime(t *testing.T, value string) *time.Time {
	t.Helper()
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse oracle fixture time %q: %v", value, err)
	}
	parsed = parsed.UTC()
	return &parsed
}

// TestGitHubPRSNormalizationMatchesLivePythonFunctions shells out to the
// live Python producer (codex H9 fix) rather than comparing against a
// hand-authored fixture. See
// testdata/python_github_prs_normalization_oracle.py's docstring for what
// this does and deliberately does not cover.
func TestGitHubPRSNormalizationMatchesLivePythonFunctions(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	srcRoot := filepath.Join(packageDir, "..", "..", "src", "dev_health_ops")
	output, err := exec.Command(
		python,
		filepath.Join(packageDir, "testdata", "python_github_prs_normalization_oracle.py"),
		filepath.Join(srcRoot, "providers", "pr_state.py"),
		filepath.Join(srcRoot, "processors", "base_git.py"),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python github/prs oracle: %v: %s", err, output)
	}
	var got []struct {
		ID                string  `json:"id"`
		State             string  `json:"state"`
		ResolvedCreatedAt *string `json:"resolved_created_at"`
		BuiltCreatedAt    *string `json:"built_created_at"`
		BuiltMergedAt     *string `json:"built_merged_at"`
		BuiltClosedAt     *string `json:"built_closed_at"`
	}
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode Python github/prs oracle: %v: %s", err, output)
	}
	byID := make(map[string]int, len(got))
	for index, entry := range got {
		byID[entry.ID] = index
	}
	if len(byID) != len(githubPRSOracleCases) {
		t.Fatalf("oracle returned %d cases, Go table has %d", len(byID), len(githubPRSOracleCases))
	}

	for _, testCase := range githubPRSOracleCases {
		testCase := testCase
		t.Run(testCase.id, func(t *testing.T) {
			index, ok := byID[testCase.id]
			if !ok {
				t.Fatalf("oracle output missing case %q", testCase.id)
			}
			oracle := got[index]

			createdAt := mustParseOracleTime(t, testCase.createdAt)
			mergedAt := mustParseOracleTime(t, testCase.mergedAt)
			closedAt := mustParseOracleTime(t, testCase.closedAt)

			gotState := normalizePRState(testCase.rawState, mergedAt)
			if gotState != oracle.State {
				t.Fatalf("normalizePRState = %q, oracle (live Python) = %q", gotState, oracle.State)
			}
			if gotState != testCase.wantState {
				t.Fatalf("normalizePRState = %q, Go table wants %q", gotState, testCase.wantState)
			}

			// resolveCreatedAt against a zero normalizedAt fallback: every
			// case in the table supplies at least one of created/merged/
			// closed, so the "now()" branch is never the one under test
			// here -- normalizedAt is passed as a recognizable sentinel so a
			// wrongly-taken fallback branch is obvious in a failure message
			// rather than silently matching by coincidence.
			sentinelNow := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			gotResolved := resolveCreatedAt(createdAt, mergedAt, closedAt, sentinelNow)
			if oracle.ResolvedCreatedAt == nil {
				t.Fatalf("oracle case %q has no resolved_created_at", testCase.id)
			}
			wantResolved, err := time.Parse(time.RFC3339, *oracle.ResolvedCreatedAt)
			if err != nil {
				t.Fatalf("parse oracle resolved_created_at: %v", err)
			}
			if !gotResolved.Equal(wantResolved) {
				t.Fatalf("resolveCreatedAt = %v, oracle (live Python) = %v", gotResolved, wantResolved)
			}
			if gotResolved.Equal(sentinelNow) {
				t.Fatalf("resolveCreatedAt fell through to the now() branch; "+
					"case %q should have a non-empty created/merged/closed input", testCase.id)
			}

			// codex M3: the oracle DECODED built_created_at/built_merged_at/
			// built_closed_at from the live build_git_pull_request(...) result
			// (not merely from the standalone coerce_created_at/
			// normalize_pr_state calls above) but never compared them against
			// anything -- a Python builder change that dropped, renamed, or
			// transformed those fields on the actual GitPullRequest object
			// would stay green. Assert all three explicitly.
			if oracle.BuiltCreatedAt == nil {
				t.Fatalf("oracle case %q: build_git_pull_request result has no created_at", testCase.id)
			}
			builtCreatedAt, err := time.Parse(time.RFC3339, *oracle.BuiltCreatedAt)
			if err != nil {
				t.Fatalf("parse oracle built_created_at: %v", err)
			}
			if !builtCreatedAt.Equal(wantResolved) {
				t.Fatalf("build_git_pull_request(...).created_at = %v, want %v "+
					"(coerce_created_at's own result) -- the builder's "+
					"composition of coerce_created_at diverged from calling it directly",
					builtCreatedAt, wantResolved)
			}
			// merged_at/closed_at are passed straight through by
			// build_git_pull_request (processors/base_git.py:
			// "merged_at": merged_at, "closed_at": closed_at) -- the object's
			// fields must equal the RAW inputs, not the resolved created_at.
			assertOracleTimePointerMatchesInput(t, testCase.id, "built_merged_at", oracle.BuiltMergedAt, mergedAt)
			assertOracleTimePointerMatchesInput(t, testCase.id, "built_closed_at", oracle.BuiltClosedAt, closedAt)
		})
	}
}

// assertOracleTimePointerMatchesInput compares an oracle-decoded,
// ISO8601-string-or-null field against the *time.Time (possibly nil) that
// was fed into the live Python call for the same case.
func assertOracleTimePointerMatchesInput(
	t *testing.T,
	caseID, field string,
	oracleValue *string,
	want *time.Time,
) {
	t.Helper()
	if oracleValue == nil {
		if want != nil {
			t.Fatalf("case %q: oracle %s=nil, Go input=%v", caseID, field, want)
		}
		return
	}
	if want == nil {
		t.Fatalf("case %q: oracle %s=%s, Go input=nil", caseID, field, *oracleValue)
	}
	parsed, err := time.Parse(time.RFC3339, *oracleValue)
	if err != nil {
		t.Fatalf("case %q: parse oracle %s: %v", caseID, field, err)
	}
	if !parsed.Equal(*want) {
		t.Fatalf("case %q: oracle %s=%v, Go input=%v", caseID, field, parsed, *want)
	}
}
