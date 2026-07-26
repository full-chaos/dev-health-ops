package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// This file wires the (github, prs) list-inclusion-decision boundary
// (pullOutsideKnownWindow / pullCrossedSinceBoundary, both
// github_prs_route.go) to the "github/prs/window" pair registered in
// testdata/oracle_pairs/github_prs_window.py. See that file's module
// docstring for why this pair's Python side is a pinned, freshness-checked
// copy rather than a live execution of _collect_github_pr_objects (unlike
// github_prs_row.py) -- a documented, narrower scope, not a silent one.

func windowDecisionOracleCase(id string, updatedAt, since, until *string) oracleCase {
	input := map[string]any{}
	if updatedAt != nil {
		input["updated_at"] = *updatedAt
	} else {
		input["updated_at"] = nil
	}
	if since != nil {
		input["since"] = *since
	} else {
		input["since"] = nil
	}
	if until != nil {
		input["until"] = *until
	} else {
		input["until"] = nil
	}
	return oracleCase{ID: id, Input: input}
}

func windowOracleTime(value string) *string { return &value }

func windowDecisionOracleCases() []oracleCase {
	return []oracleCase{
		windowDecisionOracleCase(
			"included_in_window",
			windowOracleTime("2026-07-10T09:00:00Z"),
			windowOracleTime("2026-07-01T00:00:00Z"), windowOracleTime("2026-07-31T23:59:59Z"),
		),
		windowDecisionOracleCase(
			"after_until_skip_only",
			windowOracleTime("2026-08-01T00:00:00Z"),
			windowOracleTime("2026-07-01T00:00:00Z"), windowOracleTime("2026-07-31T23:59:59Z"),
		),
		windowDecisionOracleCase(
			"before_since_stop",
			windowOracleTime("2026-06-01T00:00:00Z"),
			windowOracleTime("2026-07-01T00:00:00Z"), windowOracleTime("2026-07-31T23:59:59Z"),
		),
		// codex H3: the case the pre-fix Go code got wrong -- updated_at
		// unknown, since/until both set. Python's isinstance(updated_at,
		// datetime) guard means BOTH comparisons are skipped, so this item
		// is unconditionally included and iteration continues; it is
		// neither excluded nor a stop signal.
		windowDecisionOracleCase(
			"null_updated_at_included",
			nil,
			windowOracleTime("2026-07-01T00:00:00Z"), windowOracleTime("2026-07-31T23:59:59Z"),
		),
		windowDecisionOracleCase(
			"no_window_bounds",
			windowOracleTime("2026-07-10T09:00:00Z"),
			nil, nil,
		),
	}
}

// windowDecisionResult is the concrete, complete result type for the
// "github/prs/window" pair: exactly the two fields _decide's own
// `return {"excluded": ..., "stop": ...}` literal can emit (per
// github_prs_window.py's reflected_fields). Returning this struct (not a
// hand-built map[string]any) is what makes the Go side's completeness a
// compiler guarantee rather than a runtime choice (codex finding #1) --
// see oracle_compare_test.go's typedEncode doc comment.
type windowDecisionResult struct {
	Excluded bool `json:"excluded"`
	Stop     bool `json:"stop"`
}

// buildWindowDecisionForOracle is the (correct, current, production)
// Go-side decision builder: parses the case's inputs into exactly the
// values Collect/filterGitHubPullWindow would have (a time.Time and a
// Claim), then calls the REAL, current pullOutsideKnownWindow and
// pullCrossedSinceBoundary.
func buildWindowDecisionForOracle(
	t *testing.T,
	input map[string]any,
	outsideWindow func(time.Time, Claim) bool,
	crossedBoundary func(json.RawMessage, Claim) bool,
) windowDecisionResult {
	t.Helper()
	updatedAtStr, _ := input["updated_at"].(string)
	updatedAt := firstTime(updatedAtStr)

	claim := Claim{}
	if sinceStr, ok := input["since"].(string); ok {
		since := firstTime(sinceStr)
		claim.SinceAt = &since
	}
	if untilStr, ok := input["until"].(string); ok {
		until := firstTime(untilStr)
		claim.BeforeAt = &until
	}

	excluded := outsideWindow(updatedAt, claim)

	rawItem, err := json.Marshal(gitHubPullListItem{Number: 1, UpdatedAt: updatedAtStr})
	if err != nil {
		t.Fatalf("marshal synthetic list item: %v", err)
	}
	stop := crossedBoundary(rawItem, claim)

	return windowDecisionResult{Excluded: excluded, Stop: stop}
}

// TestGenericOracleMatchesLivePythonForWindowDecision is the "current code
// is clean" half for this boundary.
func TestGenericOracleMatchesLivePythonForWindowDecision(t *testing.T) {
	builder := func(t *testing.T, input map[string]any) windowDecisionResult {
		return buildWindowDecisionForOracle(t, input, pullOutsideKnownWindow, pullCrossedSinceBoundary)
	}
	compareRowsAgainstPythonOracle(t, "github/prs/window", windowDecisionOracleCases(), builder, nil)
}

// buggyPullOutsideKnownWindowExcludesUnknown reproduces the exact pre-H3-fix
// defect: a missing/unparseable updated_at was treated as OUTSIDE the
// window (excluded) instead of unconditionally included -- the "empty
// success" trap named in the H3 doc comment on pullOutsideKnownWindow.
func buggyPullOutsideKnownWindowExcludesUnknown(updatedAt time.Time, claim Claim) bool {
	windowKnown := !updatedAt.IsZero()
	if !windowKnown {
		return true // pre-fix: treated "unknown" as "excluded", not "included"
	}
	before := claim.SinceAt != nil && updatedAt.Before(claim.SinceAt.UTC())
	after := claim.BeforeAt != nil && updatedAt.After(claim.BeforeAt.UTC())
	return before || after
}

// TestGenericOracleRediscoversWindowGuardDefect is CHAOS-3162's acceptance
// gate for the list-inclusion-decision boundary: the same generic
// comparator, cases, and pair id, but with the pre-H3-fix decision function
// substituted, must report a divergence on the null-updated_at case.
func TestGenericOracleRediscoversWindowGuardDefect(t *testing.T) {
	cases := windowDecisionOracleCases()
	buggyBuilder := func(t *testing.T, input map[string]any) windowDecisionResult {
		return buildWindowDecisionForOracle(
			t, input, buggyPullOutsideKnownWindowExcludesUnknown, pullCrossedSinceBoundary,
		)
	}
	requireOracleRediscovers(
		t, "rediscovers pre-H3 null-updated_at window guard bug",
		"github/prs/window", cases, buggyBuilder, nil,
	)
}
