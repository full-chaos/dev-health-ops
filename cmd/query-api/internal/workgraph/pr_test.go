package workgraph

import (
	"context"
	"strings"
	"testing"
)

func TestParsePRDetailID(t *testing.T) {
	const repo = "11111111-1111-1111-1111-111111111111"
	for _, tc := range []struct {
		name       string
		id         string
		wantRepo   string
		wantNumber int
		wantOK     bool
	}{
		{"hash-pr separator", repo + "#pr42", repo, 42, true},
		{"bare hash separator", repo + "#42", repo, 42, true},
		{"colon separator", repo + ":42", repo, 42, true},
		{"slash-pr separator", repo + "/pr/42", repo, 42, true},
		{"upper-cased repo id is lower-cased", strings.ToUpper(repo) + "#pr42", repo, 42, true},
		{"missing number", repo + "#pr", "", 0, false},
		{"missing separator", repo + "42", "", 0, false},
		{"repo id too short", "11111111-1111-1111-1111-11111111111#pr1", "", 0, false},
		{"non-hex repo id", strings.Repeat("g", 36) + "#pr1", "", 0, false},
		{"empty string", "", "", 0, false},
		{"trailing garbage after number", repo + "#pr42x", "", 0, false},
		// KNOWN DIVERGENCE from Python (codex round 1 on #2190, P2): Python's
		// bare `\d` matches Unicode decimal digits too, and `int()` accepts
		// them, so `#pr١٢` (Arabic-Indic for "12") resolves to 12 there.
		// Go's RE2 `\d` is ASCII-only -- this case pins the current, safe
		// (reject rather than silently diverge) Go behavior so a future
		// pattern change doesn't regress it unnoticed either way. See
		// prDetailIDPattern's own doc comment for the full rationale.
		{"unicode-digit id is rejected (KNOWN DIVERGENCE from Python)", repo + "#pr١٢", "", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotRepo, gotNumber, gotOK := ParsePRDetailID(tc.id)
			if gotOK != tc.wantOK || gotRepo != tc.wantRepo || gotNumber != tc.wantNumber {
				t.Fatalf("ParsePRDetailID(%q) = (%q, %d, %v), want (%q, %d, %v)",
					tc.id, gotRepo, gotNumber, gotOK, tc.wantRepo, tc.wantNumber, tc.wantOK)
			}
		})
	}
}

// TestPRCoreRowExists covers CHAOS-4980's nil-for-unknown existence
// check: a found row reports true, an empty result reports false, and the
// query text targets git_pull_requests without FINAL (see PRCoreRowExists's
// own doc comment for why FINAL is unnecessary here).
func TestPRCoreRowExists(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rows       [][]any
		wantExists bool
	}{
		{"row found", [][]any{{uint32(42)}}, true},
		{"no row", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{responses: []*fakeRowScanner{{rows: tc.rows}}}
			got, err := PRCoreRowExists(context.Background(), client, "org1", "11111111-1111-1111-1111-111111111111", 42)
			if err != nil {
				t.Fatalf("PRCoreRowExists: %v", err)
			}
			if got != tc.wantExists {
				t.Fatalf("PRCoreRowExists = %v, want %v", got, tc.wantExists)
			}
			if client.calls != 1 {
				t.Fatalf("want exactly 1 query, got %d", client.calls)
			}
			sql := client.statements[0]
			if !strings.Contains(sql, "git_pull_requests") {
				t.Fatalf("query does not target git_pull_requests:\n%s", sql)
			}
			if strings.Contains(sql, "FINAL") {
				t.Fatalf("existence check should not use FINAL (see PRCoreRowExists's doc comment):\n%s", sql)
			}
		})
	}
}

// TestResolveLinkedIssues_DispatchesOnFlagAndMapsRows is the resolver-facing
// (workgraph-package) half of CHAOS-4980's flag-state coverage: with the
// native flag on, ResolveLinkedIssues must issue the fast (version_rank)
// query; with it off (unset), the FINAL oracle query -- and in both cases
// the returned rows must map 1:1 onto model.PullRequestIssueLink. The
// graph-package Pr resolver test (pr_resolver_test.go) pins the same two
// states one layer up, through the actual GraphQL resolver.
func TestResolveLinkedIssues_DispatchesOnFlagAndMapsRows(t *testing.T) {
	row := []any{"issue:OPS-9", 0.75, "native", "native-token"}

	for _, tc := range []struct {
		name        string
		flagValue   string
		wantInQuery string
	}{
		{"flag on: fast path", "1", "version_rank"},
		{"flag unset: FINAL oracle", "", "FINAL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.flagValue == "" {
				t.Setenv(investmentMaterializeNativeEnabledEnv, "")
			} else {
				t.Setenv(investmentMaterializeNativeEnabledEnv, tc.flagValue)
			}

			client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{row}}}}
			got, err := ResolveLinkedIssues(context.Background(), client, "org1", "11111111-1111-1111-1111-111111111111", 42)
			if err != nil {
				t.Fatalf("ResolveLinkedIssues: %v", err)
			}
			if client.calls != 1 {
				t.Fatalf("want exactly 1 query, got %d", client.calls)
			}
			if !strings.Contains(client.statements[0], tc.wantInQuery) {
				t.Fatalf("query does not contain %q:\n%s", tc.wantInQuery, client.statements[0])
			}
			if len(got) != 1 {
				t.Fatalf("got %d linked issues, want 1: %+v", len(got), got)
			}
			want := row
			if got[0].WorkItemID != want[0] || got[0].Confidence != want[1] || got[0].Provenance != want[2] || got[0].Evidence != want[3] {
				t.Fatalf("mapped row = %+v, want work_item_id=%v confidence=%v provenance=%v evidence=%v",
					got[0], want[0], want[1], want[2], want[3])
			}
		})
	}
}
