package workgraph

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
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

// --- CHAOS-4991: FetchPRCoreRow / ResolveReviews / ResolveCommits ---------

func timePtr(t time.Time) *time.Time { return &t }
func uint32Ptr(v uint32) *uint32     { return &v }

const testRepoID = "11111111-1111-1111-1111-111111111111"

// TestFetchPRCoreRow_NoRow pins the nil-for-unknown contract FetchPRCoreRow
// shares with PRCoreRowExists: an empty result set returns ok=false, not
// an error and not a zero-value row masquerading as found.
func TestFetchPRCoreRow_NoRow(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}
	row, ok, err := FetchPRCoreRow(context.Background(), client, "org1", testRepoID, 42)
	if err != nil {
		t.Fatalf("FetchPRCoreRow: %v", err)
	}
	if ok {
		t.Fatalf("got ok=true for an empty result, want false: %+v", row)
	}
}

// TestFetchPRCoreRow_ScansAndMapsEveryColumn is the red-first proof this
// port's whole column set actually reaches PRCoreRow -- every nullable
// column populated on this row, so a field silently dropped in the
// Scan/mapping code shows up as a mismatch here, not just on a real
// ClickHouse's driver-type quirks (that's pr_integration_test.go's job).
func TestFetchPRCoreRow_ScansAndMapsEveryColumn(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	merged := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	closed := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	firstReview := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	firstComment := time.Date(2026, 1, 2, 6, 0, 0, 0, time.UTC)

	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{{
		"acme/widgets",            // repo_name
		strPtr("Fix the thing"),   // title
		strPtr("body text"),       // body
		strPtr("merged"),          // state
		strPtr("Ada Lovelace"),    // author_name
		strPtr("ada@example.com"), // author_email
		created,                   // created_at
		timePtr(merged),           // merged_at
		timePtr(closed),           // closed_at
		strPtr("feature/x"),       // head_branch
		strPtr("main"),            // base_branch
		uint32Ptr(120),            // additions
		uint32Ptr(40),             // deletions
		uint32Ptr(7),              // changed_files
		timePtr(firstReview),      // first_review_at
		timePtr(firstComment),     // first_comment_at
		uint32(2),                 // changes_requested_count
		uint32(3),                 // reviews_count
		uint32(5),                 // comments_count
	}}}}}

	row, ok, err := FetchPRCoreRow(context.Background(), client, "org1", testRepoID, 42)
	if err != nil {
		t.Fatalf("FetchPRCoreRow: %v", err)
	}
	if !ok {
		t.Fatal("got ok=false, want true")
	}
	if client.calls != 1 {
		t.Fatalf("want exactly 1 query, got %d", client.calls)
	}
	sql := client.statements[0]
	for _, want := range []string{"git_pull_requests", "FINAL", "repos", "GROUP BY", "anyLast(repos.repo)"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("query missing %q:\n%s", want, sql)
		}
	}

	check := func(name string, got, want any) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if row.RepoName == nil || *row.RepoName != "acme/widgets" {
		t.Errorf("RepoName = %v, want acme/widgets", row.RepoName)
	}
	if row.Title == nil || *row.Title != "Fix the thing" {
		t.Errorf("Title = %v", row.Title)
	}
	if row.Body == nil || *row.Body != "body text" {
		t.Errorf("Body = %v", row.Body)
	}
	if row.State == nil || *row.State != "merged" {
		t.Errorf("State = %v", row.State)
	}
	if row.AuthorName == nil || *row.AuthorName != "Ada Lovelace" {
		t.Errorf("AuthorName = %v", row.AuthorName)
	}
	if row.AuthorEmail == nil || *row.AuthorEmail != "ada@example.com" {
		t.Errorf("AuthorEmail = %v", row.AuthorEmail)
	}
	check("CreatedAt", row.CreatedAt, created)
	if row.MergedAt == nil || !row.MergedAt.Equal(merged) {
		t.Errorf("MergedAt = %v", row.MergedAt)
	}
	if row.ClosedAt == nil || !row.ClosedAt.Equal(closed) {
		t.Errorf("ClosedAt = %v", row.ClosedAt)
	}
	if row.HeadBranch == nil || *row.HeadBranch != "feature/x" {
		t.Errorf("HeadBranch = %v", row.HeadBranch)
	}
	if row.BaseBranch == nil || *row.BaseBranch != "main" {
		t.Errorf("BaseBranch = %v", row.BaseBranch)
	}
	if row.Additions == nil || *row.Additions != 120 {
		t.Errorf("Additions = %v", row.Additions)
	}
	if row.Deletions == nil || *row.Deletions != 40 {
		t.Errorf("Deletions = %v", row.Deletions)
	}
	if row.ChangedFiles == nil || *row.ChangedFiles != 7 {
		t.Errorf("ChangedFiles = %v", row.ChangedFiles)
	}
	if row.FirstReviewAt == nil || !row.FirstReviewAt.Equal(firstReview) {
		t.Errorf("FirstReviewAt = %v", row.FirstReviewAt)
	}
	if row.FirstCommentAt == nil || !row.FirstCommentAt.Equal(firstComment) {
		t.Errorf("FirstCommentAt = %v", row.FirstCommentAt)
	}
	check("ChangesRequestedCount", row.ChangesRequestedCount, 2)
	check("ReviewsCount", row.ReviewsCount, 3)
	check("CommentsCount", row.CommentsCount, 5)
}

// TestFetchPRCoreRow_NullableColumnsComeBackNil covers every nullable
// column at its actual nil value -- a Scan destination mismatch on any of
// them (e.g. a *uint32 forgotten as *int32) fails loudly here rather than
// only against a real driver.
func TestFetchPRCoreRow_NullableColumnsComeBackNil(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{{
		"", (*string)(nil), (*string)(nil), (*string)(nil), (*string)(nil), (*string)(nil),
		created, (*time.Time)(nil), (*time.Time)(nil), (*string)(nil), (*string)(nil),
		(*uint32)(nil), (*uint32)(nil), (*uint32)(nil), (*time.Time)(nil), (*time.Time)(nil),
		uint32(0), uint32(0), uint32(0),
	}}}}}

	row, ok, err := FetchPRCoreRow(context.Background(), client, "org1", testRepoID, 42)
	if err != nil {
		t.Fatalf("FetchPRCoreRow: %v", err)
	}
	if !ok {
		t.Fatal("got ok=false, want true")
	}
	// join_use_nulls=0 case (FetchPRCoreRow's own doc comment): an empty
	// repo_name means "no matching repo", mapped to nil, not "".
	if row.RepoName != nil {
		t.Errorf("RepoName = %v, want nil for an empty anyLast(repos.repo)", row.RepoName)
	}
	for name, got := range map[string]any{
		"Title": row.Title, "Body": row.Body, "State": row.State,
		"AuthorName": row.AuthorName, "AuthorEmail": row.AuthorEmail,
		"MergedAt": row.MergedAt, "ClosedAt": row.ClosedAt,
		"HeadBranch": row.HeadBranch, "BaseBranch": row.BaseBranch,
		"Additions": row.Additions, "Deletions": row.Deletions, "ChangedFiles": row.ChangedFiles,
		"FirstReviewAt": row.FirstReviewAt, "FirstCommentAt": row.FirstCommentAt,
	} {
		switch v := got.(type) {
		case *string:
			if v != nil {
				t.Errorf("%s = %v, want nil", name, *v)
			}
		case *time.Time:
			if v != nil {
				t.Errorf("%s = %v, want nil", name, *v)
			}
		case *int:
			if v != nil {
				t.Errorf("%s = %v, want nil", name, *v)
			}
		}
	}
}

// TestResolveReviews_ScansAndOrders pins ResolveReviews's query shape
// (submitted_at ASC, review_id ASC -- pr.py's own order) and its mapping
// into model.PullRequestReview.
func TestResolveReviews_ScansAndOrders(t *testing.T) {
	submitted := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"rev-1", "octocat", "approved", submitted},
	}}}}

	got, err := ResolveReviews(context.Background(), client, "org1", testRepoID, 42)
	if err != nil {
		t.Fatalf("ResolveReviews: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("want exactly 1 query, got %d", client.calls)
	}
	sql := client.statements[0]
	for _, want := range []string{"git_pull_request_reviews", "ORDER BY submitted_at ASC, review_id ASC", "LIMIT 500"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("query missing %q:\n%s", want, sql)
		}
	}
	if len(got) != 1 {
		t.Fatalf("got %d reviews, want 1", len(got))
	}
	want := model.PullRequestReview{ReviewID: "rev-1", Reviewer: "octocat", State: "approved", SubmittedAt: submitted}
	if got[0] != want {
		t.Fatalf("got %+v, want %+v", got[0], want)
	}
}

// TestResolveReviews_NoRows_ReturnsEmptyNotNil matches pr.py's
// list-comprehension-over-empty-rows behavior -- an empty, non-nil slice,
// same convention featureflags.go documents for its own list field.
func TestResolveReviews_NoRows_ReturnsEmptyNotNil(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}
	got, err := ResolveReviews(context.Background(), client, "org1", testRepoID, 42)
	if err != nil {
		t.Fatalf("ResolveReviews: %v", err)
	}
	if got == nil {
		t.Fatal("got nil slice, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("got %d reviews, want 0", len(got))
	}
}

// TestResolveCommits_ScansAndMaps pins ResolveCommits's query shape (the
// toFloat64(argMax(...)) cast -- see ResolveCommits's own doc comment for
// why -- and the FINAL/GROUP BY/ORDER BY/LIMIT shape) and its mapping
// into model.PullRequestCommit, including that AuthorWhen/Confidence/
// Provenance/Evidence are always non-nil pointers (never a bare literal
// nil in the composite literal -- see registered_document_field_gate_test.go's
// populatability oracle, which scores a field by whether ANY composite
// literal site assigns something other than nil).
func TestResolveCommits_ScansAndMaps(t *testing.T) {
	authorWhen := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"deadbeef", strPtr("fix: the bug"), strPtr("Ada Lovelace"), strPtr("ada@example.com"), authorWhen, 0.92, "native", "api_pr_commits"},
	}}}}

	got, err := ResolveCommits(context.Background(), client, "org1", testRepoID, 42)
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("want exactly 1 query, got %d", client.calls)
	}
	sql := client.statements[0]
	for _, want := range []string{
		"work_graph_pr_commit", "FINAL", "git_commits",
		"toFloat64(argMax(link.confidence, link.last_synced))",
		"GROUP BY link.commit_hash",
		"ORDER BY anyLast(commit.author_when) ASC, link.commit_hash ASC",
		"LIMIT 500",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("query missing %q:\n%s", want, sql)
		}
	}
	if len(got) != 1 {
		t.Fatalf("got %d commits, want 1", len(got))
	}
	c := got[0]
	if c.Hash != "deadbeef" {
		t.Errorf("Hash = %q", c.Hash)
	}
	if c.Message == nil || *c.Message != "fix: the bug" {
		t.Errorf("Message = %v", c.Message)
	}
	if c.AuthorName == nil || *c.AuthorName != "Ada Lovelace" {
		t.Errorf("AuthorName = %v", c.AuthorName)
	}
	if c.AuthorEmail == nil || *c.AuthorEmail != "ada@example.com" {
		t.Errorf("AuthorEmail = %v", c.AuthorEmail)
	}
	if c.AuthorWhen == nil || !c.AuthorWhen.Equal(authorWhen) {
		t.Errorf("AuthorWhen = %v", c.AuthorWhen)
	}
	if c.Confidence == nil || *c.Confidence != 0.92 {
		t.Errorf("Confidence = %v", c.Confidence)
	}
	if c.Provenance == nil || *c.Provenance != "native" {
		t.Errorf("Provenance = %v", c.Provenance)
	}
	if c.Evidence == nil || *c.Evidence != "api_pr_commits" {
		t.Errorf("Evidence = %v", c.Evidence)
	}
}

// TestResolveCommits_NoRows_ReturnsEmptyNotNil mirrors
// TestResolveReviews_NoRows_ReturnsEmptyNotNil for the commits reader.
func TestResolveCommits_NoRows_ReturnsEmptyNotNil(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}
	got, err := ResolveCommits(context.Background(), client, "org1", testRepoID, 42)
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if got == nil {
		t.Fatal("got nil slice, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("got %d commits, want 0", len(got))
	}
}
