package graph

// Unit tests for the Pr resolver (schema.resolvers.go), CHAOS-4980,
// completed by CHAOS-4991. Same shape as analytics_resolver_test.go: a
// bare context (no HTTP round trip) is the only way to reach the
// empty-claims branch, and a fake ClickHouse client that fails loudly on
// any unexpected call makes an accidental fall-through visible instead of
// silently returning a misleadingly "successful" result.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
)

const testRepoID = "11111111-1111-1111-1111-111111111111"

// fakePrRowScanner and fakePrCHClient are minimal, package-local fakes --
// workgraph_test.go's fakeClient/fakeRowScanner are unexported to that
// package and cannot be reused here.
type fakePrRowScanner struct {
	rows   [][]any
	cursor int
}

func (f *fakePrRowScanner) Next() bool { return f.cursor < len(f.rows) }

func (f *fakePrRowScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("pr resolver test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *float64:
			*ptr = row[i].(float64)
		case *uint32:
			*ptr = row[i].(uint32)
		case *time.Time:
			*ptr = row[i].(time.Time)
		// Nullable-column double-pointer cases -- see workgraph_test.go's
		// fakeRowScanner (same convention, duplicated here because this
		// package cannot import that unexported test type): a nullable
		// ClickHouse column scans into a **T (FetchPRCoreRow/ResolveCommits
		// pass &someNillableVar where someNillableVar is itself a pointer).
		case **string:
			if row[i] == nil {
				*ptr = nil
			} else {
				*ptr = row[i].(*string)
			}
		case **time.Time:
			if row[i] == nil {
				*ptr = nil
			} else {
				*ptr = row[i].(*time.Time)
			}
		case **uint32:
			if row[i] == nil {
				*ptr = nil
			} else {
				*ptr = row[i].(*uint32)
			}
		default:
			return errors.New("pr resolver test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakePrRowScanner) Err() error   { return nil }
func (f *fakePrRowScanner) Close() error { return nil }

// emptyPrRowScanner is a zero-row response -- FetchPRCoreRow's "not
// found" case. Distinct from a nil *fakePrRowScanner so fakePrCHClient can
// tell "no response queued for this call" (test bug -- fail loudly) apart
// from "queued an intentionally empty result" (a real, expected shape).
var emptyPrRowScanner = &fakePrRowScanner{rows: nil}

// fakePrCHClient dispatches queued responses by call ORDER (same
// convention as workgraph_test.go's fakeClient) -- responses[0] answers
// the Pr resolver's first ClickHouse call (FetchPRCoreRow), responses[1]
// the second (ResolveReviews), responses[2] the third (ResolveCommits),
// responses[3] the fourth (ResolveLinkedIssues) -- the last three are only
// reached when the first found a row. A call past the end of responses
// fails loudly -- an unexpected extra query (e.g. the core-row fetch not
// short-circuiting on "not found") shows up as a test failure, not a
// silently wrong result.
type fakePrCHClient struct {
	responses  []*fakePrRowScanner
	calls      int
	statements []string
}

func (f *fakePrCHClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	i := f.calls
	f.calls++
	f.statements = append(f.statements, statement)
	if i >= len(f.responses) {
		return nil, errors.New("fakePrCHClient: no response queued -- unexpected query")
	}
	return f.responses[i], nil
}

// prCoreRowFixture is a fully-populated 19-column FetchPRCoreRow response
// row, reused by every test below that needs the "PR exists" branch --
// keeping every field distinguishable makes a transposed-field mapping
// bug visible as a value mismatch rather than a coincidental pass.
func prCoreRowFixture() []any {
	created := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	merged := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	return []any{
		"acme/widgets",            // repo_name
		strPtr("Fix the thing"),   // title
		strPtr("body text"),       // body
		strPtr("merged"),          // state
		strPtr("Ada Lovelace"),    // author_name
		strPtr("ada@example.com"), // author_email
		created,                   // created_at
		&merged,                   // merged_at
		(*time.Time)(nil),         // closed_at
		strPtr("feature/x"),       // head_branch
		strPtr("main"),            // base_branch
		uint32Ptr(120),            // additions
		uint32Ptr(40),             // deletions
		uint32Ptr(7),              // changed_files
		(*time.Time)(nil),         // first_review_at
		(*time.Time)(nil),         // first_comment_at
		uint32(2),                 // changes_requested_count
		uint32(3),                 // reviews_count
		uint32(5),                 // comments_count
	}
}

func strPtr(s string) *string    { return &s }
func uint32Ptr(v uint32) *uint32 { return &v }

func TestPr_RejectsMissingClaims(t *testing.T) {
	ch := &fakePrCHClient{}
	r := &Resolver{ClickHouse: ch}
	_, err := r.Query().Pr(context.Background(), "org-1", testRepoID+"#pr1")
	asAuthorizationError(t, err)
	if ch.calls != 0 {
		t.Fatal("ClickHouse must not be reached when claims are missing")
	}
}

func TestPr_RejectsEmptyOrgIDClaim(t *testing.T) {
	ch := &fakePrCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""})
	_, err := r.Query().Pr(ctx, "org-1", testRepoID+"#pr1")
	asAuthorizationError(t, err)
	if ch.calls != 0 {
		t.Fatal("ClickHouse must not be reached when the OrgID claim is empty")
	}
}

func TestPr_InvalidIDReturnsNilWithoutError(t *testing.T) {
	ch := &fakePrCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	got, err := r.Query().Pr(ctx, "org-1", "not-a-valid-pr-id")
	if err != nil {
		t.Fatalf("expected no error for an unparsable id, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected a nil result for an unparsable id, got %+v", got)
	}
	if ch.calls != 0 {
		t.Fatal("ClickHouse must not be reached when the id fails to parse")
	}
}

// TestPr_UnknownPRReturnsNilWithoutError is CHAOS-4980's nil-for-unknown
// coverage (team-lead ruling), re-pinned against CHAOS-4991's
// FetchPRCoreRow-based existence check: an empty core-row result must
// make the Pr resolver return nil, exactly like Python's resolve_pr does
// when _fetch_pr_row comes back empty -- and must NOT go on to fetch
// reviews/commits/linkedIssues for a PR that doesn't exist (exactly one
// ClickHouse call).
func TestPr_UnknownPRReturnsNilWithoutError(t *testing.T) {
	ch := &fakePrCHClient{responses: []*fakePrRowScanner{emptyPrRowScanner}}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	got, err := r.Query().Pr(ctx, "org-1", testRepoID+"#pr999")
	if err != nil {
		t.Fatalf("expected no error for an unknown PR, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected a nil result for an unknown PR, got %+v", got)
	}
	if ch.calls != 1 {
		t.Fatalf("expected exactly 1 ClickHouse call (the core-row fetch only), got %d: %v", ch.calls, ch.statements)
	}
}

// TestPr_FullyPopulatesEveryField is CHAOS-4991's class-closing resolver
// proof: with the core row, one review, one commit, and one linked issue
// all present, every field of the returned PullRequestDetail must be
// populated from its corresponding source row -- not just linkedIssues
// (CHAOS-4980's partial scope). A field silently left at its zero value
// here would also fail registered_document_field_gate_test.go, but this
// test proves the VALUE is right, not just that some non-nil literal was
// assigned somewhere in the source.
func TestPr_FullyPopulatesEveryField(t *testing.T) {
	submittedAt := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)
	authorWhen := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)

	ch := &fakePrCHClient{responses: []*fakePrRowScanner{
		{rows: [][]any{prCoreRowFixture()}}, // call 0: FetchPRCoreRow
		{rows: [][]any{ // call 1: ResolveReviews
			{"rev-1", "octocat", "approved", submittedAt},
		}},
		{rows: [][]any{ // call 2: ResolveCommits
			{"deadbeef", strPtr("fix: the bug"), strPtr("Ada Lovelace"), strPtr("ada@example.com"), authorWhen, 0.92, "native", "api_pr_commits"},
		}},
		{rows: [][]any{ // call 3: ResolveLinkedIssues
			{"issue:OPS-1", 0.9, "native", "token-a"},
		}},
	}}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	got, err := r.Query().Pr(ctx, "org-1", testRepoID+"#pr42")
	if err != nil {
		t.Fatalf("Pr: %v", err)
	}
	if got == nil {
		t.Fatal("expected a non-nil PullRequestDetail")
	}
	if ch.calls != 4 {
		t.Fatalf("expected exactly 4 ClickHouse calls (core row + reviews + commits + linked issues), got %d: %v", ch.calls, ch.statements)
	}

	if got.ID != testRepoID+"#pr42" || got.RepoID != testRepoID || got.Number != 42 || got.OrgID != "org-1" {
		t.Fatalf("got id/repoId/number/orgId = %q/%q/%d/%q, want %q/%q/42/%q",
			got.ID, got.RepoID, got.Number, got.OrgID, testRepoID+"#pr42", testRepoID, "org-1")
	}
	if got.RepoName == nil || *got.RepoName != "acme/widgets" {
		t.Errorf("RepoName = %v", got.RepoName)
	}
	if got.Title == nil || *got.Title != "Fix the thing" {
		t.Errorf("Title = %v", got.Title)
	}
	if got.Body == nil || *got.Body != "body text" {
		t.Errorf("Body = %v", got.Body)
	}
	if got.State == nil || *got.State != "merged" {
		t.Errorf("State = %v", got.State)
	}
	if got.AuthorName == nil || *got.AuthorName != "Ada Lovelace" {
		t.Errorf("AuthorName = %v", got.AuthorName)
	}
	if got.AuthorEmail == nil || *got.AuthorEmail != "ada@example.com" {
		t.Errorf("AuthorEmail = %v", got.AuthorEmail)
	}
	if got.MergedAt == nil {
		t.Error("MergedAt = nil, want non-nil")
	}
	if got.ClosedAt != nil {
		t.Errorf("ClosedAt = %v, want nil (fixture leaves it unset)", got.ClosedAt)
	}
	if got.HeadBranch == nil || *got.HeadBranch != "feature/x" {
		t.Errorf("HeadBranch = %v", got.HeadBranch)
	}
	if got.BaseBranch == nil || *got.BaseBranch != "main" {
		t.Errorf("BaseBranch = %v", got.BaseBranch)
	}
	if got.Additions == nil || *got.Additions != 120 {
		t.Errorf("Additions = %v", got.Additions)
	}
	if got.Deletions == nil || *got.Deletions != 40 {
		t.Errorf("Deletions = %v", got.Deletions)
	}
	if got.ChangedFiles == nil || *got.ChangedFiles != 7 {
		t.Errorf("ChangedFiles = %v", got.ChangedFiles)
	}
	if got.ChangesRequestedCount != 2 || got.ReviewsCount != 3 || got.CommentsCount != 5 {
		t.Errorf("counts = %d/%d/%d, want 2/3/5", got.ChangesRequestedCount, got.ReviewsCount, got.CommentsCount)
	}

	if len(got.Reviews) != 1 {
		t.Fatalf("got %d reviews, want 1: %+v", len(got.Reviews), got.Reviews)
	}
	if r := got.Reviews[0]; r.ReviewID != "rev-1" || r.Reviewer != "octocat" || r.State != "approved" || !r.SubmittedAt.Equal(submittedAt) {
		t.Errorf("review = %+v", r)
	}

	if len(got.Commits) != 1 {
		t.Fatalf("got %d commits, want 1: %+v", len(got.Commits), got.Commits)
	}
	if c := got.Commits[0]; c.Hash != "deadbeef" || c.Message == nil || *c.Message != "fix: the bug" ||
		c.Confidence == nil || *c.Confidence != 0.92 || c.Provenance == nil || *c.Provenance != "native" {
		t.Errorf("commit = %+v", c)
	}

	if len(got.LinkedIssues) != 1 {
		t.Fatalf("got %d linked issues, want 1: %+v", len(got.LinkedIssues), got.LinkedIssues)
	}
	link := got.LinkedIssues[0]
	if link.WorkItemID != "issue:OPS-1" || link.Confidence != 0.9 || link.Provenance != "native" || link.Evidence != "token-a" {
		t.Fatalf("mapped link = %+v, want work_item_id=issue:OPS-1 confidence=0.9 provenance=native evidence=token-a", link)
	}
}

// TestPr_FlagStates is CHAOS-4980's resolver-level flag-state coverage,
// re-pinned at the (now fourth) ResolveLinkedIssues call: with
// WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED=1 the Pr resolver must
// reach fetchLinkedIssueRowsFastPath's query shape; unset, the FINAL
// oracle's. Both must map to an identical, correctly-populated
// linkedIssues list -- the workgraph-package parity tests
// (pr_test.go/pr_integration_test.go) prove the underlying reader/mapping
// agree; this test proves the resolver actually reaches that code path in
// both states, all the way through the GraphQL-facing model, AFTER the
// core-row fetch (call 0), reviews (call 1), and commits (call 2) all
// succeed.
func TestPr_FlagStates(t *testing.T) {
	for _, tc := range []struct {
		name        string
		flagValue   string
		wantInQuery string
	}{
		{"flag on: fast path", "1", "version_rank"},
		{"flag unset: FINAL oracle", "", "FINAL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED", tc.flagValue)

			ch := &fakePrCHClient{responses: []*fakePrRowScanner{
				{rows: [][]any{prCoreRowFixture()}}, // call 0: FetchPRCoreRow
				{rows: nil},                         // call 1: ResolveReviews
				{rows: nil},                         // call 2: ResolveCommits
				{rows: [][]any{{"issue:OPS-1", 0.9, "native", "token-a"}}}, // call 3: ResolveLinkedIssues
			}}
			r := &Resolver{ClickHouse: ch}
			ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

			got, err := r.Query().Pr(ctx, "org-1", testRepoID+"#pr42")
			if err != nil {
				t.Fatalf("Pr: %v", err)
			}
			if got == nil {
				t.Fatal("expected a non-nil PullRequestDetail")
			}
			if ch.calls != 4 {
				t.Fatalf("expected exactly 4 ClickHouse calls (core row + reviews + commits + linked issues), got %d: %v", ch.calls, ch.statements)
			}
			if !strings.Contains(ch.statements[3], tc.wantInQuery) {
				t.Fatalf("linked-issues query does not contain %q:\n%s", tc.wantInQuery, ch.statements[3])
			}
			if got.ID != testRepoID+"#pr42" || got.RepoID != testRepoID || got.Number != 42 || got.OrgID != "org-1" {
				t.Fatalf("got id/repoId/number/orgId = %q/%q/%d/%q, want %q/%q/42/%q",
					got.ID, got.RepoID, got.Number, got.OrgID, testRepoID+"#pr42", testRepoID, "org-1")
			}
			if len(got.LinkedIssues) != 1 {
				t.Fatalf("got %d linked issues, want 1: %+v", len(got.LinkedIssues), got.LinkedIssues)
			}
			link := got.LinkedIssues[0]
			if link.WorkItemID != "issue:OPS-1" || link.Confidence != 0.9 || link.Provenance != "native" || link.Evidence != "token-a" {
				t.Fatalf("mapped link = %+v, want work_item_id=issue:OPS-1 confidence=0.9 provenance=native evidence=token-a", link)
			}
		})
	}
}
