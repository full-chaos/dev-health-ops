package graph

// Unit tests for the Pr resolver (schema.resolvers.go), CHAOS-4980. Same
// shape as analytics_resolver_test.go: a bare context (no HTTP round trip)
// is the only way to reach the empty-claims branch, and a fake ClickHouse
// client that fails loudly on any unexpected call makes an accidental
// fall-through visible instead of silently returning a misleadingly
// "successful" result.

import (
	"context"
	"errors"
	"strings"
	"testing"

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
		default:
			return errors.New("pr resolver test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakePrRowScanner) Err() error   { return nil }
func (f *fakePrRowScanner) Close() error { return nil }

// fakePrCHClient always fails unless a response is queued -- so an
// unexpected call (e.g. the auth check not firing before the query) shows
// up as a loud test failure rather than a silently wrong result.
type fakePrCHClient struct {
	response  *fakePrRowScanner
	called    bool
	statement string
}

func (f *fakePrCHClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.called = true
	f.statement = statement
	if f.response == nil {
		return nil, errors.New("fakePrCHClient: no response queued -- unexpected query")
	}
	return f.response, nil
}

func TestPr_RejectsMissingClaims(t *testing.T) {
	ch := &fakePrCHClient{}
	r := &Resolver{ClickHouse: ch}
	_, err := r.Query().Pr(context.Background(), "org-1", testRepoID+"#pr1")
	asAuthorizationError(t, err)
	if ch.called {
		t.Fatal("ClickHouse must not be reached when claims are missing")
	}
}

func TestPr_RejectsEmptyOrgIDClaim(t *testing.T) {
	ch := &fakePrCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""})
	_, err := r.Query().Pr(ctx, "org-1", testRepoID+"#pr1")
	asAuthorizationError(t, err)
	if ch.called {
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
	if ch.called {
		t.Fatal("ClickHouse must not be reached when the id fails to parse")
	}
}

// TestPr_FlagStates is CHAOS-4980's resolver-level flag-state coverage:
// with WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED=1 the Pr resolver
// must reach fetchLinkedIssueRowsFastPath's query shape; unset, the FINAL
// oracle's. Both must map to an identical, correctly-populated
// linkedIssues list -- the workgraph-package parity tests
// (pr_test.go/pr_integration_test.go) prove the underlying reader/mapping
// agree; this test proves the resolver actually reaches that code path in
// both states, all the way through the GraphQL-facing model.
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

			ch := &fakePrCHClient{response: &fakePrRowScanner{
				rows: [][]any{{"issue:OPS-1", 0.9, "native", "token-a"}},
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
			if !ch.called {
				t.Fatal("expected ClickHouse to be reached for a valid id and authorized claims")
			}
			if !strings.Contains(ch.statement, tc.wantInQuery) {
				t.Fatalf("query does not contain %q:\n%s", tc.wantInQuery, ch.statement)
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
