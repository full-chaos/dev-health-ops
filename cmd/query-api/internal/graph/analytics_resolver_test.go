package graph

// Unit tests for the Analytics resolver's org-id authorization branches
// (schema.resolvers.go). This package had NO unit test file before this
// one -- the only existing coverage of the AUTHORIZATION_ERROR shape is
// query_route_integration_test.go, gated behind `//go:build integration`
// (needs a live Postgres testcontainer), and even that file only
// exercises the mismatch branch indirectly through a real HTTP round
// trip. The EMPTY-CLAIMS branch (authctx.FromContext returning ok=false,
// or an empty OrgID) cannot be reached through query_route.go's real HTTP
// path at all -- the router always calls authctx.WithClaims with a
// non-empty OrgID once verifier.Verify succeeds -- so the only way to
// exercise it is a direct resolver call with a bare context, which is
// what this file does.

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// fakeAnalyticsCHClient always returns an error -- these tests exist to
// prove the auth check fires BEFORE any ClickHouse call, so a client
// that would fail loudly if reached makes an accidental fall-through
// visible instead of silently returning an empty, misleadingly
// "successful" result.
type fakeAnalyticsCHClient struct{ called bool }

func (f *fakeAnalyticsCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.called = true
	return nil, errors.New("fakeAnalyticsCHClient: should not have been called")
}

func asAuthorizationError(t *testing.T, err error) *gqlerror.Error {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var gqlErr *gqlerror.Error
	if !errors.As(err, &gqlErr) {
		t.Fatalf("expected a *gqlerror.Error, got %T: %v", err, err)
	}
	code, _ := gqlErr.Extensions["code"].(string)
	if code != "AUTHORIZATION_ERROR" {
		t.Fatalf(`expected extensions["code"] = "AUTHORIZATION_ERROR", got %q (message: %q)`, code, gqlErr.Message)
	}
	return gqlErr
}

func TestAnalytics_RejectsMissingClaims(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	// No authctx.WithClaims call at all -- authctx.FromContext returns ok=false.
	_, err := r.Query().Analytics(context.Background(), "org-1", model.AnalyticsRequestInput{})
	asAuthorizationError(t, err)
	if ch.called {
		t.Fatal("ClickHouse must not be reached when claims are missing")
	}
}

func TestAnalytics_RejectsEmptyOrgIDClaim(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""})
	_, err := r.Query().Analytics(ctx, "org-1", model.AnalyticsRequestInput{})
	asAuthorizationError(t, err)
	if ch.called {
		t.Fatal("ClickHouse must not be reached when the OrgID claim is empty")
	}
}

func TestAnalytics_RejectsMismatchedOrgIDArgument(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	// The GraphQL argument names a DIFFERENT org than the authenticated
	// claim -- this is the exact shape FeatureFlags's precedent guards
	// against (schema.resolvers.go:156-164), copied here in shape, not
	// string (see the Analytics resolver's own doc comment for why the
	// message text is not copied forward).
	_, err := r.Query().Analytics(ctx, "org-2", model.AnalyticsRequestInput{})
	gqlErr := asAuthorizationError(t, err)
	if gqlErr.Message == "org_id is required for all analytics queries" {
		t.Fatal("must not propagate FeatureFlags's copy-paste-artifact message text forward")
	}
	if ch.called {
		t.Fatal("ClickHouse must not be reached when orgId does not match the authenticated claim")
	}
}

func TestAnalytics_MatchingOrgIDReachesResolve(t *testing.T) {
	// A minimal empty batch (no timeseries/breakdowns/sankey/flowMatrix)
	// short-circuits analytics.Resolve to an empty, successful result
	// WITHOUT ever calling the ClickHouse client (Phase 0's repo-filter
	// resolution is skipped when filters is nil, and Phase 1's gather is
	// trivially empty) -- so this test proves the auth check does NOT
	// fire on a legitimate matching org, not that the full pipeline
	// works end to end (that's internal/analytics's own test suite).
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	result, err := r.Query().Analytics(ctx, "org-1", model.AnalyticsRequestInput{})
	if err != nil {
		t.Fatalf("expected no error for a matching org-id with an empty batch, got %v", err)
	}
	if result == nil {
		t.Fatal("expected a non-nil AnalyticsResult")
	}
	if ch.called {
		t.Fatal("an empty batch should never reach ClickHouse")
	}
}
