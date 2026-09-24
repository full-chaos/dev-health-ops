package graph

// Unit tests for the WorkUnitTeamAttributions resolver (schema.resolvers.go),
// the public GraphQL seam over workgraph.ResolveWorkUnitTeamAttributions.
// This resolver had no direct unit coverage before this file -- same gap
// analytics_resolver_test.go's own doc comment names for Analytics before
// it existed: the only way to reach the empty-claims branch is a direct
// resolver call with a bare context, and the only way to prove the
// resolver queries the AUTHENTICATED org (never the caller-supplied
// GraphQL argument, per this resolver's own doc comment) is a client that
// records what it was actually asked for.

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// capturingWorkUnitTeamAttributionsCHClient records the bindings of the
// one query it expects to see, then fails loudly -- same "a client that
// errors on any reachable call makes an accidental fall-through visible"
// convention as fakeAnalyticsCHClient/fakePrCHClient in this package.
type capturingWorkUnitTeamAttributionsCHClient struct {
	bindings []clickhouse.Binding
}

func (f *capturingWorkUnitTeamAttributionsCHClient) Query(_ context.Context, _ string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.bindings = bindings
	return nil, errors.New("capturingWorkUnitTeamAttributionsCHClient: reached past the guard, as expected")
}

func bindingValueByName(bindings []clickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

func TestWorkUnitTeamAttributions_RejectsMissingClaims(t *testing.T) {
	ch := &capturingWorkUnitTeamAttributionsCHClient{}
	r := &Resolver{ClickHouse: ch}
	_, err := r.Query().WorkUnitTeamAttributions(context.Background(), "org-1", nil, nil)
	asAuthorizationError(t, err)
	if ch.bindings != nil {
		t.Fatal("ClickHouse must not be reached when claims are missing")
	}
}

func TestWorkUnitTeamAttributions_RejectsEmptyOrgIDClaim(t *testing.T) {
	ch := &capturingWorkUnitTeamAttributionsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""})
	_, err := r.Query().WorkUnitTeamAttributions(ctx, "org-1", nil, nil)
	asAuthorizationError(t, err)
	if ch.bindings != nil {
		t.Fatal("ClickHouse must not be reached when the OrgID claim is empty")
	}
}

// TestWorkUnitTeamAttributions_UsesAuthenticatedOrgIDNotArgument pins the
// resolver's own doc comment claim: the org actually queried is the
// verified envelope's claims.OrgID, never the client-supplied orgID
// GraphQL argument -- proven here by naming a DIFFERENT org in each and
// reading back which one the underlying query actually bound.
func TestWorkUnitTeamAttributions_UsesAuthenticatedOrgIDNotArgument(t *testing.T) {
	ch := &capturingWorkUnitTeamAttributionsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-authenticated"})
	_, err := r.Query().WorkUnitTeamAttributions(ctx, "org-argument-different", nil, nil)
	requireNotAuthorizationError(t, err)
	if ch.bindings == nil {
		t.Fatal("ClickHouse was never reached for a matching, non-empty claim")
	}
	gotOrgID, ok := bindingValueByName(ch.bindings, "org_id")
	if !ok || gotOrgID != "org-authenticated" {
		t.Fatalf("org_id binding = %v (present=%v), want the authenticated claim %q, never the GraphQL argument", gotOrgID, ok, "org-authenticated")
	}
}
