package graph

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func TestCatalog_RejectsMissingClaims(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	dim := model.DimensionInputTheme
	_, err := r.Query().Catalog(context.Background(), "org-1", &dim, nil)
	asAuthorizationError(t, err)
	if ch.called {
		t.Fatal("ClickHouse must not be reached when claims are missing")
	}
}

func TestCatalog_RejectsEmptyOrgIDClaim(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""})
	dim := model.DimensionInputTheme
	_, err := r.Query().Catalog(ctx, "org-1", &dim, nil)
	asAuthorizationError(t, err)
	if ch.called {
		t.Fatal("ClickHouse must not be reached when the OrgID claim is empty")
	}
}

func TestCatalog_RejectsMismatchedOrgIDArgument(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	dim := model.DimensionInputTheme
	_, err := r.Query().Catalog(ctx, "org-2", &dim, nil)
	asAuthorizationError(t, err)
	if ch.called {
		t.Fatal("ClickHouse must not be reached for a different organization")
	}
}

func TestCatalog_DeniesEveryPrincipalKindForADifferentOrg(t *testing.T) {
	// The claims type carries only the org, so no role or superuser state can
	// widen the org a request may read.
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	dim := model.DimensionInputTheme
	for _, requested := range []string{"org-2", "", " org-1", "ORG-1"} {
		if _, err := r.Query().Catalog(ctx, requested, &dim, nil); err == nil {
			t.Fatalf("orgId %q must be denied", requested)
		} else {
			asAuthorizationError(t, err)
		}
	}
	if ch.called {
		t.Fatal("ClickHouse must not be reached for a different organization")
	}
}

func TestCatalog_QueriesTheAuthorizedOrg(t *testing.T) {
	ch := &orgCapturingCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	dim := model.DimensionInputTheme
	if _, err := r.Query().Catalog(ctx, "org-1", &dim, nil); err != nil || ch.org != "org-1" {
		t.Fatalf("err=%v org=%q", err, ch.org)
	}
}

type orgCapturingCHClient struct{ org string }

func (c *orgCapturingCHClient) Query(_ context.Context, _ string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	for _, b := range bindings {
		if b.Name == "org_id" {
			c.org, _ = b.Value.(string)
		}
	}
	return emptyRows{}, nil
}

type emptyRows struct{}

func (emptyRows) Next() bool        { return false }
func (emptyRows) Scan(...any) error { return nil }
func (emptyRows) Err() error        { return nil }
func (emptyRows) Close() error      { return nil }

func TestCatalog_StaticListsNeedNoQuery(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	res, err := r.Query().Catalog(ctx, "org-1", nil, nil)
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if ch.called || res.Values != nil || len(res.Dimensions) != 6 || len(res.Measures) != 21 {
		t.Fatalf("called=%v values=%v dims=%d measures=%d", ch.called, res.Values, len(res.Dimensions), len(res.Measures))
	}
}

func TestCatalog_QueryFailureAnswersEmptyValues(t *testing.T) {
	ch := &fakeAnalyticsCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	dim := model.DimensionInputTheme
	res, err := r.Query().Catalog(ctx, "org-1", &dim, nil)
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if !ch.called || res.Values == nil || len(res.Values) != 0 {
		t.Fatalf("called=%v values=%#v", ch.called, res.Values)
	}
}
