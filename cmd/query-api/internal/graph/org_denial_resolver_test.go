package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// scopeRecordingClient records every statement's bindings and fails, so a
// test sees exactly which org a resolver asked ClickHouse about.
type scopeRecordingClient struct{ calls [][]clickhouse.Binding }

func (c *scopeRecordingClient) Query(_ context.Context, _ string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.calls = append(c.calls, b)
	return nil, errors.New("scopeRecordingClient: reached")
}

type orgScopedCall func(r *Resolver, ctx context.Context, orgID string) error

func orgScopedCalls() map[string]orgScopedCall {
	return map[string]orgScopedCall{
		"securityAlerts": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().SecurityAlerts(ctx, orgID, nil, nil)
			return err
		},
		"securityOverview": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().SecurityOverview(ctx, orgID, nil)
			return err
		},
		"compoundingRisk": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().CompoundingRisk(ctx, orgID, nil)
			return err
		},
		"busFactor": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().BusFactor(ctx, orgID, nil)
			return err
		},
		"aiImpactSummary": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().AiImpactSummary(ctx, orgID, model.AIDateRangeInput{}, nil)
			return err
		},
		"aiComparison": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().AiComparison(ctx, orgID, model.AIDateRangeInput{}, nil)
			return err
		},
		"aiGovernanceSummary": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().AiGovernanceSummary(ctx, orgID, model.AIDateRangeInput{}, nil, 50)
			return err
		},
		"aiWorkflowDrilldown": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().AiWorkflowDrilldown(ctx, orgID, model.AIWorkflowRootTypeInputPr, "r:1", 3, 100)
			return err
		},
		"aiReviewLoad": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().AiReviewLoad(ctx, orgID, model.AIDateRangeInput{}, nil)
			return err
		},
	}
}

// Every org-scoped field denies a missing identity, an empty org, and an
// orgId argument that names another org, and reaches ClickHouse for none of them.
func TestOrgScopedFields_DenyForeignAndMissingOrg(t *testing.T) {
	for name, call := range orgScopedCalls() {
		for label, ctx := range map[string]context.Context{
			"no claims":     context.Background(),
			"empty org":     authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""}),
			"foreign orgId": authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"}),
		} {
			ch := &scopeRecordingClient{}
			orgArg := "org-1"
			if label == "foreign orgId" {
				orgArg = "org-2"
			}
			err := call(&Resolver{ClickHouse: ch}, ctx, orgArg)
			asAuthorizationError(t, err)
			if len(ch.calls) != 0 {
				t.Errorf("%s/%s: ClickHouse reached past the guard", name, label)
			}
		}
	}
}

// With a matching org the resolver reaches ClickHouse and scopes to it.
func TestOrgScopedFields_ScopeToAuthenticatedOrg(t *testing.T) {
	for name, call := range orgScopedCalls() {
		ch := &scopeRecordingClient{}
		ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
		_ = call(&Resolver{ClickHouse: ch}, ctx, "org-1")
		if len(ch.calls) == 0 {
			t.Fatalf("%s: never reached ClickHouse", name)
		}
		if v, _ := bindingValueByName(ch.calls[0], "org_id"); v != "org-1" {
			t.Errorf("%s: org_id binding = %v", name, v)
		}
	}
}
