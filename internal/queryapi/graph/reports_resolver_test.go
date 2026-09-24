package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// pgRecorder records every statement's arguments and fails, so a test sees
// exactly which org a resolver asked Postgres about.
type pgRecorder struct{ args [][]any }

func (p *pgRecorder) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	p.args = append(p.args, args)
	return nil, errors.New("pgRecorder: reached")
}

const reportsTestID = "01234567-89ab-cdef-0123-456789abcdef"

func savedReportCalls() map[string]func(r *Resolver, ctx context.Context, orgID string) error {
	return map[string]func(r *Resolver, ctx context.Context, orgID string) error{
		"savedReports": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().SavedReports(ctx, orgID, 50, 0)
			return err
		},
		"savedReport": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().SavedReport(ctx, orgID, reportsTestID)
			return err
		},
		"reportRuns": func(r *Resolver, ctx context.Context, orgID string) error {
			_, err := r.Query().ReportRuns(ctx, orgID, reportsTestID, 50)
			return err
		},
	}
}

// Every saved-report field denies a missing identity, an empty org and an
// orgId argument naming another org, and reaches Postgres for none of them.
func TestSavedReportFields_DenyForeignAndMissingOrg(t *testing.T) {
	for name, call := range savedReportCalls() {
		for label, ctx := range map[string]context.Context{
			"no claims":     context.Background(),
			"empty org":     authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""}),
			"foreign orgId": authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"}),
		} {
			pg := &pgRecorder{}
			orgArg := "org-1"
			if label == "foreign orgId" {
				orgArg = "org-2"
			}
			asAuthorizationError(t, call(&Resolver{Postgres: pg}, ctx, orgArg))
			if len(pg.args) != 0 {
				t.Errorf("%s/%s: Postgres reached past the guard", name, label)
			}
		}
	}
}

// With a matching org the resolver reaches Postgres and scopes to it.
func TestSavedReportFields_ScopeToAuthenticatedOrg(t *testing.T) {
	for name, call := range savedReportCalls() {
		pg := &pgRecorder{}
		ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
		_ = call(&Resolver{Postgres: pg}, ctx, "org-1")
		if len(pg.args) == 0 {
			t.Fatalf("%s: never reached Postgres", name)
		}
		found := false
		for _, a := range pg.args[0] {
			if a == "org-1" {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: org not bound in %v", name, pg.args[0])
		}
	}
}
