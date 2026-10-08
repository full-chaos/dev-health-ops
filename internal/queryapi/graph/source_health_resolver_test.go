package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

type recordingPG struct{ orgs []any }

func (p *recordingPG) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	p.orgs = append(p.orgs, args...)
	return nil, errors.New("stop after the recorded read")
}

// The field reads Postgres, so the shared ClickHouse table of org-scoped
// fields does not cover it: the same decisions are pinned here.
func TestSourceHealth_DeniesForeignAndMissingOrgBeforeAnyRead(t *testing.T) {
	for label, ctx := range map[string]context.Context{
		"no claims":     context.Background(),
		"empty org":     authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""}),
		"foreign orgId": authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1", Role: "viewer"}),
	} {
		pg := &recordingPG{}
		_, err := (&Resolver{Postgres: pg}).Query().SourceHealth(ctx, "org-2")
		asAuthorizationError(t, err)
		if len(pg.orgs) != 0 {
			t.Errorf("%s: Postgres reached past the guard", label)
		}
	}
}

func TestSourceHealth_ReadsTheCallersOrgForAnyRole(t *testing.T) {
	for _, role := range []string{"", "viewer", "member"} {
		pg := &recordingPG{}
		ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1", Role: role})
		_, _ = (&Resolver{Postgres: pg}).Query().SourceHealth(ctx, "org-1")
		if len(pg.orgs) != 1 || pg.orgs[0] != "org-1" {
			t.Errorf("role %q: Postgres args = %v, want the caller's org", role, pg.orgs)
		}
	}
}
