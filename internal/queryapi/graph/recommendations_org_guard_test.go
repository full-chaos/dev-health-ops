package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// A recommendations request whose orgId differs from the authenticated org
// is refused with Python's OrgIdAuthExtension message and never reaches
// ClickHouse; a matching orgId runs against the authenticated org.
func TestRecommendations_OrgIDMismatchIsRefused(t *testing.T) {
	win := model.WindowInput{Value: 1, Unit: model.WindowUnitWeek}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	ch := &scopeRecordingClient{}
	_, err := (&Resolver{ClickHouse: ch}).Query().Recommendations(ctx, "foreign-org", "team-a", win)
	if !recommendationsRefusedAs(err, "Access denied: cannot query org 'foreign-org'") {
		t.Fatalf("err = %v, want Access denied: cannot query org 'foreign-org'", err)
	}
	if len(ch.calls) != 0 {
		t.Fatalf("ClickHouse reached %d times for a foreign orgId, want 0", len(ch.calls))
	}

	ch = &scopeRecordingClient{}
	if _, err := (&Resolver{ClickHouse: ch}).Query().Recommendations(ctx, "org-1", "team-a", win); err != nil {
		t.Fatalf("matching orgId: err = %v", err)
	}
	if len(ch.calls) != 1 {
		t.Fatalf("matching orgId: ClickHouse calls = %d, want 1", len(ch.calls))
	}
}

// recommendationsRefusedAs reports whether err is a GraphQL error carrying exactly msg and
// the AUTHORIZATION_ERROR code.
func recommendationsRefusedAs(err error, msg string) bool {
	var ge *gqlerror.Error
	return errors.As(err, &ge) && ge.Message == msg && ge.Extensions["code"] == "AUTHORIZATION_ERROR"
}
