package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// A workItemTeamAttributions request whose orgId differs from the
// authenticated org is refused with Python's OrgIdAuthExtension message and
// never reaches ClickHouse; a matching orgId runs against the authenticated
// org.
func TestWorkItemTeamAttributions_OrgIDMismatchIsRefused(t *testing.T) {
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	ch := &scopeRecordingClient{}
	_, err := (&Resolver{ClickHouse: ch}).Query().WorkItemTeamAttributions(ctx, "foreign-org", nil, nil)
	if !workItemRefusedAs(err, "Access denied: cannot query org 'foreign-org'") {
		t.Fatalf("err = %v, want Access denied: cannot query org 'foreign-org'", err)
	}
	if len(ch.calls) != 0 {
		t.Fatalf("ClickHouse reached %d times for a foreign orgId, want 0", len(ch.calls))
	}

	for _, bad := range []string{"", " org-1"} {
		ch := &scopeRecordingClient{}
		_, err := (&Resolver{ClickHouse: ch}).Query().WorkItemTeamAttributions(ctx, bad, nil, nil)
		if !workItemRefusedAs(err, "A valid organization ID is required") {
			t.Fatalf("orgId %q: err = %v, want A valid organization ID is required", bad, err)
		}
		if len(ch.calls) != 0 {
			t.Fatalf("orgId %q reached ClickHouse %d times, want 0", bad, len(ch.calls))
		}
	}

	ch = &scopeRecordingClient{}
	_, _ = (&Resolver{ClickHouse: ch}).Query().WorkItemTeamAttributions(ctx, "org-1", nil, nil)
	if len(ch.calls) != 1 {
		t.Fatalf("matching orgId: ClickHouse calls = %d, want 1", len(ch.calls))
	}
}

// workItemRefusedAs reports whether err is a GraphQL error carrying exactly msg and
// the AUTHORIZATION_ERROR code.
func workItemRefusedAs(err error, msg string) bool {
	var ge *gqlerror.Error
	return errors.As(err, &ge) && ge.Message == msg && ge.Extensions["code"] == "AUTHORIZATION_ERROR"
}
