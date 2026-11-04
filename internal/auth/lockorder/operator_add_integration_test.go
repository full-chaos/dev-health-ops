//go:build integration

package lockorder

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// The operator CLI no longer writes a membership row (it sends the change to
// auth-service's route), so the operator-add lock cells are gone; this helper
// stays for the cells that count the public row.
func (e *env) wantOnePublicRow(t *testing.T, ctx context.Context, cell string, org, user uuid.UUID, role string) {
	t.Helper()
	got := e.scalar(t, ctx, `SELECT count(*)::text || ' ' || COALESCE(min(role), '') FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, user)
	if got != "1 "+role {
		t.Fatalf("%s: public.memberships has %q, want one %s row", cell, got, role)
	}
}
