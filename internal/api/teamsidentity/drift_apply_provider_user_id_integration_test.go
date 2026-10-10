//go:build integration

package teamsidentity

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// CHAOS-9144: an approved membership keeps provider_user_id from the staged payload.
func TestApprovedMembershipKeepsTheStagedProviderUserID(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-9144"
	row, ok := loadsJSONValue(`{"org_id":"org-9144","provider":"gitlab","team_id":"gl:g","member_id":"gl:alice","source":"native",
		"is_primary":1,"specificity":100,"priority":10,"valid_from":"2026-08-01T00:00:00","updated_at":"2026-08-10T00:00:00","provider_user_id":"42"}`).(*pyjson.Object)
	if !ok {
		t.Fatal("payload is not an object")
	}
	if err := store.insertTeamMembership(ctx, orgID, row); err != nil {
		t.Fatal(err)
	}
	var got *string
	if err := store.Conn.QueryRow(ctx, `SELECT provider_user_id FROM team_memberships FINAL WHERE org_id = ? AND member_id = 'gl:alice'`, orgID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != "42" {
		t.Errorf("provider_user_id = %v, want 42", got)
	}
}
