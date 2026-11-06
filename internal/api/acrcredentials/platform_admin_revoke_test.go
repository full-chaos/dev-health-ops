package acrcredentials

import (
	"net/http"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
)

// Product rule 1 (D6773), CHAOS-13209 F1: an admin.orgs holder is never forced to hold a membership row. With the
// token org and no row it revokes as that organization's admin, as the admin organization routes admit it
// (ensureOrgAdminAccess, internal/apiservice/admin/guards.go:39).
func TestAPlatformAdminWithNoMembershipRowRevokesAsTheOrganizationAdmin(t *testing.T) {
	user, org := uuid.New(), uuid.New()
	f := newFixture(t, true)
	f.store.superusers[user] = true
	got := f.post(f.sign(user, org.String(), jwt.MapClaims{"is_superuser": true, "role": "admin"}), credOne)
	if got.Code != http.StatusNoContent || got.Body.Len() != 0 {
		t.Fatalf("answer %d %q, want 204", got.Code, got.Body.String())
	}
	want := revokeCall{org: org.String(), credential: credOne, actorType: acrrevoke.ActorAdmin, actorID: user.String()}
	if len(f.revoker.calls) != 1 || f.revoker.calls[0] != want {
		t.Fatalf("revoke calls = %+v, want %+v", f.revoker.calls, want)
	}
}
