package aclguard_test

import (
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The auth runtime role's writes on go-api's delegation row. The end follows no switch: a security end must not
// depend on operator order or a skip branch. The start and the read of a legacy row's target follow AUTH_USERS_OWNED:
// only Start uses them, and Start refuses with the switch off. A widened, narrowed or reordered list fails here first.
func TestTheImpersonationWriteGrantIsPinned(t *testing.T) {
	if got, want := authschema.ImpersonationEndColumns(), []string{"ended_at"}; !slices.Equal(got, want) {
		t.Fatalf("ImpersonationEndColumns = %v, want %v", got, want)
	}
	want := []string{"id", "admin_user_id", "target_user_id", "target_org_id", "target_role", "created_at", "expires_at"}
	if got := authschema.ImpersonationStartColumns(); !slices.Equal(got, want) {
		t.Fatalf("ImpersonationStartColumns = %v, want %v", got, want)
	}
	if got, want := authschema.ImpersonationLegacyReadColumns(), []string{"target_user_id", "target_org_id"}; !slices.Equal(got, want) {
		t.Fatalf("ImpersonationLegacyReadColumns = %v, want %v", got, want)
	}
}
