package rule

import (
	"strings"
	"testing"
)

// StoredHoldsSQL reads the stored, unexpired assignment of the role by the principal and nothing of the principal's
// state; it is a different form from the authority rule.
func TestStoredHoldsSQLReadsTheStoredUnexpiredAssignment(t *testing.T) {
	got := StoredHoldsSQL(`"auth"`, "users.id", "'platform_admin'")
	want := `EXISTS (SELECT 1 FROM "auth".platform_role_assignments pra_stored WHERE pra_stored.principal_id = users.id` +
		` AND pra_stored.role_key = 'platform_admin' AND (pra_stored.expires_at IS NULL OR pra_stored.expires_at > statement_timestamp()))`
	if got != want {
		t.Errorf("StoredHoldsSQL = %q, want %q", got, want)
	}
	if strings.Contains(got, ".principals") || strings.Contains(got, "disabled_at") || got == HoldsSQL(`"auth"`, "users.id", "'platform_admin'") {
		t.Errorf("StoredHoldsSQL = %q reads the principal's state", got)
	}
}
