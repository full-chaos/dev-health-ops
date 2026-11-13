package policy

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionrule"
)

// go-api's and query-api's session read takes its revoke from the session rule: the
// row's own revoke, its principal's password stamp after the row was created, and
// the same two of a client session's web session.
func TestSessionStateReadsTheSharedRule(t *testing.T) {
	for _, quoted := range []string{`"auth"`, `"tenant_auth"`} {
		revocation, err := sessionrule.NewRevocation(quoted)
		if err != nil {
			t.Fatal(err)
		}
		want := `SELECT s.principal_id, ` + revocation.Revoked("s") + ` IS NOT NULL, s.legacy_token_version FROM ` + quoted + `.sessions s WHERE s.id = $1`
		if got := sessionStateSQL(quoted); got != want {
			t.Errorf("%s:\n got %q\nwant %q", quoted, got, want)
		}
	}
}
