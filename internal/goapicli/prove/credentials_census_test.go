package prove

import (
	"context"
	"strings"
	"testing"

	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// run() applies pgstorage.Boundary(dsn).Redact to the error openPostgresPool returns.
func TestOpenPostgresPoolHoldsNoLoginOfAnUnparsableDSN(t *testing.T) {
	for form, dsn := range map[string]string{
		"uri":       "postgres://census_login_q7x9:census_password_q7x9@[::1",
		"key=value": "user=census_login_q7x9 password=census_password_q7x9 host=[::1 sslmode=bogus",
	} {
		_, err := openPostgresPool(context.Background(), dsn)
		if err == nil {
			t.Fatalf("%s: no error for an unparsable DSN", form)
		}
		got := pgstorage.Boundary(dsn).Redact(err).Error()
		for _, leaked := range []string{"census_login_q7x9", "census_password_q7x9", "[::1"} {
			if strings.Contains(got, leaked) {
				t.Errorf("%s: the redacted error holds %q: %s", form, leaked, got)
			}
		}
	}
}
