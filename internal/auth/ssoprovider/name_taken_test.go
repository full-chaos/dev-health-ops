package ssoprovider

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Only the unique violation of the provider name key is a taken name: another constraint's 23505, another SQLSTATE on
// the name key, or a message that merely names the key are store faults.
func TestNameTakenIsTheNameKeyUniqueViolationOnly(t *testing.T) {
	for name, c := range map[string]struct {
		err  error
		want bool
	}{
		"name key 23505":             {&pgconn.PgError{Code: "23505", ConstraintName: "uq_sso_provider_org_name"}, true},
		"name key 23505, wrapped":    {fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23505", ConstraintName: "uq_sso_provider_org_name"}), true},
		"primary key 23505":          {&pgconn.PgError{Code: "23505", ConstraintName: "sso_providers_pkey"}, false},
		"name key, another SQLSTATE": {&pgconn.PgError{Code: "23514", ConstraintName: "uq_sso_provider_org_name"}, false},
		"text only":                  {errors.New(`duplicate key value violates unique constraint "uq_sso_provider_org_name"`), false},
		"nil":                        {nil, false},
	} {
		if got := nameTaken(c.err); got != c.want {
			t.Errorf("%s: nameTaken = %v, want %v", name, got, c.want)
		}
	}
}
