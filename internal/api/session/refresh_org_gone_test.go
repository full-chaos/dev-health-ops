package session

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRefreshOrgGoneMatchesOnlyTheRefreshTokenOrganizationKey(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"no error", nil, false},
		{"the organization key", &pgconn.PgError{Code: "23503", ConstraintName: "refresh_tokens_org_id_fkey"}, true},
		{"wrapped", fmt.Errorf("issue: %w", &pgconn.PgError{Code: "23503", ConstraintName: "refresh_tokens_org_id_fkey"}), true},
		{"the user key", &pgconn.PgError{Code: "23503", ConstraintName: "refresh_tokens_user_id_fkey"}, false},
		{"another table's organization key", &pgconn.PgError{Code: "23503", ConstraintName: "memberships_org_id_fkey"}, false},
		{"another code on the same constraint", &pgconn.PgError{Code: "23505", ConstraintName: "refresh_tokens_org_id_fkey"}, false},
		{"an unnamed error", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := refreshOrgGone(tc.err); got != tc.want {
				t.Fatalf("refreshOrgGone(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
