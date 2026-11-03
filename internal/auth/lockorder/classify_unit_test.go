package lockorder

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

func TestClassifyAfterDeleteAcceptsOnlyNamedOutcomes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want afterDelete
	}{
		{"no error", nil, afterDeleteOK},
		{"membership organization gone", fmt.Errorf("store: %w", membership.ErrOrgNotFound), afterDeleteRefused},
		{"not a member", membership.ErrNotMember, afterDeleteRefused},
		{"plan moved", fmt.Errorf("transfer: %w", membership.ErrStateMoved), afterDeleteRefused},
		{"source is no longer an owner", fmt.Errorf("transfer: %w", membership.ErrNotOwner), afterDeleteRefused},
		{"target is no longer a member", fmt.Errorf("transfer: %w", membership.ErrTargetNotMember), afterDeleteRefused},
		{"invite gone", invite.ErrInviteInvalid, afterDeleteRefused},
		{"invite organization gone", invite.ErrOrgNotFound, afterDeleteRefused},
		{"go-api membership names the deleted organization",
			fmt.Errorf("store unavailable: %w", &pgconn.PgError{Code: "23503", ConstraintName: "memberships_org_id_fkey"}), afterDeleteOrgGone},
		{"another foreign key", &pgconn.PgError{Code: "23503", ConstraintName: "audit_logs_org_id_fkey"}, afterDeleteOther},
		{"a deadlock", fmt.Errorf("store: %w", &pgconn.PgError{Code: "40P01"}), afterDeleteOther},
		{"a privilege refusal", &pgconn.PgError{Code: "42501"}, afterDeleteOther},
		{"an unnamed error", errors.New("boom"), afterDeleteOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyAfterDelete(tc.err); got != tc.want {
				t.Fatalf("classifyAfterDelete(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}
