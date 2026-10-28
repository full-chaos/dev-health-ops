package usersbackfill

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// MembershipDiffs are the membership counts of one snapshot, for the auth
// principals of kind user.
type MembershipDiffs struct {
	// PublicMissing: an active auth membership with no public row.
	PublicMissing int
	// InactiveInPublic: an auth membership that is not active, with a public row.
	InactiveInPublic int
	// RoleDiff: an active auth membership whose role no public row of the pair has.
	RoleDiff int
	// AuthMissing: a public membership with no auth row.
	AuthMissing int
}

// MembershipTarget counts the memberships auth.memberships and
// public.memberships disagree on.
type MembershipTarget interface {
	MembershipDiffs(ctx context.Context) (MembershipDiffs, error)
}

// ReconcileMemberships adds the membership counts to rep. Only
// DiffMembershipInactiveInPublic is gated: go-api removes members and changes
// roles in public.memberships only, so the other classes move with normal
// go-api writes. A failure names no value.
func ReconcileMemberships(ctx context.Context, tgt MembershipTarget, rep *Reconciliation) error {
	d, err := tgt.MembershipDiffs(ctx)
	if err != nil {
		return errors.New("usersbackfill: reading the memberships failed")
	}
	if rep.Diffs == nil {
		rep.Diffs = map[string]int{}
	}
	if d.InactiveInPublic > 0 {
		rep.Diffs[DiffMembershipInactiveInPublic] += d.InactiveInPublic
	}
	rep.MembershipPublicMissing += d.PublicMissing
	rep.MembershipRoleDiff += d.RoleDiff
	rep.MembershipAuthMissing += d.AuthMissing
	return nil
}

// MembershipDiffs counts inside the snapshot transaction, over the
// (organization, user) pairs of the auth principals of kind user. Counts only:
// no id or value leaves the query.
func (t TxTarget) MembershipDiffs(ctx context.Context) (MembershipDiffs, error) {
	q := authschema.Quote(t.Schema)
	var d MembershipDiffs
	err := t.Tx.QueryRow(ctx, `WITH pub AS (
  SELECT org_id, user_id, array_agg(DISTINCT role::text) AS roles FROM public.memberships GROUP BY org_id, user_id
), pairs AS (
  SELECT a.status, a.role, p.roles, a.id IS NOT NULL AS in_auth, p.user_id IS NOT NULL AS in_public
  FROM `+q+`.memberships a
  FULL JOIN pub p ON p.org_id = a.organization_id AND p.user_id = a.principal_id
  WHERE COALESCE(a.principal_id, p.user_id) IN (SELECT id FROM `+q+`.principals WHERE kind = 'user')
)
SELECT
  count(*) FILTER (WHERE in_auth AND status = 'active' AND NOT in_public),
  count(*) FILTER (WHERE in_auth AND status <> 'active' AND in_public),
  count(*) FILTER (WHERE in_auth AND status = 'active' AND in_public AND (role IS NULL OR roles <> ARRAY[role]::text[])),
  count(*) FILTER (WHERE NOT in_auth AND in_public)
FROM pairs`).Scan(&d.PublicMissing, &d.InactiveInPublic, &d.RoleDiff, &d.AuthMissing)
	return d, err
}
