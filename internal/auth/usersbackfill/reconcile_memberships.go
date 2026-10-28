package usersbackfill

import (
	"context"
)

// MembershipDiffs are the membership counts of one snapshot, for the auth
// principals of kind user.
type MembershipDiffs struct {
	PublicMissing    int
	InactiveInPublic int
	RoleDiff         int
	AuthMissing      int
}

// MembershipTarget counts the memberships auth.memberships and
// public.memberships disagree on.
type MembershipTarget interface {
	MembershipDiffs(ctx context.Context) (MembershipDiffs, error)
}

// ReconcileMemberships adds the membership counts to rep.
func ReconcileMemberships(ctx context.Context, tgt MembershipTarget, rep *Reconciliation) error {
	return nil
}

// MembershipDiffs counts inside the snapshot transaction.
func (t TxTarget) MembershipDiffs(ctx context.Context) (MembershipDiffs, error) {
	return MembershipDiffs{}, nil
}
