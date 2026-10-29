package usersbackfill

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// OrgMismatch is one organization with at least one diff class: its id and the class names, never a value.
type OrgMismatch struct {
	OrgID   uuid.UUID
	Classes []string
}

// OrgDiffs are the counts of one read-only snapshot of public.organizations / public.memberships against the auth schema.
type OrgDiffs struct {
	PublicOrgs, AuthOrgs, Matched                              int
	MissingInAuth, ExtraInAuth, SlugDiff, NameDiff, ActiveDiff int
	Members                                                    MembershipDiffs
	DuplicatePublicSource                                      int
	NonUserPrincipalMemberships                                int
	PublicMemberships, AuthMemberships                         int
	Mismatches                                                 []OrgMismatch
}

// OrgsTarget reads the snapshot counts.
type OrgsTarget interface {
	OrgDiffs(ctx context.Context) (OrgDiffs, error)
}

// OrgMismatchCap is the number of mismatching organization ids a report lists.
const OrgMismatchCap = 50

// OrgsReconciliation is the outcome of a read-only comparison of the organization aggregates.
type OrgsReconciliation struct {
	Exact             bool
	Diffs             OrgDiffs
	Listed            []OrgMismatch
	MismatchOrgsTotal int
}

// ReconcileOrgs is not implemented yet.
func ReconcileOrgs(ctx context.Context, tgt OrgsTarget, exact bool) (OrgsReconciliation, error) {
	return OrgsReconciliation{Exact: exact}, nil
}

// GatedTotal is not implemented yet.
func (r OrgsReconciliation) GatedTotal() int { return 0 }

// Lines is not implemented yet.
func (r OrgsReconciliation) Lines() []string { return nil }

// orgsReconcileTargetFn opens the snapshot target of the verb; tests replace it. Not implemented yet.
var orgsReconcileTargetFn = func(ctx context.Context, env cli.Env, verb string) (OrgsTarget, func(), int) {
	return nil, func() {}, cli.ExitFailure
}
