package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

var errJITNotBuilt = errors.New("identity: the sign-in join is not built")

// ErrMemberElsewhere refuses a sign-in join of an account that holds a
// membership in another organization.
var ErrMemberElsewhere = errors.New("identity: the account is a member of another organization")

// ErrAlreadyMember reports a sign-in join of an account that became a member of
// the organization after the caller read it. Nothing is written.
var ErrAlreadyMember = errors.New("identity: the account already is a member of the organization")

// SSOJoin is the organization a federated sign-in joins and the role it joins with.
type SSOJoin struct {
	OrgID uuid.UUID
	Role  string
}

// AfterJoin runs in the join's transaction after its rows are written.
type AfterJoin func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error

// SignInRole is the role a provider row's default_role gives a sign-in.
func SignInRole(string) (string, bool) { return "", false }

// JoinOnSignIn adds an existing account to join.OrgID.
func (s *Store) JoinOnSignIn(context.Context, uuid.UUID, SSOJoin, Actor, AfterJoin) error {
	return errJITNotBuilt
}

// ProvisionAndJoin is ProvisionFederated with the membership in its commit.
func (s *Store) ProvisionAndJoin(context.Context, ProvisionRequest, SSOJoin, AfterJoin) (uuid.UUID, error) {
	return uuid.Nil, errJITNotBuilt
}
