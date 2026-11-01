package oidc

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

var errJITNotBuilt = errors.New("oidc: PGJIT is not built")

// PGJIT is the MembershipJIT over the identity and session stores.
type PGJIT struct {
	Identities *identity.Store
	Sessions   *session.Store
	LoginEvent func(ctx context.Context, principal uuid.UUID, org *uuid.UUID) audit.AuditEvent
	Mint       func(ctx context.Context, issued session.Issued, req SessionRequest) (Tokens, error)
}

// JoinAndIssue is the seam's join.
func (j PGJIT) JoinAndIssue(context.Context, uuid.UUID, uuid.UUID, SessionRequest) (Tokens, error) {
	return Tokens{}, errJITNotBuilt
}

// ProvisionJoinAndIssue is the seam's new account.
func (j PGJIT) ProvisionJoinAndIssue(context.Context, FederatedAccount, uuid.UUID, SessionRequest) (Principal, Tokens, error) {
	return Principal{}, Tokens{}, errJITNotBuilt
}

// mapJITError turns the identity store's refusals into the flow's sentinels.
func mapJITError(err error) error { return err }
