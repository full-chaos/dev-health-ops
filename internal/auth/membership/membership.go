// Package membership is the auth-service Membership aggregate: the write
// authority for auth.memberships while auth-service owns users. Every verb
// commits through audit.Commit, so the auth row, go-api's public.memberships
// row, the audit row, the provisioning event and the revision change together
// or not at all.
package membership

import (
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

type Status string

const (
	StatusInvited   Status = "invited"
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
	StatusRemoved   Status = "removed"
)

// statusAbsent is a pair with no row in either table.
const statusAbsent Status = ""

type verb string

const (
	verbAdd        verb = "add"
	verbChangeRole verb = "change_role"
	verbSuspend    verb = "suspend"
	verbReactivate verb = "reactivate"
	verbRemove     verb = "remove"
	verbTransfer   verb = "transfer"
)

// OrganizationRoles are the organization roles of contracts/auth/v1/vocabulary/roles.v1.json.
var OrganizationRoles = []string{"viewer", "member", "admin", "owner"}

func isOrganizationRole(role string) bool {
	for _, r := range OrganizationRoles {
		if r == role {
			return true
		}
	}
	return false
}

// Actor is who asks for a change. The store never reads authority from it:
// Authorize decides on the locked rows.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
}

// state is the effective membership of one (organization, principal) pair.
// public.memberships is go-api's record (D6358): a public row means active
// with its role. Without one, the auth row's status counts, except an active
// auth row with no public row, which counts as removed.
type state struct {
	status Status
	role   string
}

func effective(publicRole *string, authStatus *Status, authRole *string) state {
	return state{}
}

type decision struct {
	noop bool
	next state
}

// decide is the transition table of one pair. activeOwners counts the active
// owners of the organization, the pair included.
func decide(v verb, cur state, role string, activeOwners int) (decision, error) {
	return decision{}, nil
}

// decideTransfer moves ownership: from an active owner to admin, to an active
// member to owner.
func decideTransfer(from, to state) (state, state, error) {
	return state{}, state{}, nil
}

func eventFor(v verb, id uuid.UUID, next state) audit.OutboxEvent {
	return audit.OutboxEvent{}
}

func auditFor(v verb, orgID, principalID, id uuid.UUID, a Actor, prev, next state) audit.AuditEvent {
	return audit.AuditEvent{}
}

func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, audit.ErrNoStateChange):
		return "noop"
	}
	return "error"
}
