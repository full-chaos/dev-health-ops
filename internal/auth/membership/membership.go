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
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
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

// organizationRoles are the organization roles of contracts/auth/v1/vocabulary/roles.v1.json.
var organizationRoles = [...]string{"viewer", "member", "admin", "owner"}

// OrganizationRoles returns a copy of the organization roles.
func OrganizationRoles() []string { return append([]string(nil), organizationRoles[:]...) }

func isOrganizationRole(role string) bool {
	for _, r := range organizationRoles {
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
	if publicRole != nil {
		return state{status: StatusActive, role: *publicRole}
	}
	if authStatus == nil {
		return state{status: statusAbsent}
	}
	cur := state{status: *authStatus}
	if authRole != nil {
		cur.role = *authRole
	}
	if cur.status == StatusActive {
		cur.status = StatusRemoved
	}
	return cur
}

type decision struct {
	noop bool
	next state
}

// decide is the transition table of one pair. activeOwners counts the active
// owners of the organization, the pair included. No verb removes an
// organization's last active owner (owners never go from >=1 to 0); an
// organization that already has none stays operable, and a change to owner is
// its way out.
func decide(v verb, cur state, role string, activeOwners int) (decision, error) {
	lastOwner := cur.status == StatusActive && cur.role == "owner" && activeOwners <= 1
	switch v {
	case verbAdd:
		if !isOrganizationRole(role) {
			return decision{}, ErrRoleInvalid
		}
		switch cur.status {
		case StatusActive:
			return decision{}, ErrAlreadyMember
		case StatusSuspended:
			return decision{}, ErrSuspended
		}
		return decision{next: state{status: StatusActive, role: role}}, nil
	case verbChangeRole:
		if !isOrganizationRole(role) {
			return decision{}, ErrRoleInvalid
		}
		switch cur.status {
		case StatusActive:
		case StatusSuspended:
			return decision{}, ErrSuspended
		default:
			return decision{}, ErrNotMember
		}
		if cur.role == role {
			return decision{noop: true, next: cur}, nil
		}
		if lastOwner {
			return decision{}, ErrLastOwner
		}
		return decision{next: state{status: StatusActive, role: role}}, nil
	case verbSuspend:
		switch cur.status {
		case StatusSuspended:
			return decision{noop: true, next: cur}, nil
		case StatusActive:
		default:
			return decision{}, ErrNotMember
		}
		if lastOwner {
			return decision{}, ErrLastOwner
		}
		return decision{next: state{status: StatusSuspended, role: cur.role}}, nil
	case verbReactivate:
		switch cur.status {
		case StatusActive:
			return decision{noop: true, next: cur}, nil
		case StatusSuspended:
		default:
			return decision{}, ErrNotMember
		}
		if !isOrganizationRole(cur.role) {
			return decision{}, ErrRoleMissing
		}
		return decision{next: state{status: StatusActive, role: cur.role}}, nil
	case verbRemove:
		switch cur.status {
		case StatusActive, StatusSuspended:
		default:
			return decision{}, ErrNotMember
		}
		if lastOwner {
			return decision{}, ErrLastOwner
		}
		return decision{next: state{status: StatusRemoved, role: cur.role}}, nil
	}
	return decision{}, errors.New("membership: unknown verb")
}

// decideTransfer moves ownership: from an active owner to admin, to an active
// member to owner (go-api transferOwnership, orgstore.go).
func decideTransfer(from, to state) (state, state, error) {
	if from.status != StatusActive || from.role != "owner" {
		return state{}, state{}, ErrNotOwner
	}
	if to.status != StatusActive {
		return state{}, state{}, ErrTargetNotMember
	}
	return state{status: StatusActive, role: "admin"}, state{status: StatusActive, role: "owner"}, nil
}

// eventFor is the provisioning event of a verb on one membership.
func eventFor(v verb, id uuid.UUID, next state) audit.OutboxEvent {
	e := audit.OutboxEvent{AggregateType: "membership", AggregateID: provisioning.WireID("membership", id.String())}
	switch v {
	case verbAdd, verbReactivate:
		e.EventType = "membership.activated"
		e.Change = map[string]any{"status": string(StatusActive), "role_id": "rol_" + next.role}
	case verbChangeRole, verbTransfer:
		e.EventType = "membership.role_changed"
		e.Change = map[string]any{"role_id": "rol_" + next.role}
	case verbSuspend:
		e.EventType = "membership.suspended"
		e.Change = map[string]any{"status": string(StatusSuspended)}
	case verbRemove:
		e.EventType = "membership.removed"
		e.Change = map[string]any{"status": string(StatusRemoved)}
	}
	return e
}

// auditStatus is a membership status as the audit vocabulary spells it: an
// invited membership is pending (the provisioning README spelling).
func auditStatus(s Status) string {
	if s == StatusInvited {
		return "pending"
	}
	return string(s)
}

var auditEventTypes = map[verb]string{
	verbAdd: "auth.membership.added", verbChangeRole: "auth.membership.role_changed",
	verbTransfer: "auth.membership.ownership_transferred", verbSuspend: "auth.membership.suspended",
	verbReactivate: "auth.membership.reactivated", verbRemove: "auth.membership.removed",
}

// auditFor is the audit row of a verb on one membership: ids and closed
// values only.
func auditFor(v verb, orgID, principalID, id uuid.UUID, a Actor, prev, next state) audit.AuditEvent {
	org, subject := orgID.String(), principalID.String()
	ev := audit.AuditEvent{EventType: auditEventTypes[v], Outcome: audit.OutcomeAllowed, OrganizationID: &org,
		SubjectPrincipalID: &subject, RequestID: a.RequestID,
		Attributes: map[string]any{"target_kind": "membership", "target_id": id.String()}}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	switch v {
	case verbAdd:
		ev.Attributes["reason_code"] = "membership_added"
	case verbRemove:
		ev.Attributes["reason_code"] = "membership_removed"
	default:
		ev.Attributes["reason_code"] = "membership_status_changed"
	}
	if prev.status != next.status {
		ev.Attributes["new_status"] = auditStatus(next.status)
		if prev.status != statusAbsent {
			ev.Attributes["previous_status"] = auditStatus(prev.status)
		}
		ev.Attributes["changed_fields"] = []string{"status"}
	} else if prev.role != next.role {
		ev.Attributes["changed_fields"] = []string{"role_key"}
	}
	return ev
}

// outcomeLabel maps an error to the closed outcome label set of the mutation
// counter. No id or role reaches a label.
func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, audit.ErrNoStateChange):
		return "noop"
	case errors.Is(err, ErrNotMember), errors.Is(err, ErrTargetNotMember):
		return "not_member"
	case errors.Is(err, ErrAlreadyMember):
		return "already_member"
	case errors.Is(err, ErrSuspended), errors.Is(err, ErrOrgSuspended):
		return "suspended"
	case errors.Is(err, ErrLastOwner):
		return "last_owner"
	case errors.Is(err, ErrNotOwner):
		return "not_owner"
	case errors.Is(err, ErrRoleInvalid), errors.Is(err, ErrRoleMissing):
		return "invalid"
	case errors.Is(err, ErrRevisionConflict):
		return "conflict"
	case errors.Is(err, ErrOrgNotFound), errors.Is(err, ErrPrincipalNotFound), errors.Is(err, ErrInviterNotFound):
		return "not_found"
	case errors.Is(err, ErrDenied), errors.Is(err, ErrSuperuserRequired):
		return "denied"
	case errors.Is(err, ErrNotOwned):
		return "not_owned"
	case errors.Is(err, errStateMoved):
		return "replanned"
	}
	return "error"
}
