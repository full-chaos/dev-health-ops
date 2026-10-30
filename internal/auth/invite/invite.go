// Package invite is the auth-service Invite aggregate: the write authority for
// auth.invites while auth-service owns users. An invite token is single
// purpose (org_invite_token), stored as its SHA-256 digest only, and consumed
// once; every verb commits through audit.Commit with go-api's org_invites row.
package invite

import (
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusAccepted Status = "accepted"
	StatusRevoked  Status = "revoked"
)

// DefaultTTL is go-api's invite lifetime (apiservice/admin/invites.go inviteTTL).
const DefaultTTL = 72 * time.Hour

const (
	minTTL = time.Hour
	maxTTL = 30 * 24 * time.Hour
)

type verb string

const (
	verbCreate verb = "create"
	verbAccept verb = "accept"
	verbRevoke verb = "revoke"
)

type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
	// Caller is the credential the request came with; the audit row names it.
	Caller audit.Caller
}

// row is one invite as the store reads it.
type row struct {
	found     bool
	id        uuid.UUID
	orgID     uuid.UUID
	email     string
	role      string
	status    Status
	expiresAt time.Time
}

// checkConsumable refuses a token whose invite cannot be accepted now. Every
// refusal is ErrInviteInvalid: unknown, accepted, revoked and expired look the
// same. An invite is still valid at its expires_at instant (go-api).
func checkConsumable(r row, now time.Time) error {
	if !r.found || r.status != StatusPending || r.expiresAt.Before(now) {
		return ErrInviteInvalid
	}
	return nil
}

// emailRefusal is go-api's inviteEmailRefusal: an unverified account first,
// then a different address after strip and lower.
func emailRefusal(inviteEmail, accountEmail string, verified bool) error {
	if !verified {
		return ErrEmailUnverified
	}
	if pythonparity.Lower(pythonparity.Strip(accountEmail)) != pythonparity.Lower(pythonparity.Strip(inviteEmail)) {
		return ErrEmailMismatch
	}
	return nil
}

// inviteEmailPattern is invite-create-request.v1's email pattern.
var inviteEmailPattern = regexp.MustCompile(`^[a-z0-9_%+-]+([.][a-z0-9_%+-]+)*@([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?[.])+[a-z]{2,63}$`)

// normalizeEmail refuses an address outside the contract pattern or that is
// not its own login-key fixed point.
func normalizeEmail(email string) (string, error) {
	if len(email) > 254 || !inviteEmailPattern.MatchString(email) {
		return "", ErrEmailInvalid
	}
	normalized, err := identity.NormalizeEmail(email)
	if err != nil || normalized != email {
		return "", ErrEmailInvalid
	}
	return email, nil
}

// lifetime is the invite's lifetime: DefaultTTL, or a requested one inside
// the contract's bounds.
func lifetime(requested *time.Duration) (time.Duration, error) {
	if requested == nil {
		return DefaultTTL, nil
	}
	if *requested < minTTL || *requested > maxTTL {
		return 0, ErrTTLInvalid
	}
	return *requested, nil
}

// decideRevoke: pending and unexpired -> revoked; a repeat is a no-op; an
// accepted invite stays accepted (revocation is monotonic, never a way back),
// and an expired one is no longer pending either. An invite is still valid at
// its expires_at instant.
func decideRevoke(s Status, expiresAt, now time.Time) (noop bool, err error) {
	switch {
	case s == StatusRevoked:
		return true, nil
	case s == StatusPending && !expiresAt.Before(now):
		return false, nil
	}
	return false, ErrNotPending
}

var eventTypes = map[verb]struct{ event, status string }{
	verbCreate: {"invite.created", string(StatusPending)},
	verbAccept: {"invite.accepted", string(StatusAccepted)},
	verbRevoke: {"invite.revoked", string(StatusRevoked)},
}

func eventFor(v verb, id uuid.UUID) audit.OutboxEvent {
	t := eventTypes[v]
	return audit.OutboxEvent{AggregateType: "invite", AggregateID: provisioning.WireID("invite", id.String()),
		EventType: t.event, Change: map[string]any{"status": t.status}}
}

var auditTypes = map[verb]struct{ event, reason string }{
	verbCreate: {"auth.invite.created", "invite_created"},
	verbAccept: {"auth.invite.accepted", "invite_accepted"},
	verbRevoke: {"auth.invite.revoked", "invite_revoked"},
}

// auditFor is the audit row of a verb: ids and closed values only; no address
// and no token.
func auditFor(v verb, orgID, id uuid.UUID, subject *uuid.UUID, a Actor) audit.AuditEvent {
	t := auditTypes[v]
	org := orgID.String()
	ev := audit.AuditEvent{EventType: t.event, Outcome: audit.OutcomeAllowed, OrganizationID: &org, RequestID: a.RequestID,
		Attributes: map[string]any{"reason_code": t.reason, "target_kind": "invite", "target_id": id.String()}}
	if subject != nil {
		s := subject.String()
		ev.SubjectPrincipalID = &s
	}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	a.Caller.Annotate(ev.Attributes)
	return ev
}

// outcomeLabel maps an error to the closed outcome label set of the mutation
// counter. No id, address or token reaches a label.
func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, audit.ErrNoStateChange):
		return "noop"
	case errors.Is(err, ErrInviteInvalid):
		return "invalid_invite"
	case errors.Is(err, ErrAccountInactive):
		return "inactive"
	case errors.Is(err, ErrEmailUnverified):
		return "email_unverified"
	case errors.Is(err, ErrEmailMismatch):
		return "email_mismatch"
	case errors.Is(err, ErrEmailInvalid), errors.Is(err, ErrRoleInvalid), errors.Is(err, ErrTTLInvalid):
		return "invalid"
	case errors.Is(err, ErrPendingExists):
		return "pending_exists"
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrOrgNotFound), errors.Is(err, ErrInviterNotFound):
		return "not_found"
	case errors.Is(err, ErrNotPending):
		return "not_pending"
	case errors.Is(err, ErrAlreadyMember):
		return "already_member"
	case errors.Is(err, ErrOrgSuspended):
		return "suspended"
	case errors.Is(err, ErrOrgConflict):
		return "org_conflict"
	case errors.Is(err, ErrDenied):
		return "denied"
	case errors.Is(err, ErrNotOwned):
		return "not_owned"
	}
	return "error"
}
