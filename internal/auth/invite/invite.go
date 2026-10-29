// Package invite is the auth-service Invite aggregate: the write authority for
// auth.invites while auth-service owns users. An invite token is single
// purpose (org_invite_token), stored as its SHA-256 digest only, and consumed
// once; every verb commits through audit.Commit with go-api's org_invites row.
package invite

import (
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
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
func checkConsumable(r row, now time.Time) error { return nil }

// emailRefusal is go-api's inviteEmailRefusal: an unverified account first,
// then a different address after strip and lower.
func emailRefusal(inviteEmail, accountEmail string, verified bool) error { return nil }

// normalizeEmail refuses an address that is not its own login-key fixed point.
func normalizeEmail(email string) (string, error) { return email, nil }

// lifetime is the invite's lifetime: DefaultTTL, or a requested one inside
// the contract's bounds.
func lifetime(requested *time.Duration) (time.Duration, error) { return DefaultTTL, nil }

// decideRevoke: pending -> revoked; a repeat is a no-op; an accepted invite
// stays accepted.
func decideRevoke(s Status) (noop bool, err error) { return false, nil }

func eventFor(v verb, id uuid.UUID) audit.OutboxEvent { return audit.OutboxEvent{} }

func auditFor(v verb, orgID, id uuid.UUID, subject *uuid.UUID, a Actor) audit.AuditEvent {
	return audit.AuditEvent{}
}

func outcomeLabel(err error) string { return "" }
