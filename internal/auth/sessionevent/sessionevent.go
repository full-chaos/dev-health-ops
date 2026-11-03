// Package sessionevent builds the revocation events of provisioning-event.v1 (CHAOS-11816): session.revoked for one
// session, principal.sessions_revoked and organization.sessions_revoked for a bulk end, signing_key.compromised for a
// compromised signing key. A producer queues them with audit.TxOps.AddEvent inside the mutation that wrote the state.
package sessionevent

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// The closed reasons of provisioning-event.v1 (change.reason).
const (
	ReasonCaller         = "caller"
	ReasonRefreshReuse   = "refresh_reuse"
	ReasonPasswordChange = "password_changed"
	ReasonOrgEnded       = "org_ended"
	ReasonDelegationEnd  = "delegation_ended"
	ReasonCompromised    = "compromised"
)

// BeforeLayout is the wire form of revoked_before: UTC, six fractional digits, the precision of a PostgreSQL timestamptz,
// so the value equals the revoked_at the store wrote.
const BeforeLayout = "2006-01-02T15:04:05.000000Z"

// Session is the session.revoked event of the session whose id is the uuid text sessionID.
func Session(sessionID, reason string) (audit.OutboxEvent, error) {
	id, err := uuid.Parse(sessionID)
	if err != nil {
		return audit.OutboxEvent{}, fmt.Errorf("sessionevent: not a session id")
	}
	return audit.OutboxEvent{AggregateType: "session", AggregateID: sessionid.Wire(id), EventType: "session.revoked",
		Change: map[string]any{"status": "revoked", "reason": reason}}, nil
}

// Principal is the principal.sessions_revoked scope event: every session of the principal that started before at.
func Principal(principalID, reason string, at time.Time) audit.OutboxEvent {
	return scope("principal", principalID, reason, at)
}

// Organization is the organization.sessions_revoked scope event: every session of the organization that started before at.
func Organization(organizationID, reason string, at time.Time) audit.OutboxEvent {
	return scope("organization", organizationID, reason, at)
}

func scope(aggregate, id, reason string, at time.Time) audit.OutboxEvent {
	return audit.OutboxEvent{AggregateType: aggregate, AggregateID: provisioning.WireID(aggregate, id), EventType: aggregate + ".sessions_revoked",
		Change: map[string]any{"revoked_before": at.UTC().Format(BeforeLayout), "reason": reason}}
}

// SigningKey is the signing_key.compromised event of kid. The aggregate id is provisioning.SigningKeyID(kid) (a kid may
// hold a dot, an aggregate id may not); the kid travels as change.kid.
func SigningKey(kid string) audit.OutboxEvent {
	return audit.OutboxEvent{AggregateType: "signing_key", AggregateID: provisioning.SigningKeyID(kid),
		EventType: "signing_key.compromised", Change: map[string]any{"kid": kid}}
}
