package session

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

// One case per audit row the session package writes, with and without an actor
// and a request id.
func TestEverySessionAuditRowIsAValidAuditEventV1(t *testing.T) {
	principal, target, org, id := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	actorUUID, request := uuid.New(), uuid.NewString()
	for _, actor := range []Actor{{}, {PrincipalID: &actorUUID, RequestID: &request}} {
		ctx := context.Background()
		rows := []audit.AuditEvent{
			reuseRevokedEvent(ctx, principal, "session", id),
			reuseRevokedEvent(ctx, principal, "principal", principal),
			revokedEvent(ctx, actor, principal, "session", id, false),
			revokedEvent(ctx, actor, principal, "session", id, true),
			revokedEvent(ctx, actor, principal, "principal", principal, true),
			revokedAllEvent(ctx, actor, principal),
			revisionBumpedEvent(ctx, actor, principal, id),
			unpinnedRevokedEvent(ctx),
			delegationStartedEvent(requestid.Audit(ctx), actorUUID.String(), target, org, id),
			supersededEvent(audit.AuditEvent{ActorPrincipalID: actorID(actor), RequestID: &request}, principal, id),
			supersededEvent(audit.AuditEvent{}, principal, id),
			refreshedEvent(ctx, principal, id),
		}
		for _, row := range rows {
			auditwire.MustValidate(t, row)
			if err := audit.ValidateAttributes(row.Attributes); err != nil {
				t.Fatalf("%s ValidateAttributes: %v", row.EventType, err)
			}
		}
	}
}
