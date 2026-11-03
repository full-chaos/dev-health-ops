package session

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/delegationend"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

func reuseRevokedEvent(ctx context.Context, principal, targetKind, targetID string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType:          "auth.session.refresh_reuse_revoked",
		Outcome:            audit.OutcomeDenied,
		SubjectPrincipalID: &principal,
		RequestID:          requestid.Audit(ctx),
		Attributes: map[string]any{
			"reason_code": "session_revoked", "target_kind": targetKind, "target_id": targetID,
			"credential_class": "refresh_credential",
		},
	}
}

func revokedEvent(ctx context.Context, actor Actor, principal, targetKind, targetID string, byCredential bool) audit.AuditEvent {
	attrs := map[string]any{"reason_code": "session_revoked", "target_kind": targetKind, "target_id": targetID}
	if byCredential {
		attrs["credential_class"] = "refresh_credential"
	}
	return audit.AuditEvent{
		EventType: "auth.session.revoked", Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: requestIDOf(ctx, actor),
		Attributes: attrs,
	}
}

func revokedAllEvent(ctx context.Context, actor Actor, principal string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: "auth.session.revoked_all", Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: requestIDOf(ctx, actor),
		Attributes: map[string]any{"reason_code": "session_revoked", "target_kind": "principal", "target_id": principal},
	}
}

func revisionBumpedEvent(ctx context.Context, actor Actor, principal, id string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: "auth.session.revision_bumped", Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: requestIDOf(ctx, actor),
		Attributes: map[string]any{"target_kind": "session", "target_id": id},
	}
}

func unpinnedRevokedEvent(ctx context.Context) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: "auth.session.revoked", Outcome: audit.OutcomeAllowed,
		Attributes: map[string]any{"reason_code": "session_revoked", "target_kind": "session"},
		RequestID:  requestid.Audit(ctx),
	}
}

func delegationStartedEvent(request *string, actorText, target, org, idText string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: delegationend.EventStarted, Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: &actorText, SubjectPrincipalID: &target, OrganizationID: &org, RequestID: request,
		Attributes: map[string]any{
			"reason_code": "accepted", "target_kind": "session", "target_id": idText, "action_key": ImpersonateAction,
		},
	}
}

func supersededEvent(event audit.AuditEvent, principal, predecessorID string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: EventSessionRevoked, Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: event.ActorPrincipalID, SubjectPrincipalID: &principal, OrganizationID: nil,
		RequestID: event.RequestID,
		Attributes: map[string]any{
			"reason_code": string(ReasonSessionRevoked), "target_kind": "session", "target_id": predecessorID,
			"source": "go-api-delegated",
		},
	}
}
