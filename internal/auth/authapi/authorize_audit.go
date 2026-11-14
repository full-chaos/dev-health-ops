package authapi

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

var errAuthorizeAuditNoSink = errors.New("authapi: a high-risk or delegated decision has no audit sink")

// highRiskAction reports whether the policy catalog classes the action high
// risk. An action outside the catalog has no class.
func highRiskAction(action string) bool {
	a, err := policycatalog.Default().Action(action)
	return err == nil && a.RiskClass == policycatalog.RiskHigh
}

// knownOrganization is true for a canonical organization uuid the decision
// transaction found, the only organization text that may fill the audit row's
// foreign key column.
func knownOrganization(orgID string, snap authorization.Snapshot) bool {
	id, err := uuid.Parse(orgID)
	return err == nil && id.String() == orgID && snap.OrganizationStatus(orgID) != authorization.StatusUnknown
}

// auditDecisions writes, in one transaction, one audit row for each decided
// item whose action is high risk and one for each delegated item, allow or
// deny. A delegated high-risk item writes both rows. A call with no such item
// does not touch the sink. An error means the decisions must not be served.
func (h authorizeHandlers) auditDecisions(ctx context.Context, items []authorizeItem, docs []decisionDocument) error {
	var events []audit.AuditEvent
	caller, _ := ctx.Value(authorizeCallerKey{}).(AuthorizeCaller)
	for i, item := range items {
		if highRiskAction(item.question.Action) {
			events = append(events, authorizeAuditEvent(ctx, caller, item, docs[i]))
		}
		if item.delegated {
			events = append(events, delegatedAuditEvent(ctx, caller, item, docs[i]))
		}
	}
	if len(events) == 0 {
		return nil
	}
	if h.Audit == nil {
		return errAuthorizeAuditNoSink
	}
	return h.Audit(ctx, events)
}

// authorizeAuditEvent is the row of one high-risk decision: ids and closed
// values only. The actor is the user the decision is about; the calling
// policy-enforcement point is in the caller attributes.
func authorizeAuditEvent(ctx context.Context, caller AuthorizeCaller, item authorizeItem, doc decisionDocument) audit.AuditEvent {
	q := item.question
	actor := item.principal.ID.String()
	revision := int64(item.decision.PolicyRevision)
	outcome := audit.OutcomeDenied
	if item.decision.Allowed {
		outcome = audit.OutcomeAllowed
	}
	event := audit.AuditEvent{
		EventType: audit.EventAuthorizeHighRiskDecided, Outcome: outcome, ActorPrincipalID: &actor,
		PolicyRevision: &revision, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{
			"decision_id": doc.DecisionID, "decision_reason_code": doc.ReasonCode, "action_key": q.Action,
			"resource_type": q.ResourceType, "policy_revision": revision,
		},
	}
	if item.orgKnown {
		org := q.OrganizationID
		event.OrganizationID = &org
	}
	if q.ResourceType == "repository" {
		event.Attributes["resource_id"] = q.ResourceKey
	}
	if caller.Principal != uuid.Nil {
		principal := caller.Principal
		audit.Caller{Workload: &principal, JTI: caller.TokenID}.Annotate(event.Attributes)
	}
	return event
}

// delegatedAuditEvent is the row of one delegated decision: the high-risk
// row's ids, with the real actor as actor, the target user as subject and the
// delegated session as target_kind/target_id. A row that names no actor has no
// actor column.
func delegatedAuditEvent(ctx context.Context, caller AuthorizeCaller, item authorizeItem, doc decisionDocument) audit.AuditEvent {
	event := authorizeAuditEvent(ctx, caller, item, doc)
	event.EventType = audit.EventAuthorizeDelegatedDecided
	event.ActorPrincipalID = nil
	if item.actor != uuid.Nil {
		actor := item.actor.String()
		event.ActorPrincipalID = &actor
	}
	target := item.principal.ID.String()
	event.SubjectPrincipalID = &target
	event.Attributes["target_kind"] = "session"
	event.Attributes["target_id"] = item.session.String()
	return event
}
