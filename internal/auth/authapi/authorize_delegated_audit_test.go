package authapi

import (
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

// delegatedDecidedEvent is the event type of the row of every delegated decision.
const delegatedDecidedEvent = "auth.authorize.delegated_decided"

// delegatedRows returns the auth.authorize.delegated_decided rows the sink saw.
func (f *fakeAudit) delegatedRows() []audit.AuditEvent {
	var out []audit.AuditEvent
	for _, e := range f.rows() {
		if e.EventType == delegatedDecidedEvent {
			out = append(out, e)
		}
	}
	return out
}

// checkDelegatedRow checks the ids of one delegated row: the real actor, the target as subject, the organization, the
// delegated session as target_kind/target_id, and the decision.
func checkDelegatedRow(t *testing.T, h *authorizeHandlerFixture, e audit.AuditEvent, d wireDecision, action string, outcome audit.Outcome) {
	t.Helper()
	if e.Outcome != outcome {
		t.Errorf("outcome %s, want %s", e.Outcome, outcome)
	}
	if e.ActorPrincipalID == nil || *e.ActorPrincipalID != delegatedActor.String() {
		t.Errorf("actor %v, want the real actor %s", e.ActorPrincipalID, delegatedActor)
	}
	if e.SubjectPrincipalID == nil || *e.SubjectPrincipalID != h.principal.String() {
		t.Errorf("subject %v, want the target %s", e.SubjectPrincipalID, h.principal)
	}
	if e.OrganizationID == nil || *e.OrganizationID != h.org {
		t.Errorf("organization %v, want %s", e.OrganizationID, h.org)
	}
	if attrString(t, e, "action_key") != action || attrString(t, e, "decision_id") != d.DecisionID ||
		attrString(t, e, "decision_reason_code") != d.ReasonCode {
		t.Errorf("attributes %v, decision %+v", e.Attributes, d)
	}
	if attrString(t, e, "target_kind") != "session" || attrString(t, e, "target_id") != h.sid.String() {
		t.Errorf("session attributes %v, want target_kind session and target_id %s", e.Attributes, h.sid)
	}
	if err := audit.ValidateAttributes(e.Attributes); err != nil {
		t.Error(err)
	}
	auditwire.MustValidate(t, e)
}

// A delegated allow of a low-risk read writes one delegated row with the actor and the target.
func TestAuthorizeDelegatedReadAllowWritesOneDelegatedRow(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("teams.read")
	d := h.decideOne(h.org1("teams.read"))
	if !d.Allowed {
		t.Fatalf("%+v", d)
	}
	rows := h.audit.rows()
	if len(rows) != 1 || len(h.audit.calls) != 1 || rows[0].EventType != delegatedDecidedEvent {
		t.Fatalf("rows %+v in %d calls, want one delegated row in one call", rows, len(h.audit.calls))
	}
	checkDelegatedRow(t, h, rows[0], d, "teams.read", audit.OutcomeAllowed)
}

// A delegated deny writes the delegated row too, with the deny's reason.
func TestAuthorizeDelegatedDenyWritesOneDelegatedRow(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("teams.read")
	d := h.decideOne(h.org1("analytics.read"))
	if d.Allowed || d.ReasonCode != "impersonation_denied" {
		t.Fatalf("%+v, want an impersonation_denied deny", d)
	}
	rows := h.audit.delegatedRows()
	if len(rows) != 1 {
		t.Fatalf("%d delegated rows, want 1", len(rows))
	}
	checkDelegatedRow(t, h, rows[0], d, "analytics.read", audit.OutcomeDenied)
}

// A delegated high-risk decision keeps its high-risk row and adds the delegated row, in one call.
func TestAuthorizeDelegatedHighRiskWritesBothRows(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("teams.read", "teams.write")
	d := h.decideOne(h.org1("teams.write"))
	var types []string
	for _, e := range h.audit.rows() {
		types = append(types, e.EventType)
	}
	slices.Sort(types)
	want := []string{delegatedDecidedEvent, audit.EventAuthorizeHighRiskDecided}
	slices.Sort(want)
	if !slices.Equal(types, want) || len(h.audit.calls) != 1 {
		t.Fatalf("rows %v in %d calls, want %v in one call", types, len(h.audit.calls), want)
	}
	outcome := audit.OutcomeDenied
	if d.Allowed {
		outcome = audit.OutcomeAllowed
	}
	checkDelegatedRow(t, h, h.audit.delegatedRows()[0], d, "teams.write", outcome)
}

// A delegated row with no actor (an incomplete row: delegation_invalid) still writes the row, with no actor.
func TestAuthorizeDelegatedRowWithNoActorWritesTheRowWithNoActor(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	org := uuid.MustParse(h.org)
	h.setSession(DecisionSession{Found: true, Live: true, PrincipalID: h.principal, HasDelegatedFrom: true, OrganizationID: &org,
		AMR: []string{"pwd"}, PermittedActions: []string{"teams.read"}, BaseLive: true})
	d := h.decideOne(h.org1("teams.read"))
	if d.Allowed || d.ReasonCode != "delegation_invalid" {
		t.Fatalf("%+v, want a delegation_invalid deny", d)
	}
	rows := h.audit.delegatedRows()
	if len(rows) != 1 {
		t.Fatalf("%d delegated rows, want 1", len(rows))
	}
	if rows[0].ActorPrincipalID != nil || rows[0].SubjectPrincipalID == nil || *rows[0].SubjectPrincipalID != h.principal.String() {
		t.Errorf("actor %v subject %v, want no actor and the target as subject", rows[0].ActorPrincipalID, rows[0].SubjectPrincipalID)
	}
}

// A plain session's low-risk decision still writes no row.
func TestAuthorizePlainLowRiskStillWritesNoRow(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	if rec := h.one(h.org1("teams.read")); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if n := len(h.audit.calls); n != 0 {
		t.Fatalf("%d audit calls, want 0", n)
	}
}

// A delegated decision whose row cannot be written is not served: 503 store_unavailable.
func TestAuthorizeDelegatedAuditFailureIs503(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("teams.read")
	h.audit.err = errors.New("audit store down")
	assertError(t, h.one(h.org1("teams.read")), http.StatusServiceUnavailable, reasonStoreUnavailable)
	if len(h.audit.calls) != 1 {
		t.Fatalf("%d audit calls, want 1", len(h.audit.calls))
	}
}
