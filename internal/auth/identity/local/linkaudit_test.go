package local

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func TestResetRequestedEventOfAKnownPrincipal(t *testing.T) {
	principal := uuid.New()
	org := uuid.NewString()
	e := resetRequestedEvent(principal, true, &org)
	if e.EventType != audit.EventPasswordResetRequested || e.Outcome != audit.OutcomeAllowed {
		t.Fatalf("event %s outcome %s", e.EventType, e.Outcome)
	}
	if e.SubjectPrincipalID == nil || *e.SubjectPrincipalID != principal.String() || e.ActorPrincipalID == nil || *e.ActorPrincipalID != principal.String() {
		t.Errorf("subject %v actor %v, want %s for both", e.SubjectPrincipalID, e.ActorPrincipalID, principal)
	}
	if e.OrganizationID == nil || *e.OrganizationID != org {
		t.Errorf("organization = %v, want %s", e.OrganizationID, org)
	}
	if e.Attributes["reason_code"] != "password_reset_requested" || e.Attributes["target_id"] != principal.String() {
		t.Errorf("attributes = %v", e.Attributes)
	}
}

func TestResetRequestedEventOfAKnownPrincipalWithNoMembership(t *testing.T) {
	e := resetRequestedEvent(uuid.New(), true, nil)
	if e.OrganizationID != nil || e.SubjectPrincipalID == nil {
		t.Errorf("organization %v subject %v, want no organization and a subject", e.OrganizationID, e.SubjectPrincipalID)
	}
}

func TestResetRequestedEventOfAnUnknownAddressHoldsNoPrincipal(t *testing.T) {
	// The decoy's random principal id must not reach the row.
	e := resetRequestedEvent(uuid.New(), false, nil)
	if e.EventType != audit.EventPasswordResetRequested || e.Outcome != audit.OutcomeDenied {
		t.Fatalf("event %s outcome %s", e.EventType, e.Outcome)
	}
	if e.SubjectPrincipalID != nil || e.ActorPrincipalID != nil || e.OrganizationID != nil {
		t.Errorf("subject %v actor %v organization %v, want none", e.SubjectPrincipalID, e.ActorPrincipalID, e.OrganizationID)
	}
	if e.Attributes["reason_code"] != "principal_not_found" {
		t.Errorf("reason = %v", e.Attributes["reason_code"])
	}
	if _, ok := e.Attributes["target_id"]; ok {
		t.Error("an unknown address row carries a target id")
	}
}

func TestResetRequestedEventsAreValidAndHoldNoAddress(t *testing.T) {
	org := uuid.NewString()
	for name, e := range map[string]audit.AuditEvent{
		"known":   resetRequestedEvent(uuid.New(), true, &org),
		"no org":  resetRequestedEvent(uuid.New(), true, nil),
		"unknown": resetRequestedEvent(uuid.New(), false, nil),
	} {
		if err := audit.ValidateAttributes(e.Attributes); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		for key, value := range e.Attributes {
			if strings.Contains(fmt.Sprint(value), "@") {
				t.Errorf("%s: attribute %s holds an address", name, key)
			}
		}
	}
}
