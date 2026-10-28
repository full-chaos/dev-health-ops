package identity

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/authrows"
)

func TestCredentialAuditEventsCarryTheOrganizationAndAllowlistedChanges(t *testing.T) {
	subject := uuid.NewString()
	org := uuid.NewString()
	actor := SelfSignUp(nil)
	changes := func(e audit.AuditEvent) []string {
		fields, _ := e.Attributes["changed_fields"].([]string)
		return fields
	}

	t.Run("a registration that creates an organization", func(t *testing.T) {
		e := provisionAudit(subject, actor, &org)
		if e.OrganizationID == nil || *e.OrganizationID != org {
			t.Errorf("organization = %v, want %s", e.OrganizationID, org)
		}
		if got, want := changes(e), []string{"email", "organization_id"}; !reflect.DeepEqual(got, want) {
			t.Errorf("changed_fields = %v, want %v", got, want)
		}
	})
	t.Run("a registration with no organization", func(t *testing.T) {
		e := provisionAudit(subject, actor, nil)
		if e.OrganizationID != nil {
			t.Errorf("organization = %v, want none", *e.OrganizationID)
		}
		if got, want := changes(e), []string{"email"}; !reflect.DeepEqual(got, want) {
			t.Errorf("changed_fields = %v, want %v", got, want)
		}
	})
	t.Run("an e-mail verification", func(t *testing.T) {
		e := verifiedAudit(subject, actor, &org)
		if e.EventType != "auth.user.email_verified" || e.OrganizationID == nil || *e.OrganizationID != org {
			t.Errorf("event %s organization %v", e.EventType, e.OrganizationID)
		}
	})
	for name, cause := range map[string]PasswordCause{"a reset": PasswordReset, "a change": PasswordChanged} {
		t.Run("a password "+name, func(t *testing.T) {
			e := passwordAudit(cause, subject, actor, &org)
			if e.OrganizationID == nil || *e.OrganizationID != org {
				t.Errorf("organization = %v, want %s", e.OrganizationID, org)
			}
		})
	}
}

func TestCredentialAuditEventsAreValidAndHoldNoAddress(t *testing.T) {
	subject := uuid.NewString()
	org := uuid.NewString()
	actor := SelfSignUp(nil)
	events := map[string]audit.AuditEvent{
		"provision":       provisionAudit(subject, actor, &org),
		"provision, none": provisionAudit(subject, actor, nil),
		"verified":        verifiedAudit(subject, actor, &org),
		"reset":           passwordAudit(PasswordReset, subject, actor, &org),
		"change":          passwordAudit(PasswordChanged, subject, actor, nil),
	}
	for name, e := range events {
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

func TestEveryCredentialAuditEventIsMappedByTheCorpus(t *testing.T) {
	mapped := authrows.CredentialAuditEvents()
	subject := uuid.NewString()
	for _, e := range []audit.AuditEvent{
		provisionAudit(subject, SelfSignUp(nil), nil),
		verifiedAudit(subject, SelfSignUp(nil), nil),
		passwordAudit(PasswordReset, subject, SelfSignUp(nil), nil),
		passwordAudit(PasswordChanged, subject, SelfSignUp(nil), nil),
	} {
		if _, ok := mapped[e.EventType]; !ok {
			t.Errorf("%s has no golden audit mapping in authrows", e.EventType)
		}
	}
	if _, ok := mapped[audit.EventPasswordResetRequested]; !ok {
		t.Errorf("%s has no golden audit mapping in authrows", audit.EventPasswordResetRequested)
	}
	if got := mapped[audit.EventPasswordResetRequested]; got != (authrows.AuditMapping{Action: "password_reset_requested", ResourceType: "user", Description: "Password reset requested"}) {
		t.Errorf("reset-requested mapping = %+v", got)
	}
}
