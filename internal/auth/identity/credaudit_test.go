package identity

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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
		e := provisionAudit(subject, actor, true)
		if e.OrganizationID != nil {
			t.Errorf("organization = %v, want none: auth.organizations does not hold the new organization", *e.OrganizationID)
		}
		if got, want := changes(e), []string{"email", "organization_id"}; !reflect.DeepEqual(got, want) {
			t.Errorf("changed_fields = %v, want %v", got, want)
		}
	})
	t.Run("a registration with no organization", func(t *testing.T) {
		e := provisionAudit(subject, actor, false)
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
		"provision":       provisionAudit(subject, actor, true),
		"provision, none": provisionAudit(subject, actor, false),
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
		provisionAudit(subject, SelfSignUp(nil), false),
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

type sqlRecorder struct{ sqls []string }

type noRow struct{}

func (noRow) Scan(...any) error { return pgx.ErrNoRows }

func (r *sqlRecorder) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.sqls = append(r.sqls, sql)
	return noRow{}
}

// The auth runtime role reads only the auth schema's tables: it holds no SELECT on public.memberships.
func TestAuditOrganizationReadsOnlyTheAuthSchema(t *testing.T) {
	rec := &sqlRecorder{}
	org, err := AuditOrganization(context.Background(), rec, `"auth"`, uuid.New())
	if err != nil || org != nil {
		t.Fatalf("AuditOrganization = %v, %v; want nil, nil for a principal with no membership", org, err)
	}
	if len(rec.sqls) != 1 {
		t.Fatalf("statements = %d, want 1", len(rec.sqls))
	}
	sql := rec.sqls[0]
	if strings.Contains(sql, "public.") {
		t.Errorf("the lookup reads a public table: %s", sql)
	}
	for _, want := range []string{`"auth".memberships`, `"auth".organizations`, "ORDER BY COALESCE(m.joined_at, m.created_at), m.joined_at NULLS LAST, m.created_at, m.id", "m.status <> 'removed'"} {
		if !strings.Contains(sql, want) {
			t.Errorf("the lookup lacks %q: %s", want, sql)
		}
	}
}
