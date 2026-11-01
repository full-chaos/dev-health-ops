//go:build integration

package saml_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

var recordedAt = time.Date(2026, 10, 30, 12, 0, 0, 123456000, time.UTC)

func withRecord(h *harness) {
	h.svc.ProviderFailure = ssoaudit.PGProviderFailure{DB: h.db.runtime, Now: func() time.Time { return recordedAt }}
}

// wantRecord checks the provider row (text, time, status kept) and that count audit rows were written, each the row
// go-api's own writer writes for recordSSOFailure.
func wantRecord(t *testing.T, h *harness, stage, message string, count int) {
	t.Helper()
	var status, lastError string
	var atNow bool
	if err := h.db.admin.QueryRow(context.Background(), `SELECT status, coalesce(last_error, '<null>'),
	coalesce(last_error_at = $2 AND updated_at = $2, false) FROM public.sso_providers WHERE id = $1`, h.provider.ID, recordedAt).Scan(&status, &lastError, &atNow); err != nil {
		t.Fatal(err)
	}
	if status != "active" || lastError != message || !atNow {
		t.Fatalf("provider status %q last_error %q at now %v, want active, %q, true", status, lastError, atNow, message)
	}
	got := auditRowsOf(t, h, h.provider.ID.String())
	marker := "go-api-" + uuid.NewString()
	sanitized := pythonparity.SanitizeErrorTextHardened(message, 4000)
	meta := pyjson.NewObject()
	meta.Set("protocol", "saml")
	if stage != "" {
		meta.Set("stage", stage)
	}
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (audit.PGWriter{Now: func() time.Time { return recordedAt }}).Write(context.Background(), h.db.admin, audit.Entry{
		OrgID: h.provider.OrgID, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider, ResourceID: marker,
		Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		t.Fatal(err)
	}
	want := auditRowsOf(t, h, marker)
	if len(got) != count || len(want) != 1 {
		t.Fatalf("%d rows, want %d: %q", len(got), count, got)
	}
	for _, row := range got {
		if row != want[0] {
			t.Fatalf("row %q\ngo-api %q", row, want[0])
		}
	}
}

// signsIn proves the provider still serves a linked account.
func signsIn(t *testing.T, h *harness) {
	t.Helper()
	user := h.linkedUser(uniqueEmail("u"), "user-after")
	signIn, err := h.consume(h.respond(resp{nameID: nid("user-after"), email: uniqueEmail("after")}), nil)
	if err != nil || signIn.PrincipalID != user {
		t.Fatalf("sign-in %+v err %v, want the linked account", signIn, err)
	}
}

func setProviderConfig(t *testing.T, h *harness, expression string) {
	t.Helper()
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET config = `+expression+` WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
}

// A provider with no usable certificate: a post with any base64 body and no flow reaches the check, as in go-api
// (saml.go:541-552). Each post is one record: one provider write, one audit row; the status stays. Once the
// administrator fixes the row, the provider signs in.
func TestSAMLCertificateFailuresRecordOncePerPostAndKeepTheProviderActive(t *testing.T) {
	for name, tc := range map[string]struct{ break_, message string }{
		"no certificate":             {`(config::jsonb - 'certificate')::json`, "SAML certificate is required for validation"},
		"certificate does not parse": {`jsonb_set(config::jsonb, '{certificate}', '"not a certificate"')::json`, "SAML signature validation failed"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, true)
			withRecord(h)
			var saved string
			if err := h.db.admin.QueryRow(context.Background(), `SELECT config::text FROM public.sso_providers WHERE id = $1`, h.provider.ID).Scan(&saved); err != nil {
				t.Fatal(err)
			}
			setProviderConfig(t, h, tc.break_)
			for attempt := 1; attempt <= 2; attempt++ {
				if _, err := h.consume("YW55", nil); err == nil {
					t.Fatal("the post was not refused")
				}
				wantRecord(t, h, "", tc.message, attempt)
			}
			if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET config = $2::json WHERE id = $1`, h.provider.ID, saved); err != nil {
				t.Fatal(err)
			}
			signsIn(t, h)
		})
	}
}

// A signed assertion with no SubjectConfirmation is go-api's record; the same assertion unsigned records nothing.
func TestSAMLNoSubjectConfirmationRecordsOnlyWhenSigned(t *testing.T) {
	h := newHarness(t, true)
	withRecord(h)
	if _, err := h.consume(h.respond(resp{nameID: nid("unlinked-" + uuid.NewString()), email: uniqueEmail("x"), noConfirmation: true, unsignedAssertion: true}), nil); err == nil {
		t.Fatal("the unsigned assertion was not refused")
	}
	if got := auditRowsOf(t, h, h.provider.ID.String()); len(got) != 0 {
		t.Fatalf("audit rows %q, want none", got)
	}
	if _, err := h.consume(h.respond(resp{nameID: nid("unlinked-" + uuid.NewString()), email: uniqueEmail("x"), noConfirmation: true}), nil); err == nil {
		t.Fatal("the assertion was not refused")
	}
	wantRecord(t, h, "", "SAML subject confirmation missing", 1)
	signsIn(t, h)
}

// Auto-provisioning off and a new account: go-api's provisioning record; the refusal stays.
func TestSAMLAutoProvisionOffRecordsTheProvisioningStage(t *testing.T) {
	h := newHarness(t, true)
	withRecord(h)
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET auto_provision_users = false WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	_, err := h.consume(h.respond(resp{nameID: nid("unlinked-" + uuid.NewString()), email: uniqueEmail("new")}), nil)
	if !errors.Is(err, saml.ErrAutoProvisionDisabled) {
		t.Fatalf("err = %v, want ErrAutoProvisionDisabled", err)
	}
	wantRecord(t, h, "provisioning", "User not found and auto-provisioning disabled", 1)
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET auto_provision_users = true WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	signsIn(t, h)
}
