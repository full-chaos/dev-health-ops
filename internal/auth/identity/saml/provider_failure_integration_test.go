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

func auditRowsOf(t *testing.T, h *harness, resourceID string) []string {
	t.Helper()
	rows, err := h.db.admin.Query(context.Background(), `SELECT concat_ws('|', org_id::text, coalesce(user_id::text, '<null>'), action,
	resource_type, coalesce(description, '<null>'), coalesce(changes::text, '<null>'), coalesce(request_metadata::text, '<null>'),
	status, coalesce(error_message, '<null>'), to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US'))
FROM public.audit_logs WHERE resource_id = $1 ORDER BY created_at, id`, resourceID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// An assertion without an email on the real database: the sign-in is refused as before (ErrEmailMissing), the
// provider row records go-api's failure text and time and keeps its status, exactly one public.audit_logs row is
// written (the row go-api's own writer writes for recordSSOFailure, protocol saml, no stage), and the next sign-in
// with an email works.
func TestSAMLAssertionWithoutEmailRecordsTheFailureAndKeepsTheProviderActive(t *testing.T) {
	h := newHarness(t, true)
	now := time.Date(2026, 10, 30, 12, 0, 0, 123456000, time.UTC)
	h.svc.ProviderFailure = ssoaudit.PGProviderFailure{DB: h.db.runtime, Now: func() time.Time { return now }}
	const message = "SAML assertion missing email attribute"
	_, err := h.consume(h.respond(resp{nameID: nid("unlinked-" + uuid.NewString())}), nil)
	if !errors.Is(err, saml.ErrEmailMissing) {
		t.Fatalf("err = %v, want ErrEmailMissing", err)
	}
	var status, lastError string
	var atNow bool
	if err := h.db.admin.QueryRow(context.Background(), `SELECT status, coalesce(last_error, '<null>'),
	coalesce(last_error_at = $2 AND updated_at = $2, false) FROM public.sso_providers WHERE id = $1`, h.provider.ID, now).Scan(&status, &lastError, &atNow); err != nil {
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
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (audit.PGWriter{Now: func() time.Time { return now }}).Write(context.Background(), h.db.admin, audit.Entry{
		OrgID: h.provider.OrgID, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider, ResourceID: marker,
		Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		t.Fatal(err)
	}
	want := auditRowsOf(t, h, marker)
	if len(got) != 1 || len(want) != 1 || got[0] != want[0] {
		t.Fatalf("rows %q\ngo-api %q", got, want)
	}
	if _, err := h.consume(h.respond(resp{nameID: nid("next-" + uuid.NewString()), email: uniqueEmail("next")}), nil); errors.Is(err, saml.ErrProviderInactive) {
		t.Fatalf("the next sign-in through the provider: %v", err)
	}
}
