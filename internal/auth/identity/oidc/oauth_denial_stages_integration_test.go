//go:build integration

package oidc

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// goAPIStageRow writes, through go-api's own writer and as recordSSOAuditOnly builds it, go-api's row for an OAuth
// refusal of stage with message, under a marker resource id, and reads it back.
func (h *harness) goAPIStageRow(t *testing.T, orgID uuid.UUID, stage, message string) auditRow {
	t.Helper()
	marker := "go-api-" + uuid.NewString()
	sanitized := pythonparity.SanitizeErrorTextHardened(message, 4000)
	meta := pyjson.NewObject()
	meta.Set("protocol", "oauth")
	meta.Set("stage", stage)
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (audit.PGWriter{Now: func() time.Time { return h.now }}).Write(context.Background(), h.env.admin, audit.Entry{
		OrgID: orgID, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider, ResourceID: marker,
		Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		t.Fatal(err)
	}
	rows := h.auditRows(t, `resource_id = $1`, marker)
	if len(rows) != 1 {
		t.Fatalf("go-api rows %+v", rows)
	}
	return rows[0]
}

// One real OAuth callback per audited stage on the real database: the refusal answers as before, with exactly one
// public.audit_logs row, the row go-api's own writer writes for the same refusal (the resource id aside), and
// nothing else written.
func TestOAuthRefusalStagesWriteGoAPIsRows(t *testing.T) {
	for name, tc := range map[string]struct {
		setup   func(h *harness, provider uuid.UUID)
		account func() fakeAccount
		cookie  bool
		status  int
		reason  Reason
		stage   string
		message string
	}{
		"state_auth: no login cookie": {nil, func() fakeAccount { return fakeAccountWith(uniqueEmail("s"), true) }, false,
			http.StatusBadRequest, ReasonMalformed, "state_auth", "OAuth state mismatch"},
		"email_verification": {nil, func() fakeAccount { return fakeAccountWith(uniqueEmail("u"), false) }, true,
			http.StatusForbidden, ReasonEmailUnverified, "email_verification", "no verified email address is available from this OAuth provider"},
		"provisioning: not a member": {nil, func() fakeAccount { return fakeAccountWith(uniqueEmail("n"), true) }, true,
			http.StatusForbidden, ReasonMembershipRequired, "provisioning", "membership required"},
		"provisioning: auto-provisioning off": {func(h *harness, provider uuid.UUID) {
			if _, err := h.env.admin.Exec(context.Background(), `UPDATE public.sso_providers SET auto_provision_users = false WHERE id = $1`, provider); err != nil {
				panic(err)
			}
		}, func() fakeAccount { return fakeAccountWith(uniqueEmail("a"), true) }, true,
			http.StatusForbidden, ReasonProvisioningDisabled, "provisioning", "user not found and auto-provisioning is disabled"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: false})
			provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
			if tc.setup != nil {
				tc.setup(h, provider)
			}
			before := h.snapshot(t)
			_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), tc.account(), tc.cookie)
			wantDenied(t, failure, tc.status, tc.reason)
			wantUnchanged(t, h, before)
			got := h.auditRows(t, `resource_id = $1`, provider.String())
			if len(got) != 1 {
				t.Fatalf("audit rows %+v, want exactly one", got)
			}
			want := h.goAPIStageRow(t, h.orgID, tc.stage, tc.message)
			want.resourceID = provider.String()
			if got[0] != want {
				t.Fatalf("row %+v\ngo-api %+v", got[0], want)
			}
		})
	}
}

// A new account whose email another account already holds: go-api's identity conflict (provision.go) is the
// provisioning row "identity conflict"; the 409 answers as before and nothing else is written.
func TestOAuthEmailCollisionWritesGoAPIsIdentityConflictRow(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	email := uniqueEmail("held")
	h.seedPublicUser(t, email, "local", "")
	before := h.snapshot(t)
	_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(email, true), true)
	wantDenied(t, failure, http.StatusConflict, ReasonEmailCollision)
	wantUnchanged(t, h, before)
	got := h.auditRows(t, `resource_id = $1`, provider.String())
	if len(got) != 1 {
		t.Fatalf("audit rows %+v, want exactly one", got)
	}
	want := h.goAPIStageRow(t, h.orgID, "provisioning", "identity conflict")
	want.resourceID = provider.String()
	if got[0] != want {
		t.Fatalf("row %+v\ngo-api %+v", got[0], want)
	}
}
