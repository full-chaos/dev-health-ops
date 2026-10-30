//go:build integration

package oidc

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type providerRecord struct {
	status, lastError string
	atNow             bool
}

func (h *harness) providerRecord(t *testing.T, provider uuid.UUID) providerRecord {
	t.Helper()
	var r providerRecord
	if err := h.env.admin.QueryRow(context.Background(), `SELECT status, coalesce(last_error, '<null>'),
	coalesce(last_error_at = $2 AND updated_at = $2, false) FROM public.sso_providers WHERE id = $1`,
		provider, h.now.UTC()).Scan(&r.status, &r.lastError, &r.atNow); err != nil {
		t.Fatal(err)
	}
	return r
}

// goAPIFailureRow writes, through go-api's own writer and as recordSSOFailure builds it (protocol, the stage if any), go-api's
// row for a provider failure with message, under a marker resource id, and reads it back.
func (h *harness) goAPIFailureRow(t *testing.T, orgID uuid.UUID, protocol, stage, message string) auditRow {
	t.Helper()
	marker := "go-api-" + uuid.NewString()
	sanitized := pythonparity.SanitizeErrorTextHardened(message, 4000)
	meta := pyjson.NewObject()
	meta.Set("protocol", protocol)
	if stage != "" {
		meta.Set("stage", stage)
	}
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

// A provider error of a real OAuth callback on the real database: the answer is unchanged, the provider row records
// the failure text and time and keeps its status, exactly one public.audit_logs row is written (the row go-api's own
// writer writes for recordSSOFailure, the resource id aside), and the next sign-in through the provider works.
func TestOAuthProviderErrorsRecordTheFailureAndKeepTheProviderActive(t *testing.T) {
	for name, tc := range map[string]struct {
		fail    func(h *harness)
		code    string
		message string
	}{
		"token exchange refused": {nil, "code-never-granted", "OAuth token exchange failed"},
		"profile fetch 502":      {func(h *harness) { h.idp.oauth.failProfile = true }, "", "OAuth userinfo request failed"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: false})
			provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
			if tc.fail != nil {
				h.idp.oauth.mu.Lock()
				tc.fail(h)
				h.idp.oauth.mu.Unlock()
			}
			before := h.snapshot(t)
			started := h.startOAuth(t, provider)
			var failure *Error
			if tc.code != "" {
				_, _, err := h.svc.CallbackOAuth(context.Background(), provider, CallbackInput{Code: tc.code, State: started.state,
					RequestID: &h.requestID, Cookie: func(name string) (string, bool) {
						return started.cookie.Value, name == started.cookie.Name
					}})
				failure, _ = err.(*Error)
			} else {
				_, failure = h.finishOAuth(t, provider, client, started, fakeAccountWith(uniqueEmail("p"), true), true)
			}
			wantDenied(t, failure, http.StatusBadRequest, ReasonDependency)
			wantUnchanged(t, h, before)
			if got := h.providerRecord(t, provider); got != (providerRecord{status: "active", lastError: tc.message, atNow: true}) {
				t.Fatalf("provider %+v, want active with last_error %q at now", got, tc.message)
			}
			got := h.auditRows(t, `resource_id = $1`, provider.String())
			if len(got) != 1 {
				t.Fatalf("audit rows %+v, want exactly one", got)
			}
			want := h.goAPIFailureRow(t, h.orgID, "oauth", "", tc.message)
			want.resourceID = provider.String()
			if got[0] != want {
				t.Fatalf("row %+v\ngo-api %+v", got[0], want)
			}
			h.idp.oauth.mu.Lock()
			h.idp.oauth.failProfile = false
			h.idp.oauth.mu.Unlock()
			next := fakeAccountWith(uniqueEmail("next"), true)
			h.seedOAuthMember(t, h.orgID, "oauth_gitlab", next)
			if _, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), next, true); failure != nil {
				t.Fatalf("the next sign-in through the provider: %+v", failure)
			}
		})
	}
}

// Without the UPDATE privilege the provider error is go-api's 500 (h.fail), not the 400, and nothing is written: the
// record and its audit row are one transaction, written before the answer.
func TestOAuthProviderFailureRecordWriteFailureIsThe500(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, _ := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	var role string
	if err := h.env.runtime.QueryRow(context.Background(), `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	quoted := `"` + strings.ReplaceAll(role, `"`, `""`) + `"`
	if _, err := h.env.admin.Exec(context.Background(), `REVOKE UPDATE ON public.sso_providers FROM `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := h.env.admin.Exec(context.Background(), `GRANT UPDATE (`+strings.Join(authschema.SSOProviderWriteColumns(), ", ")+`) ON public.sso_providers TO `+quoted); err != nil {
			t.Errorf("restore the provider write grant: %v", err)
		}
	})
	started := h.startOAuth(t, provider)
	_, _, err := h.svc.CallbackOAuth(context.Background(), provider, CallbackInput{Code: "code-never-granted", State: started.state,
		RequestID: &h.requestID, Cookie: func(name string) (string, bool) { return started.cookie.Value, name == started.cookie.Name }})
	failure, _ := err.(*Error)
	if failure == nil || failure.Status != http.StatusInternalServerError || failure.Reason != ReasonStoreUnavailable {
		t.Fatalf("failure %+v, want 500 store_unavailable", failure)
	}
	if got := h.auditRows(t, `resource_id = $1`, provider.String()); len(got) != 0 {
		t.Fatalf("audit rows %+v, want none", got)
	}
	if got := h.providerRecord(t, provider); got.lastError != "<null>" || got.status != "active" {
		t.Fatalf("provider %+v, want untouched", got)
	}
}

// A token issued to another client is auth-service's own check (go-api's profile fetch makes no such check, so go-api
// records nothing there): the refusal answers as before and writes nothing to sso_providers or audit_logs.
func TestOAuthAudienceRefusalRecordsNothing(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	h.idp.oauth.mu.Lock()
	h.idp.oauth.tokenClient = "client-of-another-app"
	h.idp.oauth.mu.Unlock()
	before := h.snapshot(t)
	_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(uniqueEmail("aud"), true), true)
	wantDenied(t, failure, http.StatusBadRequest, ReasonAudience)
	wantUnchanged(t, h, before)
	if got := h.providerRecord(t, provider); got.status != "active" || got.lastError != "<null>" {
		t.Fatalf("provider %+v, want untouched", got)
	}
	if got := h.auditRows(t, `resource_id = $1`, provider.String()); len(got) != 0 {
		t.Fatalf("audit rows %+v, want none", got)
	}
}
