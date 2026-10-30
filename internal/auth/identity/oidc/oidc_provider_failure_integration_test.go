//go:build integration

package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// callbackWithCode runs the callback of a started flow with a code the provider never granted.
func (h *harness) callbackWithCode(s started, code string) *Error {
	_, _, err := h.svc.Callback(context.Background(), h.providerID, CallbackInput{Code: code, State: s.state, RequestID: &h.requestID,
		Cookie: func(name string) (string, bool) { return s.cookie.Value, name == s.cookie.Name }})
	var failure *Error
	errors.As(err, &failure)
	return failure
}

// A real OIDC callback per failure class on the real database: the answer, the provider row (text, time, status kept),
// exactly one audit_logs row, the row go-api's own writer writes (the resource id aside), and the next sign-in of an
// existing member through the same provider ends in a session.
func TestOIDCProviderFailuresRecordGoAPIsRowAndKeepTheProviderActive(t *testing.T) {
	const memberSub = "sub-next"
	for name, tc := range map[string]struct {
		opts    harnessOpts
		before  func(h *harness)
		run     func(t *testing.T, h *harness, s started) *Error
		after   func(h *harness)
		status  int
		reason  Reason
		stage   string
		message string
	}{
		"token exchange": {run: func(t *testing.T, h *harness, s started) *Error { return h.callbackWithCode(s, "code-never-granted") },
			status: http.StatusBadRequest, reason: ReasonDependency, message: "OIDC token exchange failed"},
		"discovery fetch": {opts: harnessOpts{discovery: true},
			before: func(h *harness) { h.idp.failDiscovery = true },
			run:    func(t *testing.T, h *harness, s started) *Error { return h.callbackWithCode(s, "code-1") },
			after:  func(h *harness) { h.idp.failDiscovery = false },
			status: http.StatusBadRequest, reason: ReasonDependency, message: "Failed to fetch OIDC discovery document"},
		"no id token": {run: func(t *testing.T, h *harness, s started) *Error { _, f := h.finish(t, s, ""); return f },
			status: http.StatusBadRequest, reason: ReasonMalformed, message: "OIDC token response missing id_token"},
		"id token signature": {run: func(t *testing.T, h *harness, s started) *Error {
			other, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			_, f := h.finish(t, s, signRS256(t, other, map[string]any{"alg": "RS256", "kid": h.idp.kid, "typ": "JWT"}, h.claims(s.nonce, "sub-1", uniqueEmail("sig"))))
			return f
		}, status: http.StatusBadRequest, reason: ReasonBadSignature, message: "OIDC id_token validation failed"},
		"nonce": {run: func(t *testing.T, h *harness, s started) *Error {
			_, f := h.finish(t, s, h.sign(t, h.claims("another-nonce", "sub-1", uniqueEmail("nonce"))))
			return f
		}, status: http.StatusBadRequest, reason: ReasonNonceMismatch, message: "OIDC nonce mismatch"},
		"userinfo": {opts: harnessOpts{userinfo: true},
			before: func(h *harness) { h.idp.failUserinfo = true },
			run: func(t *testing.T, h *harness, s started) *Error {
				_, f := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-1", uniqueEmail("ui"))))
				return f
			},
			after:  func(h *harness) { h.idp.failUserinfo, h.idp.userinfo = false, map[string]any{"sub": memberSub} },
			status: http.StatusBadRequest, reason: ReasonDependency, message: "OIDC userinfo request failed"},
		"no email": {run: func(t *testing.T, h *harness, s started) *Error {
			_, f := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-1", "")))
			return f
		}, status: http.StatusBadRequest, reason: ReasonMalformed, message: "OIDC claims missing email"},
		"auto-provisioning off": {opts: harnessOpts{noAutoProvision: true}, run: func(t *testing.T, h *harness, s started) *Error {
			_, f := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-new", uniqueEmail("new"))))
			return f
		}, status: http.StatusBadRequest, reason: ReasonProvisioningDisabled, stage: "provisioning", message: "User not found and auto-provisioning disabled"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, tc.opts)
			before := h.snapshot(t)
			// Authorize reads the discovery document too, so the provider breaks after the flow started.
			flow := h.start(t)
			if tc.before != nil {
				h.idp.mu.Lock()
				tc.before(h)
				h.idp.mu.Unlock()
			}
			failure := tc.run(t, h, flow)
			wantDenied(t, failure, tc.status, tc.reason)
			wantUnchanged(t, h, before)
			if got := h.providerRecord(t, h.providerID); got != (providerRecord{status: "active", lastError: tc.message, atNow: true}) {
				t.Fatalf("provider %+v, want active with last_error %q at now", got, tc.message)
			}
			got := h.auditRows(t, `resource_id = $1`, h.providerID.String())
			if len(got) != 1 {
				t.Fatalf("audit rows %+v, want exactly one", got)
			}
			want := h.goAPIFailureRow(t, h.orgID, "oidc", tc.stage, tc.message)
			want.resourceID = h.providerID.String()
			if got[0] != want {
				t.Fatalf("row %+v\ngo-api %+v", got[0], want)
			}
			if tc.after != nil {
				h.idp.mu.Lock()
				tc.after(h)
				h.idp.mu.Unlock()
			}
			email := uniqueEmail("next")
			h.seedOIDCMember(t, memberSub, email)
			s := h.start(t)
			if _, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, memberSub, email))); failure != nil {
				t.Fatalf("the next sign-in through the provider: %+v", failure)
			}
		})
	}
}

// Checks go-api does not make record nothing: the provider row and audit_logs stay as they were.
func TestOIDCRefusalsGoAPIDoesNotRecordWriteNothing(t *testing.T) {
	other := "https://untrusted.example"
	for name, tc := range map[string]struct {
		opts   harnessOpts
		before func(h *harness)
		claims func(c map[string]any)
		reason Reason
	}{
		"azp differs":              {claims: func(c map[string]any) { c["azp"] = "another-client" }, reason: ReasonAudience},
		"userinfo subject differs": {opts: harnessOpts{userinfo: true}, before: func(h *harness) { h.idp.userinfo = map[string]any{"sub": "another-subject"} }, reason: ReasonMalformed},
		"document issuer differs":  {opts: harnessOpts{discovery: true}, before: func(h *harness) { h.idp.docIssuer = &other }, reason: ReasonDependency},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, tc.opts)
			s := h.start(t)
			if tc.before != nil {
				h.idp.mu.Lock()
				tc.before(h)
				h.idp.mu.Unlock()
			}
			claims := h.claims(s.nonce, "sub-1", uniqueEmail("own"))
			if tc.claims != nil {
				tc.claims(claims)
			}
			_, failure := h.finish(t, s, h.sign(t, claims))
			wantDenied(t, failure, http.StatusBadRequest, tc.reason)
			if got := h.providerRecord(t, h.providerID); got.status != "active" || got.lastError != "<null>" {
				t.Fatalf("provider %+v, want untouched", got)
			}
			if got := h.auditRows(t, `resource_id = $1`, h.providerID.String()); len(got) != 0 {
				t.Fatalf("audit rows %+v, want none", got)
			}
		})
	}
}

// Without the UPDATE privilege an OIDC provider failure is go-api's 500 and nothing is written.
func TestOIDCProviderFailureRecordWriteFailureIsThe500(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	var role string
	if err := h.env.runtime.QueryRow(context.Background(), `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	quoted := `"` + strings.ReplaceAll(role, `"`, `""`) + `"`
	if _, err := h.env.admin.Exec(context.Background(), `REVOKE UPDATE ON public.sso_providers FROM `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := h.env.admin.Exec(context.Background(), `GRANT UPDATE (`+strings.Join(authschema.SSOProviderFailureColumns(), ", ")+`) ON public.sso_providers TO `+quoted); err != nil {
			t.Errorf("restore the provider failure grant: %v", err)
		}
	})
	failure := h.callbackWithCode(h.start(t), "code-"+uuid.NewString())
	if failure == nil || failure.Status != http.StatusInternalServerError || failure.Reason != ReasonStoreUnavailable {
		t.Fatalf("failure %+v, want 500 store_unavailable", failure)
	}
	if got := h.auditRows(t, `resource_id = $1`, h.providerID.String()); len(got) != 0 {
		t.Fatalf("audit rows %+v, want none", got)
	}
	if got := h.providerRecord(t, h.providerID); got.lastError != "<null>" || got.status != "active" {
		t.Fatalf("provider %+v, want untouched", got)
	}
}
