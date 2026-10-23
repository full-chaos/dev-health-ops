package admin

import (
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func withProcessLicense(t *testing.T) {
	t.Helper()
	licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise", Features: map[string]bool{
		auditLogFeature: true, ipAllowlistFeature: true, retentionFeature: true,
	}})
	t.Cleanup(func() { licensing.SetProcessLicense(nil) })
}

func withOrgClaim(req *http.Request) *http.Request {
	user := *policy.UserFrom(req.Context())
	user.OrgID = validUUID
	return req.WithContext(policy.WithUser(req.Context(), &user))
}

func withQuery(raw string) func(*http.Request) *http.Request {
	return func(req *http.Request) *http.Request {
		req.URL.RawQuery = raw
		return req
	}
}

// TestMalformedAdminIDsAreNamed400 pins a malformed id of a route's own input
// (path value, create body, or filter query) answering a fixed named 400
// instead of the generic 500. The handlers run with no store: the id check
// must answer before any store call.
func TestMalformedAdminIDsAreNamed400(t *testing.T) {
	withProcessLicense(t)
	h := &handlers{}
	empty := withBody(`{}`)
	cases := []struct {
		name    string
		handler http.HandlerFunc
		method  string
		setup   func(*http.Request) *http.Request
		detail  string
	}{
		{"flag patch", h.updateFeatureFlag, http.MethodPatch, compose(empty, pathValues("flag_id", "nope")), "Invalid feature flag id"},
		{"override create feature_id", h.createFeatureOverride, http.MethodPost,
			compose(withBody(`{"feature_id":"nope"}`), pathValues("org_id", validUUID)), "Invalid feature id"},
		{"audit get", h.getAuditLog, http.MethodGet, pathValues("log_id", "nope"), "Invalid audit log id"},
		{"audit user activity", h.getUserAuditActivity, http.MethodGet, pathValues("user_id", "nope"), "Invalid user id"},
		{"audit list user_id", h.listAuditLogs, http.MethodGet, withQuery("user_id=not-a-uuid"), "Invalid user id"},
		{"platform audit list user_id", h.listPlatformAuditLogs, http.MethodGet, withQuery("user_id=zzz"), "Invalid user id"},
		{"ip get", h.getIPAllowlistEntry, http.MethodGet, pathValues("entry_id", "xyz"), "Invalid IP allowlist entry id"},
		{"ip get check", h.getIPAllowlistEntry, http.MethodGet, pathValues("entry_id", "check"), "Invalid IP allowlist entry id"},
		{"ip delete", h.deleteIPAllowlistEntry, http.MethodDelete, pathValues("entry_id", "xyz"), "Invalid IP allowlist entry id"},
		{"retention get", h.getRetentionPolicy, http.MethodGet, pathValues("policy_id", "nope"), "Invalid retention policy id"},
		{"retention delete", h.deleteRetentionPolicy, http.MethodDelete, pathValues("policy_id", "nope"), "Invalid retention policy id"},
		{"retention delete resource-types", h.deleteRetentionPolicy, http.MethodDelete, pathValues("policy_id", "resource-types"), "Invalid retention policy id"},
		{"retention execute", h.executeRetentionPolicy, http.MethodPost, compose(empty, pathValues("policy_id", "nope")), "Invalid retention policy id"},
		{"create org owner_user_id", h.createOrganization, http.MethodPost,
			withBody(`{"name":"Acme","owner_user_id":"nope"}`), "Invalid user id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := answerWith(c.handler, c.method, compose(withOrgClaim, c.setup))
			expect400(t, rec, c.detail)
		})
	}
}

// TestMalformedAdminIDAnswerNeverEchoesInput guards ruling 107: no input text
// in the body.
func TestMalformedAdminIDAnswerNeverEchoesInput(t *testing.T) {
	withProcessLicense(t)
	h := &handlers{}
	const planted = "PLANTED-ID-TEXT-10609"
	rec := answerWith(h.getAuditLog, http.MethodGet, compose(withOrgClaim, pathValues("log_id", planted)))
	if rec.Code != http.StatusBadRequest || strings.Contains(rec.Body.String(), planted) {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}
