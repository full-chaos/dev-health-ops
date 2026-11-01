package saml_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

type recordedFailures struct {
	rows []ssoaudit.Row
	err  error
}

func (r *recordedFailures) Failed(_ context.Context, row ssoaudit.Row) error {
	r.rows = append(r.rows, row)
	return r.err
}

// An assertion without an email is go-api's provider failure (recordSSOFailure: protocol saml, no stage, the text
// of its error), recorded once before the answer, which is go-api's 400 "SAML authentication failed", for a linked
// and an unknown NameID alike; no lookup and no session run.
func TestSAMLAssertionWithoutEmailRecordsTheProviderFailure(t *testing.T) {
	for _, linked := range []bool{true, false} {
		failures := &recordedFailures{}
		a := postACSEmailRecorded(t, "", linked, nil, failures)
		if a.status != http.StatusBadRequest || strings.TrimSpace(a.body) != `{"detail":"SAML authentication failed"}` {
			t.Fatalf("linked=%v: ACS = %d %s, want 400 SAML authentication failed", linked, a.status, a.body)
		}
		if a.calls != 0 || a.sessions != 0 {
			t.Fatalf("linked=%v: %d store calls, %d sessions", linked, a.calls, a.sessions)
		}
		want := ssoaudit.Row{OrgID: a.provider.OrgID, ProviderID: a.provider.ID, Protocol: "saml",
			ErrorMessage: "SAML assertion missing email attribute"}
		if len(failures.rows) != 1 || failures.rows[0] != want {
			t.Fatalf("linked=%v: records %+v, want exactly %+v", linked, failures.rows, want)
		}
	}
}

// The other email refusals of a SAML sign-in record nothing (go-api answers them without recordSSOFailure).
func TestSAMLOtherEmailRefusalsRecordNoProviderFailure(t *testing.T) {
	for name, tc := range map[string]struct {
		email string
		list  []string
	}{
		"off the list": {"u@other.test", []string{"example.test"}},
		"no at":        {"u.example.test", []string{"example.test"}},
		"unusable":     {"u\x7f@example.test", nil},
		"success":      {"u@example.test", nil},
	} {
		failures := &recordedFailures{}
		postACSEmailRecorded(t, tc.email, true, tc.list, failures)
		if len(failures.rows) != 0 {
			t.Errorf("%s: records %+v, want none", name, failures.rows)
		}
	}
}

// A record that cannot be written is go-api's 500, never the 400.
func TestSAMLProviderFailureRecordWriteFailureIsThe500(t *testing.T) {
	a := postACSEmailRecorded(t, "", true, nil, &recordedFailures{err: errors.New("permission denied for table sso_providers")})
	if a.status != http.StatusInternalServerError || strings.Contains(a.body, "permission") || a.sessions != 0 {
		t.Fatalf("ACS = %d %s with %d sessions, want the 500", a.status, a.body, a.sessions)
	}
}

// The other provider failures go-api records (recordSSOFailure): one record with go-api's fixed text, the protocol and
// the stage if any, naming the provider row, and go-api's answer. The certificate failures run before any signature
// check, so an anonymous post with any base64 body reaches them on a provider without a usable certificate.
func TestSAMLProviderFailuresRecordGoAPIsText(t *testing.T) {
	noCertificate := func(p *saml.Provider) { p.Config.Certificate = "" }
	badCertificate := func(p *saml.Provider) { p.Config.Certificate = "not a certificate" }
	for name, tc := range map[string]struct {
		c           acsCase
		stage, text string
		detail      string
	}{
		"no certificate":             {acsCase{provider: noCertificate, raw: "YW55"}, "", "SAML certificate is required for validation", "SAML authentication failed"},
		"certificate does not parse": {acsCase{provider: badCertificate, raw: "YW55"}, "", "SAML signature validation failed", "SAML authentication failed"},
		"no subject confirmation":    {acsCase{email: "u@example.test", response: func(o *resp) { o.noConfirmation = true }}, "", "SAML subject confirmation missing", "SAML authentication failed"},
		"auto-provisioning off":      {acsCase{email: "u@example.test", provider: func(p *saml.Provider) { p.AutoProvision = false }}, "provisioning", "User not found and auto-provisioning disabled", "SAML user provisioning failed"},
	} {
		t.Run(name, func(t *testing.T) {
			failures := &recordedFailures{}
			tc.c.failures = failures
			a := postACSCase(t, tc.c)
			if a.status != http.StatusBadRequest || strings.TrimSpace(a.body) != `{"detail":"`+tc.detail+`"}` {
				t.Fatalf("ACS = %d %s, want 400 %s", a.status, a.body, tc.detail)
			}
			want := ssoaudit.Row{OrgID: a.provider.OrgID, ProviderID: a.provider.ID, Protocol: "saml", Stage: tc.stage, ErrorMessage: tc.text}
			if len(failures.rows) != 1 || failures.rows[0] != want || a.sessions != 0 {
				t.Fatalf("records %+v, %d sessions, want exactly %+v", failures.rows, a.sessions, want)
			}
		})
	}
}

// Refusals go-api answers without recordSSOFailure record nothing: a signature it refuses (audit only, stage
// signature_auth), an assertion that has confirmations but no bearer one (go-api signs in), a body that is not base64.
func TestSAMLRefusalsGoAPIDoesNotRecordWriteNoRecord(t *testing.T) {
	for name, c := range map[string]acsCase{
		"unsigned, no confirmation": {email: "u@example.test", response: func(o *resp) { o.noConfirmation, o.unsignedAssertion = true, true }},
		"unsigned":                  {email: "u@example.test", response: func(o *resp) { o.unsignedAssertion = true }},
		"sender-vouches only":       {email: "u@example.test", response: func(o *resp) { o.senderVouches = true }},
		"not base64":                {raw: "!!!"},
		"success":                   {email: "u@example.test", provider: func(p *saml.Provider) { p.AllowedDomains = []string{"example.test"} }},
	} {
		t.Run(name, func(t *testing.T) {
			failures := &recordedFailures{}
			c.failures = failures
			postACSCase(t, c)
			if len(failures.rows) != 0 {
				t.Fatalf("records %+v, want none", failures.rows)
			}
		})
	}
}

// A record that cannot be written is go-api's 500, for every class.
func TestSAMLProviderFailureWriteErrorsAreThe500(t *testing.T) {
	for name, c := range map[string]acsCase{
		"no certificate":             {provider: func(p *saml.Provider) { p.Config.Certificate = "" }, raw: "YW55"},
		"certificate does not parse": {provider: func(p *saml.Provider) { p.Config.Certificate = "not a certificate" }, raw: "YW55"},
		"no subject confirmation":    {email: "u@example.test", response: func(o *resp) { o.noConfirmation = true }},
		"auto-provisioning off":      {email: "u@example.test", provider: func(p *saml.Provider) { p.AutoProvision = false }},
	} {
		t.Run(name, func(t *testing.T) {
			c.failures = &recordedFailures{err: errors.New("permission denied for table sso_providers")}
			if a := postACSCase(t, c); a.status != http.StatusInternalServerError || strings.Contains(a.body, "permission") {
				t.Fatalf("ACS = %d %s, want the 500", a.status, a.body)
			}
		})
	}
}
