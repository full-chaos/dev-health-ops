package saml_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

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
