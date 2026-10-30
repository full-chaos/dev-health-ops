package saml_test

import (
	"net/http"
	"strings"
	"testing"
)

// A linked NameID whose asserted domain is off the provider's list is refused before the lookup, with the answer
// an unknown NameID gets (go-api saml.go parity), and no session is issued.
func TestSAMLACSRefusesALinkedIdentityOffTheDomainList(t *testing.T) {
	for name, tc := range map[string]struct {
		email  string
		status int
		body   string
		reason string
	}{
		"off the list": {"u@other.test", http.StatusForbidden, `{"detail":"Email domain is not allowed for this provider"}`, "domain_not_allowed"},
		"no at":        {"u.example.test", http.StatusBadRequest, "", "malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			linked := postACSEmailWithList(t, tc.email, true, []string{"example.test"})
			unknown := postACSEmailWithList(t, tc.email, false, []string{"example.test"})
			if linked.status != unknown.status || linked.body != unknown.body {
				t.Fatalf("linked %d %s, unknown %d %s: the answers differ", linked.status, linked.body, unknown.status, unknown.body)
			}
			if linked.status != tc.status || (tc.body != "" && strings.TrimSpace(linked.body) != tc.body) {
				t.Fatalf("ACS = %d %s, want %d %s", linked.status, linked.body, tc.status, tc.body)
			}
			if linked.calls != 0 || linked.sessions != 0 || !strings.Contains(linked.logs, `"reason_code":"`+tc.reason+`"`) {
				t.Fatalf("%d store calls, %d sessions; log %s", linked.calls, linked.sessions, linked.logs)
			}
		})
	}
}

func TestSAMLACSRefusesALinkedIdentityWithNoEmail(t *testing.T) {
	linked := postACSEmailWithList(t, "", true, nil)
	if linked.status != http.StatusBadRequest || linked.calls != 0 || linked.sessions != 0 {
		t.Fatalf("ACS = %d %s, %d store calls, %d sessions", linked.status, linked.body, linked.calls, linked.sessions)
	}
}

// Control: a linked NameID whose domain is on the list (ASCII case-insensitive) signs in.
func TestSAMLACSSignsInALinkedIdentityTheListAllows(t *testing.T) {
	if a := postACSEmailWithList(t, "u@EXAMPLE.test", true, []string{"example.test"}); a.status != http.StatusOK || a.calls == 0 || a.sessions != 1 {
		t.Fatalf("ACS = %d %s, %d store calls, %d sessions", a.status, a.body, a.calls, a.sessions)
	}
}
