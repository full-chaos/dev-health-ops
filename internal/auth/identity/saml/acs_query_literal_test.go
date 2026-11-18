package saml

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The HTTP-POST binding is the body alone: a ";" or a bad escape in the ACS
// URL's query no longer refuses the post, and the body is read as before.
func TestSAMLACSFormIgnoresTheURLQuery(t *testing.T) {
	for name, tc := range map[string]struct {
		target, body, want string
		ok                 bool
	}{
		"query with a semicolon":        {"/acs?x=;", "SAMLResponse=PHg%2B&RelayState=%2Fafter", "PHg+", true},
		"query with a bad escape":       {"/acs?x=%zz", "SAMLResponse=abc", "abc", true},
		"query naming SAMLResponse":     {"/acs?SAMLResponse=q", "SAMLResponse=abc", "abc", true},
		"query only":                    {"/acs?SAMLResponse=q", "RelayState=x", "", false},
		"malformed body escape":         {"/acs?x=;", "SAMLResponse=%zz", "", false},
		"semicolon in the body":         {"/acs", "SAMLResponse=a;b", "", false},
		"body past the size limit":      {"/acs?x=;", "SAMLResponse=" + strings.Repeat("a", maxBodyBytes), "", false},
		"repeated SAMLResponse in body": {"/acs?x=;", "SAMLResponse=a&SAMLResponse=b", "", false},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			in, ok := decodeACS(rec, req)
			if got := in.SAMLResponse; ok != tc.ok || got != tc.want {
				t.Fatalf("decodeACS = %q, %v (status %d %s); want %q, %v", got, ok, rec.Code, rec.Body, tc.want, tc.ok)
			}
			if !ok && rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("refusal status %d, want 422", rec.Code)
			}
		})
	}
}
