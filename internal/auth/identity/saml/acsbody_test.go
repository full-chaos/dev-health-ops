package saml

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSAMLACSReadsTheHTTPPostBindingAndTheJSONBody(t *testing.T) {
	for name, tc := range map[string]struct {
		contentType, body, want string
		ok                      bool
	}{
		"HTTP-POST form":            {"application/x-www-form-urlencoded", "SAMLResponse=PHg%2B&RelayState=%2Fafter", "PHg+", true},
		"HTTP-POST form, charset":   {"application/x-www-form-urlencoded; charset=utf-8", "SAMLResponse=abc", "abc", true},
		"form without SAMLResponse": {"application/x-www-form-urlencoded", "RelayState=x", "", false},
		"form, SAMLResponse twice":  {"application/x-www-form-urlencoded", "SAMLResponse=a&SAMLResponse=b", "", false},
		"JSON SAMLResponse":         {"application/json", `{"SAMLResponse":"abc","RelayState":"x"}`, "abc", true},
		"JSON saml_response":        {"application/json", `{"saml_response":"abc"}`, "abc", true},
		"JSON without a response":   {"application/json", `{"RelayState":"x"}`, "", false},
		"JSON, no content type":     {"", `{"SAMLResponse":"abc"}`, "abc", true},
		"JSON, +json media type":    {"application/problem+json", `{"SAMLResponse":"abc"}`, "abc", true},
		"form, RelayState twice":    {"application/x-www-form-urlencoded", "SAMLResponse=a&RelayState=1&RelayState=2", "", false},
		"text/plain":                {"text/plain", `{"SAMLResponse":"abc"}`, "", false},
		"multipart form":            {"multipart/form-data; boundary=x", "--x\r\nContent-Disposition: form-data; name=\"SAMLResponse\"\r\n\r\nabc\r\n--x--\r\n", "", false},
		"XML":                       {"application/xml", "<x/>", "", false},
		"malformed content type":    {"application/x-www-form-urlencoded; ;;", "SAMLResponse=abc", "", false},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/acs", strings.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			got, ok := readSAMLResponse(rec, req)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("readSAMLResponse = %q, %v (status %d %s); want %q, %v", got, ok, rec.Code, rec.Body, tc.want, tc.ok)
			}
			if !ok && rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("refusal status %d, want 422", rec.Code)
			}
		})
	}
}

func TestSAMLBindingCookieReachesACrossSiteFormPost(t *testing.T) {
	c := bindingCookie("n", "v", "/api/v1/auth/saml/x/acs", 600)
	if c.SameSite != http.SameSiteNoneMode || !c.Secure || !c.HttpOnly || c.Path != "/api/v1/auth/saml/x/acs" {
		t.Fatalf("binding cookie %+v", c)
	}
}
