package saml

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type oneProvider struct{ p Provider }

func (o oneProvider) Provider(context.Context, uuid.UUID) (Provider, error) { return o.p, nil }

type denySSO struct{}

func (denySSO) OrgHasSSO(context.Context, uuid.UUID) (bool, error) { return false, nil }

func TestSAMLRoutesAnswer402BeforeReadingTheBody(t *testing.T) {
	p := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active"}
	h := Handlers{Service: &Service{Providers: oneProvider{p}, Entitlements: denySSO{}, BaseURL: func() string { return "https://sp.example.com" }}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /saml/{provider_id}/initiate", h.Initiate)
	mux.HandleFunc("POST /saml/{provider_id}/acs", h.ACS)
	for name, tc := range map[string]struct{ path, contentType, body string }{
		"initiate, malformed JSON": {"initiate", "application/json", "{"},
		"initiate, text/plain":     {"initiate", "text/plain", "x"},
		"acs, malformed JSON":      {"acs", "application/json", "{"},
		"acs, XML":                 {"acs", "application/xml", "<x/>"},
		"acs, bad content type":    {"acs", "application/x-www-form-urlencoded; ;;", "SAMLResponse=x"},
		"acs, empty form":          {"acs", "application/x-www-form-urlencoded", ""},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusPaymentRequired || !strings.Contains(rec.Body.String(), "feature_not_licensed") {
				t.Fatalf("%d %s, want 402 feature_not_licensed", rec.Code, rec.Body)
			}
		})
	}
}

func TestSAMLDomainAllowedIgnoresCase(t *testing.T) {
	for _, tc := range []struct {
		allowed []string
		email   string
		want    bool
	}{
		{[]string{"example.com"}, "a@EXAMPLE.COM", true},
		{[]string{"Example.COM"}, "a@example.com", true},
		{[]string{"example.com"}, "a@example.org", false},
		{[]string{"example.com"}, "a@sub.example.com", false},
		{nil, "a@any.example", true},
		{[]string{"example.com"}, "no-at-sign", false},
	} {
		if got := (Provider{AllowedDomains: tc.allowed}).DomainAllowed(tc.email); got != tc.want {
			t.Errorf("DomainAllowed(%v, %q) = %v, want %v", tc.allowed, tc.email, got, tc.want)
		}
	}
}
