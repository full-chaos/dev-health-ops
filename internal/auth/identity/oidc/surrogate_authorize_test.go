package oidc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// authorize reads redirect_uri from the body; encoding/json turns a lone
// surrogate escape into U+FFFD before the handler sees it, so the body is
// scanned as raw bytes.
func TestAuthorizeRefusesALoneSurrogateRedirectURIAsStringUnicode(t *testing.T) {
	c := routeCases[0]
	for _, escape := range []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`, `\ud83d\ud83d`, `\ude00\ud83d`} {
		row := c.activeRow()
		env := newUnitEnv(t, &fakeProviders{before: row})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oidc/"+row.ID.String()+"/authorize",
			strings.NewReader(`{"redirect_uri":"https://app.example/cb`+escape+`"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := env.serve(c, req)
		body := rec.Body.String()
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(body, `"string_unicode"`) ||
			!strings.Contains(body, `"loc":["body","redirect_uri"]`) || strings.Contains(body, "app.example") || strings.Contains(body, "�") {
			t.Errorf("%s: status=%d body=%s", escape, rec.Code, body)
		}
	}
}

// A valid pair, an empty value, a null and an absent field are not refused
// by the scan (they reach the existing checks).
func TestAuthorizeKeepsAcceptingAPairedRedirectURI(t *testing.T) {
	c := routeCases[0]
	for _, body := range []string{`{"redirect_uri":"https://app.example/cb😀"}`, `{"redirect_uri":""}`, `{"redirect_uri":null}`, `{}`, `{"redirect_uri":"https://app.example/cb\\ud800"}`} {
		row := c.activeRow()
		env := newUnitEnv(t, &fakeProviders{before: row})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oidc/"+row.ID.String()+"/authorize", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if rec := env.serve(c, req); strings.Contains(rec.Body.String(), "string_unicode") {
			t.Errorf("%s: refused as string_unicode: %s", body, rec.Body.String())
		}
	}
}

// The OAuth authorize route reads the body for its shape only and never uses
// redirect_uri, so a lone surrogate there is answered exactly as any other
// value is (the api plane ignores the field the same way).
func TestOAuthAuthorizeIgnoresALoneSurrogateRedirectURI(t *testing.T) {
	c := routeCases[2]
	serve := func(body string) (int, string) {
		row := c.activeRow()
		env := newUnitEnv(t, &fakeProviders{before: row})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oauth/"+row.ID.String()+"/authorize", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := env.serve(c, req)
		return rec.Code, rec.Body.String()
	}
	wantStatus, _ := serve(`{"redirect_uri":"https://app.example/cb"}`)
	for _, escape := range []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`, `\ud83d\ud83d`} {
		status, body := serve(`{"redirect_uri":"https://app.example/cb` + escape + `"}`)
		if status != wantStatus || strings.Contains(body, "string_unicode") {
			t.Errorf("%s: status=%d want %d body=%s", escape, status, wantStatus, body)
		}
	}
}

// A redirect_uri that is not a string keeps its own type error: the scan
// reads only a JSON string as the field, never the strings nested in an
// object or an array value.
func TestAuthorizeAnswersTheTypeErrorForANonStringRedirectURI(t *testing.T) {
	c := routeCases[0]
	for _, value := range []string{`{"x":"\ud800"}`, `["\ud800"]`} {
		row := c.activeRow()
		env := newUnitEnv(t, &fakeProviders{before: row})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oidc/"+row.ID.String()+"/authorize", strings.NewReader(`{"redirect_uri":`+value+`}`))
		req.Header.Set("Content-Type", "application/json")
		rec := env.serve(c, req)
		body := rec.Body.String()
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(body, `"string_type"`) || strings.Contains(body, "string_unicode") {
			t.Errorf("%s: status=%d body=%s", value, rec.Code, body)
		}
	}
}
