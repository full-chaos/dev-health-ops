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
