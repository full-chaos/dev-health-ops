package oidc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func serveBody(t *testing.T, c routeCase, kind, body string) (int, string) {
	t.Helper()
	row := c.activeRow()
	env := newUnitEnv(t, &fakeProviders{before: row})
	step := "authorize"
	if c.callback {
		step = "callback"
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+kind+"/"+row.ID.String()+"/"+step, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := env.serve(c, req)
	return rec.Code, rec.Body.String()
}

// A body that is not UTF-8 is answered before any field is read, as the api
// plane answers it.
func TestJSONRoutesAnswer400ForABodyThatIsNotUTF8(t *testing.T) {
	for _, c := range routeCases[:4] {
		kind := "oidc"
		if c.oauth {
			kind = "oauth"
		}
		for _, body := range []string{"{\"redirect_uri\":\"a\xffb\"}", "{\"code\":\"\xc0\",\"state\":\"s\"}", "{\"x\":\"\xe2\x82\"}"} {
			status, text := serveBody(t, c, kind, body)
			if status != http.StatusBadRequest || text != `{"detail":"There was an error parsing the body"}` {
				t.Errorf("%s %q: status=%d body=%s", c.route, body, status, text)
			}
		}
	}
}

// A surrogate sent as raw bytes is read as a lone surrogate by the api plane,
// so the authorize route refuses it as it refuses the escape form.
func TestAuthorizeRefusesRawSurrogateBytesAsStringUnicode(t *testing.T) {
	c := routeCases[0]
	for _, raw := range []string{"\xed\xa0\x80", "\xed\xbf\xbf", "\xed\xa0\xbd\xed\xb8\x80"} {
		status, body := serveBody(t, c, "oidc", `{"redirect_uri":"https://app.example/cb`+raw+`"}`)
		if status != http.StatusUnprocessableEntity || !strings.Contains(body, `"string_unicode"`) || !strings.Contains(body, `"loc":["body","redirect_uri"]`) ||
			strings.Contains(body, "app.example") || strings.Contains(body, "�") {
			t.Errorf("%q: status=%d body=%s", raw, status, body)
		}
	}
}

// Valid text, valid multibyte characters and the code points on either side
// of the surrogate range are not refused, and the OAuth route keeps ignoring
// redirect_uri.
func TestRawBodiesThatAreValidKeepTheirAnswers(t *testing.T) {
	authorize, oauth := routeCases[0], routeCases[2]
	wantOAuth, _ := serveBody(t, oauth, "oauth", `{"redirect_uri":"https://app.example/cb"}`)
	for _, value := range []string{"é名😀", "\xed\x9f\xbf", "\xee\x80\x80"} {
		if _, body := serveBody(t, authorize, "oidc", `{"redirect_uri":"https://app.example/`+value+`"}`); strings.Contains(body, "string_unicode") || strings.Contains(body, "error parsing") {
			t.Errorf("authorize %q: %s", value, body)
		}
	}
	for _, value := range []string{"\xed\xa0\x80", "é名😀"} {
		if status, body := serveBody(t, oauth, "oauth", `{"redirect_uri":"https://app.example/`+value+`"}`); status != wantOAuth || strings.Contains(body, "string_unicode") {
			t.Errorf("oauth %q: status=%d want %d body=%s", value, status, wantOAuth, body)
		}
	}
}

// A body with a syntax error keeps the answer this route always gave (the api
// plane answers 422 json_invalid there; only the invalid-byte class is a 400).
func TestJSONRoutesKeepTheirAnswerForABodyWithASyntaxError(t *testing.T) {
	for _, c := range routeCases[:4] {
		kind := "oidc"
		if c.oauth {
			kind = "oauth"
		}
		for _, body := range []string{`{"redirect_uri":"https://app.example/cb"`, `{"code":`, `{"redirect_uri" "x"}`, `[1,`} {
			status, text := serveBody(t, c, kind, body)
			if status != http.StatusUnprocessableEntity || !strings.Contains(text, "model_attributes_type") {
				t.Errorf("%s %q: status=%d body=%s", c.route, body, status, text)
			}
		}
	}
}

// The raw check runs on the one member the route keeps, never on another
// member or on a key.
func TestAuthorizeOnlyReadsTheRedirectURIMemberForRawSurrogateBytes(t *testing.T) {
	c := routeCases[0]
	wantStatus, _ := serveBody(t, c, "oidc", `{"redirect_uri":"https://app.example/cb"}`)
	for _, body := range []string{
		"{\"note\":\"\xed\xa0\x80\",\"redirect_uri\":\"https://app.example/cb\"}",
		"{\"\xed\xa0\x80\":1,\"redirect_uri\":\"https://app.example/cb\"}",
		"{\"use_pkce\":true,\"extra\":[\"\xed\xa0\x80\"],\"redirect_uri\":\"https://app.example/cb\"}",
	} {
		status, text := serveBody(t, c, "oidc", body)
		if status != wantStatus || strings.Contains(text, "string_unicode") {
			t.Errorf("%q: status=%d want %d body=%s", body, status, wantStatus, text)
		}
	}
}

func serveBodyAs(t *testing.T, c routeCase, kind, body, contentType string) (int, string) {
	t.Helper()
	row := c.activeRow()
	env := newUnitEnv(t, &fakeProviders{before: row})
	step := "authorize"
	if c.callback {
		step = "callback"
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+kind+"/"+row.ID.String()+"/"+step, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := env.serve(c, req)
	return rec.Code, rec.Body.String()
}

// The api plane reads a body as JSON only for no content type, application/json
// and application/*+json; any other content type is a raw body it never
// decodes, so the 400 for a body that is not UTF-8 applies to those three.
func TestAuthorizeAnswers400ForABodyThatIsNotUTF8OnlyWhenItIsJSON(t *testing.T) {
	c := routeCases[0]
	const body = "{\"x\":\"\xe2\x82\"}"
	for _, contentType := range []string{"application/json", "", "application/vnd.example+json", "application/json; charset=utf-8", "APPLICATION/JSON"} {
		if status, text := serveBodyAs(t, c, "oidc", body, contentType); status != http.StatusBadRequest || text != `{"detail":"There was an error parsing the body"}` {
			t.Errorf("content type %q: status=%d body=%s", contentType, status, text)
		}
	}
	wantStatus, _ := serveBodyAs(t, c, "oidc", `{"x":"ok"}`, "text/plain")
	for _, contentType := range []string{"text/plain", "application/x-www-form-urlencoded", "application/octet-stream", "multipart/form-data; boundary=x", "text/json", "application/jsonx"} {
		if status, text := serveBodyAs(t, c, "oidc", body, contentType); status != wantStatus || strings.Contains(text, "error parsing the body") {
			t.Errorf("content type %q: status=%d want %d body=%s", contentType, status, wantStatus, text)
		}
	}
}
