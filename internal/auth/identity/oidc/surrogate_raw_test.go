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
