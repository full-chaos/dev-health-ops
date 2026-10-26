package social

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func exchange(body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	Handlers{}.Exchange(rec, req)
	return rec
}

// A body that is not UTF-8 is answered before any field is read, as the api
// plane answers it; the other answers of the route are as they were.
func TestExchangeAnswers400ForABodyThatIsNotUTF8(t *testing.T) {
	for _, body := range []string{"{\"provider\":\"github\",\"provider_access_token\":\"a\xffb\"}", "{\"provider\":\"\xc0\"}", "{\"x\":\"\xe2\x82\"}"} {
		rec := exchange(body)
		if rec.Code != http.StatusBadRequest || strings.TrimSpace(rec.Body.String()) != `{"detail":"There was an error parsing the body"}` {
			t.Errorf("%q: status=%d body=%s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestExchangeKeepsItsOtherAnswersForReadableBodies(t *testing.T) {
	for body, want := range map[string]string{
		`{"provider":"nope","provider_access_token":"t"}`:           "provider must be one of github, gitlab, google",
		`{"provider":"github"}`:                                     "provider and provider_access_token are required",
		"{\"provider\":\"nope\",\"provider_access_token\":\"é名😀\"}": "provider must be one of github, gitlab, google",
		`[]`:                                   "provider and provider_access_token are required",
		strings.Repeat("x", maxRequestBody+10): "provider and provider_access_token are required",
	} {
		rec := exchange(body)
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%.40q: status=%d body=%s want %q", body, rec.Code, rec.Body.String(), want)
		}
	}
}
