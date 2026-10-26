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

func TestExchangeKeepsItsAnswerForABodyWithASyntaxError(t *testing.T) {
	for _, body := range []string{`{"provider":"github"`, `{"provider":`, `{"provider" "x"}`, `[1,`} {
		rec := exchange(body)
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "provider and provider_access_token are required") {
			t.Errorf("%q: status=%d body=%s", body, rec.Code, rec.Body.String())
		}
	}
}

func exchangeAs(body, contentType string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	Handlers{}.Exchange(rec, req)
	return rec
}

// The api plane reads a body as JSON only for no content type, application/json
// and application/*+json; any other content type is a raw body it never decodes.
func TestExchangeAnswers400ForABodyThatIsNotUTF8OnlyWhenItIsJSON(t *testing.T) {
	const body = "{\"provider\":\"nope\",\"provider_access_token\":\"a\xffb\"}"
	for _, contentType := range []string{"application/json", "", "application/vnd.example+json", "application/json; charset=utf-8"} {
		if rec := exchangeAs(body, contentType); rec.Code != http.StatusBadRequest || strings.TrimSpace(rec.Body.String()) != `{"detail":"There was an error parsing the body"}` {
			t.Errorf("content type %q: status=%d body=%s", contentType, rec.Code, rec.Body.String())
		}
	}
	for _, contentType := range []string{"text/plain", "application/x-www-form-urlencoded", "application/octet-stream", "multipart/form-data; boundary=x", "text/json"} {
		if rec := exchangeAs(body, contentType); rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "provider must be one of") {
			t.Errorf("content type %q: status=%d body=%s", contentType, rec.Code, rec.Body.String())
		}
	}
}
