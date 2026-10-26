package saml

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const unreadableBody = `{"detail":"There was an error parsing the body"}`

func TestInitiateAnswers400ForABodyThatIsNotUTF8(t *testing.T) {
	for _, body := range []string{"{\"relay_state\":\"a\xffb\"}", "{\"x\":\"\xc0\"}", "{\"relay_state\":\"\xe2\x82\"}"} {
		rec, _ := initiateRequest(t, body)
		if rec.Code != http.StatusBadRequest || rec.Body.String() != unreadableBody {
			t.Errorf("%q: status=%d body=%s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestInitiateRefusesRawSurrogateBytesAsStringUnicode(t *testing.T) {
	for _, raw := range []string{"\xed\xa0\x80", "\xed\xbf\xbf", "\xed\xa0\xbd\xed\xb8\x80"} {
		rec, _ := initiateRequest(t, `{"relay_state":"a`+raw+`b"}`)
		body := rec.Body.String()
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(body, `"string_unicode"`) ||
			!strings.Contains(body, `"loc":["body","relay_state"]`) || strings.Contains(body, "�") || strings.Contains(body, "RelayState") {
			t.Errorf("%q: status=%d body=%s", raw, rec.Code, body)
		}
	}
	for _, value := range []string{"é名😀", "\xed\x9f\xbf", "\xee\x80\x80"} {
		rec, _ := initiateRequest(t, `{"relay_state":"`+value+`"}`)
		if rec.Code != http.StatusOK {
			t.Errorf("%q: status=%d body=%s", value, rec.Code, rec.Body.String())
		}
	}
}

// The ACS JSON branch reads the same bytes; its form branch and every
// non-JSON content type keep their answers.
func TestACSAnswers400ForAJSONBodyThatIsNotUTF8AndLeavesTheFormAlone(t *testing.T) {
	p := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active", Config: Config{SSOURL: "https://idp.example/sso"}}
	h := Handlers{Service: &Service{Providers: oneProvider{p}, Entitlements: allowAll{}, Flows: &flowsOnly{}, BaseURL: func() string { return "https://sp.example.com" }}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /saml/{provider_id}/acs", h.ACS)
	post := func(contentType, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/acs", strings.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	for _, body := range []string{"{\"SAMLResponse\":\"a\xff\"}", "{\"RelayState\":\"\xc0\",\"saml_response\":\"x\"}"} {
		if rec := post("application/json", body); rec.Code != http.StatusBadRequest || rec.Body.String() != unreadableBody {
			t.Errorf("json %q: status=%d body=%s", body, rec.Code, rec.Body.String())
		}
	}
	form := url.Values{"SAMLResponse": {"a\xffb"}, "RelayState": {"\xed\xa0\x80"}}.Encode()
	if rec := post("application/x-www-form-urlencoded", form); rec.Code == http.StatusBadRequest && rec.Body.String() == unreadableBody {
		t.Errorf("the form branch answered the unreadable-body error: %s", rec.Body.String())
	}
}
