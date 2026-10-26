package saml

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func initiateRequest(t *testing.T, body string) (*httptest.ResponseRecorder, Provider) {
	t.Helper()
	p := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active", Config: Config{SSOURL: "https://idp.example/sso"}}
	h := Handlers{Service: &Service{Providers: oneProvider{p}, Entitlements: allowAll{}, Flows: &flowsOnly{}, BaseURL: func() string { return "https://sp.example.com" }}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /saml/{provider_id}/initiate", h.Initiate)
	req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/initiate", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec, p
}

// encoding/json turns a lone surrogate escape into U+FFFD before the handler
// sees the string, so the body is scanned as raw bytes.
func TestInitiateRefusesALoneSurrogateRelayStateAsStringUnicode(t *testing.T) {
	for _, escape := range []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`, `\ud83d\ud83d`, `\ude00\ud83d`} {
		rec, _ := initiateRequest(t, `{"relay_state":"a`+escape+`b"}`)
		body := rec.Body.String()
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(body, `"string_unicode"`) ||
			!strings.Contains(body, `"loc":["body","relay_state"]`) || strings.Contains(body, "�") || strings.Contains(body, "RelayState") {
			t.Errorf("%s: status=%d body=%s", escape, rec.Code, body)
		}
	}
}

// encoding/json matches the member name case-insensitively and forwards it.
func TestInitiateRefusesALoneSurrogateRelayStateUnderAnyCaseOfTheName(t *testing.T) {
	for _, body := range []string{`{"RELAY_STATE":"\ud800"}`, `{"Relay_State":"a\udc00"}`, `{"relay_state":"ok","RELAY_state":"\ud83d"}`} {
		if rec, _ := initiateRequest(t, body); rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), `"string_unicode"`) {
			t.Errorf("%s: status=%d body=%s", body, rec.Code, rec.Body.String())
		}
	}
	if rec, _ := initiateRequest(t, `{"note":"\ud800","relay_state":"ok"}`); rec.Code != http.StatusOK {
		t.Errorf("a surrogate in a member the route never reads: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInitiateKeepsForwardingAPairedRelayState(t *testing.T) {
	for body, want := range map[string]string{
		`{"relay_state":"a😀b"}`:       "a😀b",
		`{"relay_state":"a\\ud800b"}`: `a\ud800b`,
		`{"relay_state":"plain"}`:     "plain",
	} {
		rec, _ := initiateRequest(t, body)
		var out struct {
			RedirectURL string `json:"redirect_url"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
			t.Errorf("%s: status=%d body=%s", body, rec.Code, rec.Body.String())
			continue
		}
		parsed, err := url.Parse(out.RedirectURL)
		if err != nil || parsed.Query().Get("RelayState") != want {
			t.Errorf("%s: RelayState=%q err=%v want %q", body, parsed.Query().Get("RelayState"), err, want)
		}
	}
	for _, body := range []string{`{}`, `{"relay_state":null}`, `{"relay_state":""}`} {
		if rec, _ := initiateRequest(t, body); rec.Code != http.StatusOK {
			t.Errorf("%s: status=%d body=%s", body, rec.Code, rec.Body.String())
		}
	}
}

// encoding/json keeps the last matching member (a later null resets the
// pointer), so the value the route forwards decides, not any member.
func TestInitiateRefusesOnlyTheRelayStateThatWouldBeForwarded(t *testing.T) {
	cases := []struct {
		body        string
		wantStatus  int
		wantForward string
	}{
		{`{"RELAY_STATE":"\ud800","relay_state":"ok"}`, http.StatusOK, "ok"},
		{`{"relay_state":"ok","RELAY_STATE":"\ud800"}`, http.StatusUnprocessableEntity, ""},
		{`{"relay_state":"\ud800","relay_state":"ok"}`, http.StatusOK, "ok"},
		{`{"relay_state":"ok","relay_state":"\ud800"}`, http.StatusUnprocessableEntity, ""},
		{`{"relay_state":"\ud800","relay_state":null}`, http.StatusOK, ""},
		{`{"relay_state":null,"relay_state":"\ud800"}`, http.StatusUnprocessableEntity, ""},
	}
	for _, tc := range cases {
		rec, _ := initiateRequest(t, tc.body)
		if rec.Code != tc.wantStatus {
			t.Errorf("%s: status=%d want %d body=%s", tc.body, rec.Code, tc.wantStatus, rec.Body.String())
			continue
		}
		if tc.wantStatus != http.StatusOK {
			continue
		}
		var out struct {
			RedirectURL string `json:"redirect_url"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		parsed, err := url.Parse(out.RedirectURL)
		if err != nil || parsed.Query().Get("RelayState") != tc.wantForward {
			t.Errorf("%s: forwarded RelayState=%q want %q (%v)", tc.body, parsed.Query().Get("RelayState"), tc.wantForward, err)
		}
	}
}

func TestInitiateKeepsTheDecodeErrorForDataAfterTheObject(t *testing.T) {
	for _, body := range []string{`{"relay_state":"\ud800"} x`, `{"relay_state":"\ud800"}{}`, `{"relay_state":"\ud800"`} {
		rec, _ := initiateRequest(t, body)
		text := rec.Body.String()
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(text, "Request body must be a JSON object") || strings.Contains(text, "string_unicode") {
			t.Errorf("%s: status=%d body=%s", body, rec.Code, text)
		}
	}
}
