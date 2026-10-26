package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

func registerRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	parsed, outcome, _, err := pybody.Read(request)
	if err != nil || outcome != pybody.Ready {
		t.Fatalf("read: %v %v", outcome, err)
	}
	return request.WithContext(policy.WithBody(request.Context(), parsed))
}

// A lone surrogate in a name is refused by the validator itself, so the
// handler (which opens the transaction) is never reached: the handlers here
// have no Pool, and reaching register would panic.
func TestRegisterRefusesALoneSurrogateNameBeforeTheHandler(t *testing.T) {
	for name, body := range map[string]string{
		"full_name":     `{"email":"a@example.com","password":"Correct-horse-battery-9","full_name":"\ud800"}`,
		"org_name":      `{"email":"a@example.com","password":"Correct-horse-battery-9","org_name":"Team \udc00"}`,
		"both":          `{"email":"a@example.com","password":"Correct-horse-battery-9","full_name":"x\udbff","org_name":"\ud800"}`,
		"high then low": `{"email":"a@example.com","password":"Correct-horse-battery-9","full_name":"\ud800\ud800"}`,
	} {
		w := httptest.NewRecorder()
		if _, ok := validateRegister(w, registerRequest(t, body)); ok {
			t.Fatalf("%s: the validator accepted a lone surrogate", name)
		}
		if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), `"string_unicode"`) {
			t.Fatalf("%s: %d %s", name, w.Code, w.Body.String())
		}
		field := name
		if name == "both" || name == "high then low" {
			field = "full_name"
		}
		if !strings.Contains(w.Body.String(), `"`+field+`"`) {
			t.Fatalf("%s: loc does not name %s: %s", name, field, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "Correct-horse") {
			t.Fatalf("%s: the body echoes the password", name)
		}
	}
}

func TestRegisterAcceptsEmojiAndPairedEscapesInNames(t *testing.T) {
	for name, body := range map[string]string{
		"emoji":  `{"email":"a@example.com","password":"Correct-horse-battery-9","full_name":"Ann 😀","org_name":"Ünï 名前 😀"}`,
		"empty":  `{"email":"a@example.com","password":"Correct-horse-battery-9","full_name":"","org_name":""}`,
		"absent": `{"email":"a@example.com","password":"Correct-horse-battery-9"}`,
		"null":   `{"email":"a@example.com","password":"Correct-horse-battery-9","full_name":null}`,
	} {
		w := httptest.NewRecorder()
		if _, ok := validateRegister(w, registerRequest(t, body)); !ok {
			t.Fatalf("%s refused: %d %s", name, w.Code, w.Body.String())
		}
	}
}

func TestRegisterPasswordWithALoneSurrogateKeepsItsRefusal(t *testing.T) {
	w := httptest.NewRecorder()
	if _, ok := validateRegister(w, registerRequest(t, `{"email":"a@example.com","password":"abcdefgh1\ud800"}`)); ok ||
		w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), `"string_unicode"`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}
