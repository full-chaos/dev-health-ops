package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func resetRequest(t *testing.T, password string) *http.Request {
	t.Helper()
	value, err := pyjson.DecodeString(`{"token":"t","new_password":"` + password + `"}`)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/reset-password", nil)
	return req.WithContext(policy.WithBody(req.Context(), pybody.Body{Value: value}))
}

// A reset password the policy refuses is answered 422 with the violations
// before the pool is touched: handlers{} has no pool, so a call that reached
// token redemption would panic.
func TestResetPasswordRefusesElevenCharactersBeforeRedeeming(t *testing.T) {
	rec := httptest.NewRecorder()
	handlers{}.resetPassword(rec, resetRequest(t, "Aa1-xyzabcd"))
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	if strings.Contains(body, "Aa1-xyzabcd") {
		t.Errorf("body %s echoes the password", body)
	}
	for _, want := range []string{"Password validation failed", "Password must be at least 12 characters long"} {
		if !strings.Contains(body, want) {
			t.Errorf("body %s lacks %q", body, want)
		}
	}
}

func TestResetPasswordPolicyAcceptsTwelveAndPinsTheMaximum(t *testing.T) {
	for password, want := range map[string]bool{
		"Aa1-xyzabcd":                   false,
		"Aa1-xyzabcde":                  true,
		"A1" + strings.Repeat("b", 126): true,
		"A1" + strings.Repeat("b", 127): false,
		"Aa1-xyz":                       false,
		"PassWord1000":                  false,
		"correct-horse-battery-staple-no-digits-here": false,
	} {
		rec := httptest.NewRecorder()
		_, ok := validateResetPasswordPolicy(rec, resetRequest(t, password))
		if ok != want {
			t.Errorf("%d characters %q: accepted = %v, want %v (status %d)", len(password), password, ok, want, rec.Code)
		}
	}
}
