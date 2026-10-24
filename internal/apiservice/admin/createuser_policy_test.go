package admin

import (
	"net/http"
	"strings"
	"testing"
)

// A create-user password the shared policy refuses is answered 422 with the
// violations before any store call: handlers{} has no store.
func TestCreateUserRefusesElevenCharacterPassword(t *testing.T) {
	h := &handlers{}
	rec := answerWith(h.createUser, http.MethodPost,
		withBody(`{"email":"new@example.com","password":"Aa1-xyzabcd"}`))
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "Aa1-xyzabcd") {
		t.Errorf("body %s echoes the password", rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "Password must be at least 12 characters long") {
		t.Errorf("body %s lacks the policy violation", rec.Body)
	}
}

// The refusal has sign-up's body: detail.message and detail.errors, with no
// detail.violations.
func TestCreateUserPolicyRefusalHasTheSignUpShape(t *testing.T) {
	h := &handlers{}
	rec := answerWith(h.createUser, http.MethodPost,
		withBody(`{"email":"new@example.com","password":"Aa1-xyzabcd"}`))
	want := `{"detail":{"message":"Password validation failed","errors":["Password must be at least 12 characters long"]}}`
	if rec.Code != http.StatusUnprocessableEntity || strings.TrimSpace(rec.Body.String()) != want {
		t.Fatalf("status %d body %s, want 422 %s", rec.Code, rec.Body, want)
	}
}

func TestCreateUserRefusesPasswordOverTheByteLimit(t *testing.T) {
	h := &handlers{}
	rec := answerWith(h.createUser, http.MethodPost,
		withBody(`{"email":"new@example.com","password":"`+strings.Repeat("\u00e9", 36)+`A1"}`))
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "no more than 72 bytes") {
		t.Fatalf("status %d body %s, want 422 with the byte violation", rec.Code, rec.Body)
	}
}
