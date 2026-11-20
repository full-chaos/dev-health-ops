package authapi

import (
	"net/http"
	"testing"
)

// The verify token is read the way FastAPI reads it: a ";" or a bad escape
// stays in the value (an unknown token, 400), never reads as a missing one (422).
func TestVerifyEmailReadsTheTokenLiterally(t *testing.T) {
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"?token=x", http.StatusBadRequest},
		{"?token=t;x", http.StatusBadRequest},
		{"?token=%zz", http.StatusBadRequest},
		{"?token=%FF", http.StatusBadRequest},
		{"?token=", http.StatusUnprocessableEntity},
		{"?x=;", http.StatusUnprocessableEntity},
	} {
		c := newCredFixture(t)
		if r := c.call(http.MethodGet, PatternVerifyEmail+tc.query, "", nil); r.Code != tc.status {
			t.Errorf("%s: %d %s, want %d", tc.query, r.Code, r.Body.String(), tc.status)
		}
	}
}
