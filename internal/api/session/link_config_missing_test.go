package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testLinkSecret is the link secret of a handler under test that must pass the config check.
const testLinkSecret = "unit-test-link-secret-0123456789abcdef"

// With no LINK_TOKEN_SECRET the invite join answers 503 config_missing before
// it opens a transaction: the handler here has no Pool, so any query would
// panic.
func TestLinkRoutesAnswer503WithoutLinkSecret(t *testing.T) {
	h := handlers{}
	t.Run("invite join", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		_, _, ok := h.joinByInvite(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/auth/accept-invite", nil), nil, nil, "token", "Invite accepted", time.Now())
		if ok || recorder.Code != http.StatusServiceUnavailable || !(strings.Contains(recorder.Body.String(), `"reason_code":"config_missing"`) && strings.Contains(recorder.Body.String(), "LINK_TOKEN_SECRET")) {
			t.Fatalf("joinByInvite ok=%v status %d body %q, want 503 config_missing", ok, recorder.Code, recorder.Body.String())
		}
	})
}
