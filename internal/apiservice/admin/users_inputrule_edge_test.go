package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// The address trims these characters at an edge (Python str.strip) on the admin routes too; the frozen oracle sends
// \x1c and \x1f around an address. The form rule looks at the trimmed address, so the edge characters are not refused.
func TestAdminUserEmailsKeepTheirEdgeTrimmedCharacters(t *testing.T) {
	for _, esc := range []string{`\t`, `\n`, `\r`, `\u000b`, `\u000c`, ` `, `\u001c`, `\u001d`, `\u001e`, `\u001f`, `\u0085`, ` `} {
		address := esc + `edge@example.test` + esc
		code, response, _, _ := createUserWithBody(t, `{"email":"`+address+`"}`)
		if code == http.StatusUnprocessableEntity {
			t.Errorf("create %q: refused: %s", esc, response)
		}
		request := httptest.NewRequest(http.MethodPatch, "/api/v1/admin/users/x", strings.NewReader(`{"email":"`+address+`"}`))
		request.Header.Set("Content-Type", "application/json")
		decoded, outcome, _, err := pybody.Read(request)
		if err != nil || outcome != pybody.Ready {
			t.Fatalf("body: %v %v", err, outcome)
		}
		var errs pybody.Errors
		patch := parseUserUpdate(&errs, decoded)
		if len(errs) != 0 || patch.Email == nil {
			t.Errorf("update %q: errors %+v email set %v", esc, errs, patch.Email != nil)
		}
	}
}
