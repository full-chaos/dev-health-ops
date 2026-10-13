package admin

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// The superadmin form once posted {"password": ...} without admin_password:
// the 422 for the missing field echoed the whole request, new password included.
func TestSetPasswordValidationErrorNeverEchoesSubmittedValues(t *testing.T) {
	const probe = "zz-probe-value-81f3"
	bodies := map[string]string{
		"admin_password missing": `{"password":"` + probe + `"}`,
		"password too short":     `{"admin_password":"` + probe + `","password":"abc"}`,
		"admin_password short":   `{"admin_password":"abc","password":"` + probe + `"}`,
	}
	for name, raw := range bodies {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", strings.NewReader(raw))
			body, outcome, _, err := pybody.Read(req)
			if err != nil || outcome != pybody.Ready {
				t.Fatalf("read: %v %v", err, outcome)
			}
			_, errs := parseSetPasswordBody(body)
			if len(errs) == 0 {
				t.Fatal("expected a validation error")
			}
			rendered, _ := json.Marshal(pybody.Detail(errs))
			if strings.Contains(string(rendered), probe) {
				t.Fatalf("422 body echoes a submitted value: %s", rendered)
			}
			if strings.Contains(string(rendered), `"password":"abc"`) && strings.Contains(raw, `"password":"abc"`) {
				t.Fatalf("422 body echoes the submitted password field: %s", rendered)
			}
		})
	}
}
