package pybody

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// serveEmailStr is what a route does with the helpers: 422 with the
// detail list, else 200 with the value the handler received. A body that
// cannot be rendered (a lone surrogate) is Starlette's plain 500.
func serveEmailStr(t *testing.T, path string, body []byte) (int, string) {
	t.Helper()
	request := httptest.NewRequest("POST", path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	parsed, outcome, decodeErr, err := Read(request)
	if err != nil || outcome != Ready {
		t.Fatalf("read %s: outcome=%v err=%v", body, outcome, err)
	}
	var errs Errors
	if decodeErr != nil {
		errs = append(errs, *decodeErr)
	}
	var value pyjson.Value
	if object, ok := errs.Object(parsed); ok {
		var email string
		var present bool
		if path == "/required" {
			email, present = errs.RequiredEmailStr(object, "email")
		} else {
			email, present = errs.OptionalEmailStr(object, "email")
		}
		if present {
			value = email
		}
	}
	status := 200
	out := pyjson.NewObject()
	out.Set("email", value)
	rendered := pyjson.Value(out)
	if len(errs) > 0 {
		status, rendered = 422, Detail(errs)
	}
	encoded, err := pyjson.Marshal(rendered)
	if err != nil {
		return 500, "Internal Server Error"
	}
	return status, string(encoded)
}
