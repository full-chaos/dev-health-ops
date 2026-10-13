package pybody_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

const sentinelSecret = "zz-probe-value-81f3"

func render(t *testing.T, errs pybody.Errors) string {
	t.Helper()
	raw, err := json.Marshal(pybody.Detail(errs))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestDetailNeverEchoesSecretFieldValues(t *testing.T) {
	body := pyjson.NewObject()
	body.Set("password", sentinelSecret)
	body.Set("email", "someone@example.com")
	nested := pyjson.NewObject()
	nested.Set("client_secret", sentinelSecret)
	body.Set("config", nested)
	body.Set("tokens", []pyjson.Value{sentinelSecret})

	cases := map[string]pybody.Errors{
		"missing sibling echoes whole object": {{Type: "missing", Loc: []pyjson.Value{"body", "admin_password"}, Msg: "Field required", Input: body}},
		"too short secret field":              {{Type: "string_too_short", Loc: []pyjson.Value{"body", "password"}, Msg: "x", Input: sentinelSecret}},
		"admin_password field":                {{Type: "string_too_short", Loc: []pyjson.Value{"body", "admin_password"}, Msg: "x", Input: sentinelSecret}},
		"secret in list item":                 {{Type: "x", Loc: []pyjson.Value{"body", "tokens", int64(0)}, Msg: "x", Input: sentinelSecret}},
	}
	for name, errs := range cases {
		t.Run(name, func(t *testing.T) {
			if got := render(t, errs); strings.Contains(got, sentinelSecret) {
				t.Fatalf("422 body carries a submitted secret value: %s", got)
			}
		})
	}
}

func TestDetailStillEchoesNonSecretInput(t *testing.T) {
	errs := pybody.Errors{{Type: "int_type", Loc: []pyjson.Value{"body", "limit"}, Msg: "x", Input: "abc"}}
	if got := render(t, errs); !strings.Contains(got, `"input":"abc"`) {
		t.Fatalf("non-secret input must keep Python parity: %s", got)
	}
}
