package pybody_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The 422 detail names the failing field and rule, never the submitted value:
// FastAPI's "input" can be a password or the whole request body.
func TestDetailWritesNeitherInputNorCtx(t *testing.T) {
	const probe = "zz-probe-value-81f3"
	body := pyjson.NewObject()
	body.Set("password", probe)
	ctx := pyjson.NewObject()
	ctx.Set("error", probe)
	errs := pybody.Errors{
		{Type: "missing", Loc: []pyjson.Value{"body", "admin_password"}, Msg: "Field required", Input: body},
		{Type: "string_too_short", Loc: []pyjson.Value{"body", "password"}, Msg: "x", Input: probe, Ctx: ctx},
		{Type: "int_type", Loc: []pyjson.Value{"body", "limit"}, Msg: "x", Input: "abc"},
	}
	raw, err := json.Marshal(pybody.Detail(errs))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{probe, `"input"`, `"ctx"`, "abc"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("422 body carries %q: %s", forbidden, raw)
		}
	}
	for _, kept := range []string{`"type"`, `"loc"`, `"msg"`} {
		if !strings.Contains(string(raw), kept) {
			t.Fatalf("422 body lost %s: %s", kept, raw)
		}
	}
}
