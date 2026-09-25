package pybody

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestRequiredUUIDErrorShapes(t *testing.T) {
	const good = "0d9f2f7e-1c34-4f6f-9a53-8b7f6c1d2e3a"
	object := pyjson.NewObject()
	object.Set("ok", good)
	object.Set("braced", "{"+good+"}")
	object.Set("bad", "abcd")
	object.Set("number", pyjson.IntOf(5))
	object.Set("null", nil)

	var errs Errors
	if _, ok := errs.RequiredUUID(object, "ok"); !ok {
		t.Fatal("a hyphenated uuid was refused")
	}
	if _, ok := errs.RequiredUUID(object, "braced"); !ok {
		t.Fatal("a braced uuid was refused")
	}
	if len(errs) != 0 {
		t.Fatalf("errors for valid uuids: %v", errs)
	}
	for _, tc := range []struct{ field, kind, msg string }{
		{"bad", "uuid_parsing", "Input should be a valid UUID, invalid length: expected length 32 for simple format, found 4"},
		{"number", "uuid_type", "UUID input should be a string, bytes or UUID object"},
		{"null", "uuid_type", "UUID input should be a string, bytes or UUID object"},
		{"absent", "missing", "Field required"},
	} {
		var e Errors
		if _, ok := e.RequiredUUID(object, tc.field); ok || len(e) != 1 {
			t.Fatalf("%s: ok/errs = %v", tc.field, e)
		}
		if e[0].Type != tc.kind || e[0].Msg != tc.msg {
			t.Errorf("%s: got %s %q, want %s %q", tc.field, e[0].Type, e[0].Msg, tc.kind, tc.msg)
		}
	}
}
