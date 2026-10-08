package admin_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// redactVolatileText blanks the string value of every member named in keys,
// at any depth, in the raw response text (venueoracle.RedactJSON), and changes
// nothing else: the two planes' bodies are then compared as the raw text they
// wrote, escaping, key order and number spelling included. Decoding and
// re-encoding the body first (the previous helper) would hide exactly those
// differences. A member whose value is not a string is left alone and so is
// still compared.
func redactVolatileText(body string, keys ...string) string {
	return venueoracle.RedactJSON(body, func(path []string, raw string) (string, bool) {
		if len(path) == 0 || !strings.HasPrefix(raw, `"`) {
			return "", false
		}
		for _, key := range keys {
			if path[len(path)-1] == key {
				return `""`, true
			}
		}
		return "", false
	})
}

func TestRedactVolatileTextChangesOnlyTheNamedValues(t *testing.T) {
	body := `{"id":"a1","name":"tab\there","created_at":"2026-01-01T00:00:00Z","nested":[{"id":"b2","x":1.50}],"note":"has \"id\":\"z\" inside"}`
	got := redactVolatileText(body, "id", "created_at")
	want := `{"id":"","name":"tab\there","created_at":"","nested":[{"id":"","x":1.50}],"note":"has \"id\":\"z\" inside"}`
	if got != want {
		t.Fatalf("redacted body\n got:  %s\n want: %s", got, want)
	}
}

// The point of the helper: bodies that differ only in escaping stay different
// after redaction (a decode-and-re-encode comparison made them equal).
func TestRedactVolatileTextKeepsEscapingDifferences(t *testing.T) {
	escaped := `{"id":"a","path":"x\/y"}`
	literal := `{"id":"b","path":"x/y"}`
	if redactVolatileText(escaped, "id") == redactVolatileText(literal, "id") {
		t.Fatal("bodies that differ in escaping compare equal after redaction")
	}
	if got := redactVolatileText(`{"id":"a","x":[1,2]}`, "id"); got != `{"id":"","x":[1,2]}` {
		t.Fatalf("got %s", got)
	}
	// Key order, spacing and number spelling are part of what is compared.
	if redactVolatileText(`{"a":1,"b":2}`, "id") == redactVolatileText(`{"b":2,"a":1}`, "id") {
		t.Fatal("reordered keys compare equal")
	}
	if redactVolatileText(`{"a":1.0}`, "id") == redactVolatileText(`{"a":1}`, "id") {
		t.Fatal("different number spellings compare equal")
	}
}

func TestNormalizeAuditLogDisplayNamesKeepsTheLegacyAuditPayload(t *testing.T) {
	request := venueoracle.Request{Method: "GET", Path: "/api/v1/admin/audit-logs"}
	python := `{"items":[{"id":"log","org_id":"org","user_id":"user","action":"changed","resource_type":"user","resource_id":"target","changes":{"fraction":1.0},"request_metadata":{},"status":"success","created_at":"2026-10-04T21:00:00Z"}],"total":1,"limit":50,"offset":0}`
	goResponse := `{"items":[{"id":"log","org_id":"org","user_id":"user","action":"changed","resource_type":"user","resource_id":"target","actor_display_name":"Actor","resource_display_name":"Target","changes":{"fraction":1.0},"request_metadata":{},"status":"success","created_at":"2026-10-04T21:00:00Z"}],"total":1,"limit":50,"offset":0}`
	want := `{"items":[{"id":"log","org_id":"org","user_id":"user","action":"changed","resource_type":"user","resource_id":"target","actor_display_name":null,"resource_display_name":null,"changes":{"fraction":1.0},"request_metadata":{},"status":"success","created_at":"2026-10-04T21:00:00Z"}],"total":1,"limit":50,"offset":0}`

	for name, body := range map[string]string{"python": python, "go": goResponse} {
		if got := normalizeAuditLogDisplayNames(t, request, body); got != want {
			t.Errorf("%s normalized audit body\\n got:  %s\\n want: %s", name, got, want)
		}
	}
}

// normalizeAuditLogDisplayNames preserves the frozen Python contract while
// the Go audit response adds its approved nullable display-name fields. The
// Go-only venue test exercises the resolved values; this normalizer preserves
// the differential proof for every pre-existing field on these read routes.
func normalizeAuditLogDisplayNames(t *testing.T, request venueoracle.Request, body string) string {
	t.Helper()
	if request.Method != "GET" || !strings.Contains(request.Path, "/audit-logs") {
		return body
	}
	value, err := pyjson.DecodeString(body)
	if err != nil {
		return body
	}
	normalized, changed := normalizeAuditLogDisplayNamesValue(t, value)
	if !changed {
		return body
	}
	encoded, err := pyjson.Marshal(normalized)
	if err != nil {
		t.Fatalf("encode normalized audit response: %v", err)
	}
	return string(encoded)
}

func normalizeAuditLogDisplayNamesValue(t *testing.T, value pyjson.Value) (pyjson.Value, bool) {
	t.Helper()
	switch typed := value.(type) {
	case []pyjson.Value:
		out := make([]pyjson.Value, len(typed))
		changed := false
		for index, child := range typed {
			normalized, childChanged := normalizeAuditLogDisplayNamesValue(t, child)
			out[index] = normalized
			changed = changed || childChanged
		}
		if !changed {
			return value, false
		}
		return out, true
	case *pyjson.Object:
		if isAuditLogObject(typed) {
			return normalizeAuditLogObjectDisplayNames(t, typed), true
		}
		out := pyjson.NewObject()
		changed := false
		for _, key := range typed.Keys() {
			child, _ := typed.Get(key)
			normalized, childChanged := normalizeAuditLogDisplayNamesValue(t, child)
			out.Set(key, normalized)
			changed = changed || childChanged
		}
		if !changed {
			return value, false
		}
		return out, true
	default:
		return value, false
	}
}

func isAuditLogObject(object *pyjson.Object) bool {
	for _, key := range []string{"id", "org_id", "user_id", "action", "resource_type", "resource_id"} {
		if _, ok := object.Get(key); !ok {
			return false
		}
	}
	return true
}

func normalizeAuditLogObjectDisplayNames(t *testing.T, object *pyjson.Object) *pyjson.Object {
	t.Helper()
	out := pyjson.NewObject()
	for _, key := range object.Keys() {
		if key == "actor_display_name" || key == "resource_display_name" {
			continue
		}
		value, _ := object.Get(key)
		out.Set(key, value)
		if key != "resource_id" {
			continue
		}
		for _, name := range []string{"actor_display_name", "resource_display_name"} {
			if value, ok := object.Get(name); ok && value != nil {
				if _, ok := value.(string); !ok {
					t.Errorf("audit response %s = %s; want string or null", name, pyjson.Str(value))
				}
			}
			out.Set(name, nil)
		}
	}
	return out
}

// withoutServedMemberNames removes the Go-only user_name/user_email keys from
// the member list so the differential proof still compares every field the
// frozen Python response has. The Go-only member names test checks the values.
func withoutServedMemberNames(request venueoracle.Request, body string) string {
	if request.Method != "GET" || !strings.Contains(request.Path, "/members") {
		return body
	}
	value, err := pyjson.DecodeString(body)
	if err != nil {
		return body
	}
	items, ok := value.([]pyjson.Value)
	if !ok {
		return body
	}
	out := make([]pyjson.Value, len(items))
	for i, item := range items {
		object, ok := item.(*pyjson.Object)
		if !ok {
			return body
		}
		trimmed := pyjson.NewObject()
		for _, key := range object.Keys() {
			if key == "user_name" || key == "user_email" {
				continue
			}
			child, _ := object.Get(key)
			trimmed.Set(key, child)
		}
		out[i] = trimmed
	}
	encoded, err := pyjson.Marshal(out)
	if err != nil {
		return body
	}
	return string(encoded)
}

func TestWithoutServedMemberNamesKeepsTheLegacyMemberPayload(t *testing.T) {
	request := venueoracle.Request{Method: "GET", Path: "/api/v1/admin/orgs/o/members"}
	python := `[{"id":"m","org_id":"o","user_id":"u","role":"member"}]`
	goBody := `[{"id":"m","org_id":"o","user_id":"u","role":"member","user_name":"Ari","user_email":"a@example.com"}]`
	for name, body := range map[string]string{"python": python, "go": goBody} {
		if got := withoutServedMemberNames(request, body); got != python {
			t.Errorf("%s normalized member body\n got:  %s\n want: %s", name, got, python)
		}
	}
}
