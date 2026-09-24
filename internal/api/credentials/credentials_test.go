package credentials

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func decodeObject(t *testing.T, text string) *pyjson.Object {
	t.Helper()
	value, err := pyjson.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return value.(*pyjson.Object)
}

func dump(t *testing.T, object *pyjson.Object) string {
	t.Helper()
	text, err := pyjson.Dumps(object)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

func TestNormalizeCredentialKeys(t *testing.T) {
	got := normalizeCredentialKeys("GitHub", decodeObject(t, `{"appId":"1","x":true,"privateKey":"k","installationId":"2","baseUrl":"u"}`))
	if want := `{"app_id": "1", "x": true, "private_key": "k", "installation_id": "2", "base_url": "u"}`; dump(t, got) != want {
		t.Errorf("github: %s", dump(t, got))
	}
	// A renamed key that collides with an existing one keeps the first
	// position and takes the later value, as the Python dict does.
	got = normalizeCredentialKeys("gitlab", decodeObject(t, `{"token":"a","baseUrl":"u1","base_url":"u2"}`))
	if want := `{"token": "a", "base_url": "u2"}`; dump(t, got) != want {
		t.Errorf("collision: %s", dump(t, got))
	}
	same := decodeObject(t, `{"apiKey":"k"}`)
	if normalizeCredentialKeys("custom", same) != same || dump(t, same) != `{"apiKey": "k"}` {
		t.Error("an unmapped provider is left alone")
	}
	if got := normalizeCredentialKeys("LINEAR", decodeObject(t, `{"apiKey":"k"}`)); dump(t, got) != `{"api_key": "k"}` {
		t.Errorf("the provider key is lower-cased: %s", dump(t, got))
	}
}

func TestPyEqual(t *testing.T) {
	cases := []struct {
		a, b  string
		equal bool
	}{
		{`{"a":1,"b":2}`, `{"b":2,"a":1}`, true},
		{`{"a":1}`, `{"a":true}`, true},
		{`{"a":1}`, `{"a":1.0}`, true},
		{`{"a":0}`, `{"a":false}`, true},
		{`{"a":1}`, `{"a":"1"}`, false},
		{`{"a":[1,2]}`, `{"a":[2,1]}`, false},
		{`{"a":1}`, `{"a":1,"b":2}`, false},
		{`{"a":null}`, `{"a":null}`, true},
		{`{"a":null}`, `{"a":0}`, false},
		{`{"a":12345678901234567890}`, `{"a":12345678901234567891}`, false},
		{`{"a":12345678901234567890}`, `{"a":12345678901234567890}`, true},
		{`{"a":{"b":[{"c":1}]}}`, `{"a":{"b":[{"c":true}]}}`, true},
		{`{"a":"x"}`, `{"a":"y"}`, false},
	}
	for _, c := range cases {
		if got := pyEqual(decodeObject(t, c.a), decodeObject(t, c.b)); got != c.equal {
			t.Errorf("pyEqual(%s, %s) = %v, want %v", c.a, c.b, got, c.equal)
		}
	}
}

func TestResponseJSONShape(t *testing.T) {
	at := time.Date(2026, 9, 1, 10, 0, 0, 123400000, time.UTC)
	c := credential{ID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), Provider: "github", Name: "n", IsActive: true, CreatedAt: at, UpdatedAt: at.Add(time.Second)}
	got, err := pyjson.MarshalModel(c.responseJSON())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"11111111-1111-4111-8111-111111111111","provider":"github","name":"n","is_active":true,"config":{},"last_test_at":null,"last_test_success":null,"last_test_error":null,"created_at":"2026-09-01T10:00:00.123400Z","updated_at":"2026-09-01T10:00:01.123400Z"}`
	if string(got) != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	failed, when, message := false, at, "boom"
	c.LastTestSuccess, c.LastTestAt, c.LastTestError = &failed, &when, &message
	c.Config = decodeObject(t, `{"b":1,"a":[true]}`)
	got, _ = pyjson.MarshalModel(c.responseJSON())
	want = `{"id":"11111111-1111-4111-8111-111111111111","provider":"github","name":"n","is_active":true,"config":{"b":1,"a":[true]},"last_test_at":"2026-09-01T10:00:00.123400Z","last_test_success":false,"last_test_error":"boom","created_at":"2026-09-01T10:00:00.123400Z","updated_at":"2026-09-01T10:00:01.123400Z"}`
	if string(got) != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
}

func TestRequiredDict(t *testing.T) {
	object := decodeObject(t, `{"a":{},"b":[],"c":null}`)
	var problems pybody.Errors
	if _, ok := requiredDict(&problems, object, "a"); !ok || len(problems) != 0 {
		t.Errorf("an object is accepted: %v", problems)
	}
	for _, name := range []string{"b", "c", "d"} {
		before := len(problems)
		if _, ok := requiredDict(&problems, object, name); ok || len(problems) != before+1 {
			t.Errorf("%s must be refused", name)
		}
	}
	if problems[0].Type != "dict_type" || problems[1].Type != "dict_type" || problems[2].Type != "missing" {
		t.Errorf("error types %v %v %v", problems[0].Type, problems[1].Type, problems[2].Type)
	}
}
