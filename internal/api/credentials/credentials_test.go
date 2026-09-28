package credentials

import (
	"strings"
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

// TestValidateJiraConfig is the CHAOS-7020 "validate on save" proof: a
// malformed config.atlassian_organization_id override is rejected, but its
// absence (the now-default auto-resolve path) and an explicit empty string
// (clearing a previous override) are both valid.
func TestValidateJiraConfig(t *testing.T) {
	cases := []struct {
		name     string
		json     string
		wantErr  bool
		wantText string
	}{
		{name: "absent key is valid", json: `{}`},
		{name: "nil config is valid", json: ``},
		{name: "empty string clears the override", json: `{"atlassian_organization_id": ""}`},
		{name: "whitespace-only clears the override", json: `{"atlassian_organization_id": "   "}`},
		{name: "bare UUID is valid", json: `{"atlassian_organization_id": "22125a4d-0000-4000-8000-000000000001"}`},
		{name: "uppercase UUID is valid", json: `{"atlassian_organization_id": "22125A4D-0000-4000-8000-000000000001"}`},
		{name: "ARI-wrapped UUID is valid", json: `{"atlassian_organization_id": "ari:cloud:platform::org/22125a4d-0000-4000-8000-000000000001"}`},
		{
			name: "a bare word is rejected", json: `{"atlassian_organization_id": "not-a-uuid"}`,
			wantErr: true, wantText: "config.atlassian_organization_id",
		},
		{
			name: "a wrong-provider ARI shape is rejected", json: `{"atlassian_organization_id": "ari:cloud:identity::team/22125a4d-0000-4000-8000-000000000001"}`,
			wantErr: true,
		},
		{
			name: "a non-string value is rejected", json: `{"atlassian_organization_id": 12345}`,
			wantErr: true, wantText: "must be a string",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var config *pyjson.Object
			if tc.json != "" {
				config = decodeObject(t, tc.json)
			}
			detail := validateJiraConfig(config)
			if tc.wantErr && detail == "" {
				t.Fatal("expected a validation error, got none")
			}
			if !tc.wantErr && detail != "" {
				t.Fatalf("expected no validation error, got %q", detail)
			}
			if tc.wantText != "" && !strings.Contains(detail, tc.wantText) {
				t.Errorf("detail = %q, want it to contain %q", detail, tc.wantText)
			}
		})
	}
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
