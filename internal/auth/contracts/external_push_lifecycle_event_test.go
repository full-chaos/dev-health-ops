package contracts

import (
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// forbiddenMemberWords are the words a member name of this surface may not
// contain. A name that must contain one is in allowedForbiddenWordMembers,
// which is closed and reviewed.
var forbiddenMemberWords = regexp.MustCompile(`(?i)(secret|hash|token|key|bearer|password|passwd|plaintext|digest|credential_value|passphrase|verifier|signature)`)

var allowedForbiddenWordMembers = map[string]string{
	"idempotency_key": "the outbox dedupe key: aggregate_type:aggregate_id:revision, not key material",
}

func externalPushSchema(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), ExternalPushLifecycleEventSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

// propertyNamesDeep collects every member name declared under any "properties"
// object, anywhere in the schema.
func propertyNamesDeep(node any, names map[string]bool) {
	switch value := node.(type) {
	case map[string]any:
		if props, ok := value["properties"].(map[string]any); ok {
			for name, child := range props {
				names[name] = true
				propertyNamesDeep(child, names)
			}
		}
		for key, child := range value {
			if key == "properties" {
				continue
			}
			propertyNamesDeep(child, names)
		}
	case []any:
		for _, child := range value {
			propertyNamesDeep(child, names)
		}
	}
}

func TestExternalPushLifecycleSchemaForbidsSecretFields(t *testing.T) {
	names := map[string]bool{}
	propertyNamesDeep(externalPushSchema(t), names)
	if len(names) < 15 {
		t.Fatalf("collected only %d member names (%v): the walk, not the schema, is probably wrong", len(names), names)
	}
	for name := range names {
		if !forbiddenMemberWords.MatchString(name) {
			continue
		}
		if _, ok := allowedForbiddenWordMembers[name]; !ok {
			t.Errorf("member %q suggests secret material; only %v may", name, allowedForbiddenWordMembers)
		}
	}
	for allowed := range allowedForbiddenWordMembers {
		if !names[allowed] {
			t.Errorf("allowlist entry %q is not a member of the schema", allowed)
		}
	}
	// The data objects are closed: the envelope, the payload and the change
	// object of every event type.
	schema := externalPushSchema(t)
	if schema["additionalProperties"] != false {
		t.Error("the envelope is not closed")
	}
	payload := schema["properties"].(map[string]any)["payload"].(map[string]any)
	if payload["additionalProperties"] != false {
		t.Error("the payload is not closed")
	}
	branches := schema["allOf"].([]any)
	if len(branches) != 5 {
		t.Fatalf("%d event-type branches, want 5", len(branches))
	}
	for index, branch := range branches {
		then := branch.(map[string]any)["then"].(map[string]any)["properties"].(map[string]any)
		change := then["payload"].(map[string]any)["properties"].(map[string]any)["change"].(map[string]any)
		if change["additionalProperties"] != false {
			t.Errorf("the change object of branch %d is not closed", index)
		}
	}
}

func TestTheSecretNameWalkFlagsAPlantedMember(t *testing.T) {
	schema := externalPushSchema(t)
	props := schema["properties"].(map[string]any)
	props["secret_hash"] = map[string]any{"type": "string"}
	names := map[string]bool{}
	propertyNamesDeep(schema, names)
	if !names["secret_hash"] || !forbiddenMemberWords.MatchString("secret_hash") {
		t.Fatal("a planted secret_hash member was not seen by the walk")
	}
	for _, planted := range []string{"token", "api_key", "password", "bearer", "token_hash", "verifier"} {
		if !forbiddenMemberWords.MatchString(planted) {
			t.Errorf("%q is not flagged", planted)
		}
	}
}

// TestExternalPushLifecycleTypesCarryEverySchemaMember: the parsed types and
// the schema declare the same member sets, so the parser never silently drops
// data the schema admits.
func TestExternalPushLifecycleTypesCarryEverySchemaMember(t *testing.T) {
	schema := externalPushSchema(t)
	members := func(node map[string]any) []string {
		var out []string
		for name := range node {
			out = append(out, name)
		}
		sort.Strings(out)
		return out
	}
	tags := func(typ reflect.Type) []string {
		var out []string
		for i := 0; i < typ.NumField(); i++ {
			out = append(out, strings.Split(typ.Field(i).Tag.Get("json"), ",")[0])
		}
		sort.Strings(out)
		return out
	}
	top := schema["properties"].(map[string]any)
	payload := top["payload"].(map[string]any)["properties"].(map[string]any)
	change := map[string]any{}
	for _, branch := range schema["allOf"].([]any) {
		then := branch.(map[string]any)["then"].(map[string]any)["properties"].(map[string]any)
		props := then["payload"].(map[string]any)["properties"].(map[string]any)["change"].(map[string]any)["properties"].(map[string]any)
		for name, v := range props {
			change[name] = v
		}
	}
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want []string
	}{
		{"envelope", reflect.TypeOf(ExternalPushLifecycleEvent{}), members(top)},
		{"payload", reflect.TypeOf(ExternalPushLifecyclePayload{}), members(payload)},
		{"change", reflect.TypeOf(ExternalPushLifecycleChange{}), members(change)},
	} {
		if got := tags(tc.typ); strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s: type members %v, schema members %v", tc.name, got, tc.want)
		}
	}
}

func TestExternalPushLifecycleIdempotencyKeyIsAggregateAndRevision(t *testing.T) {
	event := ExternalPushLifecycleEvent{AggregateType: ExternalPushCredentialAggregate, AggregateID: "cred-1", Payload: ExternalPushLifecyclePayload{Revision: 7}}
	if got, want := event.DedupeKey(), "external_push_credential:cred-1:7"; got != want {
		t.Fatalf("DedupeKey = %q, want %q", got, want)
	}
}

func TestExternalPushLifecycleEveryEventTypeHasAGolden(t *testing.T) {
	c := loadWireCorpus(t, provisioningSurfaces[2])
	seen := map[string]bool{}
	for _, a := range c.Accept {
		event, err := ParseExternalPushLifecycleEvent(testRoot(t), readCorpusFile(t, provisioningSurfaces[2], a.File))
		if err != nil {
			t.Fatalf("%s: %v", a.File, err)
		}
		seen[event.EventType] = true
	}
	for _, want := range []string{ExternalPushCreated, ExternalPushScoped, ExternalPushRotated, ExternalPushRevoked, ExternalPushExpired} {
		if !seen[want] {
			t.Errorf("no accept golden for %s", want)
		}
	}
}
