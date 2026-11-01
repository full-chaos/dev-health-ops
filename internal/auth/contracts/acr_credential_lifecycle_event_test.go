package contracts

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

const acrSurfaceIndex = 3

func acrSchema(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), AcrCredentialLifecycleEventSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

func acrChangeMembers(t *testing.T, schema map[string]any) map[string]any {
	t.Helper()
	change := map[string]any{}
	for _, branch := range schema["allOf"].([]any) {
		then := branch.(map[string]any)["then"].(map[string]any)["properties"].(map[string]any)
		props := then["payload"].(map[string]any)["properties"].(map[string]any)["change"].(map[string]any)["properties"].(map[string]any)
		for name, v := range props {
			change[name] = v
		}
	}
	return change
}

func TestAcrLifecycleSchemaForbidsSecretFields(t *testing.T) {
	schema := acrSchema(t)
	names := map[string]bool{}
	propertyNamesDeep(schema, names)
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
	for _, banned := range []string{"fingerprint", "token_prefix", "display_prefix", "device_code", "user_code", "principal_id"} {
		if names[banned] {
			t.Errorf("member %q must not exist on this surface", banned)
		}
	}
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

func TestAcrLifecycleFixturesCarryNoSecretShapedMember(t *testing.T) {
	s := provisioningSurfaces[acrSurfaceIndex]
	for _, entry := range loadWireCorpus(t, s).Accept {
		names := map[string]bool{}
		var walk func(node any)
		walk = func(node any) {
			switch v := node.(type) {
			case map[string]any:
				for name, child := range v {
					names[name] = true
					walk(child)
				}
			case []any:
				for _, child := range v {
					walk(child)
				}
			}
		}
		walk(decodedCorpusFile(t, s, entry.File))
		for name := range names {
			if _, ok := allowedForbiddenWordMembers[name]; !ok && forbiddenMemberWords.MatchString(name) {
				t.Errorf("%s: member %q suggests secret material", entry.File, name)
			}
		}
	}
}

func TestAcrLifecycleTypesCarryEverySchemaMember(t *testing.T) {
	schema := acrSchema(t)
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
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want []string
	}{
		{"envelope", reflect.TypeOf(AcrCredentialLifecycleEvent{}), members(top)},
		{"payload", reflect.TypeOf(AcrCredentialLifecyclePayload{}), members(payload)},
		{"change", reflect.TypeOf(AcrCredentialLifecycleChange{}), members(acrChangeMembers(t, schema))},
	} {
		if got := tags(tc.typ); strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s: type members %v, schema members %v", tc.name, got, tc.want)
		}
	}
}

func TestAcrLifecycleVocabulariesArePinned(t *testing.T) {
	schema := acrSchema(t)
	top := schema["properties"].(map[string]any)
	enum := func(node any) []string {
		var out []string
		for _, v := range node.(map[string]any)["enum"].([]any) {
			out = append(out, v.(string))
		}
		return out
	}
	if got, want := enum(top["event_type"]), []string{AcrCredentialIssued, AcrCredentialDeviceRedeemed, AcrCredentialRotated, AcrCredentialRevoked, AcrCredentialExpired}; !slices.Equal(got, want) {
		t.Errorf("event types %v, want %v", got, want)
	}
	payload := top["payload"].(map[string]any)["properties"].(map[string]any)
	if got, want := enum(payload["actor_type"]), []string{AcrActorUser, AcrActorOperator, AcrActorCredential, AcrActorSystem}; !slices.Equal(got, want) {
		t.Errorf("actor types %v, want %v", got, want)
	}
	classes := enum(acrChangeMembers(t, schema)["acr_class"])
	if len(classes) != 6 {
		t.Errorf("%d classes in the issued branch, want 6", len(classes))
	}
	if got := AcrClasses; len(got) != 6 {
		t.Errorf("AcrClasses has %d members, want 6", len(got))
	}
	if !slices.Equal(classes, AcrClasses) {
		t.Errorf("schema classes %v, Go classes %v", classes, AcrClasses)
	}
}

func TestAcrLifecycleIdempotencyKeyIsAggregateAndRevision(t *testing.T) {
	event := AcrCredentialLifecycleEvent{AggregateType: AcrCredentialAggregate, AggregateID: "cred_example", Payload: AcrCredentialLifecyclePayload{Revision: 7}}
	if got, want := event.DedupeKey(), "acr_credential:cred_example:7"; got != want {
		t.Fatalf("DedupeKey = %q, want %q", got, want)
	}
}

func TestAcrLifecycleEveryEventTypeClassAndActorHasAGolden(t *testing.T) {
	s := provisioningSurfaces[acrSurfaceIndex]
	seenEvent, seenClass, seenActor := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, a := range loadWireCorpus(t, s).Accept {
		event, err := ParseAcrCredentialLifecycleEvent(testRoot(t), readCorpusFile(t, s, a.File))
		if err != nil {
			t.Fatalf("%s: %v", a.File, err)
		}
		seenEvent[event.EventType] = true
		seenActor[event.Payload.ActorType] = true
		if event.Payload.Change.AcrClass != "" {
			seenClass[event.Payload.Change.AcrClass] = true
		}
	}
	for _, want := range []string{AcrCredentialIssued, AcrCredentialDeviceRedeemed, AcrCredentialRotated, AcrCredentialRevoked, AcrCredentialExpired} {
		if !seenEvent[want] {
			t.Errorf("no accept golden for %s", want)
		}
	}
	for _, want := range AcrClasses {
		if !seenClass[want] {
			t.Errorf("no accept golden for class %s", want)
		}
	}
	for _, want := range []string{AcrActorUser, AcrActorOperator, AcrActorCredential, AcrActorSystem} {
		if !seenActor[want] {
			t.Errorf("no accept golden for actor type %s", want)
		}
	}
}

func TestAcrLifecycleSelfActorIsCheckedByTheClient(t *testing.T) {
	s := provisioningSurfaces[acrSurfaceIndex]
	for _, tc := range []struct{ file, from, to string }{
		{"valid-revoked-self.json", `"actor_ref": "cred_exampleaaaaaaaa"`, `"actor_ref": "cred_examplebbbbbbbb"`},
		{"valid-rotated-self.json", `"actor_ref": "cred_exampleparentaaa"`, `"actor_ref": "cred_examplebbbbbbbb"`},
	} {
		raw := string(readCorpusFile(t, s, tc.file))
		if !strings.Contains(raw, tc.from) {
			t.Fatalf("%s: marker %q absent", tc.file, tc.from)
		}
		if _, err := ParseAcrCredentialLifecycleEvent(testRoot(t), []byte(raw)); err != nil {
			t.Fatalf("%s: %v", tc.file, err)
		}
		_, err := ParseAcrCredentialLifecycleEvent(testRoot(t), []byte(strings.Replace(raw, tc.from, tc.to, 1)))
		if err == nil || !strings.Contains(err.Error(), "does not equal") {
			t.Errorf("%s: a credential actor naming another credential: err = %v", tc.file, err)
		}
	}
}
