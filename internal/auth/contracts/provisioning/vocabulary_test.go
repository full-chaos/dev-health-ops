package provisioning

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

type schemaNode struct {
	Const         string                 `json:"const"`
	Enum          []string               `json:"enum"`
	Pattern       string                 `json:"pattern"`
	Type          string                 `json:"type"`
	MinItems      *int                   `json:"minItems"`
	MaxItems      *int                   `json:"maxItems"`
	MaxProperties *int                   `json:"maxProperties"`
	Items         *schemaNode            `json:"items"`
	PropertyNames *schemaNode            `json:"propertyNames"`
	Properties    map[string]*schemaNode `json:"properties"`
	AllOf         []struct {
		If   *schemaNode `json:"if"`
		Then *schemaNode `json:"then"`
	} `json:"allOf"`
}

func loadProvisioningSchema(t *testing.T) schemaNode {
	t.Helper()
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(contracts.SchemaPath(root, contracts.ProvisioningEventSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema schemaNode
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

func sorted(v []string) []string {
	out := append([]string(nil), v...)
	sort.Strings(out)
	return out
}

// TestVocabularyMatchesTheSchema: the Go table producers check
// against is the schema's, in both directions, so a verb or member added to
// one and not the other fails here rather than at a consumer.
func TestVocabularyMatchesTheSchema(t *testing.T) {
	schema := loadProvisioningSchema(t)
	enum := schema.Properties["aggregate_type"].Enum
	if len(enum) == 0 {
		t.Fatal("the schema names no aggregate_type; this test proves nothing")
	}
	if got, want := AggregateTypes(), sorted(enum); !reflect.DeepEqual(got, want) {
		t.Fatalf("aggregate types: Go %v, schema %v", got, want)
	}

	branches := map[string]*schemaNode{}
	for _, branch := range schema.AllOf {
		branches[branch.If.Properties["aggregate_type"].Const] = branch.Then
	}
	for _, name := range enum {
		then, ok := branches[name]
		if !ok {
			t.Errorf("%s has no conditional branch in the schema", name)
			continue
		}
		a, _ := AggregateOf(name)
		if want := "^" + a.IDPrefix + "[A-Za-z0-9_-]{1,64}$"; then.Properties["aggregate_id"].Pattern != want {
			t.Errorf("%s id pattern: schema %q, Go prefix %q", name, then.Properties["aggregate_id"].Pattern, a.IDPrefix)
		}
		if got, want := sorted(a.Events), sorted(then.Properties["event_type"].Enum); !reflect.DeepEqual(got, want) {
			t.Errorf("%s events: Go %v, schema %v", name, got, want)
		}
		change := then.Properties["payload"].Properties["change"]
		var members []string
		switch {
		case change.PropertyNames != nil:
			members = change.PropertyNames.Enum
		case change.MaxProperties != nil && *change.MaxProperties == 0:
		default:
			t.Errorf("%s change is neither an allowlist nor empty in the schema", name)
		}
		if got, want := sorted(a.Change), sorted(members); !reflect.DeepEqual(got, want) {
			t.Errorf("%s change members: Go %v, schema %v", name, got, want)
		}
	}
	if len(branches) != len(enum) {
		t.Errorf("schema has %d branches for %d aggregate types", len(branches), len(enum))
	}

	defined := schema.Properties["payload"].Properties["change"].Properties
	var schemaMembers, goMembers []string
	for name := range defined {
		schemaMembers = append(schemaMembers, name)
	}
	for name := range provisioningChangeMembers {
		goMembers = append(goMembers, name)
	}
	if !reflect.DeepEqual(sorted(goMembers), sorted(schemaMembers)) {
		t.Fatalf("change members: Go %v, schema %v", sorted(goMembers), sorted(schemaMembers))
	}
	for name, node := range defined {
		rule := provisioningChangeMembers[name]
		pattern, enumValues := node.Pattern, node.Enum
		if node.Type == "array" {
			if !rule.array || *node.MinItems != rule.minItems || *node.MaxItems != rule.maxItems {
				t.Errorf("change.%s array bounds differ from the schema", name)
			}
			pattern = node.Items.Pattern
		}
		if rule.pattern != pattern || !reflect.DeepEqual(sorted(rule.enum), sorted(enumValues)) {
			t.Errorf("change.%s rule: Go pattern %q enum %v, schema pattern %q enum %v", name, rule.pattern, rule.enum, pattern, enumValues)
		}
	}
}

// TestCheckEventRefusesWhatTheSchemaRefuses: each refusal is
// checked against the schema too, so the Go check is never stricter or looser
// on these shapes than the contract.
func TestCheckEventRefusesWhatTheSchemaRefuses(t *testing.T) {
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	id := "0b6f1c3e-8e4f-4b7a-9d2a-3c5e7f9a1b2d"
	cases := []struct {
		name                 string
		aggType, aggID, verb string
		change               map[string]any
		wantOK               bool
	}{
		{"valid principal", "principal", "prn_" + id, "principal.created", map[string]any{"status": "active"}, true},
		{"valid user email change", "user", "prn_" + id, "user.email_changed", map[string]any{}, true},
		{"valid organization slug", "organization", "org_" + id, "organization.updated", map[string]any{"slug": "acme-co"}, true},
		{"unknown aggregate", "session", "prn_" + id, "session.revoked", map[string]any{}, false},
		{"raw uuid id", "principal", id, "principal.created", map[string]any{}, false},
		{"id of another prefix", "organization", "prn_" + id, "organization.created", map[string]any{}, false},
		{"verb of another aggregate", "user", "prn_" + id, "principal.created", map[string]any{}, false},
		{"member of another aggregate", "user", "prn_" + id, "user.created", map[string]any{"status": "active"}, false},
		{"status outside vocabulary", "principal", "prn_" + id, "principal.disabled", map[string]any{"status": "locked"}, false},
		{"slug outside pattern", "organization", "org_" + id, "organization.updated", map[string]any{"slug": "Café_東京"}, false},
		{"provider uuid", "external_identity", "idn_" + id, "external_identity.linked", map[string]any{"provider": id}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			goErr := CheckEvent(c.aggType, c.aggID, c.verb, c.change)
			change, _ := json.Marshal(c.change)
			doc := `{"schema_version":"provisioning-event.v1","aggregate_type":"` + c.aggType + `","aggregate_id":"` + c.aggID +
				`","event_type":"` + c.verb + `","idempotency_key":"` + c.aggType + `:` + c.aggID + `:1","created_at":"2026-09-03T10:00:00Z","payload":{"revision":1,"change":` + string(change) + `}}`
			_, schemaErr := contracts.ParseProvisioningEvent(root, []byte(doc))
			if (goErr == nil) != c.wantOK {
				t.Errorf("CheckEvent = %v, want ok=%v", goErr, c.wantOK)
			}
			if (schemaErr == nil) != c.wantOK {
				t.Errorf("schema disagrees: ParseProvisioningEvent = %v, want ok=%v", schemaErr, c.wantOK)
			}
		})
	}
	if Surface != contracts.ProvisioningEventSurface {
		t.Errorf("Surface %q is not contracts' %q", Surface, contracts.ProvisioningEventSurface)
	}
	if WireID("principal", id) != "prn_"+id || WireID("session", id) != "" || WireID("principal", "a b") != "" {
		t.Error("WireID renders a wrong id")
	}
}
