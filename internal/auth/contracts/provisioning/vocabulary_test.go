package provisioning

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
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
	Required      []string               `json:"required"`
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
		if got, want := sorted(a.Required), sorted(change.Required); !reflect.DeepEqual(got, want) {
			t.Errorf("%s required change members: Go %v, schema %v", name, got, want)
		}
		perEvent := map[string][]string{}
		for _, nested := range then.AllOf {
			event := nested.If.Properties["event_type"].Const
			perEvent[event] = nested.Then.Properties["payload"].Properties["change"].Required
		}
		if len(perEvent) != len(a.EventRequired) {
			t.Errorf("%s per-event required: Go %v, schema %v", name, a.EventRequired, perEvent)
		}
		for event, want := range perEvent {
			if !reflect.DeepEqual(sorted(a.EventRequired[event]), sorted(want)) {
				t.Errorf("%s %s required change members: Go %v, schema %v", name, event, a.EventRequired[event], want)
			}
		}
		for member, node := range change.Properties {
			if node.Const != a.Fixed[member] {
				t.Errorf("%s change.%s fixed value: Go %q, schema %q", name, member, a.Fixed[member], node.Const)
			}
		}
		for member := range a.Fixed {
			if change.Properties[member] == nil {
				t.Errorf("%s change.%s is fixed in Go and not in the schema", name, member)
			}
		}
	}
	if len(branches) != len(enum) {
		t.Errorf("schema has %d branches for %d aggregate types", len(branches), len(enum))
	}

	if want := "^[a-z_]{1,32}:[A-Za-z0-9_-]{1," + strconv.Itoa(MaxWireIDLength) + "}:[0-9]{1,16}$"; schema.Properties["idempotency_key"].Pattern != want {
		t.Errorf("idempotency_key pattern %q does not bound the id at MaxWireIDLength %d", schema.Properties["idempotency_key"].Pattern, MaxWireIDLength)
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
		{"unknown aggregate", "refresh_credential", "prn_" + id, "refresh_credential.revoked", map[string]any{}, false},
		{"session revoked", "session", "ses_" + id, "session.revoked", map[string]any{"status": "revoked", "reason": "caller"}, true},
		{"session event is only revoked", "session", "ses_" + id, "session.created", map[string]any{"status": "revoked", "reason": "caller"}, false},
		{"session status is revoked", "session", "ses_" + id, "session.revoked", map[string]any{"status": "active", "reason": "caller"}, false},
		{"session reason is closed", "session", "ses_" + id, "session.revoked", map[string]any{"status": "revoked", "reason": "logged out"}, false},
		{"session without reason", "session", "ses_" + id, "session.revoked", map[string]any{"status": "revoked"}, false},
		{"session id of another prefix", "session", "prn_" + id, "session.revoked", map[string]any{"status": "revoked", "reason": "caller"}, false},
		{"principal scope revoke", "principal", "prn_" + id, "principal.sessions_revoked", map[string]any{"revoked_before": "2026-09-03T09:59:59.123456Z", "reason": "password_changed"}, true},
		{"principal scope revoke without boundary", "principal", "prn_" + id, "principal.sessions_revoked", map[string]any{"reason": "caller"}, false},
		{"principal scope revoke without reason", "principal", "prn_" + id, "principal.sessions_revoked", map[string]any{"revoked_before": "2026-09-03T09:59:59Z"}, false},
		{"principal scope revoke impossible date", "principal", "prn_" + id, "principal.sessions_revoked", map[string]any{"revoked_before": "2026-04-31T00:00:00Z", "reason": "caller"}, false},
		{"principal created needs no boundary", "principal", "prn_" + id, "principal.created", map[string]any{"status": "active"}, true},
		{"organization scope revoke", "organization", "org_" + id, "organization.sessions_revoked", map[string]any{"revoked_before": "2026-09-03T09:59:59Z", "reason": "org_ended"}, true},
		{"organization scope revoke without boundary", "organization", "org_" + id, "organization.sessions_revoked", map[string]any{"reason": "org_ended"}, false},
		{"signing key compromised", "signing_key", "key_" + id, "signing_key.compromised", map[string]any{"kid": "example.key-2026-09"}, true},
		{"signing key without kid", "signing_key", "key_" + id, "signing_key.compromised", map[string]any{}, false},
		{"signing key is only compromised", "signing_key", "key_" + id, "signing_key.activated", map[string]any{"kid": "k1"}, false},
		{"signing key kid with a slash", "signing_key", "key_" + id, "signing_key.compromised", map[string]any{"kid": "a/b"}, false},
		{"raw uuid id", "principal", id, "principal.created", map[string]any{}, false},
		{"id of another prefix", "organization", "prn_" + id, "organization.created", map[string]any{}, false},
		{"verb of another aggregate", "user", "prn_" + id, "principal.created", map[string]any{}, false},
		{"member of another aggregate", "user", "prn_" + id, "user.created", map[string]any{"status": "active"}, false},
		{"status outside vocabulary", "principal", "prn_" + id, "principal.disabled", map[string]any{"status": "locked"}, false},
		{"slug outside pattern", "organization", "org_" + id, "organization.updated", map[string]any{"slug": "Café_東京"}, false},
		{"provider uuid", "external_identity", "idn_" + id, "external_identity.linked", map[string]any{"provider": id, "principal_id": "prn_" + id}, false},
		{"identity link names its principal", "external_identity", "idn_" + id, "external_identity.linked", map[string]any{"provider": "google", "principal_id": "prn_" + id}, true},
		{"identity unlink names its principal", "external_identity", "idn_" + id, "external_identity.unlinked", map[string]any{"principal_id": "prn_" + id}, true},
		{"identity link without its principal", "external_identity", "idn_" + id, "external_identity.linked", map[string]any{"provider": "google"}, false},
		{"identity principal of another prefix", "external_identity", "idn_" + id, "external_identity.unlinked", map[string]any{"principal_id": "org_" + id}, false},
		{"id at the key bound", "principal", "prn_" + strings.Repeat("a", 60), "principal.created", map[string]any{}, true},
		{"id past the key bound", "principal", "prn_" + strings.Repeat("a", 61), "principal.created", map[string]any{}, false},
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
	if WireID("principal", id) != "prn_"+id || WireID("refresh_credential", id) != "" || WireID("principal", "a b") != "" ||
		WireID("principal", strings.Repeat("a", 60)) == "" || WireID("principal", strings.Repeat("a", 61)) != "" {
		t.Error("WireID renders a wrong id")
	}
}

func TestTheWorkloadBootstrapEventCarriesAStatusAndACredentialIDOnly(t *testing.T) {
	const prn = "prn_00000000-0000-4000-8000-000000000001"
	const crd = "crd_00000000-0000-4000-8000-000000000002"
	for _, status := range []string{"active", "revoked", "expired"} {
		if err := CheckEvent("workload", prn, "workload.bootstrap_changed", map[string]any{"status": status, "credential_id": crd}); err != nil {
			t.Errorf("status %s refused: %v", status, err)
		}
	}
	if err := CheckEvent("workload", prn, "workload.bootstrap_changed", map[string]any{"status": "revoked"}); err != nil {
		t.Errorf("a disable event without a credential id refused: %v", err)
	}
	refused := map[string]struct {
		aggregate, id, event string
		change               map[string]any
	}{
		"an overlap status":                  {"workload", prn, "workload.bootstrap_changed", map[string]any{"status": "overlap"}},
		"a principal id as the credential":   {"workload", prn, "workload.bootstrap_changed", map[string]any{"credential_id": prn}},
		"a bootstrap value as the id":        {"workload", prn, "workload.bootstrap_changed", map[string]any{"credential_id": "fcwlb_" + strings.Repeat("A", 43)}},
		"a digest member":                    {"workload", prn, "workload.bootstrap_changed", map[string]any{"secret_hash": "00"}},
		"the event on another aggregate":     {"service_account", prn, "workload.bootstrap_changed", map[string]any{"status": "active"}},
		"credential_id on another aggregate": {"organization", "org_00000000-0000-4000-8000-000000000003", "organization.updated", map[string]any{"credential_id": crd}},
	}
	for name, c := range refused {
		if err := CheckEvent(c.aggregate, c.id, c.event, c.change); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestTheServiceAccountKeyEventCarriesAStatusAndACredentialIDOnly(t *testing.T) {
	const prn = "prn_00000000-0000-4000-8000-000000000001"
	const crd = "crd_00000000-0000-4000-8000-000000000002"
	for _, status := range []string{"active", "revoked", "expired"} {
		if err := CheckEvent("service_account", prn, "service_account.key_changed", map[string]any{"status": status, "credential_id": crd}); err != nil {
			t.Errorf("status %s refused: %v", status, err)
		}
	}
	refused := map[string]struct {
		aggregate, id, event string
		change               map[string]any
	}{
		"an overlap status":                {"service_account", prn, "service_account.key_changed", map[string]any{"status": "overlap", "credential_id": crd}},
		"a principal id as the credential": {"service_account", prn, "service_account.key_changed", map[string]any{"status": "active", "credential_id": prn}},
		"a key value as the id":            {"service_account", prn, "service_account.key_changed", map[string]any{"status": "active", "credential_id": "fcsak_" + strings.Repeat("A", 43)}},
		"a digest member":                  {"service_account", prn, "service_account.key_changed", map[string]any{"secret_hash": "00"}},
		"the event on another aggregate":   {"workload", prn, "service_account.key_changed", map[string]any{"status": "active"}},
	}
	for name, c := range refused {
		if err := CheckEvent(c.aggregate, c.id, c.event, c.change); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// TestOnlyTerminalAggregatesAreOneShot: a one-shot aggregate keeps no revision counter, so it may carry exactly one
// event verb (a terminal fact) and no other aggregate is one-shot.
func TestOnlyTerminalAggregatesAreOneShot(t *testing.T) {
	for _, name := range AggregateTypes() {
		a, _ := AggregateOf(name)
		if want := name == "session"; a.OneShot != want || IsOneShot(name) != want {
			t.Errorf("%s one-shot = %v, want %v", name, a.OneShot, want)
		}
		if a.OneShot && len(a.Events) != 1 {
			t.Errorf("%s is one-shot and has %d events, want exactly one", name, len(a.Events))
		}
	}
	if IsOneShot("refresh_credential") {
		t.Error("an unknown aggregate type is one-shot")
	}
}
