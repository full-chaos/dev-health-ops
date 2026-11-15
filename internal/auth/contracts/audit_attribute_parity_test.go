package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// schemaOnlyAuditKeys are the audit-event.v1 attribute members that no
// producer writes. Each stays in the published v1 schema (removing a member
// would break a consumer) and is held out of the allowlist == schema pin with a
// reason. A key leaves this table only by gaining a writer and an allowlist
// entry; a key that is in the allowlist and in this table fails the pin.
var schemaOnlyAuditKeys = map[string]string{
	"action":                           "published in v1 for the decision audit shape; no producer writes it (the authorize row names its action as action_key)",
	"aggregate_type":                   "published in v1 for a provisioning aggregate; no audit producer writes it",
	"aggregate_revision":               "published in v1 for a provisioning aggregate; no audit producer writes it",
	"decision_internal_reason":         "kept in v1 as a legacy optional attribute of rows stored before the wire named allowed_by_platform_admin; no producer writes it and the writer allowlist refuses it",
	"purged_email_verification_tokens": "published in v1 for the auth.legacy_link_tokens.purged row; its producer, the legacy link-token purge verb, is retired with go-api's link-token code",
	"purged_password_reset_tokens":     "published in v1 for the auth.legacy_link_tokens.purged row; its producer, the legacy link-token purge verb, is retired with go-api's link-token code",
	"run_id":                           "published in v1 for the auth.legacy_link_tokens.purged row; its producer, the legacy link-token purge verb, is retired with go-api's link-token code",
}

// auditKeyWriters is the census: for every allowlist key, a non-test file on
// main that writes it as an audit attribute. The test reads the file and
// requires the quoted key in it, so a key whose last writer is deleted fails.
var auditKeyWriters = map[string]string{
	"reason_code":          "internal/auth/membership/membership.go",
	"target_kind":          "internal/auth/membership/membership.go",
	"target_id":            "internal/auth/membership/membership.go",
	"action_key":           "internal/auth/authapi/authorize_audit.go",
	"resource_kind":        "internal/auth/grant/store.go",
	"provider":             "internal/auth/authapi/login.go",
	"changed_fields":       "internal/auth/membership/membership.go",
	"previous_status":      "internal/auth/membership/membership.go",
	"new_status":           "internal/auth/membership/membership.go",
	"previous_role":        "internal/auth/membership/membership.go",
	"new_role":             "internal/auth/membership/membership.go",
	"credential_class":     "internal/auth/session/auditrows.go",
	"credential_id":        "internal/audit/caller.go",
	"source":               "internal/auth/session/auditrows.go",
	"amr":                  "internal/auth/authapi/issue.go",
	"key_id":               "internal/auth/keyrotation/store.go",
	"related_key_id":       "internal/auth/keyrotation/store.go",
	"rotation_verb":        "internal/auth/keyrotation/store.go",
	"rotation_refusal":     "internal/auth/keyrotation/store.go",
	"client_class":         "internal/auth/oauthgrant/store.go",
	"jti":                  "internal/auth/workloadexchange/exchange.go",
	"principal_id":         "internal/auth/workloadexchange/exchange.go",
	"binding_id":           "internal/auth/workloadexchange/exchange.go",
	"revision":             "internal/auth/workloadexchange/exchange.go",
	"audience":             "internal/auth/workloadexchange/exchange.go",
	"actions":              "internal/auth/workloadexchange/exchange.go",
	"assertion_key_id":     "internal/auth/workload/assertion_keys.go",
	"workload_jti":         "internal/audit/caller.go",
	"caller_principal_id":  "internal/audit/caller.go",
	"principal_enabled":    "internal/auth/workload/enable.go",
	"binding_disabled":     "internal/auth/workload/rebind.go",
	"decision_id":          "internal/auth/authapi/authorize_audit.go",
	"decision_reason_code": "internal/auth/authapi/authorize_audit.go",
	"resource_type":        "internal/auth/authapi/authorize_audit.go",
	"resource_id":          "internal/auth/authapi/authorize_audit.go",
	"policy_revision":      "internal/auth/authapi/authorize_audit.go",
}

// plannedAuditKeys are allowlist keys with no writer on main that an open PR
// writes; that PR owns them. A key moves to auditKeyWriters when its writer merges.
var plannedAuditKeys = map[string]string{}

func auditSchemaAttributes(t *testing.T) map[string]map[string]any {
	t.Helper()
	schema := readJSONFile(t, "contracts/auth/v1/jsonschema/audit-event.v1.schema.json")
	props := schema["properties"].(map[string]any)["attributes"].(map[string]any)["properties"].(map[string]any)
	out := map[string]map[string]any{}
	for k, v := range props {
		out[k] = v.(map[string]any)
	}
	return out
}

func TestAuditAllowlistKeysEqualSchemaAttributeKeys(t *testing.T) {
	schema := auditSchemaAttributes(t)
	allow := map[string]bool{}
	for _, k := range audit.AllowedAttributeKeys() {
		allow[k] = true
	}
	for k := range allow {
		if _, ok := schema[k]; !ok {
			t.Errorf("allowlist key %q is not an audit-event.v1 attribute: a producer row that carries it fails the contract", k)
		}
		if reason, ok := schemaOnlyAuditKeys[k]; ok {
			t.Errorf("allowlist key %q is also in schemaOnlyAuditKeys (%s): remove it from the table", k, reason)
		}
	}
	for k := range schema {
		if !allow[k] && schemaOnlyAuditKeys[k] == "" {
			t.Errorf("audit-event.v1 attribute %q is not in the allowlist and not in schemaOnlyAuditKeys: add the producer key or give the exclusion a reason", k)
		}
	}
	for k, reason := range schemaOnlyAuditKeys {
		if _, ok := schema[k]; !ok {
			t.Errorf("schemaOnlyAuditKeys names %q, which the schema does not have", k)
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("schemaOnlyAuditKeys[%q] has no reason", k)
		}
	}
}

func TestEveryAuditAllowlistKeyHasAWriter(t *testing.T) {
	root := testRoot(t)
	for _, key := range audit.AllowedAttributeKeys() {
		file, written := auditKeyWriters[key]
		_, planned := plannedAuditKeys[key]
		switch {
		case written && planned:
			t.Errorf("key %q is in both the writer table and the planned table", key)
		case planned:
		case !written:
			t.Errorf("allowlist key %q has no writer on main and no open PR plans one: delete it from the allowlist", key)
		default:
			raw, err := os.ReadFile(filepath.Join(root, file))
			if err != nil {
				t.Errorf("key %q: writer file: %v", key, err)
			} else if !strings.Contains(string(raw), `"`+key+`"`) {
				t.Errorf("key %q: %s does not write it", key, file)
			}
		}
	}
	for key := range auditKeyWriters {
		if !slices.Contains(audit.AllowedAttributeKeys(), key) {
			t.Errorf("writer table names %q, which is not an allowlist key", key)
		}
	}
	for key := range plannedAuditKeys {
		if !slices.Contains(audit.AllowedAttributeKeys(), key) {
			t.Errorf("planned table names %q, which is not an allowlist key", key)
		}
	}
}

func asStrings(t *testing.T, v any) []string {
	t.Helper()
	list, _ := v.([]any)
	var out []string
	for _, item := range list {
		out = append(out, item.(string))
	}
	sort.Strings(out)
	return out
}

func TestAuditAttributeRulesMatchTheSchemaTypes(t *testing.T) {
	schema := auditSchemaAttributes(t)
	vocab := audit.AttributeVocabulary()
	ids := audit.AttributeIDPatterns()
	lists := audit.AttributeListPatterns()
	numbers := audit.AttributeNumberKeys()
	for _, key := range audit.AllowedAttributeKeys() {
		prop, ok := schema[key]
		if !ok {
			continue // the key-set pin reports it
		}
		switch {
		case audit.IsListVocabularyKey(key):
			items, _ := prop["items"].(map[string]any)
			want := append([]string(nil), vocab[key]...)
			sort.Strings(want)
			if prop["type"] != "array" || !reflect.DeepEqual(asStrings(t, items["enum"]), want) || prop["minItems"] != float64(1) {
				t.Errorf("%s: schema %v is not a non-empty array of the vocabulary", key, prop)
			}
		case key == "reason_code":
			// the schema enum is a superset (authorize decision codes); TestEveryReasonCodeTheAuditProducerAcceptsValidatesOnTheWire holds the producer set inside it
		case vocab[key] != nil:
			want := append([]string(nil), vocab[key]...)
			sort.Strings(want)
			if prop["type"] != "string" || !reflect.DeepEqual(asStrings(t, prop["enum"]), want) {
				t.Errorf("%s: schema enum %v, producer vocabulary %v", key, prop["enum"], want)
			}
		case ids[key] != "":
			if prop["type"] != "string" || prop["pattern"] != ids[key] {
				t.Errorf("%s: schema %v, producer id rule %s", key, prop, ids[key])
			}
		case lists[key] != "":
			items, _ := prop["items"].(map[string]any)
			if prop["type"] != "array" || items["pattern"] != lists[key] || prop["maxItems"] != float64(audit.MaxAttributeListLength) || prop["minItems"] != nil {
				t.Errorf("%s: schema %v, producer list rule %s (max %d, may be empty)", key, prop, lists[key], audit.MaxAttributeListLength)
			}
		case slices.Contains(numbers, key):
			if prop["type"] != "integer" || prop["minimum"] != float64(0) || prop["maximum"] != float64(9007199254740991) {
				t.Errorf("%s: schema %v, producer rule is a whole number 0..2^53-1", key, prop)
			}
		default:
			t.Errorf("%s: the producer has no value rule for the key", key)
		}
	}
}

var goodAuditIDSamples = map[string]string{
	"target_id": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e", "credential_id": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e",
	"principal_id": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e", "caller_principal_id": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e",
	"binding_id": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e", "assertion_key_id": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e",
	"jti": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e", "decision_id": "dec_EXAMPLE0000000000000000001", "resource_id": "acme/widgets", "action_key": "acr.context.read",
	"key_id": "kid-2026.11", "related_key_id": "kid-2026.10", "workload_jti": "AAAAAAAAAAAAAAAAAAAAAA",
}

func auditDocWith(attributes map[string]any) map[string]any {
	return map[string]any{
		"schema_version": "audit-event.v1", "event_type": "auth.example.row", "occurred_at": "2026-11-02T00:00:00Z",
		"outcome": "allowed", "attributes": attributes,
	}
}

// Both directions, per key: a value the producer accepts validates on the
// wire, and a value the producer refuses is refused on the wire too.
func TestAuditProducerValuesAgreeWithTheSchema(t *testing.T) {
	root := testRoot(t)
	vocab := audit.AttributeVocabulary()
	check := func(key string, value any, wantOK bool) {
		t.Helper()
		attrs := map[string]any{key: value}
		producer := audit.ValidateAttributes(attrs) == nil
		wire := Validate(root, AuditEventSurface, auditDocWith(attrs)) == nil
		if producer != wantOK || wire != wantOK {
			t.Errorf("%s=%v: producer accepts %v, audit-event.v1 accepts %v, want both %v", key, value, producer, wire, wantOK)
		}
	}
	for _, key := range audit.AllowedAttributeKeys() {
		switch {
		case audit.IsListVocabularyKey(key):
			for _, v := range vocab[key] {
				check(key, []string{v}, true)
			}
			check(key, []string{"not-in-the-vocabulary"}, false)
			check(key, []string{}, false)
		case vocab[key] != nil:
			for _, v := range vocab[key] {
				check(key, v, true)
			}
			check(key, "not-in-the-vocabulary", false)
			check(key, []string{vocab[key][0]}, false)
		case audit.AttributeIDPatterns()[key] != "":
			good, ok := goodAuditIDSamples[key]
			if !ok {
				t.Errorf("%s: no good id sample", key)
				continue
			}
			check(key, good, true)
			check(key, "bad value!", false)
		case audit.AttributeListPatterns()[key] != "":
			check(key, []string{"acr.context.read"}, true)
			check(key, []string{}, true)
			check(key, []string{"Bad Action"}, false)
			check(key, slices.Repeat([]string{"acr.context.read"}, audit.MaxAttributeListLength+1), false)
		case slices.Contains(audit.AttributeNumberKeys(), key):
			check(key, int64(3), true)
			check(key, int64(-1), false)
			check(key, "3", false)
		}
	}
}

func TestAuditProducerIDsProjectToTheWireForm(t *testing.T) {
	root := testRoot(t)
	const id = "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e"
	doc := auditDocWith(map[string]any{})
	doc["actor_principal_id"], doc["subject_principal_id"], doc["organization_id"] = id, id, id
	if err := Validate(root, AuditEventSurface, doc); err == nil {
		t.Error("a raw uuid in a row id member validates: the wire form is prefixed")
	}
	doc["actor_principal_id"], doc["subject_principal_id"], doc["organization_id"] = "prn_"+id, "prn_"+id, "org_"+id
	if err := Validate(root, AuditEventSurface, doc); err != nil {
		t.Errorf("the projected ids are refused: %v", err)
	}
}
