package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// The closed vocabularies of two wire fields (CHAOS-10194):
// contracts/auth/v1/vocabulary/reason-codes.json is the single source, and the
// behaviour under test is that every listed value validates and any other value
// -- a secret-shaped one included -- is refused by the schema.

type reasonVocabulary struct {
	RevokedReason   struct{ Values []string } `json:"revoked_reason"`
	AuditReasonCode struct{ Values []string } `json:"audit_reason_code"`
}

func loadReasonVocabulary(t *testing.T) reasonVocabulary {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), "contracts/auth/v1/vocabulary/reason-codes.json"))
	if err != nil {
		t.Fatalf("reading the reason-code vocabulary: %v", err)
	}
	var v reasonVocabulary
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.RevokedReason.Values) == 0 || len(v.AuditReasonCode.Values) == 0 {
		t.Fatal("the vocabulary is empty; every assertion below would be vacuous")
	}
	return v
}

func readJSONFile(t *testing.T, rel string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func withRevokedReason(t *testing.T, surface, value string) any {
	t.Helper()
	const ex = "contracts/auth/v1/examples/"
	switch surface {
	case CredentialEntrySurface:
		d := readJSONFile(t, ex+"credential-entry/valid-revoked.json")
		d["revoked_reason"] = value
		return d
	case CredentialListSurface:
		d := readJSONFile(t, ex+"credential-list/valid-two-items.json")
		d["items"].([]any)[1].(map[string]any)["revoked_reason"] = value
		return d
	}
	t.Fatalf("unknown surface %s", surface)
	return nil
}

func withAuditReasonCode(t *testing.T, value string) any {
	t.Helper()
	d := readJSONFile(t, "contracts/auth/v1/examples/audit-event/invalid-attributes-reason-code-prose.json")
	d["attributes"].(map[string]any)["reason_code"] = value
	return d
}

var notInTheVocabulary = []string{
	"secret_0123456789abcdef0123", // lowercase and secret-shaped: fits the old code pattern
	"key_compromised",             // a plausible code no producer emits yet
	"Compromised",
	"compromised ",
	"compromise",
	"owner_request_",
	"",
	"revoked because bob@example.test leaked it",
}

func TestRevokedReasonIsAClosedVocabulary(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	root := testRoot(t)
	for _, surface := range []string{CredentialEntrySurface, CredentialListSurface} {
		for _, v := range vocab.RevokedReason.Values {
			if err := Validate(root, surface, withRevokedReason(t, surface, v)); err != nil {
				t.Errorf("%s: listed value %q refused: %v", surface, v, err)
			}
		}
		for _, v := range notInTheVocabulary {
			if err := Validate(root, surface, withRevokedReason(t, surface, v)); err == nil {
				t.Errorf("%s: value %q is not in the vocabulary but validates", surface, v)
			}
		}
	}
}

func TestAuditReasonCodeIsAClosedVocabulary(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	root := testRoot(t)
	for _, v := range vocab.AuditReasonCode.Values {
		if err := Validate(root, AuditEventSurface, withAuditReasonCode(t, v)); err != nil {
			t.Errorf("audit-event: listed value %q refused: %v", v, err)
		}
	}
	for _, v := range notInTheVocabulary {
		if err := Validate(root, AuditEventSurface, withAuditReasonCode(t, v)); err == nil {
			t.Errorf("audit-event: value %q is not in the vocabulary but validates", v)
		}
	}
}

func TestEveryRevokedReasonIsAlsoAnAuditReasonCode(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	audit := map[string]bool{}
	for _, v := range vocab.AuditReasonCode.Values {
		audit[v] = true
	}
	for _, v := range vocab.RevokedReason.Values {
		if !audit[v] {
			t.Errorf("revoked_reason %q is missing from the audit vocabulary: a revoke event could not record it", v)
		}
	}
}

// Doc-sync: the enums written in the schemas equal the vocabulary file.
func TestSchemaEnumsEqualTheVocabularyFile(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	sorted := func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	}
	enumAt := func(schema string, path ...string) []string {
		node := any(readJSONFile(t, "contracts/auth/v1/jsonschema/"+schema))
		for _, step := range path {
			node = node.(map[string]any)[step]
			if node == nil {
				t.Fatalf("%s: no %v", schema, path)
			}
		}
		var out []string
		list, _ := node.(map[string]any)["enum"].([]any)
		for _, v := range list {
			out = append(out, v.(string))
		}
		return out
	}
	for _, c := range []struct {
		schema string
		path   []string
		want   []string
	}{
		{"credential-entry.v1.schema.json", []string{"properties", "revoked_reason"}, vocab.RevokedReason.Values},
		{"credential-list.v1.schema.json", []string{"properties", "items", "items", "properties", "revoked_reason"}, vocab.RevokedReason.Values},
		{"audit-event.v1.schema.json", []string{"properties", "attributes", "properties", "reason_code"}, vocab.AuditReasonCode.Values},
	} {
		if got := enumAt(c.schema, c.path...); !reflect.DeepEqual(sorted(got), sorted(c.want)) {
			t.Errorf("%s %v: enum %v, vocabulary %v", c.schema, c.path, got, c.want)
		}
	}
}

// The approved lists are written here by hand, on purpose. The vocabulary file
// and the three schemas are checked against each other, so widening all four
// together would otherwise pass unseen; a value enters the vocabulary only by a
// deliberate edit of this list, of the Python and ajv copies, and of the file.
// A value enters with the producer that emits it (CHAOS-10194).
var approvedRevokedReasons = []string{
	"compromised",
	"owner_request",
}

var approvedAuditReasonCodes = []string{
	"allowed_by_repository_grant",
	"grant_absent",
	"repository_not_granted",
	"platform_role_absent",
	"credential_missing",
	"rate_limited",
	"current_state_unavailable",
	"authority_unavailable",
	"upstream_unavailable",
	"not_found",
	"revision_conflict",
	"internal_error",
	"compromised",
	"owner_request",
	"role_granted",
	"role_revoked",
	"membership_added",
	"membership_removed",
	"membership_status_changed",
	"grant_created",
	"grant_revoked",
	"credential_issued",
	"credential_rotated",
	"credential_revoked",
	"session_revoked",
	"organization_created",
	"organization_updated",
	"principal_disabled",
	"principal_enabled",
	"principal_projected",
	"principal_provisioned",
	"email_verified",
	"password_changed",
	"identity_linked",
	"identity_unlinked",
	"identity_projected",
	"password_reset",
	"policy_revision_activated",
	"entitlement_snapshot_applied",
	"denied_no_membership",
	"denied_no_grant",
	"denied_policy",
	"accepted",
	"invalid_credentials",
	"account_locked",
	"principal_deactivated",
	"email_unverified",
	"email_collision",
	"signup_unavailable",
	"identity_conflict",
}

func TestTheVocabularyFileIsExactlyTheApprovedLists(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	for _, c := range []struct {
		name      string
		got, want []string
	}{
		{"revoked_reason", vocab.RevokedReason.Values, approvedRevokedReasons},
		{"audit_reason_code", vocab.AuditReasonCode.Values, approvedAuditReasonCodes},
	} {
		got := append([]string(nil), c.got...)
		want := append([]string(nil), c.want...)
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: vocabulary file %v, approved list %v", c.name, got, want)
		}
	}
}

// The producer owns its reason codes: every value internal/audit accepts in an
// audit attribute must be a value the audit-event wire form accepts.
func TestEveryReasonCodeTheAuditProducerAcceptsValidatesOnTheWire(t *testing.T) {
	producer := audit.AttributeVocabulary()["reason_code"]
	if len(producer) < 20 {
		t.Fatalf("the producer vocabulary has %d reason codes; the cross-check would be vacuous", len(producer))
	}
	root := testRoot(t)
	for _, v := range producer {
		if err := Validate(root, AuditEventSurface, withAuditReasonCode(t, v)); err != nil {
			t.Errorf("producer reason_code %q is refused by audit-event.v1: %v", v, err)
		}
	}
}
