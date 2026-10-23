package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
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
	DecisionReason  struct{ Values []string } `json:"decision_reason_code"`
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
	if len(v.RevokedReason.Values) == 0 || len(v.AuditReasonCode.Values) == 0 || len(v.DecisionReason.Values) == 0 {
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
		{"authorization_decision.v1.schema.json", []string{"properties", "reason_code"}, vocab.DecisionReason.Values},
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

// decisionReasonSources is the census table: every decision reason code, its
// outcome and the committed source that names it. A code without a source is
// not in the vocabulary; it returns as a row when the producer that emits it
// is added.
const batchResponseBase = "batch-response-valid-items-disagree.json"

var decisionReasonSources = []struct{ Code, Outcome, Source string }{
	{"allowed_by_repository_grant", "allow", "contracts/auth/v1/examples/authorization_decision/decision-valid-allow-with-obligations.json"},
	{"grant_absent", "deny", "contracts/auth/v1/examples/authorization_decision/decision-valid-deny.json"},
	{"repository_not_granted", "deny", "contracts/auth/v1/examples/authorization_batch_response/batch-response-valid-items-disagree.json"},
	{"platform_role_absent", "deny", "contracts/auth/v1/examples/authorization_decision/decision-valid-platform-decision-null-org.json"},
}

var approvedDecisionReasonCodes = func() []string {
	out := make([]string, 0, len(decisionReasonSources))
	for _, r := range decisionReasonSources {
		out = append(out, r.Code)
	}
	return out
}()

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
	"provider_already_linked",
	"expired",
	"principal_not_found",
	"malformed",
}

func TestTheVocabularyFileIsExactlyTheApprovedLists(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	for _, c := range []struct {
		name      string
		got, want []string
	}{
		{"revoked_reason", vocab.RevokedReason.Values, approvedRevokedReasons},
		{"audit_reason_code", vocab.AuditReasonCode.Values, approvedAuditReasonCodes},
		{"decision_reason_code", vocab.DecisionReason.Values, approvedDecisionReasonCodes},
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

func withDecisionReasonCode(t *testing.T, surface, value string) any {
	t.Helper()
	const ex = "contracts/auth/v1/examples/"
	switch surface {
	case AuthorizationDecisionSurface:
		d := readJSONFile(t, ex+"authorization_decision/decision-valid-deny.json")
		d["reason_code"] = value
		return d
	case AuthorizationBatchResponseSurface:
		d := readJSONFile(t, ex+"authorization_batch_response/"+batchResponseBase)
		d["decisions"].([]any)[0].(map[string]any)["decision"].(map[string]any)["reason_code"] = value
		return d
	}
	t.Fatalf("unknown surface %s", surface)
	return nil
}

func TestDecisionReasonCodeIsAClosedVocabulary(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	root := testRoot(t)
	for _, surface := range []string{AuthorizationDecisionSurface, AuthorizationBatchResponseSurface} {
		for _, v := range vocab.DecisionReason.Values {
			if err := Validate(root, surface, withDecisionReasonCode(t, surface, v)); err != nil {
				t.Errorf("%s: listed value %q refused: %v", surface, v, err)
			}
		}
		for _, v := range append([]string{"denied_unknown_thing", "Denied: bob@example.test lacks access to /v1/orgs/org_1"}, notInTheVocabulary...) {
			if err := Validate(root, surface, withDecisionReasonCode(t, surface, v)); err == nil {
				t.Errorf("%s: value %q is not in the vocabulary but validates", surface, v)
			}
		}
	}
}

func reasonCodeMembers(node any) []string {
	var out []string
	switch n := node.(type) {
	case map[string]any:
		for k, v := range n {
			if s, ok := v.(string); ok && k == "reason_code" {
				out = append(out, s)
				continue
			}
			out = append(out, reasonCodeMembers(v)...)
		}
	case []any:
		for _, v := range n {
			out = append(out, reasonCodeMembers(v)...)
		}
	}
	return out
}

func TestEveryDecisionReasonCodeHasAnExistingSourceAndAnOutcome(t *testing.T) {
	root := testRoot(t)
	for _, r := range decisionReasonSources {
		if r.Source == "" || (r.Outcome != "allow" && r.Outcome != "deny") {
			t.Errorf("%s: source %q outcome %q", r.Code, r.Source, r.Outcome)
			continue
		}
		var doc any
		raw, err := os.ReadFile(filepath.Join(root, r.Source))
		if err == nil {
			err = json.Unmarshal(raw, &doc)
		}
		if err != nil {
			t.Errorf("%s: source %s: %v", r.Code, r.Source, err)
			continue
		}
		if !slices.Contains(reasonCodeMembers(doc), r.Code) {
			t.Errorf("%s: source %s carries no reason_code member with the code", r.Code, r.Source)
		}
	}
}

// Every reason_code a committed decision or batch fixture carries, valid or
// not, is either in the vocabulary or sits in a fixture the manifest rejects.
func TestEveryValidDecisionFixtureUsesAListedReasonCode(t *testing.T) {
	vocab := loadReasonVocabulary(t)
	listed := map[string]bool{}
	for _, v := range vocab.DecisionReason.Values {
		listed[v] = true
	}
	for _, dir := range []string{"authorization_decision", "authorization_batch_response"} {
		files, err := filepath.Glob(filepath.Join(testRoot(t), "contracts/auth/v1/examples", dir, "*valid*.json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.Contains(filepath.Base(f), "invalid") {
				continue
			}
			raw, _ := os.ReadFile(f)
			for _, m := range regexp.MustCompile(`"reason_code": *"([^"]*)"`).FindAllStringSubmatch(string(raw), -1) {
				if !listed[m[1]] {
					t.Errorf("%s uses reason_code %q outside the vocabulary", filepath.Base(f), m[1])
				}
			}
		}
	}
}
