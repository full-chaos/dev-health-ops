package contracts

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
)

// privateJWKMembers are every private or secret member RFC 7517, 7518 section
// 6 and RFC 8037 define: `d` (EC/OKP private key, RSA private exponent), the
// RSA CRT members `p` `q` `dp` `dq` `qi`, and `k` (symmetric key value).
var privateJWKMembers = []string{"d", "p", "q", "dp", "dq", "qi", "k"}

func TestPrivateKeyMembersAreAbsentFromTheJWKSSchemaEntirely(t *testing.T) {
	// CHAOS-4930: publishing a private key through a public endpoint is the
	// failure this contract exists to prevent. The guarantee is subtractive --
	// the member is refused because it is not declared -- so the way to break
	// it is to DECLARE one, and nothing about a declared property fails a
	// fixture unless a fixture happens to use it. This reads the schema.
	raw, err := os.ReadFile(SchemaPath(testRoot(t), JWKSSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Keys struct {
				Items struct {
					AdditionalProperties any            `json:"additionalProperties"`
					Properties           map[string]any `json:"properties"`
				} `json:"items"`
			} `json:"keys"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	item := schema.Properties.Keys.Items
	if item.AdditionalProperties != false {
		t.Fatalf("keys[] additionalProperties = %v, want false: without it every undeclared private member is accepted", item.AdditionalProperties)
	}
	if len(item.Properties) == 0 {
		t.Fatal("keys[] declares no properties; the check below would pass vacuously")
	}
	for _, member := range privateJWKMembers {
		if _, declared := item.Properties[member]; declared {
			t.Errorf("private member %q is declared by the JWKS schema", member)
		}
	}
}

func TestEveryPrivateKeyMemberHasARejectFixture(t *testing.T) {
	// One fixture per member, found by content: the member must be the thing
	// that makes the document refuse. A member with no fixture is a member the
	// subtractive guarantee has never been executed against.
	covered := map[string]bool{}
	for _, entry := range loadJWKSManifest(t).Reject {
		var document struct {
			Keys []map[string]any `json:"keys"`
		}
		if err := json.Unmarshal(loadJWKSFixture(t, entry.File), &document); err != nil {
			t.Fatalf("%s: %v", entry.File, err)
		}
		for _, key := range document.Keys {
			for _, member := range privateJWKMembers {
				if _, present := key[member]; present && entry.ExpectKeyword == "additionalProperties" {
					covered[member] = true
				}
			}
		}
	}
	var missing []string
	for _, member := range privateJWKMembers {
		if !covered[member] {
			missing = append(missing, member)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("no additionalProperties reject fixture carries private member(s) %v", missing)
	}
}
