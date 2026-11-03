package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The auth.session.issued row of a workload call carries workload_jti: the
// verified token's jti. Its rule is the workload-token schema's jti pattern, so
// every jti the verifier accepts is a valid audit value; it is never token text
// with a separator or a signature.
func TestWorkloadJTIAttributeFollowsTheTokenSchemaPattern(t *testing.T) {
	for name, value := range map[string]string{
		"a uuid":           "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
		"22 characters":    "AAAAAAAAAAAAAAAAAAAAAA",
		"64 characters":    "0123456789012345678901234567890123456789012345678901234567890123",
		"underscore, dash": "Zm9vLWJhcl9iYXotcXV4LTEyMzQ1",
		"22 base64url":     "abcDEF123_-abcDEF123_-",
	} {
		if err := ValidateAttributes(map[string]any{"workload_jti": value, "source": "go-api-delegated"}); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
	for name, value := range map[string]string{
		"21 characters": "AAAAAAAAAAAAAAAAAAAAA",
		"65 characters": "01234567890123456789012345678901234567890123456789012345678901234",
		"plus":          "AAAAAAAAAAAAAAAAAAAAA+",
		"slash":         "AAAAAAAAAAAAAAAAAAAAA/",
		"equals":        "AAAAAAAAAAAAAAAAAAAAA=",
		"dot":           "AAAAAAAAAAAAAAAAAAAAA.",
		"space":         "AAAAAAAAAAAAAAAAAAAAA ",
		"a compact JWS": "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ4In0.c2ln",
		"empty":         "",
	} {
		if err := ValidateAttributes(map[string]any{"workload_jti": value}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := ValidateAttributes(map[string]any{"workload_jti": 7}); err == nil {
		t.Error("a number was accepted")
	}
}

// The other jti key is untouched: it stays a uuid.
func TestTheJTIKeyStaysAUUID(t *testing.T) {
	if err := ValidateAttributes(map[string]any{"jti": "AAAAAAAAAAAAAAAAAAAAAA"}); err == nil {
		t.Fatal("the jti key accepted a non-uuid")
	}
}

// The audit rule and the schema pattern cannot drift.
func TestWorkloadJTIRuleEqualsTheTokenSchemaPattern(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "contracts", "auth", "v1", "jsonschema", "workload-token.v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	var pattern string
	var walk func(any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			if jti, ok := value["jti"].(map[string]any); ok {
				if p, ok := jti["pattern"].(string); ok && pattern == "" {
					pattern = p
				}
			}
			for _, child := range value {
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	walk(document)
	if pattern == "" {
		t.Fatal("the schema names no jti pattern")
	}
	rule := attributeIDRules["workload_jti"]
	if rule == nil || rule.String() != pattern {
		t.Fatalf("workload_jti rule %v, want the schema pattern %s", rule, pattern)
	}
}
