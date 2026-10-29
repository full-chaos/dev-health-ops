package audit

import "testing"

// The auth.session.issued row of a workload call carries workload_jti: the
// verified token's jti, a uuid as the exchange mints it. It is an id, never token text, and never a credential_id.
func TestWorkloadJTIAttributeIsAnAllowlistedIDNeverTokenText(t *testing.T) {
	if err := ValidateAttributes(map[string]any{"workload_jti": "3f2504e0-4f89-41d3-9a0c-0305e82c3301", "source": "go-api-delegated"}); err != nil {
		t.Fatalf("a uuid jti is refused: %v", err)
	}
	for name, value := range map[string]string{
		"a compact JWS":        "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ4In0.c2ln",
		"too short":            "abc",
		"65 chars":             "01234567890123456789012345678901234567890123456789012345678901234",
		"a service credential": "svc_authsess_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"a space":              "AAAAAAAAAAAAAAAAAAAAAA ",
		"empty":                "",
	} {
		if err := ValidateAttributes(map[string]any{"workload_jti": value}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := ValidateAttributes(map[string]any{"workload_jti": 7}); err == nil {
		t.Error("a number was accepted")
	}
}
