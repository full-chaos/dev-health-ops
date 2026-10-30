package audit

import (
	"testing"

	"github.com/google/uuid"
)

// An internal route's audit row names the credential it was called with: a
// service credential row id, or the workload principal and its token's jti.
// The zero Caller names nothing.
func TestCallerAnnotateNamesTheCallingCredential(t *testing.T) {
	workload := uuid.MustParse("6f1c2a4e-5b7d-4c3e-9a2b-1d0e8f7a6b5c")
	for name, c := range map[string]struct {
		caller Caller
		want   map[string]any
	}{
		"service credential": {Caller{CredentialID: "8f9dbc98-72f8-4f22-827b-eeb3c0ead8f0"},
			map[string]any{"credential_id": "8f9dbc98-72f8-4f22-827b-eeb3c0ead8f0"}},
		"workload": {Caller{Workload: &workload, JTI: "AAAAAAAAAAAAAAAAAAAAAA"},
			map[string]any{"caller_principal_id": workload.String(), "workload_jti": "AAAAAAAAAAAAAAAAAAAAAA"}},
		"none": {Caller{}, map[string]any{}},
	} {
		attributes := map[string]any{}
		c.caller.Annotate(attributes)
		if len(attributes) != len(c.want) {
			t.Fatalf("%s: %v, want %v", name, attributes, c.want)
		}
		for key, value := range c.want {
			if attributes[key] != value {
				t.Fatalf("%s: %s = %v, want %v", name, key, attributes[key], value)
			}
		}
		if err := ValidateAttributes(attributes); err != nil {
			t.Fatalf("%s: the annotated attributes are refused: %v", name, err)
		}
	}
}

// caller_principal_id is a principal uuid: never a prn_ wire id, token text or
// free text.
func TestCallerPrincipalIDIsAUUID(t *testing.T) {
	if err := ValidateAttributes(map[string]any{"caller_principal_id": "6f1c2a4e-5b7d-4c3e-9a2b-1d0e8f7a6b5c"}); err != nil {
		t.Fatalf("a uuid was refused: %v", err)
	}
	for name, value := range map[string]any{
		"wire id":   "prn_bxwqTlt9TD6aKx0OjqtrXA",
		"upper":     "6F1C2A4E-5B7D-4C3E-9A2B-1D0E8F7A6B5C",
		"jws":       "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ4In0.c2ln",
		"free text": "go-api",
		"empty":     "",
		"number":    7,
	} {
		if err := ValidateAttributes(map[string]any{"caller_principal_id": value}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
