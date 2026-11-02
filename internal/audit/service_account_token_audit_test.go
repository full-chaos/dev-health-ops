package audit

import (
	"errors"
	"testing"
)

// auth.service_account.token_issued is an append-only row: the token is
// stateless, so no security state changes. It names the jti, the account, the
// key row and the audience; never the key value, its digest or a token.
func TestServiceAccountTokenIssuedIsAppendOnlyWithClosedAttributes(t *testing.T) {
	if EventServiceAccountTokenIssued != "auth.service_account.token_issued" {
		t.Fatalf("EventServiceAccountTokenIssued = %q", EventServiceAccountTokenIssued)
	}
	if !appendOnlyEventTypes[EventServiceAccountTokenIssued] {
		t.Fatal("auth.service_account.token_issued is not append-only")
	}
	ok := map[string]any{
		"jti": "3f2504e0-4f89-41d3-9a0c-0305e82c3301", "principal_id": "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e",
		"credential_id": "8f9dbc98-72f8-4f22-827b-eeb3c0ead8f0", "revision": int64(3), "audience": "dev-health-api",
		"credential_class": "service_account_token",
	}
	event := AuditEvent{EventType: EventServiceAccountTokenIssued, Outcome: OutcomeAllowed, Attributes: ok}
	if err := event.validate(); err != nil {
		t.Fatalf("the issued service account token row is refused: %v", err)
	}
	if err := (Mutation{Apply: nil, Audit: event}).validate(); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("Commit accepted an append-only event: %v", err)
	}
	for name, bad := range map[string]map[string]any{
		"the key value":            {"service_account_key": "x"},
		"a key digest":             {"secret_hash": "x"},
		"a token":                  {"access_token": "x"},
		"a subject token":          {"subject_token": "x"},
		"jti not a uuid":           {"jti": "eyJhbGciOiJFZERTQSJ9"},
		"credential id wire form":  {"credential_id": "crd_AAAA"},
		"principal wire id":        {"principal_id": "prn_AAAA"},
		"audience not an audience": {"audience": "query-api"},
		"class not a class":        {"credential_class": "service_account_tokens"},
		"revision as text":         {"revision": "3"},
	} {
		if err := ValidateAttributes(bad); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
