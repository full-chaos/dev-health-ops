package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestValidateAttributesAcceptsOnlyTheAllowlist(t *testing.T) {
	allowed := map[string]any{
		"reason_code": "role_granted", "target_kind": "membership", "membership_revision": int64(4),
		"changed_fields": []string{"role_key", "status"}, "display_prefix": "fcacr_ab12",
	}
	if err := ValidateAttributes(allowed); err != nil {
		t.Fatalf("allowed attributes refused: %v", err)
	}
	if err := ValidateAttributes(nil); err != nil {
		t.Fatalf("nil attributes refused: %v", err)
	}
}

// The redaction rows: every named leak the ticket lists, as a key AND as a
// value under an allowed key.
func TestValidateAttributesRefusesSecretsAndNeverEchoesThem(t *testing.T) {
	const marker = "S3CR3T-MARKER-VALUE"
	for _, key := range []string{"password", "token", "access_token", "refresh_token", "authorization", "authorization_code",
		"code", "request_body", "body", "provider_response", "secret", "client_secret", "private_key", "email", "headers"} {
		err := ValidateAttributes(map[string]any{key: marker})
		if err == nil {
			t.Errorf("key %q was accepted", key)
			continue
		}
		if !errors.Is(err, ErrAttributeNotAllowed) || !errors.Is(err, ErrMutationFailed) {
			t.Errorf("key %q: err = %v, want ErrAttributeNotAllowed wrapping ErrMutationFailed", key, err)
		}
		if strings.Contains(err.Error(), marker) {
			t.Errorf("key %q: the error echoed the value: %v", key, err)
		}
	}

	for name, value := range map[string]string{
		"jwt":         "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1c3IifQ.c2lnbmF0dXJl",
		"bearer":      "Bearer abcdef0123456789",
		"basic":       "basic dXNlcjpwYXNzd29yZA==",
		"pem":         "-----BEGIN PRIVATE KEY-----",
		"acr":         "fcacr_0123456789abcdef0123",
		"push":        "fcpush_0123456789abcdef0123",
		"service":     "svc_worker_0123456789abcdef",
		"refresh":     "rt_0123456789abcdef0123",
		"overlong":    strings.Repeat("a", maxAttributeValueLen+1),
		"in-the-list": "x",
	} {
		attrs := map[string]any{"reason_code": value}
		if name == "in-the-list" {
			attrs = map[string]any{"changed_fields": []string{"ok", "Bearer abcdef0123456789"}}
		}
		err := ValidateAttributes(attrs)
		if err == nil {
			t.Errorf("%s value was accepted", name)
			continue
		}
		if strings.Contains(err.Error(), value) && len(value) > 8 {
			t.Errorf("%s: the error echoed the value", name)
		}
	}

	if err := ValidateAttributes(map[string]any{"reason_code": map[string]any{"nested": "x"}}); err == nil {
		t.Error("a nested object was accepted")
	}
}

func TestMutationValidateAppliesTheAllowlist(t *testing.T) {
	m := Mutation{
		Apply: func(_ context.Context, _ TxOps) error { return nil },
		Audit: AuditEvent{EventType: "x", Outcome: OutcomeAllowed, Attributes: map[string]any{"password": "p"}},
		Event: OutboxEvent{AggregateType: "a", AggregateID: "b", EventType: "c", IdempotencyKey: "k"},
	}
	if err := m.validate(); !errors.Is(err, ErrAttributeNotAllowed) {
		t.Fatalf("validate = %v, want ErrAttributeNotAllowed", err)
	}
}
