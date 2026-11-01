package audit

import (
	"errors"
	"testing"
)

// The operator CLI is a source an audit row may name, and no other free value is.
func TestOperatorCLIIsAnAllowedAuditSource(t *testing.T) {
	ok := map[string]any{"reason_code": "role_granted", "target_kind": "principal",
		"target_id": "11111111-1111-4111-8111-111111111111", "source": SourceOperatorCLI}
	if err := ValidateAttributes(ok); err != nil {
		t.Fatalf("the operator CLI source is refused: %v", err)
	}
	if SourceOperatorCLI != "operator-cli" {
		t.Fatalf("SourceOperatorCLI = %q", SourceOperatorCLI)
	}
	if err := ValidateAttributes(map[string]any{"source": "operator"}); err == nil {
		t.Error("an unlisted source was accepted")
	}
}

// A refused platform_owner grant or revoke records an attempt with no state
// change: append-only, written by Append, refused by Commit.
func TestPlatformOwnerRefusalsAreAppendOnly(t *testing.T) {
	for _, eventType := range []string{EventPlatformOwnerGrantRefused, EventPlatformOwnerRevokeRefused} {
		if !appendOnlyEventTypes[eventType] {
			t.Errorf("%s is not append-only", eventType)
		}
		event := AuditEvent{EventType: eventType, Outcome: OutcomeDenied,
			Attributes: map[string]any{"reason_code": "denied_policy", "target_kind": "principal",
				"target_id": "11111111-1111-4111-8111-111111111111"}}
		if err := event.validate(); err != nil {
			t.Errorf("%s: the refusal row is refused: %v", eventType, err)
		}
		m := Mutation{Apply: nil, Audit: event}
		if err := m.validate(); !errors.Is(err, ErrMutationFailed) {
			t.Errorf("%s: Commit accepted an append-only event: %v", eventType, err)
		}
	}
	if EventPlatformOwnerGrantRefused != "auth.platform_owner.grant_refused" ||
		EventPlatformOwnerRevokeRefused != "auth.platform_owner.revoke_refused" {
		t.Error("the refusal event type names changed")
	}
}
