package audit

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestLegacyLinkPurgeAttributesAreCountsAndARunIDOnly(t *testing.T) {
	ok := map[string]any{
		"purged_email_verification_tokens": int64(3), "purged_password_reset_tokens": int64(0), "run_id": "roll-1.purge-2",
	}
	if err := ValidateAttributes(ok); err != nil {
		t.Fatalf("the purge attributes were refused: %v", err)
	}
	for name, attrs := range map[string]map[string]any{
		"count as text":    {"purged_password_reset_tokens": "3"},
		"run id with text": {"run_id": "has spaces and a token eyJhbGciOiJF"},
		"run id empty":     {"run_id": ""},
		"run id too long":  {"run_id": "a23456789012345678901234567890123456789012345678901234567890123456"},
	} {
		if err := ValidateAttributes(attrs); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestCommitPublicPurgeRefusesAnyOtherEventTypeAndCommitRefusesThisOne(t *testing.T) {
	ctx := context.Background()
	other := AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed}
	if err := CommitPublicPurge(ctx, nil, authschema.ValidatedIdentifier{}, func(context.Context, TxOps) error { return nil }, other); !errors.Is(err, ErrNotPublicPurge) {
		t.Fatalf("another event type: err = %v, want ErrNotPublicPurge", err)
	}
	purge := AuditEvent{EventType: EventLegacyLinkTokensPurged, Outcome: OutcomeAllowed}
	err := Mutation{Apply: func(context.Context, TxOps) error { return nil }, Audit: purge, OffStream: OffStreamLinkCredentialLifecycle}.validate()
	if err == nil {
		t.Fatal("Commit accepted the public-purge event type")
	}
}
