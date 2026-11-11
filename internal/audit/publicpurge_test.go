package audit

import (
	"context"
	"testing"
)

func TestCommitRefusesTheRetiredPublicPurgeEventType(t *testing.T) {
	purge := AuditEvent{EventType: EventLegacyLinkTokensPurged, Outcome: OutcomeAllowed}
	err := Mutation{Apply: func(context.Context, TxOps) error { return nil }, Audit: purge, OffStream: OffStreamLinkCredentialLifecycle}.validate()
	if err == nil {
		t.Fatal("Commit accepted the public-purge event type")
	}
}
