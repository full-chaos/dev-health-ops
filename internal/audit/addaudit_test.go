package audit

import (
	"errors"
	"testing"
)

func probeRow() AuditEvent {
	return AuditEvent{EventType: "auth.session.delegation_ended", Outcome: OutcomeAllowed}
}

func TestAddAuditRefusesATransactionWithNoQueue(t *testing.T) {
	if err := (TxOps{}).AddAudit(probeRow()); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("AddAudit with no queue = %v, want ErrMutationFailed", err)
	}
}

func TestAddAuditRefusesAClosedQueue(t *testing.T) {
	tx := TxOps{queue: &auditQueue{closed: true}}
	if err := tx.AddAudit(probeRow()); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("AddAudit on a closed queue = %v, want ErrMutationFailed", err)
	}
}

func TestAddAuditChecksEachRowAsCommitDoes(t *testing.T) {
	bad := probeRow()
	bad.Attributes = map[string]any{"not_on_the_allowlist": "x"}
	queue := &auditQueue{}
	if err := (TxOps{queue: queue}).AddAudit(probeRow(), bad); err == nil {
		t.Fatal("a row outside the attribute allowlist was queued")
	}
	if len(queue.events) != 0 {
		t.Fatalf("a refused call queued %d rows", len(queue.events))
	}
}
