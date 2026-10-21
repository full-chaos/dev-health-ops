//go:build integration

package audit

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// TestAppendWritesOneAuditRowAsTheRuntimeRole drives Append as the runtime
// role: the commit guard (migration 0006) admits the audit row, and the row
// carries the request id and the context's client address.
func TestAppendWritesOneAuditRowAsTheRuntimeRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)

	requestID := uuid.NewString()
	ctx = clientaddr.With(ctx, netip.MustParseAddr("198.51.100.7"))
	event := AuditEvent{
		EventType: EventLoginFailed, Outcome: OutcomeDenied, RequestID: &requestID,
		Attributes: map[string]any{"reason_code": "invalid_credentials", "provider": "local"},
	}
	if err := Append(ctx, writer, env.schema, event); err != nil {
		t.Fatalf("Append: %v", err)
	}
	var count int
	var eventType, outcome, gotRequest, gotIP, reason string
	if err := env.pool.QueryRow(ctx, `
		SELECT count(*) OVER (), event_type, outcome, request_id, host(client_ip), attributes->>'reason_code'
		FROM auth.security_audit_events WHERE event_type = $1`, EventLoginFailed).
		Scan(&count, &eventType, &outcome, &gotRequest, &gotIP, &reason); err != nil {
		t.Fatalf("reading the row: %v", err)
	}
	if count != 1 || outcome != "denied" || gotRequest != requestID || gotIP != "198.51.100.7" || reason != "invalid_credentials" {
		t.Fatalf("row: count %d outcome %s request %s ip %s reason %s", count, outcome, gotRequest, gotIP, reason)
	}
	var outbox int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events`).Scan(&outbox); err != nil || outbox != 0 {
		t.Fatalf("outbox rows %d (%v): Append writes no event", outbox, err)
	}
}

func TestAppendRefusalWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	err := Append(ctx, writer, env.schema, AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed})
	if !errors.Is(err, ErrNotAppendOnly) {
		t.Fatalf("err = %v", err)
	}
	var count int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rows %d (%v)", count, err)
	}
}
