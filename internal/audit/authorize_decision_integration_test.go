//go:build integration

package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func highRiskEvent(outcome Outcome, actor *string) AuditEvent {
	attrs := highRiskAttrs()
	attrs["decision_id"] = "dec_" + strings.Repeat(string(rune('A'+len(outcome))), 26)
	return AuditEvent{EventType: EventAuthorizeHighRiskDecided, Outcome: outcome, ActorPrincipalID: actor, Attributes: attrs}
}

func TestAppendAllWritesEveryRowAsTheRuntimeRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	if err := AppendAll(ctx, writer, env.schema, highRiskEvent(OutcomeAllowed, nil), highRiskEvent(OutcomeDenied, nil)); err != nil {
		t.Fatalf("AppendAll: %v", err)
	}
	var count int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, EventAuthorizeHighRiskDecided).Scan(&count); err != nil || count != 2 {
		t.Fatalf("rows %d (%v), want 2", count, err)
	}
}

// All rows or none: a row the database refuses rolls the earlier rows back.
func TestAppendAllWritesNothingWhenOneRowFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	unknown := uuid.NewString()
	err := AppendAll(ctx, writer, env.schema, highRiskEvent(OutcomeAllowed, nil), highRiskEvent(OutcomeDenied, &unknown))
	if !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	var count int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rows %d (%v), want 0: the first row must roll back", count, err)
	}
}

// The group writer as the runtime role: concurrent calls share transactions,
// and a call whose row the database refuses (an unknown actor) fails alone.
func TestGroupWriterWritesConcurrentCallsAsTheRuntimeRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	w, err := NewGroupWriter(GroupWriterConfig{Begin: writer.Begin, Schema: env.schema})
	if err != nil {
		t.Fatal(err)
	}
	const calls = 16
	unknown := uuid.NewString()
	errs := make(chan error, calls)
	for i := range calls {
		go func() {
			var actor *string
			if i == calls/2 {
				actor = &unknown
			}
			errs <- w.Append(ctx, highRiskEvent(OutcomeAllowed, actor))
		}()
	}
	failed := 0
	for range calls {
		if err := <-errs; err != nil {
			if !errors.Is(err, ErrMutationFailed) {
				t.Fatalf("err = %v, want ErrMutationFailed", err)
			}
			failed++
		}
	}
	var count int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, EventAuthorizeHighRiskDecided).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if failed != 1 || count != calls-1 {
		t.Fatalf("failed calls %d, rows %d; want 1 and %d", failed, count, calls-1)
	}
}
