//go:build integration

package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

type cancelAfterInsert struct{ cancel context.CancelFunc }

func (cancelAfterInsert) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (c cancelAfterInsert) TraceQueryEnd(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if strings.Contains(strings.ToLower(data.CommandTag.String()), "insert") {
		c.cancel()
	}
}

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

func TestAppendAllKeepsTheCanceledCommitClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	base := runtimeRolePool(t, ctx, env.pool)
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	cfg := base.Config().Copy()
	cfg.ConnConfig.Tracer = cancelAfterInsert{cancel: cancelCall}
	writer, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(writer.Close)
	err = AppendAll(callCtx, writer, env.schema, highRiskEvent(OutcomeAllowed, nil))
	if !errors.Is(err, ErrMutationFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrMutationFailed wrapping context.Canceled", err)
	}
	if got := logging.ErrorClass(err); got != logging.ErrorClassCanceled {
		t.Fatalf("error class = %q, want %q", got, logging.ErrorClassCanceled)
	}
	var count int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rows %d (%v), want 0", count, err)
	}
}
