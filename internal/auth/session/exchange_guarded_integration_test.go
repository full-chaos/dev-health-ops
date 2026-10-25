//go:build integration

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func refuseGuard(context.Context, Queryer, Record) error { return ErrGuardRefused }

func liveSuccessors(t *testing.T, e *env, ctx context.Context, sessionID uuid.UUID) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials
		WHERE session_id = $1 AND parent_id IS NOT NULL AND revoked_at IS NULL`, sessionID.String())
}

func TestExchangeGuardedRefusalWritesNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.goUser(t, ctx, 1))
	result, err := e.store.ExchangeGuarded(ctx, issued.Credential, refuseGuard)
	if !errors.Is(err, ErrGuardRefused) || result.Session.ID != issued.Session.ID || result.Credential.Reveal() != "" {
		t.Fatalf("ExchangeGuarded = %+v, %v; want the guard refusal with the session and no credential", result, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, issued.Session.ID.String()); n != 1 {
		t.Fatalf("%d credential rows after the refusal, want the original only", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND consumed_at IS NULL AND revoked_at IS NULL`, issued.Session.ID.String()); n != 1 {
		t.Fatalf("the original credential was consumed or revoked")
	}
	if again, err := e.store.Exchange(ctx, issued.Credential); err != nil || again.Reason != ReasonAccepted {
		t.Fatalf("the refused credential no longer exchanges: %s, %v", again.Reason, err)
	}
}

func TestExchangeGuardedPinMissingLeavesZeroLiveSuccessors(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.goUser(t, ctx, 1))
	if _, err := e.admin.Exec(ctx, `UPDATE auth.sessions SET legacy_token_version = NULL WHERE id = $1`, issued.Session.ID.String()); err != nil {
		t.Fatal(err)
	}
	result, err := e.store.ExchangeGuarded(ctx, issued.Credential, func(context.Context, Queryer, Record) error { return nil })
	if err != nil || result.Reason != ReasonRevisionStale {
		t.Fatalf("ExchangeGuarded = %s, %v; want revision_stale", result.Reason, err)
	}
	if n := liveSuccessors(t, e, ctx, issued.Session.ID); n != 0 {
		t.Fatalf("%d live successor rows after the refusal, want 0", n)
	}
}

func TestExchangeGuardedStalePinInsertsNoSuccessor(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 1)
	issued := e.create(t, ctx, user)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET token_version = 2 WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	guard := func(ctx context.Context, q Queryer, record Record) error {
		var tv int64
		if err := q.QueryRow(ctx, `SELECT COALESCE(token_version, 0) FROM public.users WHERE id = $1`, record.PrincipalID).Scan(&tv); err != nil {
			return err
		}
		if record.LegacyTokenVersion == nil || *record.LegacyTokenVersion != tv {
			return ErrGuardRefused
		}
		return nil
	}
	if _, err := e.store.ExchangeGuarded(ctx, issued.Credential, guard); !errors.Is(err, ErrGuardRefused) {
		t.Fatalf("ExchangeGuarded err = %v; want the guard refusal", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND parent_id IS NOT NULL`, issued.Session.ID.String()); n != 0 {
		t.Fatalf("%d successor rows inserted, want 0", n)
	}
}

func TestExchangeGuardedCompletesOnAOneConnectionPool(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 1)
	issued := e.create(t, ctx, user)
	cfg := e.runtime.Config().Copy()
	cfg.MaxConns = 1
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	cfgStore := e.newStore(t, e.keyText, Config{})
	cfgStore.pool = one
	guard := func(ctx context.Context, q Queryer, record Record) error {
		var active bool
		return q.QueryRow(ctx, `SELECT COALESCE(is_active, false) FROM public.users WHERE id = $1`, record.PrincipalID).Scan(&active)
	}
	done := make(chan error, 1)
	go func() {
		result, err := cfgStore.ExchangeGuarded(ctx, issued.Credential, guard)
		if err == nil && result.Reason != ReasonAccepted {
			err = errors.New("not accepted: " + string(result.Reason))
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("guarded exchange on one connection: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the guarded exchange deadlocked on a one-connection pool")
	}
}
