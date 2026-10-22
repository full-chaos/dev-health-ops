//go:build integration

package local

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// raceMarker is what `check_go.sh auth race-integration` counts: a race test
// logs it only when it ran with -race against a live database.
const raceMarker = "AUTH_RACE_EXECUTED"

func markRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
}

type pgEnv struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	attempts *PGAttempts
}

// newPGEnv applies the auth schema to a container of its own and connects as
// the runtime role, so every write runs with the production grants and
// through the 0010 login_attempts triggers.
func newPGEnv(t *testing.T, ctx context.Context) *pgEnv {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	// go-api's tables: a session pins public.users.token_version, and the
	// legacy exchange reads and revokes public.refresh_tokens.
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("local_login_runtime", instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	const password = "local-login-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	cfg.MaxConns = 32
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return &pgEnv{admin: admin, runtime: runtime, attempts: NewPGAttempts(runtime, schema)}
}

func ident(s string) []byte { h := sha256.Sum256([]byte(s)); return h[:] }

type row struct {
	count  int
	first  *time.Time
	locked *time.Time
}

func (e *pgEnv) row(t *testing.T, ctx context.Context, id []byte) (row, bool) {
	t.Helper()
	var r row
	err := e.admin.QueryRow(ctx, `SELECT attempt_count, first_attempt_at, locked_until FROM auth.login_attempts WHERE identifier_hash = $1`, id).
		Scan(&r.count, &r.first, &r.locked)
	if err != nil {
		return row{}, false
	}
	return r, true
}

// seed inserts a pre-aged row: the live-lock triggers stay enabled.
func (e *pgEnv) seed(t *testing.T, ctx context.Context, id []byte, count int, lockedUntil string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.login_attempts (identifier_hash, attempt_count, first_attempt_at, locked_until, created_at, updated_at)
		VALUES ($1, $2, now() - interval '2 hours', `+lockedUntil+`, now() - interval '2 hours', now() - interval '2 hours')`, id, count); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestFiveFailuresLockForFifteenMinutes(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	id := ident("five")
	for i := 1; i <= 4; i++ {
		if err := e.attempts.RecordFailure(ctx, id); err != nil {
			t.Fatal(err)
		}
		if locked, _, err := e.attempts.Check(ctx, id); err != nil || locked {
			t.Fatalf("after %d failures: locked=%v err=%v", i, locked, err)
		}
	}
	if err := e.attempts.RecordFailure(ctx, id); err != nil {
		t.Fatal(err)
	}
	locked, remaining, err := e.attempts.Check(ctx, id)
	if err != nil || !locked {
		t.Fatalf("after 5 failures: locked=%v err=%v", locked, err)
	}
	if remaining < 14*time.Minute || remaining > 15*time.Minute {
		t.Fatalf("remaining = %v, want about 15m", remaining)
	}
}

func TestFailureWhileLockedNeverShortensOrCountsMore(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	id := ident("held")
	e.seed(t, ctx, id, 5, `now() + interval '10 minutes'`)
	before, _ := e.row(t, ctx, id)
	for i := 0; i < 3; i++ {
		if err := e.attempts.RecordFailure(ctx, id); err != nil {
			t.Fatalf("RecordFailure while locked: %v", err)
		}
	}
	after, _ := e.row(t, ctx, id)
	if after.count != 5 || !after.locked.Equal(*before.locked) {
		t.Fatalf("row moved while locked: before=%+v after=%+v", before, after)
	}
}

func TestLiveLockCannotBeClearedOnlyExpiredOnesAre(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	live, expired := ident("live"), ident("expired")
	e.seed(t, ctx, live, 5, `now() + interval '10 minutes'`)
	e.seed(t, ctx, expired, 5, `now() - interval '1 minute'`)

	if err := e.attempts.Clear(ctx, live); err == nil {
		t.Fatal("Clear of a live lock succeeded")
	}
	if _, ok := e.row(t, ctx, live); !ok {
		t.Fatal("the live lock row is gone")
	}
	if locked, _, err := e.attempts.Check(ctx, expired); err != nil || locked {
		t.Fatalf("expired: locked=%v err=%v", locked, err)
	}
	r, ok := e.row(t, ctx, expired)
	if !ok || r.count != 0 || r.locked != nil || r.first != nil {
		t.Fatalf("expired lock not reset by Check: %+v ok=%v", r, ok)
	}
	if err := e.attempts.Clear(ctx, expired); err != nil {
		t.Fatalf("Clear of an expired row: %v", err)
	}
	if _, ok := e.row(t, ctx, expired); ok {
		t.Fatal("the cleared row remains")
	}
}

func TestExpiredLockRestartsTheCountAtOne(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	id := ident("restart")
	e.seed(t, ctx, id, 5, `now() - interval '1 minute'`)
	if err := e.attempts.RecordFailure(ctx, id); err != nil {
		t.Fatal(err)
	}
	r, _ := e.row(t, ctx, id)
	if r.count != 1 || r.locked != nil {
		t.Fatalf("row = %+v, want count 1 and no lock", r)
	}
}

func TestClearOfAnUnknownIdentifierIsNotAnError(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	if err := e.attempts.Clear(ctx, ident("never")); err != nil {
		t.Fatal(err)
	}
}

func TestIdentifierMustBe32Bytes(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	if err := e.attempts.RecordFailure(ctx, []byte("short")); err == nil {
		t.Fatal("a 5-byte identifier was accepted")
	}
}

func TestIdentifierHashIsImmutableAtTheDatabase(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	id := ident("immutable")
	if err := e.attempts.RecordFailure(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.runtime.Exec(ctx, `UPDATE auth.login_attempts SET identifier_hash = $2 WHERE identifier_hash = $1`, id, ident("other")); err == nil {
		t.Fatal("the runtime role retargeted a row")
	}
}

// TestRaceTenParallelFailuresLockOnce: ten concurrent failures on one
// identifier end with one row, locked once, and a count of exactly 5: the
// failures after the lock only touch updated_at.
func TestRaceTenParallelFailuresLockOnce(t *testing.T) {
	markRace(t)
	ctx := context.Background()
	e := newPGEnv(t, ctx)
	id := ident("parallel")
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- e.attempts.RecordFailure(ctx, id)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("RecordFailure: %v", err)
		}
	}
	var rows int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.login_attempts WHERE identifier_hash = $1`, id).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows = %d err=%v", rows, err)
	}
	r, _ := e.row(t, ctx, id)
	if r.count != 5 || r.locked == nil {
		t.Fatalf("row = %+v, want count 5 and a lock", r)
	}
}
