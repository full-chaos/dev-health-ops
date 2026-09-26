//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// CHAOS-6937. The posture statement carries its own server-side
// statement_timeout, set LOCAL to its own read-only transaction: a client that
// gives up (or whose cancel request is lost on the way through a pooler) must
// not leave the statement running on a pooled server connection, and the
// setting must not leak to whatever uses that connection next.
func TestPostureStatementRunsUnderItsOwnServerSideTimeoutThatDoesNotLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env, roles, _ := startOracleEnv(t, ctx)
	pool := env.pools[roles.domain]

	// The bound is what the constant says, inside the transaction ...
	answer, err := queryPostureAnswer(ctx, pool, rolePostureStatementTimeout,
		"SELECT current_setting('statement_timeout') = '10s'")
	if err != nil || !answer {
		t.Fatalf("statement_timeout inside the posture transaction = %v (err %v), want 10s", answer, err)
	}
	// ... it ends the statement on the SERVER with no client deadline at all ...
	started := time.Now()
	_, err = queryPostureAnswer(ctx, pool, 300*time.Millisecond, "SELECT pg_sleep(30) IS NOT NULL")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("slow statement = %v, want SQLSTATE 57014 from the server", err)
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("the server-side timeout took %s to fire", took)
	}
	// ... and does not leak: the same pooled connection, outside the
	// transaction, is back to the server default.
	var setting string
	if err := pool.QueryRow(ctx, "SELECT current_setting('statement_timeout')").Scan(&setting); err != nil {
		t.Fatal(err)
	}
	if setting != "0" {
		t.Fatalf("statement_timeout after the posture transaction = %q, want the default 0", setting)
	}

	// Every posture statement goes through it: with the bound made impossibly
	// short, each role's real check reports "the database never answered"
	// (ErrUnavailable, SQLSTATE 57014), never a refusal and never a pass.
	previous := rolePostureStatementTimeout
	rolePostureStatementTimeout = time.Millisecond
	t.Cleanup(func() { rolePostureStatementTimeout = previous })
	for name, check := range map[string]func() error{
		"domain (CheckRolePosture)": func() error {
			return CheckDomainAuthorization(ctx, env.pools[roles.domain], roles.domain, grantSchema)
		},
		"coordinator (CheckRolePosture)": func() error {
			return CheckCoordinatorAuthorization(ctx, env.pools[roles.coordinator], roles.coordinator, grantSchema)
		},
		"queue (CheckQueueAuthorization)": func() error {
			return CheckQueueAuthorization(ctx, env.pools[roles.queue], roles.queue, grantSchema)
		},
	} {
		err := check()
		if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrPostureRefused) {
			t.Errorf("%s under a 1 ms statement_timeout = %v, want ErrUnavailable and not a refusal", name, err)
			continue
		}
		if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
			t.Errorf("%s: %v does not carry SQLSTATE 57014", name, err)
		}
	}
}
