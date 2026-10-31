//go:build integration

package authschema

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// CHAOS-12200: a migration that needs an ACCESS EXCLUSIVE lock (0037's ALTER
// TABLE credential_registry) waits behind any open transaction on the table,
// and while it waits every NEW reader of the table queues behind it. auth
// readers of credential_registry would stall for as long as the oldest open
// reader stays open.

const credentialClassMigrationVersion = 37

// reopenCredentialClassMigration takes 0037 off the version table, so the next
// Apply runs its ALTER TABLE again. The ALTER drops and re-adds the constraint
// it already holds, so running it a second time is a no-op on the table's
// shape: what the test exercises is only its lock.
func reopenCredentialClassMigration(t *testing.T, ctx context.Context, f *authFixture) {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	var found bool
	for _, m := range migrations {
		if m.Version == credentialClassMigrationVersion {
			found = true
			if !strings.Contains(m.SQL, "ALTER TABLE credential_registry") {
				t.Fatalf("version %d no longer alters credential_registry; this test needs a migration that does", m.Version)
			}
		}
	}
	if !found {
		t.Fatalf("version %d is not in the lineage", credentialClassMigrationVersion)
	}
	tag, err := f.admin.Exec(ctx, `DELETE FROM auth.schema_migrations WHERE version = $1`, credentialClassMigrationVersion)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("take version %d off the version table: rows=%d err=%v", credentialClassMigrationVersion, tag.RowsAffected(), err)
	}
}

// openCredentialRegistryReader opens a transaction on the runtime role that
// has read credential_registry and stays open: the ACCESS SHARE lock it holds
// is what a migration's ACCESS EXCLUSIVE request waits behind.
func openCredentialRegistryReader(t *testing.T, ctx context.Context, f *authFixture) pgx.Tx {
	t.Helper()
	tx, err := f.runtime.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the open reader: %v", err)
	}
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM auth.credential_registry`).Scan(&n); err != nil {
		t.Fatalf("open reader reads credential_registry: %v", err)
	}
	return tx
}

// waitForCredentialRegistryLockWaiter returns once some backend waits for a
// lock on credential_registry. It fails when none shows up: a test that went on
// without seeing the waiter would measure nothing.
func waitForCredentialRegistryLockWaiter(t *testing.T, ctx context.Context, f *authFixture) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := f.admin.QueryRow(ctx, `
			SELECT count(*) FROM pg_locks l
			JOIN pg_class c ON c.oid = l.relation
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'auth' AND c.relname = 'credential_registry' AND NOT l.granted`,
		).Scan(&waiting); err != nil {
			t.Fatalf("read pg_locks: %v", err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no backend ever waited for a lock on credential_registry; the migration did not reach its ALTER")
}

// testLockTimeout and testLockAttempts keep the integration runs short; the
// unit tests pin the defaults.
const (
	testLockTimeout  = 500 * time.Millisecond
	testLockAttempts = 2
)

func (f *authFixture) lockOptions(logger *slog.Logger) Options {
	return Options{
		Schema: f.schema, RuntimeRole: f.runtimeRole, Logger: logger,
		LockTimeout: testLockTimeout, LockAttempts: testLockAttempts,
	}
}

func (f *authFixture) credentialClassRecorded(t *testing.T, ctx context.Context) bool {
	t.Helper()
	var recorded bool
	if err := f.admin.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM auth.schema_migrations WHERE version = $1)`, credentialClassMigrationVersion,
	).Scan(&recorded); err != nil {
		t.Fatalf("read the version table: %v", err)
	}
	return recorded
}

func TestAMigrationBehindAnOpenAuthReaderGivesUpInsteadOfQueueingEveryAuthReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f := newAuthFixture(t, ctx)
	reopenCredentialClassMigration(t, ctx, f)

	holder := openCredentialRegistryReader(t, ctx, f)
	defer func() { _ = holder.Rollback(context.WithoutCancel(ctx)) }()

	// testLockAttempts waits plus the pauses between them, with slack.
	const bound = 15 * time.Second
	logger, sink := newCapturedLogger()
	applyCtx, applyCancel := context.WithCancel(ctx)
	defer applyCancel()
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := Apply(applyCtx, f.migration, f.lockOptions(logger))
		done <- err
	}()

	waitForCredentialRegistryLockWaiter(t, ctx, f)

	// The queue: a new reader, started while the migration waits, is served
	// once the attempt gives up its place, not when the open reader ends.
	readCtx, readCancel := context.WithTimeout(ctx, bound)
	defer readCancel()
	readStarted := time.Now()
	var n int
	readErr := f.runtime.QueryRow(readCtx, `SELECT count(*) FROM auth.credential_registry`).Scan(&n)
	readWait := time.Since(readStarted)
	t.Logf("a new credential_registry reader started while the migration waited: err=%v after %s", readErr, readWait.Round(time.Millisecond))

	var applyErr error
	select {
	case applyErr = <-done:
		t.Logf("Apply returned after %s: %v", time.Since(started).Round(time.Millisecond), applyErr)
	case <-time.After(time.Until(started.Add(bound))):
		t.Fatalf("Apply still waits for its lock %s after it started, behind one open reader: no lock_timeout", bound)
	}
	if readErr != nil {
		t.Fatalf("a new reader was not served while the migration waited: %v", readErr)
	}
	if readWait > testLockTimeout+2*time.Second {
		t.Fatalf("a new reader waited %s, want at most about one lock timeout (%s)", readWait, testLockTimeout)
	}
	if !errors.Is(applyErr, ErrMigrationLockTimeout) || !errors.Is(applyErr, ErrMigrationFailed) {
		t.Fatalf("Apply = %v, want ErrMigrationLockTimeout and ErrMigrationFailed", applyErr)
	}
	if warnings := sink.lockWarnings(t); len(warnings) != testLockAttempts {
		t.Fatalf("want %d lock warnings, got %v", testLockAttempts, warnings)
	}
	if f.credentialClassRecorded(t, ctx) {
		t.Fatal("version 37 was recorded by a run that gave up on its lock")
	}

	// When the open reader ends, the next run applies the migration.
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("end the open reader: %v", err)
	}
	if _, err := Apply(ctx, f.migration, f.lockOptions(logger)); err != nil {
		t.Fatalf("Apply after the reader ended: %v", err)
	}
	if !f.credentialClassRecorded(t, ctx) {
		t.Fatal("version 37 is not recorded after the reader ended")
	}
}

func TestAMigrationRetriedAfterTheOpenReaderEndsApplies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f := newAuthFixture(t, ctx)
	reopenCredentialClassMigration(t, ctx, f)

	holder := openCredentialRegistryReader(t, ctx, f)
	defer func() { _ = holder.Rollback(context.WithoutCancel(ctx)) }()

	logger, sink := newCapturedLogger()
	done := make(chan error, 1)
	go func() {
		_, err := Apply(ctx, f.migration, f.lockOptions(logger))
		done <- err
	}()

	// End the open reader once the first attempt has timed out and said so.
	deadline := time.Now().Add(15 * time.Second)
	for len(sink.lockWarnings(t)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the first attempt never logged its lock timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("end the open reader: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Apply = %v, want the retry to apply the migration", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Apply did not return after the open reader ended")
	}
	warnings := sink.lockWarnings(t)
	if len(warnings) != 1 || warnings[0]["outcome"] != "retrying" || warnings[0]["version"] != float64(credentialClassMigrationVersion) {
		t.Fatalf("want one retrying warning for version 37, got %v", warnings)
	}
	if !f.credentialClassRecorded(t, ctx) {
		t.Fatal("version 37 is not recorded after the retry")
	}
}

// A second auth-migrate run waits for the first one's advisory lock for as
// long as the first one runs. The lock timeout must not reach that wait: it is
// set on each migration's transaction, never on the session.
func TestASecondMigratorStillWaitsForTheFirstPastTheLockTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f := newAuthFixture(t, ctx)

	first, err := f.admin.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire the first migrator's connection: %v", err)
	}
	defer first.Release()
	if _, err := first.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		t.Fatalf("first migrator takes the migration lock: %v", err)
	}

	// Several times everything the lock policy allows one migration.
	hold := 4 * testLockAttempts * testLockTimeout
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := Apply(ctx, f.migration, f.lockOptions(nil))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the second migrator returned (%v) after %s while the first held the migration lock", err, time.Since(started))
	case <-time.After(hold):
	}
	if _, err := first.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryLockKey); err != nil {
		t.Fatalf("first migrator releases the migration lock: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second migrator = %v, want it to run once the first released the lock", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the second migrator did not finish after the first released the lock")
	}
}
