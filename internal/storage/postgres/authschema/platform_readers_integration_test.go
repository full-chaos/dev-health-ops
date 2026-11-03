//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const platformReadStatement = `SELECT a.principal_id, a.role_key, a.expires_at, p.id, p.kind, p.disabled_at
	FROM auth.platform_role_assignments a JOIN auth.principals p ON p.id = a.principal_id`

func platformReaderOptions(env *authFixture, owned bool, reader string) Options {
	options := readerOptions(env, reader)
	options.PlatformRolesOwned = owned
	return options
}

func readerViolationsWith(t *testing.T, ctx context.Context, env *authFixture, options Options) []Violation {
	t.Helper()
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	return found
}

func readerColumnGrants(t *testing.T, ctx context.Context, env *authFixture, reader string) string {
	t.Helper()
	rows, err := env.admin.Query(ctx, `SELECT table_name || '.' || column_name || ':' || privilege_type
		FROM information_schema.column_privileges WHERE grantee = $1 AND table_schema = $2 ORDER BY 1`, reader, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var entry string
		if err := rows.Scan(&entry); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, entry)
	}
	return strings.Join(columns, ",")
}

const (
	sessionColumnGrants  = "sessions.id:SELECT,sessions.legacy_token_version:SELECT,sessions.principal_id:SELECT,sessions.revoked_at:SELECT"
	platformColumnGrants = "platform_role_assignments.expires_at:SELECT,platform_role_assignments.principal_id:SELECT,platform_role_assignments.role_key:SELECT," +
		"principals.disabled_at:SELECT,principals.id:SELECT,principals.kind:SELECT,"
)

// While owned, a session reader reads exactly the session columns and the
// platform role columns (both directions of the posture), and a re-apply
// restores exactly that.
func TestReaderRolesReadExactlyTheirColumnsWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, pool := newSessionReader(t, ctx, env, "auth_platform_reader")
	on := platformReaderOptions(env, true, reader)
	if _, err := Apply(ctx, env.migration, on); err != nil {
		t.Fatalf("Apply owned: %v", err)
	}
	if err := readerStatement(ctx, pool, platformReadStatement); err != nil {
		t.Fatalf("the reader cannot read the platform role columns: %v", err)
	}
	for _, refused := range []string{
		`SELECT granted_by FROM auth.platform_role_assignments`,
		`SELECT id FROM auth.platform_role_assignments`,
		`SELECT display_name FROM auth.principals`,
		`SELECT * FROM auth.principals`,
		`DELETE FROM auth.platform_role_assignments`,
	} {
		if err := readerStatement(ctx, pool, refused); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s as the reader: err = %v, want permission denied", refused, err)
		}
	}
	if got, want := readerColumnGrants(t, ctx, env, reader), platformColumnGrants+sessionColumnGrants; got != want {
		t.Errorf("column grants = %s, want %s", got, want)
	}
	if found := readerViolationsWith(t, ctx, env, on); len(found) != 0 {
		t.Fatalf("posture after Apply owned: %s", describe(found))
	}
	if found := readerViolationsWith(t, ctx, env, platformReaderOptions(env, false, reader)); !strings.Contains(describe(found), "principals.kind") {
		t.Errorf("the not-owned posture does not name the platform read the reader holds:\n%s", describe(found))
	}
	q := Quote(mustValidatedIdentifier(reader))
	for _, change := range []string{
		fmt.Sprintf(`GRANT SELECT (granted_by) ON auth.platform_role_assignments TO %s`, q),
		fmt.Sprintf(`GRANT SELECT (display_name) ON auth.principals TO %s`, q),
		fmt.Sprintf(`REVOKE SELECT (kind) ON auth.principals FROM %s`, q),
		fmt.Sprintf(`REVOKE SELECT (expires_at) ON auth.platform_role_assignments FROM %s`, q),
	} {
		if _, err := env.migration.Exec(ctx, change); err != nil {
			t.Fatalf("%s: %v", change, err)
		}
		if found := readerViolationsWith(t, ctx, env, on); !strings.Contains(describe(found), reader) {
			t.Errorf("after %q the posture does not name the reader:\n%s", change, describe(found))
		}
		if _, err := Apply(ctx, env.migration, on); err != nil {
			t.Fatalf("re-Apply: %v", err)
		}
		if found := readerViolationsWith(t, ctx, env, on); len(found) != 0 {
			t.Errorf("after re-Apply the posture is not exact: %s", describe(found))
		}
	}
}

// Not owned, the reader holds the session read alone.
func TestReaderRolesHoldNoPlatformReadWhileNotOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, pool := newSessionReader(t, ctx, env, "auth_platform_reader")
	if _, err := Apply(ctx, env.migration, platformReaderOptions(env, false, reader)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := readerStatement(ctx, pool, platformReadStatement); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("the platform read while not owned: %v, want permission denied", err)
	}
	if got := readerColumnGrants(t, ctx, env, reader); got != sessionColumnGrants {
		t.Errorf("column grants = %s, want %s", got, sessionColumnGrants)
	}
	if found := readerViolationsWith(t, ctx, env, platformReaderOptions(env, true, reader)); !strings.Contains(describe(found), "missing grant") {
		t.Errorf("the owned posture does not name the missing platform read:\n%s", describe(found))
	}
}

// Turning the switch off takes the platform read from the readers: a narrowing
// that is refused (nothing changed) unless acknowledged; with DeferNarrowing
// and the acknowledgement the readers keep it (old owned pods still read) and
// the posture excuses it.
func TestTurningTheSwitchOffIsANarrowing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	reader, pool := newSessionReader(t, ctx, env, "auth_platform_reader")
	if _, err := Apply(ctx, env.migration, platformReaderOptions(env, true, reader)); err != nil {
		t.Fatalf("Apply owned: %v", err)
	}
	off := platformReaderOptions(env, false, reader)
	_, err := Apply(ctx, env.migration, off)
	var narrowing *NarrowingError
	if !errors.As(err, &narrowing) || !strings.Contains(err.Error(), reader) || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("turning the switch off: %v, want a narrowing that names the reader's platform read", err)
	}
	if err := readerStatement(ctx, pool, platformReadStatement); err != nil {
		t.Fatalf("the refused run changed the reader's grants: %v", err)
	}

	deferred := off
	deferred.AcknowledgeNarrowing, deferred.DeferNarrowing = narrowing.Digest, true
	if _, err := Apply(ctx, env.migration, deferred); err != nil {
		t.Fatalf("Apply off, deferred and acknowledged: %v", err)
	}
	if err := readerStatement(ctx, pool, platformReadStatement); err != nil {
		t.Fatalf("a deferred narrowing removed the platform read: %v", err)
	}

	acknowledged := off
	acknowledged.AcknowledgeNarrowing = narrowing.Digest
	if _, err := Apply(ctx, env.migration, acknowledged); err != nil {
		t.Fatalf("Apply off, acknowledged: %v", err)
	}
	if err := readerStatement(ctx, pool, platformReadStatement); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("after the acknowledged narrowing: %v, want permission denied", err)
	}
	if found := readerViolationsWith(t, ctx, env, off); len(found) != 0 {
		t.Errorf("posture after the narrowing: %s", describe(found))
	}
	if _, err := Apply(ctx, env.migration, off); err != nil {
		t.Errorf("a second run off narrows nothing: %v", err)
	}
}
