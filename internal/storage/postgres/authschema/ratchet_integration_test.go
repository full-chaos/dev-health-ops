//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func hasPrivilege(t *testing.T, ctx context.Context, env *authFixture, table, privilege string) bool {
	t.Helper()
	var has bool
	if err := env.admin.QueryRow(ctx,
		`SELECT has_table_privilege($1, $2, $3)`, env.runtimeRole, env.schema+"."+table, privilege,
	).Scan(&has); err != nil {
		t.Fatalf("has_table_privilege: %v", err)
	}
	return has
}

func manifestWithout(table string) []TablePrivilege {
	var kept []TablePrivilege
	for _, entry := range RuntimePosture() {
		if entry.Table != table {
			kept = append(kept, entry)
		}
	}
	return kept
}

// A manifest that silently lost an entry must be refused against the role's
// real prior grants, and the refusal must leave those grants in place.
func TestRatchetRefusesASilentlyNarrowedManifestAndRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	options := Options{Schema: env.schema, RuntimeRole: env.runtimeRole}

	if !hasPrivilege(t, ctx, env, "sessions", "DELETE") {
		t.Fatal("precondition: the fixture runtime role must hold DELETE on sessions")
	}
	err = applyRuntimeGrants(ctx, conn.Conn(), manifestWithout("sessions"), options)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) || !errors.Is(err, ErrRuntimeGrantsNarrowed) {
		t.Fatalf("applyRuntimeGrants = %v, want *NarrowingError", err)
	}
	if !hasPrivilege(t, ctx, env, "sessions", "DELETE") {
		t.Fatal("a refused narrowing must roll back; the role lost DELETE on sessions")
	}

	options.AcknowledgeNarrowing = "0000000000000000"
	if err := applyRuntimeGrants(ctx, conn.Conn(), manifestWithout("sessions"), options); !errors.Is(err, ErrRuntimeGrantsNarrowed) {
		t.Fatalf("a wrong digest must still refuse; got %v", err)
	}

	options.AcknowledgeNarrowing = narrowed.Digest
	if err := applyRuntimeGrants(ctx, conn.Conn(), manifestWithout("sessions"), options); err != nil {
		t.Fatalf("the printed digest must acknowledge the same narrowing: %v", err)
	}
	if hasPrivilege(t, ctx, env, "sessions", "DELETE") {
		t.Fatal("an acknowledged narrowing must take effect")
	}
}

func TestRatchetIgnoresInheritedGrantsAndFirstRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	options := Options{Schema: env.schema, RuntimeRole: env.runtimeRole}

	group := env.runtimeRole + "_grp"
	for _, statement := range []string{
		fmt.Sprintf(`CREATE ROLE %s NOLOGIN`, Quote(mustValidatedIdentifier(group))),
		fmt.Sprintf(`GRANT TRUNCATE ON %s.sessions TO %s`, Quote(mustValidatedIdentifier(env.schema)), Quote(mustValidatedIdentifier(group))),
		fmt.Sprintf(`GRANT %s TO %s`, Quote(mustValidatedIdentifier(group)), Quote(mustValidatedIdentifier(env.runtimeRole))),
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		_, _ = env.admin.Exec(context.Background(), fmt.Sprintf(`DROP OWNED BY %s`, Quote(mustValidatedIdentifier(group))))
	})

	before, err := snapshotDirectGrants(ctx, conn, env.schema, env.runtimeRole)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range before {
		if g.Privilege == "TRUNCATE" {
			t.Fatalf("an inherited grant leaked into the direct snapshot: %v", g)
		}
	}
	if err := applyRuntimeGrants(ctx, conn.Conn(), RuntimePosture(), options); err != nil {
		t.Fatalf("an unchanged manifest over a role with inherited grants must pass: %v", err)
	}

	empty, err := snapshotDirectGrants(ctx, conn, env.schema, "no_such_role_for_ratchet")
	if err != nil || len(empty) != 0 {
		t.Fatalf("a role that does not exist holds nothing: %v %v", empty, err)
	}
}

func TestRatchetSeesALostGrantOption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	options := Options{Schema: env.schema, RuntimeRole: env.runtimeRole}

	if _, err := env.admin.Exec(ctx, fmt.Sprintf(
		`GRANT SELECT ON %s.sessions TO %s WITH GRANT OPTION`,
		Quote(mustValidatedIdentifier(env.schema)), Quote(mustValidatedIdentifier(env.runtimeRole)),
	)); err != nil {
		t.Fatal(err)
	}
	if err := applyRuntimeGrants(ctx, conn.Conn(), RuntimePosture(), options); !errors.Is(err, ErrRuntimeGrantsNarrowed) {
		t.Fatalf("losing WITH GRANT OPTION must refuse; got %v", err)
	}
}

// Two different lost grants whose display forms are identical must not share an
// acknowledgement.
func TestRatchetDigestDoesNotCoverADifferentDottedIdentifierGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	options := Options{Schema: env.schema, RuntimeRole: env.runtimeRole}
	schema := Quote(mustValidatedIdentifier(env.schema))
	role := Quote(mustValidatedIdentifier(env.runtimeRole))

	for _, statement := range []string{
		fmt.Sprintf(`CREATE TABLE %s."a" ("b.c" int, c int)`, schema),
		fmt.Sprintf(`CREATE TABLE %s."a.b" ("c" int)`, schema),
		fmt.Sprintf(`GRANT SELECT ("b.c") ON %s."a" TO %s`, schema, role),
	} {
		if _, err := env.migration.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	var first *NarrowingError
	if err := applyRuntimeGrants(ctx, conn.Conn(), RuntimePosture(), options); !errors.As(err, &first) {
		t.Fatalf("first loss must refuse; got %v", err)
	}
	if _, err := env.migration.Exec(ctx, fmt.Sprintf(`REVOKE SELECT ("b.c") ON %s."a" FROM %s`, schema, role)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.migration.Exec(ctx, fmt.Sprintf(`GRANT SELECT ("c") ON %s."a.b" TO %s`, schema, role)); err != nil {
		t.Fatal(err)
	}
	options.AcknowledgeNarrowing = first.Digest
	if err := applyRuntimeGrants(ctx, conn.Conn(), RuntimePosture(), options); !errors.Is(err, ErrRuntimeGrantsNarrowed) {
		t.Fatalf("an acknowledgement for a.\"b.c\" must not cover \"a.b\".c; got %v", err)
	}
}
