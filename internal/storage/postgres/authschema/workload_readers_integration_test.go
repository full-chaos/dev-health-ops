//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func columnGrantsOf(t *testing.T, ctx context.Context, env *authFixture, role string) string {
	t.Helper()
	rows, err := env.admin.Query(ctx, `SELECT table_name || '.' || column_name || ':' || privilege_type
		FROM information_schema.column_privileges WHERE grantee = $1 AND table_schema = $2 ORDER BY 1`, role, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var entry string
		if err := rows.Scan(&entry); err != nil {
			t.Fatal(err)
		}
		out = append(out, entry)
	}
	return strings.Join(out, ",")
}

// go-api's role (a workload reader) holds SessionReaderReads plus the workload registry read, exactly; query-api's
// role holds SessionReaderReads alone and has no grant on workloads or principals; unset, nobody holds the read.
func TestWorkloadReaderRoleReadsTheRegistryAndTheQueryAPIRoleDoesNot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	api, apiPool := newSessionReader(t, ctx, env, "auth_wl_api")
	query, queryPool := newSessionReader(t, ctx, env, "auth_wl_query")
	options := func(workload ...string) Options {
		return Options{Schema: env.schema, RuntimeRole: env.runtimeRole, SessionReaderRoles: []string{api, query}, WorkloadReaderRoles: workload,
			Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	}
	// Never set: nobody holds the read and the run is not refused.
	if _, err := Apply(ctx, env.migration, options()); err != nil {
		t.Fatalf("Apply with no workload reader (never set): %v", err)
	}
	if _, err := Apply(ctx, env.migration, options(api)); err != nil {
		t.Fatalf("Apply with a workload reader: %v", err)
	}
	// Set to set: a re-apply changes nothing and is not refused.
	if _, err := Apply(ctx, env.migration, options(api)); err != nil {
		t.Fatalf("Apply again with the same workload reader: %v", err)
	}

	const registry = `SELECT w.principal_id, w.name, w.organization_id, w.audiences, w.disabled_at, p.id, p.disabled_at
		FROM auth.workloads w JOIN auth.principals p ON p.id = w.principal_id`
	if err := readerStatement(ctx, apiPool, registry); err != nil {
		t.Fatalf("the go-api role cannot read the registry row: %v", err)
	}
	if err := readerStatement(ctx, apiPool, `SELECT id, principal_id, revoked_at, legacy_token_version, created_at, web_session_id FROM auth.sessions`); err != nil {
		t.Fatalf("the go-api role lost its session read: %v", err)
	}
	for _, refused := range []string{`SELECT * FROM auth.workloads`, `SELECT id, kind FROM auth.principals`, `SELECT 1 FROM auth.workload_bindings`,
		`SELECT 1 FROM auth.credential_registry`, `UPDATE auth.workloads SET disabled_at = now()`} {
		if err := readerStatement(ctx, apiPool, refused); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s as the go-api role: err = %v, want permission denied", refused, err)
		}
	}
	for _, refused := range []string{registry, `SELECT principal_id FROM auth.workloads`, `SELECT id FROM auth.principals`} {
		if err := readerStatement(ctx, queryPool, refused); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s as the query-api role: err = %v, want permission denied", refused, err)
		}
	}
	if got, want := columnGrantsOf(t, ctx, env, query), "sessions.created_at:SELECT,sessions.id:SELECT,sessions.legacy_token_version:SELECT,sessions.principal_id:SELECT,sessions.revoked_at:SELECT,sessions.web_session_id:SELECT,users.password_updated_at:SELECT,users.principal_id:SELECT"; got != want {
		t.Errorf("the query-api role's column grants = %s, want %s", got, want)
	}
	wantAPI := "principals.disabled_at:SELECT,principals.id:SELECT,sessions.created_at:SELECT,sessions.id:SELECT,sessions.legacy_token_version:SELECT,sessions.principal_id:SELECT,sessions.revoked_at:SELECT,sessions.web_session_id:SELECT,users.password_updated_at:SELECT,users.principal_id:SELECT," +
		"workloads.audiences:SELECT,workloads.disabled_at:SELECT,workloads.name:SELECT,workloads.organization_id:SELECT,workloads.principal_id:SELECT"
	if got := columnGrantsOf(t, ctx, env, api); got != wantAPI {
		t.Errorf("the go-api role's column grants = %s, want %s", got, wantAPI)
	}
	for _, has := range []struct {
		role, table, column string
		want                bool
	}{{api, "workloads", "principal_id", true}, {query, "workloads", "principal_id", false}, {query, "principals", "disabled_at", false}} {
		var got bool
		if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, $2, $3, 'SELECT')`, has.role, env.schema+"."+has.table, has.column).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != has.want {
			t.Errorf("has_column_privilege(%s, %s.%s) = %v, want %v", has.role, has.table, has.column, got, has.want)
		}
	}
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if found, err := VerifyRuntimePosture(ctx, conn.Conn(), options(api)); err != nil || len(found) != 0 {
		t.Fatalf("posture with the workload reader: %v %s", err, describe(found))
	}
	if found, err := VerifyRuntimePosture(ctx, conn.Conn(), options()); err != nil || len(found) == 0 {
		t.Fatalf("a go-api role holding the workload read while no role is named for it must be a violation: %v %s", err, describe(found))
	}

	// Set to unset: removing a read a running service depends on is a narrowing, refused until the digest it prints is
	// acknowledged; the refusal changes nothing.
	refusal := options()
	_, err = Apply(ctx, env.migration, refusal)
	digest := narrowingDigestOf(t, err)
	if err := readerStatement(ctx, apiPool, registry); err != nil {
		t.Fatalf("the refused run changed the go-api role's read: %v", err)
	}
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("set to unset: %v", err)
	}
	for _, want := range []string{"workloads", "principals"} {
		found := false
		for _, lost := range narrowed.Lost {
			found = found || lost.Object == want
		}
		if !found {
			t.Errorf("the refusal does not name a lost %s column: %+v", want, narrowed.Lost)
		}
	}
	acknowledged := options()
	acknowledged.AcknowledgeNarrowing = digest
	if _, err := Apply(ctx, env.migration, acknowledged); err != nil {
		t.Fatalf("Apply with no workload reader and the acknowledgement: %v", err)
	}
	if err := readerStatement(ctx, apiPool, registry); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("unset and acknowledged, the go-api role still reads the registry: %v", err)
	}
	if found, err := VerifyRuntimePosture(ctx, conn.Conn(), options()); err != nil || len(found) != 0 {
		t.Fatalf("posture with no workload reader: %v %s", err, describe(found))
	}
}

// The same set-to-unset refusal with the platform roles switch ON: the principals read stays (it is the platform read
// too), the workloads read is revoked, and the run still needs the acknowledgement.
func TestWorkloadReaderRemovalNeedsTheAcknowledgementWithPlatformRolesOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	api, apiPool := newSessionReader(t, ctx, env, "auth_wl_api_on")
	options := func(workload ...string) Options {
		return Options{Schema: env.schema, RuntimeRole: env.runtimeRole, SessionReaderRoles: []string{api}, WorkloadReaderRoles: workload,
			PlatformRolesOwned: true, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	}
	if _, err := Apply(ctx, env.migration, options(api)); err != nil {
		t.Fatalf("Apply with a workload reader: %v", err)
	}
	_, err := Apply(ctx, env.migration, options())
	digest := narrowingDigestOf(t, err)
	if err := readerStatement(ctx, apiPool, `SELECT principal_id FROM auth.workloads`); err != nil {
		t.Fatalf("the refused run changed the read: %v", err)
	}
	acknowledged := options()
	acknowledged.AcknowledgeNarrowing = digest
	if _, err := Apply(ctx, env.migration, acknowledged); err != nil {
		t.Fatalf("acknowledged: %v", err)
	}
	if err := readerStatement(ctx, apiPool, `SELECT principal_id FROM auth.workloads`); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("the workloads read survives: %v", err)
	}
	if err := readerStatement(ctx, apiPool, `SELECT id, kind, disabled_at FROM auth.principals`); err != nil {
		t.Fatalf("the platform roles read of principals was lost: %v", err)
	}
}
