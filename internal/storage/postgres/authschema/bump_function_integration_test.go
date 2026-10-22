//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The increment-only token-version function is held to its declared posture:
// the runtime role executes it and nothing else may.
func TestTheDeclaredDefinerFunctionIsHeldToItsPosture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f := newAuthFixture(t, ctx)
	conn, err := pgx.Connect(ctx, fixtureDSN(t, f.instance.URI, f.migrationRole))
	if err != nil {
		t.Fatalf("migration connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	options := Options{Schema: f.schema, RuntimeRole: f.runtimeRole}

	functionViolationsOf := func() []string {
		violations, err := VerifyRuntimePosture(ctx, conn, options)
		if err != nil {
			t.Fatalf("VerifyRuntimePosture: %v", err)
		}
		var out []string
		for _, v := range violations {
			if v.Kind == "function" {
				out = append(out, v.String())
			}
		}
		return out
	}
	if got := functionViolationsOf(); len(got) != 0 {
		t.Fatalf("the applied posture reports function violations: %v", got)
	}
	var executes bool
	if err := f.admin.QueryRow(ctx, `SELECT has_function_privilege($1, 'auth.bump_legacy_token_version(uuid)', 'EXECUTE')`, f.runtimeRole).Scan(&executes); err != nil || !executes {
		t.Fatalf("the runtime role cannot execute the function: %v", err)
	}

	if _, err := f.admin.Exec(ctx, `GRANT EXECUTE ON FUNCTION auth.bump_legacy_token_version(uuid) TO PUBLIC`); err != nil {
		t.Fatal(err)
	}
	got := functionViolationsOf()
	if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), "PUBLIC can execute it") {
		t.Errorf("a PUBLIC grant was not reported: %v", got)
	}
	if _, err := f.admin.Exec(ctx, `REVOKE ALL ON FUNCTION auth.bump_legacy_token_version(uuid) FROM PUBLIC`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.admin.Exec(ctx, `REVOKE EXECUTE ON FUNCTION auth.bump_legacy_token_version(uuid) FROM `+Quote(mustValidatedIdentifier(f.runtimeRole))); err != nil {
		t.Fatal(err)
	}
	got = functionViolationsOf()
	if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), "missing grant") {
		t.Errorf("a missing runtime grant was not reported: %v", got)
	}
}
