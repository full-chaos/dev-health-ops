//go:build integration

package authschema

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestSSOProviderWriteGrantIsTheRecordColumnsWhateverTheSwitch: with the users-owned switch off and on, the runtime
// role holds UPDATE on exactly the SSOProviderWriteColumns of public.sso_providers, keeps its read columns, holds
// no table-level write, and the posture is clean; the provider failure record and the sign-in record run, and an UPDATE
// of the status or of any other column, an INSERT and a DELETE are refused.
func TestSSOProviderWriteGrantIsTheRecordColumnsWhateverTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	want := slices.Clone(SSOProviderWriteColumns())
	slices.Sort(want)
	read := slices.Clone(publicReadOracle[ssoProvidersTable])
	slices.Sort(read)
	for _, owned := range []bool{false, true} {
		options := env.options(&bytes.Buffer{})
		options.UsersOwned = owned
		if _, err := Apply(ctx, env.migration, options); err != nil {
			t.Fatalf("Apply (users owned %v): %v", owned, err)
		}
		conn, err := env.migration.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
		conn.Release()
		if err != nil {
			t.Fatalf("VerifyRuntimePosture: %v", err)
		}
		if len(found) > 0 {
			t.Fatalf("posture (users owned %v) is not clean:\n%s", owned, describe(found))
		}
		if got := env.heldColumns(t, ctx, ssoProvidersTable, "UPDATE"); !slices.Equal(got, want) {
			t.Errorf("users owned %v: UPDATE columns %v, want %v", owned, got, want)
		}
		if got := env.heldColumns(t, ctx, ssoProvidersTable, "SELECT"); !slices.Equal(got, read) {
			t.Errorf("users owned %v: SELECT columns %v, want %v", owned, got, read)
		}
		for _, privilege := range []string{"INSERT", "REFERENCES"} {
			if got := env.heldColumns(t, ctx, ssoProvidersTable, privilege); len(got) != 0 {
				t.Errorf("users owned %v: %s held on %v", owned, privilege, got)
			}
		}
		for _, privilege := range []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE", "TRIGGER"} {
			if env.tablePrivilege(t, ctx, ssoProvidersTable, privilege) {
				t.Errorf("users owned %v: table-level %s held", owned, privilege)
			}
		}
		if err := env.runtimeStatement(ctx, `UPDATE public.sso_providers SET last_error = 'x', last_error_at = now(), updated_at = now()
WHERE id = '00000000-0000-0000-0000-000000000000'::uuid`); err != nil {
			t.Errorf("users owned %v: the provider failure record by the runtime role: %v", owned, err)
		}
		if err := env.runtimeStatement(ctx, `UPDATE public.sso_providers SET last_login_at = now(), updated_at = now()
WHERE id = '00000000-0000-0000-0000-000000000000'::uuid`); err != nil {
			t.Errorf("users owned %v: the sign-in record by the runtime role: %v", owned, err)
		}
		for name, statement := range map[string]string{
			"update status":            `UPDATE public.sso_providers SET status = 'error' WHERE false`,
			"update config":            `UPDATE public.sso_providers SET config = '{}'::json WHERE false`,
			"update encrypted_secrets": `UPDATE public.sso_providers SET encrypted_secrets = NULL WHERE false`,
			"insert":                   `INSERT INTO public.sso_providers DEFAULT VALUES`,
			"delete":                   `DELETE FROM public.sso_providers WHERE false`,
		} {
			if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("users owned %v: %s by the runtime role: err = %v, want permission denied", owned, name, err)
			}
		}
	}
}

// Without the grant the posture names each missing column UPDATE.
func TestAMissingSSOProviderWriteGrantIsAPostureViolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	role := Quote(mustValidatedIdentifier(env.options(&bytes.Buffer{}).RuntimeRole))
	for _, column := range []string{"last_error", "last_login_at"} {
		if _, err := env.migration.Exec(ctx, `REVOKE UPDATE (`+column+`) ON public.sso_providers FROM `+role); err != nil {
			t.Fatal(err)
		}
		var named bool
		for _, v := range env.violations(t, ctx) {
			if v.Object == "sso_providers."+column && v.Privilege == "UPDATE" && strings.Contains(v.Detail, "missing grant") {
				named = true
			}
		}
		if !named {
			t.Fatalf("violations %+v do not name the missing UPDATE (%s)", env.violations(t, ctx), column)
		}
	}
}
