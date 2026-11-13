//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func (e *publicReadsEnv) applyOwned(t *testing.T, ctx context.Context, owned bool, ack string) error {
	t.Helper()
	options := e.options(&bytes.Buffer{})
	options.SSOProvidersOwned = owned
	options.AcknowledgeNarrowing = ack
	_, err := Apply(ctx, e.migration, options)
	return err
}

func (e *publicReadsEnv) postureClean(t *testing.T, ctx context.Context, owned bool) {
	t.Helper()
	options := e.options(&bytes.Buffer{})
	options.SSOProvidersOwned = owned
	conn, err := e.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	conn.Release()
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	if len(found) > 0 {
		t.Fatalf("posture (sso providers owned %v) is not clean:\n%s", owned, describe(found))
	}
}

// With the switch on the runtime role holds exactly SSOProviderOwn on public.sso_providers (plus the record columns and
// the public reads it always holds), the posture is clean, the writes the routes run are allowed and every other write is
// refused; off, none of it is held and the posture is clean.
func TestSSOProviderOwnGrantIsExactlyTheDeclaredPrivilegesWithTheSwitchOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	own := SSOProviderOwn()
	union := func(a, b []string) []string {
		out := slices.Clone(a)
		for _, v := range b {
			if !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
		slices.Sort(out)
		return out
	}
	sorted := func(a []string) []string { out := slices.Clone(a); slices.Sort(out); return out }

	if err := env.applyOwned(t, ctx, false, ""); err != nil {
		t.Fatalf("Apply, switch off: %v", err)
	}
	env.postureClean(t, ctx, false)
	if got := env.heldColumns(t, ctx, ssoProvidersTable, "INSERT"); len(got) != 0 {
		t.Errorf("switch off: INSERT held on %v", got)
	}
	if env.tablePrivilege(t, ctx, ssoProvidersTable, "DELETE") {
		t.Error("switch off: table-level DELETE held")
	}

	if err := env.applyOwned(t, ctx, true, ""); err != nil {
		t.Fatalf("Apply, switch on: %v", err)
	}
	env.postureClean(t, ctx, true)
	if got := env.heldColumns(t, ctx, ssoProvidersTable, "INSERT"); !slices.Equal(got, sorted(own.Insert)) {
		t.Errorf("INSERT columns %v, want %v", got, sorted(own.Insert))
	}
	if got, want := env.heldColumns(t, ctx, ssoProvidersTable, "UPDATE"), union(own.Update, SSOProviderWriteColumns()); !slices.Equal(got, want) {
		t.Errorf("UPDATE columns %v, want %v", got, want)
	}
	if got, want := env.heldColumns(t, ctx, ssoProvidersTable, "SELECT"), union(publicReadOracle[ssoProvidersTable], own.Select); !slices.Equal(got, want) {
		t.Errorf("SELECT columns %v, want %v", got, want)
	}
	if !env.tablePrivilege(t, ctx, ssoProvidersTable, "DELETE") {
		t.Error("switch on: table-level DELETE not held")
	}
	for _, privilege := range []string{"INSERT", "UPDATE", "TRUNCATE", "TRIGGER", "REFERENCES"} {
		if env.tablePrivilege(t, ctx, ssoProvidersTable, privilege) {
			t.Errorf("switch on: table-level %s held", privilege)
		}
	}
	for name, statement := range map[string]string{
		"insert":        `INSERT INTO public.sso_providers (id, org_id, name, protocol, config, created_at, updated_at) SELECT gen_random_uuid(), gen_random_uuid(), 'n', 'oidc', '{}'::json, now(), now() WHERE false`,
		"update config": `UPDATE public.sso_providers SET config = '{}'::json, encrypted_secrets = NULL, name = 'n', updated_at = now() WHERE false`,
		"delete":        `DELETE FROM public.sso_providers WHERE false`,
		"read":          `SELECT name, last_error, last_login_at, updated_at, last_metadata_sync_at, last_error_at FROM public.sso_providers WHERE false`,
	} {
		if err := env.runtimeStatement(ctx, statement); err != nil {
			t.Errorf("switch on: %s by the runtime role: %v", name, err)
		}
	}
	for name, statement := range map[string]string{
		"update id":         `UPDATE public.sso_providers SET id = id WHERE false`,
		"update org_id":     `UPDATE public.sso_providers SET org_id = org_id WHERE false`,
		"update protocol":   `UPDATE public.sso_providers SET protocol = protocol WHERE false`,
		"update created_at": `UPDATE public.sso_providers SET created_at = created_at WHERE false`,
		"truncate":          `TRUNCATE public.sso_providers`,
	} {
		if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("switch on: %s by the runtime role: err = %v, want permission denied", name, err)
		}
	}

	// Turning the switch off again narrows the role: refused without the acknowledgement, and with it the grants go.
	err := env.applyOwned(t, ctx, false, "")
	if !errors.Is(err, ErrRuntimeGrantsNarrowed) {
		t.Fatalf("switch off again without the acknowledgement: err = %v, want ErrRuntimeGrantsNarrowed", err)
	}
	if !env.tablePrivilege(t, ctx, ssoProvidersTable, "DELETE") {
		t.Error("the refused narrowing took the DELETE away")
	}
	if err := env.applyOwned(t, ctx, false, narrowingDigestOf(t, err)); err != nil {
		t.Fatalf("switch off with the acknowledgement: %v", err)
	}
	env.postureClean(t, ctx, false)
	if got := env.heldColumns(t, ctx, ssoProvidersTable, "INSERT"); len(got) != 0 {
		t.Errorf("after the acknowledged narrowing INSERT is held on %v", got)
	}
	if env.tablePrivilege(t, ctx, ssoProvidersTable, "DELETE") {
		t.Error("after the acknowledged narrowing table-level DELETE is held")
	}
}

// Without a granted column the posture names it, and a DELETE the role holds beyond the declared set is named too.
func TestAMissingSSOProviderOwnGrantIsAPostureViolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	if err := env.applyOwned(t, ctx, true, ""); err != nil {
		t.Fatal(err)
	}
	role := Quote(mustValidatedIdentifier(env.options(&bytes.Buffer{}).RuntimeRole))
	options := env.options(&bytes.Buffer{})
	options.SSOProvidersOwned = true
	named := func(object, privilege string) bool {
		conn, err := env.migration.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range found {
			if v.Object == object && v.Privilege == privilege && strings.Contains(v.Detail, "missing grant") {
				return true
			}
		}
		return false
	}
	for statement, want := range map[string][2]string{
		`REVOKE INSERT (config) ON public.sso_providers FROM ` + role:     {"sso_providers.config", "INSERT"},
		`REVOKE UPDATE (name) ON public.sso_providers FROM ` + role:       {"sso_providers.name", "UPDATE"},
		`REVOKE SELECT (last_error) ON public.sso_providers FROM ` + role: {"sso_providers.last_error", "SELECT"},
		`REVOKE DELETE ON public.sso_providers FROM ` + role:              {"sso_providers", "DELETE"},
	} {
		if _, err := env.migration.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
		if !named(want[0], want[1]) {
			t.Errorf("after %q the posture does not name the missing %s %s", statement, want[1], want[0])
		}
	}
}

// The migrator precheck refuses, naming the table, a run of the switch on whose migration login lacks the grant option.
func TestTheMigratorPrecheckNamesAnSSOProviderPrivilegeItLacks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	if err := env.applyOwned(t, ctx, true, ""); err != nil {
		t.Fatal(err)
	}
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	options := env.options(&bytes.Buffer{})
	options.SSOProvidersOwned = true
	if err := migratorGrantOptionPrecheck(ctx, conn.Conn(), options); err != nil {
		t.Fatalf("the precheck with the grant options held: %v", err)
	}
}
