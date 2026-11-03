//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/ownermove"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-12614 fresh path: a NOSUPERUSER login that owns the database applies the baseline and the chain through
// `dho migrate postgres upgrade` under MIGRATION_OWNER_ROLE, and builds the same schema (privileges included, owners
// aside) as the superuser build; the same verb with a superuser DSN under the pin refuses before any DDL.
func TestUpgradeAsTheNosuperuserDatabaseOwnerUnderThePin(t *testing.T) {
	ctx := context.Background()
	instance := isolatedInstance(t)
	admin := connect(t, instance.URI)
	migrator, err := containers.RoleName("pin_migrator", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "owner-pin-test-only"
	if _, err := admin.Exec(ctx, "CREATE ROLE "+migrator+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
		t.Fatal(err)
	}
	pinnedDB := scratchDatabase(t, admin)
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "ALTER DATABASE "+pinnedDB+" OWNER TO CURRENT_USER")
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pinnedDB+" WITH (FORCE)")
		_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+migrator)
	})
	if _, err := admin.Exec(ctx, "ALTER DATABASE "+pinnedDB+" OWNER TO "+migrator); err != nil {
		t.Fatal(err)
	}
	superURI := databaseURI(t, instance.URI, pinnedDB)
	parsed, err := url.Parse(superURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(migrator, password)
	migratorURI := parsed.String()

	upgrade := func(uri string) (int, string) {
		t.Helper()
		resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
			return secrets.NewValue(uri), "test", true
		})
		var run func(context.Context, cli.Env) int
		for _, child := range pgmigrate.Command(resolve).Children {
			if child.Name == "upgrade" {
				run = child.Run
			}
		}
		var stdout, stderr bytes.Buffer
		settings := map[string]string{pgmigrate.CutoverEnv: "1", ownermove.EnvOwnerRole: migrator}
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		return run(ctx, cli.Env{Lookup: lookup, Stdout: &stdout, Stderr: &stderr}), stdout.String() + stderr.String()
	}
	tables := func() int {
		var n int
		if err := connect(t, superURI).QueryRow(ctx, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if code, out := upgrade(superURI); code == cli.ExitOK || !strings.Contains(out, "owner_pin_failed") || !strings.Contains(out, "migration login is") {
		t.Fatalf("a superuser DSN under the pin: exit %d\n%s", code, out)
	}
	if n := tables(); n != 0 {
		t.Fatalf("the refused run left %d relations in public", n)
	}
	started := time.Now()
	code, out := upgrade(migratorURI)
	if code != cli.ExitOK || !strings.Contains(out, `"baseline_applied"`) || !strings.Contains(out, ownermove.PinVerifiedMessage) {
		t.Fatalf("the pinned NOSUPERUSER owner: exit %d\n%s", code, out)
	}
	pinned := capture(t, ctx, instance, pinnedDB, window{started, time.Now()})
	if err := ownermove.VerifyPin(ctx, connect(t, migratorURI), migrator, "river"); err != nil {
		t.Fatalf("the pin after the upgrade: %v", err)
	}

	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	superDB := scratchDatabase(t, admin)
	superStarted := time.Now()
	if _, err := pgmigrate.Upgrade(ctx, connect(t, databaseURI(t, instance.URI, superDB)), baseline, chain); err != nil {
		t.Fatal(err)
	}
	super := capture(t, ctx, instance, superDB, window{superStarted, time.Now()})
	pinned.Cutover, pinned.RiverSchema = baseline.Cutover, baseline.RiverSchema
	super.Cutover, super.RiverSchema = baseline.Cutover, baseline.RiverSchema
	if diff := compare(roundTrip(t, ctx, instance, admin, pinned), roundTrip(t, ctx, instance, admin, super)); diff != "" {
		t.Fatalf("the NOSUPERUSER owner's build differs from the superuser build: %s", diff)
	}
}
