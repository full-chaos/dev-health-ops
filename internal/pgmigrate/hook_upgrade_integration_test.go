//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// oldestSupportedProdHead is the newest Alembic application revision a production database may
// still be at when a release's migrate hook runs. It is the revision the deployed database was
// upgraded to by the Python `dev-hops migrate postgres` hook before the hook became the Go verb;
// rev 186's hook failed on such a database (CHAOS-6801: below_head 0138 < 0140). Raise it only
// when no deployed database is at the old revision any more.
const oldestSupportedProdHead = "0138"

// hookStatesSHA256 pins testdata/golden/hook_states.json (the record verb rewrites it).
const hookStatesSHA256 = "8999636f1fbe4b31a3a9b8b3f653c57263d08b354f9c7246a8d784121cfcf327"

// TestHookVerbUpgradesAnOldProductionDatabaseToTheAlembicHead is the differential behind
// CHAOS-6801. A production database that is at an older Alembic head must be brought to the head by
// the verb the chart's pre-upgrade migrate hook runs (`dho migrate upgrade`, whose first step is
// `dho migrate postgres upgrade`), and the result must be the database the real Alembic upgrade
// builds from the same starting point: same schema, same recorded heads, same seeded rows.
//
// It is the executed proof the empty-database differential (TestBaselineIsTheFrozenPythonUpgrade)
// cannot give: that one applies the baseline to an EMPTY database, the one starting point the hook
// never has in production. Trap #415: it runs the verb the hook runs, not a stand-in.
func TestHookVerbUpgradesAnOldProductionDatabaseToTheAlembicHead(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	if instance.Container == nil {
		t.Fatal("this test dumps databases with the server's own pg_dump and needs a container instance, not a remote DSN")
	}
	admin := connect(t, instance.URI)
	golden, frozen := openStatesGolden(t, "testdata/golden/hook_states.json", hookStatesSHA256, "TestHookVerbUpgradesAnOldProductionDatabaseToTheAlembicHead",
		[]stateSpec{{Kind: "era", Revisions: []string{oldestSupportedProdHead}}, {Kind: "era-head", Revisions: []string{oldestSupportedProdHead}}})

	t.Setenv(pgmigrate.CutoverEnv, "1")
	t.Setenv(pgmigrate.RiverSchemaEnv, productionSettings.RiverSchema)
	os.Unsetenv("MIGRATION_DATABASE_URI")
	// 1. the old release's database.
	// The REAL Python (the old release's hook, run at the old head) built it once; the frozen capture is restored
	// here and asserted (heads, tables, row counts) before the hook sees it.
	eraName := "era:" + oldestSupportedProdHead
	eraRestored := frozen.restoreState(t, ctx, instance, admin, eraName)
	eraDB := eraRestored.database
	era := capture(t, ctx, instance, eraDB, window{})
	if want := []string{"0066", oldestSupportedProdHead}; strings.Join(era.Heads, ",") != strings.Join(want, ",") {
		t.Fatalf("the old-release database records heads %v, want %v", era.Heads, want)
	}

	// 2. a fork of it (no connection may be open on the template).
	if _, err := admin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '"+eraDB+"' AND pid <> pg_backend_pid()"); err != nil {
		t.Fatal(err)
	}
	fork := func() string {
		name := scratchDatabaseName(t)
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE "+eraDB); err != nil {
			t.Fatalf("fork the old-release database: %v", err)
		}
		t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
		return name
	}
	hookDB := fork()
	// The capture replaces a timestamp stamped inside its window (padded by 2 s) with now(). The era's
	// seed rows carry a fixed literal stamp (the recording replaced theirs by eraStampLiteral), so they
	// stay literal: a hook that rewrote one would show. Keep the pause so the window never reaches them.
	time.Sleep(3 * time.Second)

	// 3a. the truth: the real Alembic upgrade of the old database to head, frozen (see above).

	// 3b. the hook's verb, exactly as the migrate Job runs its first step.
	hookStarted := time.Now()
	stdout, stderr := new(bytes.Buffer), new(bytes.Buffer)
	group := rivermigrate.Command()
	verb := findVerb(t, group, "postgres", "upgrade")
	code := verb.Run(ctx, cli.Env{
		Lookup: func(key string) (string, bool) {
			switch key {
			case "MIGRATION_DATABASE_URI":
				return databaseURI(t, instance.URI, hookDB), true
			case pgmigrate.CutoverEnv:
				return "1", true
			case pgmigrate.RiverSchemaEnv:
				return productionSettings.RiverSchema, true
			}
			return "", false
		},
		Stdout: stdout, Stderr: stderr,
	})
	if code != cli.ExitOK {
		t.Fatalf("`dho migrate postgres upgrade` (the migrate hook's first step) exited %d on a database at %s:\nstdout: %s\nstderr: %s",
			code, oldestSupportedProdHead, stdout, stderr)
	}
	hookWindow := window{hookStarted, time.Now()}

	// 4. same database.
	alembicCapture := frozen.state(t, "era-head:"+oldestSupportedProdHead).Capture
	hookCapture := capture(t, ctx, instance, hookDB, hookWindow)
	hookCapture.Cutover, hookCapture.RiverSchema = productionSettings.Cutover, productionSettings.RiverSchema
	alembicCanonical := roundTrip(t, ctx, instance, admin, alembicCapture)
	hookCanonical := roundTrip(t, ctx, instance, admin, hookCapture)
	if diff := compare(hookCanonical, alembicCanonical); diff != "" {
		t.Fatalf("the migrate hook's verb built a different database from a %s database than the Alembic upgrade: %s", oldestSupportedProdHead, diff)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// scratchDatabaseName returns a fresh random database name (the caller creates and drops it).
func scratchDatabaseName(t *testing.T) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	return "dho_pg_hook_" + hex.EncodeToString(suffix)
}

// findVerb finds a verb by path under group.
func findVerb(t *testing.T, group cli.Command, path ...string) cli.Command {
	t.Helper()
	node := group
	for _, name := range path {
		found := false
		for _, child := range node.Children {
			if child.Name == name {
				node, found = child, true
				break
			}
		}
		if !found {
			t.Fatalf("no %q under %q", name, node.Name)
		}
	}
	return node
}
