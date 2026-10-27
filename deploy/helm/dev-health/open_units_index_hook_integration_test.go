//go:build integration

package devhealth_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The open-units-index hook Job, RENDERED by helm with the flag on, is executed with its own args and the environment its pod
// would see, on a database one revision before 0161. It exits 0 and the index ends valid and ready; revision 0161 then
// finds it and does nothing.
func TestRenderedOpenUnitsIndexHookBuildsAValidIndex(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	position := -1
	for i, file := range chain {
		if file.Revision == "0161" {
			position = i
		}
	}
	if position < 0 {
		t.Fatal("the chain holds no revision 0161")
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain[:position]); err != nil {
		t.Fatalf("upgrade to the revision before 0161: %v", err)
	}

	args, environment := renderedOpenUnitsIndexArgs(t, "migrations.hook.secretData.MIGRATION_DATABASE_URI="+instance.URI)
	if environment["MIGRATION_DATABASE_URI"] != instance.URI {
		t.Fatal("the elevated DSN did not reach the hook's environment")
	}
	lookup := func(key string) (string, bool) { value, ok := environment[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	if code := runDho(ctx, args, lookup, &stdout, &stderr); code != 0 {
		t.Fatalf("the rendered hook %v failed: exit %d\nstdout:\n%s\nstderr:\n%s", args, code, stdout.String(), stderr.String())
	}
	var valid bool
	if err := conn.QueryRow(ctx, `SELECT i.indisvalid AND i.indisready FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'ix_sync_run_units_open_run' AND c.relnamespace = 'public'::regnamespace`).Scan(&valid); err != nil || !valid {
		t.Fatalf("the index after the hook: valid=%v err=%v\nstdout: %s", valid, err, stdout.String())
	}
	t.Logf("hook stdout: %s", stdout.String())
	result, err := pgmigrate.Upgrade(ctx, conn, baseline, chain)
	if err != nil || len(result.Applied) == 0 || result.Applied[0] != "0161" {
		t.Fatalf("upgrade after the hook: %+v err %v", result, err)
	}
}
