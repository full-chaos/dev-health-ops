//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestCommandEndToEnd drives `dho migrate postgres status|upgrade` through
// the command itself, on three databases: an empty one (status, upgrade,
// status, upgrade again), one whose alembic_version records the heads over
// no schema, and one that holds only a function. The last two must be
// refused by upgrade and reported by status, and neither verb may create a
// table there.
func TestCommandEndToEnd(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	admin := connect(t, instance.URI)

	verb := func(name, uri string) (int, map[string]any, string) {
		t.Helper()
		resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
			return secrets.NewValue(uri), "test", true
		})
		var run func(context.Context, cli.Env) int
		for _, child := range pgmigrate.Command(resolve).Children {
			if child.Name == name {
				run = child.Run
			}
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) {
			value, ok := map[string]string{pgmigrate.CutoverEnv: "1"}[key]
			return value, ok
		}
		code := run(ctx, cli.Env{Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		var out map[string]any
		source := stdout.Bytes()
		if code != cli.ExitOK {
			source = stderr.Bytes()
		}
		if err := json.Unmarshal(source, &out); err != nil {
			t.Fatalf("%s: exit %d, output %q is not JSON: %v", name, code, source, err)
		}
		return code, out, stderr.String()
	}
	tables := func(conn *pgx.Conn) int {
		t.Helper()
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('r','p')").Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	t.Run("an extension's objects alone", func(t *testing.T) {
		uri := databaseURI(t, instance.URI, scratchDatabase(t, admin))
		conn := connect(t, uri)
		if _, err := conn.Exec(ctx, "CREATE EXTENSION pgcrypto"); err != nil {
			t.Fatal(err)
		}
		if code, out, _ := verb("status", uri); code != cli.ExitOK || out["state"] != "empty" {
			t.Fatalf("status with only an extension = %d %v, want empty", code, out)
		}
	})

	t.Run("an empty database", func(t *testing.T) {
		uri := databaseURI(t, instance.URI, scratchDatabase(t, admin))
		conn := connect(t, uri)
		if code, out, _ := verb("status", uri); code != cli.ExitOK || out["state"] != "empty" || tables(conn) != 0 {
			t.Fatalf("status on empty = %d %v, %d tables", code, out, tables(conn))
		}
		if code, out, stderr := verb("upgrade", uri); code != cli.ExitOK || out["action"] != "baseline_applied" {
			t.Fatalf("upgrade on empty = %d %v %s", code, out, stderr)
		}
		baseline, err := pgmigrate.LoadBaseline()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tables(conn), len(baseline.Tables()); got != want {
			t.Fatalf("upgrade created %d tables, want the baseline's %d", got, want)
		}
		if code, out, _ := verb("status", uri); code != cli.ExitOK || out["state"] != "at_head" {
			t.Fatalf("status after upgrade = %d %v", code, out)
		}
		if code, out, _ := verb("upgrade", uri); code != cli.ExitOK || out["action"] != "up_to_date" {
			t.Fatalf("second upgrade = %d %v", code, out)
		}
	})

	for name, testCase := range map[string]struct {
		setup, state, code, detail string
	}{
		"the heads recorded over no schema": {
			"CREATE TABLE alembic_version (version_num varchar(32) PRIMARY KEY); INSERT INTO alembic_version VALUES ('0066'), ('0142')",
			"schema_mismatch", "schema_mismatch", "table(s) the head creates are absent",
		},
		"only a function": {
			"CREATE FUNCTION public.foreign_marker() RETURNS int LANGUAGE sql AS 'SELECT 1'",
			"foreign", "foreign_database", "no public.alembic_version table",
		},
		"only an enum type": {
			"CREATE TYPE public.foreign_mood AS ENUM ('a')",
			"foreign", "foreign_database", "no public.alembic_version table",
		},
		"only a River job row": {
			"CREATE SCHEMA river; CREATE TABLE river.river_job (id bigserial PRIMARY KEY, kind text NOT NULL); " +
				"INSERT INTO river.river_job (kind) VALUES ('metrics.remaining.extra_metrics')",
			"foreign", "foreign_database", "no public.alembic_version table",
		},
	} {
		t.Run(name, func(t *testing.T) {
			uri := databaseURI(t, instance.URI, scratchDatabase(t, admin))
			conn := connect(t, uri)
			if _, err := conn.Exec(ctx, testCase.setup); err != nil {
				t.Fatal(err)
			}
			before := tables(conn)
			if code, out, _ := verb("status", uri); code != cli.ExitOK || out["state"] != testCase.state {
				t.Fatalf("status = %d %v, want state %s", code, out, testCase.state)
			}
			code, out, stderr := verb("upgrade", uri)
			errorBody, _ := out["error"].(map[string]any)
			if code != cli.ExitFailure || errorBody["code"] != testCase.code || !strings.Contains(stderr, testCase.detail) {
				t.Fatalf("upgrade = %d %s, want the %s refusal", code, stderr, testCase.code)
			}
			if after := tables(conn); after != before {
				t.Fatalf("the refused upgrade changed the table count from %d to %d", before, after)
			}
		})
	}
}

// TestChainRevision applies a revision after the head through the same path
// a checked-in sql/ file takes: on a database at the head it runs the SQL and
// moves the application head in alembic_version, keeping the cutover head; a
// re-run applies nothing; a revision that fails leaves neither its objects
// nor a moved head behind.
func TestChainRevision(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	admin := connect(t, instance.URI)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	versions := func(conn *pgx.Conn) []string {
		t.Helper()
		rows, err := conn.Query(ctx, "SELECT version_num FROM alembic_version ORDER BY version_num")
		if err != nil {
			t.Fatal(err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	exists := func(conn *pgx.Conn, table string) bool {
		t.Helper()
		var found bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", "public."+table).Scan(&found); err != nil {
			t.Fatal(err)
		}
		return found
	}

	conn := connect(t, databaseURI(t, instance.URI, scratchDatabase(t, admin)))
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, nil); err != nil {
		t.Fatalf("apply the baseline: %v", err)
	}
	good := []pgmigrate.ChainFile{{Revision: "0143", Name: "0143_probe.sql", SQL: "CREATE TABLE chain_probe (id int PRIMARY KEY);"}}
	result, err := pgmigrate.Upgrade(ctx, conn, baseline, good)
	if err != nil || result.Action != "chain_applied" || strings.Join(result.Applied, ",") != "0143" {
		t.Fatalf("the chain revision = %+v, %v; want chain_applied [0143]", result, err)
	}
	if got := strings.Join(versions(conn), ","); got != "0066,0143" || !exists(conn, "chain_probe") {
		t.Fatalf("after the revision alembic_version = %s, chain_probe present %v; want 0066,0143 and present", got, exists(conn, "chain_probe"))
	}
	again, err := pgmigrate.Upgrade(ctx, conn, baseline, good)
	if err != nil || again.Action != "up_to_date" || len(again.Applied) != 0 {
		t.Fatalf("re-run = %+v, %v; want up_to_date", again, err)
	}

	bad := append(good, pgmigrate.ChainFile{Revision: "0144", Name: "0144_broken.sql", SQL: "CREATE TABLE chain_broken (id int); SELECT 1/0;"})
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, bad); err == nil || !strings.Contains(err.Error(), "0144_broken.sql") {
		t.Fatalf("a failing revision = %v, want an error naming 0144_broken.sql", err)
	}
	if got := strings.Join(versions(conn), ","); got != "0066,0143" || exists(conn, "chain_broken") {
		t.Fatalf("after the failed revision alembic_version = %s, chain_broken present %v; want 0066,0143 and absent", got, exists(conn, "chain_broken"))
	}
}
