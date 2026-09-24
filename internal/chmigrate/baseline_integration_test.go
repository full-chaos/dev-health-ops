//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// updateEnv, set to 1, rewrites baseline/contract<N>.json from the executed
// chain instead of comparing against it. Only a person regenerating the head
// sets it; CI never does, so there the capture is a drift check.
const updateEnv = "DHO_CH_BASELINE_UPDATE"

// TestBaselineIsTheExecutedPythonChain is the head's provenance and the
// differential oracle, for each ordering contract:
//
//  1. capture: run the REAL Python chain on a fresh database and read back
//     every table and view, the seeded rows and the recorded versions. The
//     checked-in baseline must equal that capture (drift check, by
//     execution: nothing here reads a digest).
//  2. oracle: `chmigrate.Upgrade` on a second fresh database must produce
//     exactly what the Python chain produced -- same CREATE statements, same
//     seeded rows, same versions.
//  3. a second Upgrade applies nothing; a database missing one baseline
//     version is refused as below the head; a contract-1 head read under
//     contract 2 is below the head by migration 067.
func TestBaselineIsTheExecutedPythonChain(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close clickhouse: %v", err)
		}
	})
	admin := openDatabase(t, instance.URI, "")
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}

	var contractOneHead string
	for _, contract := range []int{1, 2} {
		t.Run(fmt.Sprintf("contract%d", contract), func(t *testing.T) {
			t.Setenv(chmigrate.OrderingContractEnv, fmt.Sprint(contract))

			pythonDB := scratchDatabase(t, admin)
			chschema.ApplyChain(ctx, t, httpDSN(t, ctx, instance, pythonDB))
			captured := capture(t, ctx, openDatabase(t, instance.URI, pythonDB), pythonDB, contract)
			if captured.Contract != contract {
				t.Fatalf("captured contract %d, want %d", captured.Contract, contract)
			}

			updating := os.Getenv(updateEnv) == "1"
			checkedIn := captured
			if updating {
				writeBaseline(t, captured)
			} else if checkedIn, err = chmigrate.LoadBaseline(contract); err != nil {
				t.Fatal(err)
			}
			if diff := compare(captured, checkedIn); diff != "" {
				t.Fatalf("baseline/contract%d.json is not what the Python chain builds today (%s); "+
					"regenerate it with %s=1 go test -tags=integration -run TestBaselineIsTheExecutedPythonChain ./internal/chmigrate",
					contract, diff, updateEnv)
			}

			goDB := scratchDatabase(t, admin)
			goConn := openDatabase(t, instance.URI, goDB)
			db, database, err := chmigrate.NewConnDB(ctx, goConn)
			if err != nil {
				t.Fatal(err)
			}
			if database != goDB {
				t.Fatalf("connection migrates %q, want %q", database, goDB)
			}
			result, err := chmigrate.Upgrade(ctx, db, checkedIn, chain)
			if err != nil {
				t.Fatalf("upgrade a fresh database: %v", err)
			}
			if result.Action != "baseline_applied" || len(result.Created) != len(checkedIn.Objects) {
				t.Fatalf("fresh upgrade = %+v, want baseline_applied creating %d objects", result, len(checkedIn.Objects))
			}
			if diff := compare(capture(t, ctx, goConn, goDB, contract), captured); diff != "" {
				t.Fatalf("dho migrate clickhouse built a different database than the Python chain: %s", diff)
			}

			again, err := chmigrate.Upgrade(ctx, db, checkedIn, chain)
			if err != nil {
				t.Fatalf("re-run: %v", err)
			}
			if again.Action != "up_to_date" || len(again.Created) != 0 || len(again.Seeded) != 0 || len(again.Applied) != 0 {
				t.Fatalf("re-run changed the database: %+v", again)
			}

			if contract == 1 {
				contractOneHead = goDB
			}
			if contract == 1 && !updating {
				contractTwo, err := chmigrate.LoadBaseline(2)
				if err != nil {
					t.Fatal(err)
				}
				_, err = chmigrate.Upgrade(ctx, db, contractTwo, chain)
				var below chmigrate.BelowHeadError
				if !errors.As(err, &below) || !reflect.DeepEqual(below.Missing, []string{"067_operational_ordering_contract.py"}) {
					t.Fatalf("a contract-1 head upgraded under contract 2 = %v, want below the head by 067 only", err)
				}
			}

			dropped := checkedIn.Versions[len(checkedIn.Versions)/2]
			if err := goConn.Exec(ctx, "ALTER TABLE schema_migrations DELETE WHERE version = ? SETTINGS mutations_sync = 2", dropped); err != nil {
				t.Fatal(err)
			}
			_, err = chmigrate.Upgrade(ctx, db, checkedIn, chain)
			var below chmigrate.BelowHeadError
			if !errors.As(err, &below) || !reflect.DeepEqual(below.Missing, []string{dropped}) {
				t.Fatalf("upgrade with %s unrecorded = %v, want below the head naming it", dropped, err)
			}

			// An interrupted baseline leaves every object but no version. The
			// resume must read each object back from the real server, find it
			// identical to the baseline, create nothing and record the versions.
			if err := goConn.Exec(ctx, "TRUNCATE TABLE schema_migrations"); err != nil {
				t.Fatal(err)
			}
			resumed, err := chmigrate.Upgrade(ctx, db, checkedIn, chain)
			if err != nil {
				t.Fatalf("resume over a complete baseline: %v", err)
			}
			if resumed.Action != "baseline_applied" || len(resumed.Created) != 0 || len(resumed.Seeded) != 0 {
				t.Fatalf("resume = %+v, want the baseline recorded with nothing created", resumed)
			}
			if diff := compare(capture(t, ctx, goConn, goDB, contract), captured); diff != "" {
				t.Fatalf("the resumed database differs from the Python chain's: %s", diff)
			}
		})
	}
	if contractOneHead == "" {
		t.Fatal("the contract-1 case did not run")
	}
}

func scratchDatabase(t *testing.T, admin driver.Conn) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "dho_ch_baseline_" + hex.EncodeToString(suffix)
	ctx := context.Background()
	if err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() {
		if err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" SYNC"); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	return name
}

// openDatabase opens a native connection to database on the server behind uri
// (the uri's own database when database is empty).
func openDatabase(t *testing.T, uri, database string) driver.Conn {
	t.Helper()
	options, err := clickhouse.ParseDSN(uri)
	if err != nil {
		t.Fatal(err)
	}
	if database != "" {
		options.Auth.Database = database
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	return conn
}

func httpDSN(t *testing.T, ctx context.Context, instance *containers.Instance, database string) string {
	t.Helper()
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	return parsed.String()
}

// capture reads a migrated database back into baseline form: every object
// but a materialized view's `.inner` table (its view's CREATE makes it), with
// the database name removed; the rows of every non-empty table except
// schema_migrations, whose content is the version list.
func capture(t *testing.T, ctx context.Context, conn driver.Conn, database string, contract int) chmigrate.Baseline {
	t.Helper()
	baseline := chmigrate.Baseline{Contract: contract, Rows: map[string]string{}}
	rows, err := conn.Query(ctx, "SELECT name, engine, create_table_query, total_rows FROM system.tables "+
		"WHERE database = ? AND name NOT LIKE '.inner%' ORDER BY name", database)
	if err != nil {
		t.Fatal(err)
	}
	var withRows []string
	for rows.Next() {
		var object chmigrate.Object
		var total *uint64
		if err := rows.Scan(&object.Name, &object.Engine, &object.Create, &total); err != nil {
			t.Fatal(err)
		}
		object.Create = chmigrate.StripDatabase(object.Create, database)
		if strings.Contains(object.Create, database) {
			t.Fatalf("%s: the database name survives normalization: %s", object.Name, object.Create)
		}
		baseline.Objects = append(baseline.Objects, object)
		if object.IsView() || object.Name == chmigrate.SchemaMigrationsTable {
			continue
		}
		if total == nil {
			t.Fatalf("table %s (%s) reports no row count, so its seed rows cannot be captured", object.Name, object.Engine)
		}
		if *total > 0 {
			withRows = append(withRows, object.Name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for _, table := range withRows {
		lines, err := conn.Query(ctx, fmt.Sprintf("SELECT formatRow('JSONEachRow', *) FROM `%s`.`%s` ORDER BY tuple(*)", database, table))
		if err != nil {
			t.Fatal(err)
		}
		var body strings.Builder
		for lines.Next() {
			var line string
			if err := lines.Scan(&line); err != nil {
				t.Fatal(err)
			}
			body.WriteString(line)
		}
		if err := lines.Err(); err != nil {
			t.Fatal(err)
		}
		lines.Close()
		baseline.Rows[table] = body.String()
	}
	versions, err := conn.Query(ctx, "SELECT version FROM `"+database+"`.schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	for versions.Next() {
		var version string
		if err := versions.Scan(&version); err != nil {
			t.Fatal(err)
		}
		baseline.Versions = append(baseline.Versions, version)
	}
	if err := versions.Err(); err != nil {
		t.Fatal(err)
	}
	versions.Close()
	if len(baseline.Objects) == 0 || len(baseline.Versions) == 0 {
		t.Fatalf("database %s holds %d objects and %d versions after the chain", database, len(baseline.Objects), len(baseline.Versions))
	}
	return baseline
}

// compare returns "" when two baselines are equal, else what differs first.
func compare(got, want chmigrate.Baseline) string {
	if !reflect.DeepEqual(got.Versions, want.Versions) {
		return fmt.Sprintf("versions differ: %d vs %d (%v)", len(got.Versions), len(want.Versions), firstDifference(got.Versions, want.Versions))
	}
	gotObjects, wantObjects := map[string]chmigrate.Object{}, map[string]chmigrate.Object{}
	for _, object := range got.Objects {
		gotObjects[object.Name] = object
	}
	for _, object := range want.Objects {
		wantObjects[object.Name] = object
	}
	names := map[string]bool{}
	for name := range gotObjects {
		names[name] = true
	}
	for name := range wantObjects {
		names[name] = true
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	for _, name := range sorted {
		if gotObjects[name] != wantObjects[name] {
			return fmt.Sprintf("object %s differs:\n  got  %+v\n  want %+v", name, gotObjects[name], wantObjects[name])
		}
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		for table := range union(got.Rows, want.Rows) {
			if got.Rows[table] != want.Rows[table] {
				return fmt.Sprintf("seed rows of %s differ:\n  got  %q\n  want %q", table, got.Rows[table], want.Rows[table])
			}
		}
	}
	return ""
}

func union(a, b map[string]string) map[string]bool {
	out := map[string]bool{}
	for key := range a {
		out[key] = true
	}
	for key := range b {
		out[key] = true
	}
	return out
}

func firstDifference(a, b []string) string {
	for i := 0; i < len(a) || i < len(b); i++ {
		var x, y string
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			return fmt.Sprintf("index %d: %q vs %q", i, x, y)
		}
	}
	return "none"
}

func writeBaseline(t *testing.T, baseline chmigrate.Baseline) {
	t.Helper()
	data, err := json.MarshalIndent(baseline, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("baseline", fmt.Sprintf("contract%d.json", baseline.Contract))
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s: %d objects, %d seeded tables, %d versions", path, len(baseline.Objects), len(baseline.Rows), len(baseline.Versions))
}

// splitProgram prints, for every .sql migration, the statements the Python
// runner executes -- split_sql_statements itself, not a copy of it.
const splitProgram = `
import json, pathlib, sys
from dev_health_ops.migrations.clickhouse import split_sql_statements
directory = pathlib.Path(sys.argv[1])
out = {p.name: split_sql_statements(p.read_text(encoding="utf-8")) for p in sorted(directory.glob("*.sql"))}
print(json.dumps(out))
`

// TestSplitterMatchesPython runs the Python runner's own splitter over every
// real .sql migration and requires the Go port to produce the same statements.
func TestSplitterMatchesPython(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "src", "dev_health_ops", "migrations", "clickhouse")
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", splitProgram, directory)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.Output()
	if err != nil {
		t.Fatal(pyoracle.RunError(python, err, output))
	}
	var want map[string][]string
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode the Python split: %v", err)
	}
	if len(want) < 90 {
		t.Fatalf("the Python split covered %d files; the chain has more than 90 .sql migrations", len(want))
	}
	for name, statements := range want {
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		got := chmigrate.SplitStatements(string(data))
		if len(statements) == 0 {
			statements = nil
		}
		if !reflect.DeepEqual(got, statements) {
			t.Errorf("%s: Go split %d statements, Python %d; first difference %s", name, len(got), len(statements), firstDifference(got, statements))
		}
	}
}
