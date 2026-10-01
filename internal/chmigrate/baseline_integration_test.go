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
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// productionContract is the ordering contract the head is captured with:
// the one production runs (read from prod, 2026-09-24).
const productionContract = 2

// TestBaselineMatchesTheFrozenPythonChain is the head's provenance and the
// differential oracle:
//
//  1. golden: testdata/python_chain_contract2.json is what the REAL Python
//     chain built under production's contract on a fresh database (every table
//     and view, the seeded rows and the recorded versions), recorded once; the
//     Python producer is deleted with the Python CLI.
//  2. oracle and drift check: `chmigrate.Upgrade` -- the baseline plus every
//     chain file after it -- on a fresh database must produce exactly what the
//     golden holds: the same CREATE statements, seeded rows and versions. This
//     is by execution; nothing here reads a digest, and the Go chain is never
//     compared with itself.
//  3. a second Upgrade applies nothing; a database missing one baseline
//     version is refused as below the head; an interrupted baseline resumes;
//     an unversioned database holding an unrelated table is refused as
//     foreign; status reads every one of these states without writing.
func TestBaselineMatchesTheFrozenPythonChain(t *testing.T) {
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

	captured := loadFrozenChain(t, "python_chain_contract2.json")
	checkedIn, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}

	// The golden is the Python chain's end state at the last migration it was
	// recorded on. A chain file added after that has no Python build to record
	// it, so the oracle covers exactly the files the golden recorded; the
	// rest of this test runs the whole chain.
	recorded := map[string]bool{}
	for _, version := range captured.Versions {
		recorded[version] = true
	}
	for _, version := range checkedIn.Versions {
		if !recorded[version] {
			t.Fatalf("the golden does not record head version %s", version)
		}
	}
	var recordedChain []chmigrate.ChainFile
	for _, file := range chain {
		if recorded[file.Version] {
			recordedChain = append(recordedChain, file)
		}
	}
	oracleDB := scratchDatabase(t, admin)
	oracleConn := openDatabase(t, instance.URI, oracleDB)
	oracleStore, _, err := chmigrate.NewConnDB(ctx, oracleConn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, oracleStore, checkedIn, recordedChain); err != nil {
		t.Fatalf("upgrade a fresh database to the recorded head: %v", err)
	}
	if diff := compare(capture(t, ctx, oracleConn, oracleDB, productionContract), captured); diff != "" {
		t.Fatalf("dho migrate clickhouse built a different database than the Python chain: %s", diff)
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
	requireStatus(t, ctx, goConn, goDB, db, checkedIn, chain, "empty")
	result, err := chmigrate.Upgrade(ctx, db, checkedIn, chain)
	if err != nil {
		t.Fatalf("upgrade a fresh database: %v", err)
	}
	if result.Action != "baseline_applied" || len(result.Created) != len(checkedIn.Objects) || len(result.Applied) != len(chain) {
		t.Fatalf("fresh upgrade = %+v, want baseline_applied creating %d objects and applying %d chain files", result, len(checkedIn.Objects), len(chain))
	}
	requireStatus(t, ctx, goConn, goDB, db, checkedIn, chain, "at_head")

	again, err := chmigrate.Upgrade(ctx, db, checkedIn, chain)
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if again.Action != "up_to_date" || len(again.Created) != 0 || len(again.Seeded) != 0 || len(again.Applied) != 0 {
		t.Fatalf("re-run changed the database: %+v", again)
	}

	dropped := checkedIn.Versions[len(checkedIn.Versions)/2]
	if err := goConn.Exec(ctx, "ALTER TABLE schema_migrations DELETE WHERE version = ? SETTINGS mutations_sync = 2", dropped); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, ctx, goConn, goDB, db, checkedIn, chain, "below_head")
	_, err = chmigrate.Upgrade(ctx, db, checkedIn, chain)
	var below chmigrate.BelowHeadError
	if !errors.As(err, &below) || !reflect.DeepEqual(below.Missing, []string{dropped}) {
		t.Fatalf("upgrade with %s unrecorded = %v, want below the head naming it", dropped, err)
	}

	// An interrupted baseline leaves every baseline object but no version
	// (the chain runs only after the versions are recorded, so none of its
	// objects exist yet). The resume must read each object back from the real
	// server, find it identical to the baseline, create nothing and record
	// the versions.
	resumeDB := scratchDatabase(t, admin)
	resumeConn := openDatabase(t, instance.URI, resumeDB)
	resumeStore, _, err := chmigrate.NewConnDB(ctx, resumeConn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, resumeStore, checkedIn, nil); err != nil {
		t.Fatalf("apply the baseline alone: %v", err)
	}
	if err := resumeConn.Exec(ctx, "TRUNCATE TABLE schema_migrations"); err != nil {
		t.Fatal(err)
	}
	resumed, err := chmigrate.Upgrade(ctx, resumeStore, checkedIn, nil)
	if err != nil {
		t.Fatalf("resume over a complete baseline: %v", err)
	}
	if resumed.Action != "baseline_applied" || len(resumed.Created) != 0 || len(resumed.Seeded) != 0 {
		t.Fatalf("resume = %+v, want the baseline recorded with nothing created", resumed)
	}
	if diff := compare(capture(t, ctx, resumeConn, resumeDB, productionContract), checkedIn); diff != "" {
		t.Fatalf("the resumed database differs from the baseline: %s", diff)
	}

	// Version rows alone do not prove the schema: with the head recorded and
	// a baseline view dropped, status reports schema_mismatch naming it and
	// upgrade refuses without creating anything.
	var view chmigrate.Object
	for _, object := range checkedIn.Objects {
		if object.IsView() {
			view = object
			break
		}
	}
	if err := resumeConn.Exec(ctx, "DROP VIEW "+view.Name); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, ctx, resumeConn, resumeDB, resumeStore, checkedIn, nil, "schema_mismatch")
	_, err = chmigrate.Upgrade(ctx, resumeStore, checkedIn, nil)
	var mismatch chmigrate.SchemaMismatchError
	if !errors.As(err, &mismatch) || !reflect.DeepEqual(mismatch.MissingObjects, []string{view.Name}) {
		t.Fatalf("upgrade with %s dropped = %v, want a schema mismatch naming it", view.Name, err)
	}
	if err := resumeConn.Exec(ctx, view.Create); err != nil {
		t.Fatalf("recreate %s: %v", view.Name, err)
	}

	// A chain file after the head is applied, recorded, and reported as the
	// head.
	probe := []chmigrate.ChainFile{{Version: "999_probe.sql", SQL: "CREATE TABLE chain_probe (x Int8) ENGINE = Memory;\n"}}
	applied, err := chmigrate.Upgrade(ctx, resumeStore, checkedIn, probe)
	if err != nil || applied.Action != "chain_applied" || !reflect.DeepEqual(applied.Applied, []string{"999_probe.sql"}) || applied.Head != "999_probe.sql" {
		t.Fatalf("the chain probe = %+v, %v; want chain_applied [999_probe.sql] with head 999_probe.sql", applied, err)
	}
	status, err := chmigrate.ReadStatus(ctx, resumeStore, checkedIn, probe)
	if err != nil || status.State != "at_head" || status.Head != "999_probe.sql" || len(status.Pending) != 0 {
		t.Fatalf("status after the probe = %+v, %v; want at_head with head 999_probe.sql", status, err)
	}

	// An unversioned database holding a table the baseline does not create
	// is not dho's to migrate.
	foreignDB := scratchDatabase(t, admin)
	foreignConn := openDatabase(t, instance.URI, foreignDB)
	if err := foreignConn.Exec(ctx, "CREATE TABLE unrelated_sentinel (x Int8) ENGINE = Memory"); err != nil {
		t.Fatal(err)
	}
	foreignStore, _, err := chmigrate.NewConnDB(ctx, foreignConn)
	if err != nil {
		t.Fatal(err)
	}
	requireStatus(t, ctx, foreignConn, foreignDB, foreignStore, checkedIn, chain, "foreign")
	_, err = chmigrate.Upgrade(ctx, foreignStore, checkedIn, chain)
	var foreign chmigrate.ForeignDatabaseError
	if !errors.As(err, &foreign) || !reflect.DeepEqual(foreign.Objects, []string{"unrelated_sentinel"}) {
		t.Fatalf("upgrade over an unrelated table = %v, want a foreign-database refusal naming it", err)
	}
	if count := objectCount(t, ctx, foreignConn, foreignDB); count != 1 {
		t.Fatalf("a refused upgrade left %d objects, want the 1 it found", count)
	}

	// The command reads the real chain, so the database it reports as at_head
	// must have applied every chain file, not only the probe above.
	if _, err := chmigrate.Upgrade(ctx, resumeStore, checkedIn, chain); err != nil {
		t.Fatalf("apply the real chain to the resumed database: %v", err)
	}

	// `status --check`, the wait-for-migrations probe, through the command:
	// exit 0 only at the head, 1 for anything else, the JSON printed either
	// way, and nothing written.
	emptyDB := scratchDatabase(t, admin)
	for _, testCase := range []struct {
		database, state string
		code            int
	}{
		{resumeDB, "at_head", cli.ExitOK},
		{foreignDB, "foreign", cli.ExitFailure},
		{emptyDB, "empty", cli.ExitFailure},
	} {
		dsn, err := url.Parse(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		dsn.Path = "/" + testCase.database
		var run func(context.Context, cli.Env) int
		for _, child := range chmigrate.Command().Children {
			if child.Name == "status" {
				run = child.Run
			}
		}
		var stdout, stderr strings.Builder
		lookup := func(key string) (string, bool) {
			value, ok := map[string]string{chmigrate.ClickHouseURIKey: dsn.String(), chmigrate.OrderingContractEnv: "2"}[key]
			return value, ok
		}
		conn := openDatabase(t, instance.URI, testCase.database)
		objectsBefore := objectCount(t, ctx, conn, testCase.database)
		code := run(ctx, cli.Env{Args: []string{"--check"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		var status chmigrate.Status
		if err := json.Unmarshal([]byte(stdout.String()), &status); err != nil || status.State != testCase.state || code != testCase.code {
			t.Fatalf("%s: status --check exit %d, stdout %q (%v), stderr %q; want %s and exit %d",
				testCase.database, code, stdout.String(), err, stderr.String(), testCase.state, testCase.code)
		}
		if after := objectCount(t, ctx, conn, testCase.database); after != objectsBefore {
			t.Fatalf("%s: status --check changed the object count from %d to %d", testCase.database, objectsBefore, after)
		}
	}
}

// requireStatus reads the status of the database and requires the state,
// and that reading it changed neither its objects nor its versions.
func requireStatus(t *testing.T, ctx context.Context, conn driver.Conn, database string, db chmigrate.DB, baseline chmigrate.Baseline, chain []chmigrate.ChainFile, want string) {
	t.Helper()
	objectsBefore, versionsBefore := objectCount(t, ctx, conn, database), versionCount(t, ctx, conn, database)
	status, err := chmigrate.ReadStatus(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.State != want {
		t.Fatalf("status = %+v, want state %s", status, want)
	}
	if objects, versions := objectCount(t, ctx, conn, database), versionCount(t, ctx, conn, database); objects != objectsBefore || versions != versionsBefore {
		t.Fatalf("status changed the database: objects %d -> %d, versions %d -> %d", objectsBefore, objects, versionsBefore, versions)
	}
}

func objectCount(t *testing.T, ctx context.Context, conn driver.Conn, database string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ?", database).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func versionCount(t *testing.T, ctx context.Context, conn driver.Conn, database string) uint64 {
	t.Helper()
	var exists uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = 'schema_migrations'", database).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		return 0
	}
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM `"+database+"`.schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
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

// loadFrozenChain reads a database state recorded from the real Python chain
// (testdata/<name>): the objects, seeded rows and versions it built. The
// producer is deleted with the Python CLI; the file is its recorded output.
func loadFrozenChain(t *testing.T, name string) chmigrate.Baseline {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var recorded chmigrate.Baseline
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	if len(recorded.Objects) == 0 || len(recorded.Versions) == 0 {
		t.Fatalf("%s holds %d objects and %d versions", name, len(recorded.Objects), len(recorded.Versions))
	}
	return recorded
}

// freezePoint is the last migration a Python producer ran: the recorded
// splitter output covers every migration at or below it. A later migration has
// no Python truth and is held to Go-only splitter invariants.
const freezePoint = "099_team_project_ownership_last_synced.sql"

func splitGolden(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "python_split.json"))
	if err != nil {
		t.Fatalf("the recorded Python split is missing: %v", err)
	}
	return data
}

// TestSplitterMatchesFrozenPythonSplit holds the Go splitter to the Python
// runner's split_sql_statements. Every migration at or below freezePoint (the
// head's .sql versions and every chain file up to it) must have a recorded
// Python split (testdata/python_split.json, digest-pinned), and the Go split of
// the recorded text must equal it. A chain file above freezePoint has no
// Python truth; it is held to Go-only splitter invariants.
func TestSplitterMatchesFrozenPythonSplit(t *testing.T) {
	var want map[string]struct {
		SQL        string   `json:"sql"`
		Statements []string `json:"statements"`
	}
	if err := json.Unmarshal(splitGolden(t), &want); err != nil {
		t.Fatalf("decode the Python split: %v", err)
	}
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	chainText := map[string]string{}
	required := map[string]bool{}
	for _, version := range head.Versions {
		if strings.HasSuffix(version, ".sql") {
			required[version] = true
		}
	}
	for _, file := range chain {
		chainText[file.Version] = file.SQL
		if file.Version <= freezePoint {
			required[file.Version] = true
		}
	}
	if len(required) < 90 || !required[freezePoint] {
		t.Fatalf("%d migrations at or below the freeze point %s (present: %v); expected more than 90 including it",
			len(required), freezePoint, required[freezePoint])
	}
	for name := range required {
		if _, ok := want[name]; !ok {
			t.Errorf("%s is at or below the freeze point and has no recorded Python split", name)
		}
	}
	for name, recorded := range want {
		got := chmigrate.SplitStatements(recorded.SQL)
		statements := recorded.Statements
		if len(statements) == 0 {
			statements = nil
		}
		if !reflect.DeepEqual(got, statements) {
			t.Errorf("%s: Go split %d statements, Python %d; first difference %s", name, len(got), len(statements), firstDifference(got, statements))
		}
		if text, ok := chainText[name]; ok && text != recorded.SQL {
			t.Errorf("chain file %s is not the text the Python split was recorded from", name)
		}
	}
	// Above the freeze point: no Python truth, so the splitter's own
	// invariants. Every statement is non-blank, carries no trailing
	// separator, is not comment-only, and the split is deterministic.
	for _, file := range chain {
		if file.Version <= freezePoint {
			continue
		}
		statements := chmigrate.SplitStatements(file.SQL)
		if len(statements) == 0 {
			t.Errorf("%s: the Go split found no statement", file.Version)
		}
		if !reflect.DeepEqual(statements, chmigrate.SplitStatements(file.SQL)) {
			t.Errorf("%s: the Go split is not deterministic", file.Version)
		}
		for index, statement := range statements {
			trimmed := strings.TrimSpace(statement)
			if trimmed == "" || strings.HasSuffix(trimmed, ";") || strings.HasPrefix(trimmed, "--") && !strings.Contains(trimmed, "\n") {
				t.Errorf("%s: statement %d violates the splitter invariants: %q", file.Version, index, statement)
			}
		}
	}
}
