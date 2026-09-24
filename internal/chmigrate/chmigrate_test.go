package chmigrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func testBaseline() Baseline {
	return Baseline{
		Contract: 1,
		Versions: []string{"000_a.sql", "001_b.py"},
		Objects: []Object{
			{Name: "schema_migrations", Engine: "MergeTree", Create: "CREATE TABLE schema_migrations (version String) ENGINE = MergeTree ORDER BY version"},
			{Name: "t", Engine: "MergeTree", Create: "CREATE TABLE t (x Int8) ENGINE = MergeTree ORDER BY x"},
			{Name: "v", Engine: "View", Create: "CREATE VIEW v AS SELECT x FROM t"},
			{Name: "seeded", Engine: "MergeTree", Create: "CREATE TABLE seeded (x Int8) ENGINE = MergeTree ORDER BY x"},
		},
		Rows: map[string]string{"seeded": "{\"x\":1}\n"},
	}
}

func TestDecide(t *testing.T) {
	baseline := testBaseline()
	chain := []ChainFile{{Version: "002_c.sql"}, {Version: "003_d.sql"}}
	for name, testCase := range map[string]struct {
		applied map[string]bool
		want    Plan
	}{
		"empty":          {map[string]bool{}, Plan{State: StateEmpty, Pending: chain}},
		"below the head": {map[string]bool{"000_a.sql": true}, Plan{State: StateBelowHead, Missing: []string{"001_b.py"}}},
		"at the head":    {map[string]bool{"000_a.sql": true, "001_b.py": true}, Plan{State: StateAtHead, Pending: chain}},
		"part of the chain applied": {
			map[string]bool{"000_a.sql": true, "001_b.py": true, "002_c.sql": true},
			Plan{State: StateAtHead, Pending: chain[1:]},
		},
		"an unknown extra version": {
			map[string]bool{"000_a.sql": true, "001_b.py": true, "067_x.py": true, "002_c.sql": true, "003_d.sql": true},
			Plan{State: StateAtHead},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := Decide(testCase.applied, baseline, chain); !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("Decide = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

// fakeDB is a database of named objects, row counts and versions.
type fakeDB struct {
	objects  map[string]string
	rows     map[string]uint64
	versions map[string]bool
	executed []string
	failOn   string
}

func newFakeDB() *fakeDB {
	return &fakeDB{objects: map[string]string{}, rows: map[string]uint64{}, versions: map[string]bool{}}
}

func (f *fakeDB) Exec(_ context.Context, statement string) error {
	if f.failOn != "" && strings.Contains(statement, f.failOn) {
		return errors.New("injected failure")
	}
	f.executed = append(f.executed, statement)
	switch {
	case strings.HasPrefix(statement, "CREATE "):
		fields := strings.Fields(statement)
		f.objects[fields[2]] = statement
	case strings.HasPrefix(statement, "INSERT INTO "+SchemaMigrationsTable):
		for _, part := range strings.Split(statement, "('")[1:] {
			f.versions[part[:strings.Index(part, "'")]] = true
		}
	case strings.HasPrefix(statement, "INSERT INTO `"):
		name := strings.TrimPrefix(statement, "INSERT INTO `")
		f.rows[name[:strings.Index(name, "`")]]++
	}
	return nil
}

func (f *fakeDB) AppliedVersions(context.Context) (map[string]bool, error) {
	out := map[string]bool{}
	for version := range f.versions {
		out[version] = true
	}
	return out, nil
}

func (f *fakeDB) ObjectCreate(_ context.Context, name string) (string, bool, error) {
	create, ok := f.objects[name]
	return create, ok, nil
}

func (f *fakeDB) RowCount(_ context.Context, table string) (uint64, error) { return f.rows[table], nil }

func TestUpgradeAppliesTheBaselineThenTheChain(t *testing.T) {
	db := newFakeDB()
	chain := []ChainFile{{Version: "002_c.sql", SQL: "-- a comment; with a semicolon\nCREATE TABLE IF NOT EXISTS c (x Int8) ENGINE = Memory;\n"}}
	result, err := Upgrade(context.Background(), db, testBaseline(), chain)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "baseline_applied" || len(result.Created) != 4 || !reflect.DeepEqual(result.Seeded, []string{"seeded"}) ||
		!reflect.DeepEqual(result.Applied, []string{"002_c.sql"}) {
		t.Fatalf("result = %+v", result)
	}
	// Views come after the seed rows: a materialized view present while rows
	// land would re-emit them.
	insert, view := -1, -1
	for index, statement := range db.executed {
		if strings.HasPrefix(statement, "INSERT INTO `seeded`") {
			insert = index
		}
		if strings.HasPrefix(statement, "CREATE VIEW v") {
			view = index
		}
	}
	if insert < 0 || view < 0 || view < insert {
		t.Fatalf("seed insert at %d, view at %d; the view must come after the rows: %q", insert, view, db.executed)
	}
	for _, version := range []string{"000_a.sql", "001_b.py", "002_c.sql"} {
		if !db.versions[version] {
			t.Fatalf("version %s not recorded: %v", version, db.versions)
		}
	}

	again, err := Upgrade(context.Background(), db, testBaseline(), chain)
	if err != nil || again.Action != "up_to_date" || len(again.Created)+len(again.Seeded)+len(again.Applied) != 0 {
		t.Fatalf("re-run = %+v, %v; want up_to_date with nothing done", again, err)
	}
}

// An upgrade interrupted before the versions were recorded resumes: objects
// already created are kept (they match), missing ones are created, a table
// that already holds rows is not seeded twice.
func TestUpgradeResumesAnInterruptedBaseline(t *testing.T) {
	db := newFakeDB()
	db.failOn = "CREATE VIEW"
	if _, err := Upgrade(context.Background(), db, testBaseline(), nil); err == nil {
		t.Fatal("the injected failure did not stop the upgrade")
	}
	if len(db.versions) != 0 {
		t.Fatalf("versions recorded after a failed baseline: %v", db.versions)
	}
	db.failOn = ""
	result, err := Upgrade(context.Background(), db, testBaseline(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Created, []string{"v"}) || len(result.Seeded) != 0 || db.rows["seeded"] != 1 {
		t.Fatalf("resume = %+v, seeded rows %d; want only the view created and no second seed", result, db.rows["seeded"])
	}
}

func TestUpgradeRefusesAnObjectItDidNotCreate(t *testing.T) {
	db := newFakeDB()
	db.objects["t"] = "CREATE TABLE t (x Int16) ENGINE = MergeTree ORDER BY x"
	if _, err := Upgrade(context.Background(), db, testBaseline(), nil); err == nil || !strings.Contains(err.Error(), "different definition") {
		t.Fatalf("upgrade over a foreign t = %v, want a refusal", err)
	}
}

func TestUpgradeRefusesADatabaseBelowTheHead(t *testing.T) {
	db := newFakeDB()
	db.versions["000_a.sql"] = true
	_, err := Upgrade(context.Background(), db, testBaseline(), nil)
	var below BelowHeadError
	if !errors.As(err, &below) || !reflect.DeepEqual(below.Missing, []string{"001_b.py"}) {
		t.Fatalf("upgrade below the head = %v", err)
	}
	for _, want := range []string{"below the head for ordering contract 1", "001_b.py", "OPERATIONAL_ORDERING_CONTRACT=1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
	if len(db.executed) != 0 {
		t.Fatalf("a refused upgrade executed %q", db.executed)
	}
}

// A chain file that fails part-way is not recorded, so the next run starts it
// again.
func TestAChainFileIsRecordedOnlyAfterEveryStatement(t *testing.T) {
	db := newFakeDB()
	for _, version := range testBaseline().Versions {
		db.versions[version] = true
	}
	db.failOn = "second"
	chain := []ChainFile{{Version: "002_c.sql", SQL: "SELECT 'first';\nSELECT 'second';\n"}}
	if _, err := Upgrade(context.Background(), db, testBaseline(), chain); err == nil || !strings.Contains(err.Error(), "002_c.sql statement 2") {
		t.Fatalf("upgrade = %v, want the failing statement named", err)
	}
	if db.versions["002_c.sql"] {
		t.Fatal("a failed chain file was recorded")
	}
}

func TestParseContract(t *testing.T) {
	for _, testCase := range []struct {
		raw     string
		present bool
		want    int
		fails   bool
	}{{"", false, 1, false}, {"1", true, 1, false}, {"2", true, 2, false}, {"", true, 0, true}, {"3", true, 0, true}} {
		got, err := ParseContract(testCase.raw, testCase.present)
		if (err != nil) != testCase.fails || got != testCase.want {
			t.Fatalf("ParseContract(%q, %v) = %d, %v", testCase.raw, testCase.present, got, err)
		}
	}
}

// The checked-in heads load, and contract 2 is contract 1 plus migration 067,
// the one migration the contract changes.
func TestBaselinesLoad(t *testing.T) {
	one, err := LoadBaseline(1)
	if err != nil {
		t.Fatal(err)
	}
	two, err := LoadBaseline(2)
	if err != nil {
		t.Fatal(err)
	}
	extra := map[string]bool{}
	for _, version := range two.Versions {
		extra[version] = true
	}
	for _, version := range one.Versions {
		if !extra[version] {
			t.Fatalf("contract 1 records %s, contract 2 does not", version)
		}
		delete(extra, version)
	}
	if !reflect.DeepEqual(extra, map[string]bool{"067_operational_ordering_contract.py": true}) {
		t.Fatalf("contract 2 adds %v, want only 067", extra)
	}
	for _, baseline := range []Baseline{one, two} {
		for _, object := range baseline.Objects {
			if strings.Contains(object.Create, "dho_ch_baseline_") {
				t.Fatalf("contract %d object %s still names its capture database", baseline.Contract, object.Name)
			}
		}
	}
}

// Until the Python chain is deleted, a migration after the head exists in
// both places: every file of the Python chain that neither baseline records is
// a .sql file here, byte-identical, and every file here is in the Python
// chain. A .py migration after the head cannot be applied by dho, so it is
// refused outright.
func TestChainAfterHeadMatchesThePythonChain(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	pythonDir := filepath.Join(filepath.Dir(file), "..", "..", "src", "dev_health_ops", "migrations", "clickhouse")
	entries, err := os.ReadDir(pythonDir)
	if err != nil {
		t.Fatal(err)
	}
	head := map[string]bool{}
	for _, contract := range []int{1, 2} {
		baseline, err := LoadBaseline(contract)
		if err != nil {
			t.Fatal(err)
		}
		for _, version := range baseline.Versions {
			head[version] = true
		}
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	ours := map[string]string{}
	for _, file := range chain {
		ours[file.Version] = file.SQL
	}
	seen := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == "__init__.py" || (!strings.HasSuffix(name, ".sql") && !strings.HasSuffix(name, ".py")) {
			continue
		}
		seen++
		if head[name] {
			continue
		}
		if !strings.HasSuffix(name, ".sql") {
			t.Errorf("%s is a Python migration after the head; dho cannot apply it -- write it as a .sql file", name)
			continue
		}
		data, err := os.ReadFile(filepath.Join(pythonDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if ours[name] != string(data) {
			t.Errorf("%s is after the head but internal/chmigrate/sql/%s is missing or differs", name, name)
		}
		delete(ours, name)
	}
	for name := range ours {
		t.Errorf("internal/chmigrate/sql/%s is not in the Python chain", name)
	}
	// The Python runner globs *.py, so it records __init__.py as a version;
	// every other head version is a file of the chain.
	if !head["__init__.py"] || seen != len(head)-1 {
		t.Fatalf("read %d migrations from %s for %d head versions (with __init__.py: %v): the walk is broken", seen, pythonDir, len(head), head["__init__.py"])
	}
}

func TestStripDatabase(t *testing.T) {
	got := StripDatabase("CREATE MATERIALIZED VIEW db1.mv TO `db1`.target AS SELECT * FROM db1.source", "db1")
	if want := "CREATE MATERIALIZED VIEW mv TO target AS SELECT * FROM source"; got != want {
		t.Fatalf("StripDatabase = %q, want %q", got, want)
	}
}

// The verbs refuse arguments and a bad contract before touching a database,
// and report missing configuration as JSON on stderr.
func TestCommandRefusesBeforeConnecting(t *testing.T) {
	for name, testCase := range map[string]struct {
		args []string
		env  map[string]string
		code int
		want string
	}{
		"a positional argument": {[]string{"extra"}, nil, cli.ExitUsage, "positional arguments"},
		"a bad contract":        {nil, map[string]string{OrderingContractEnv: "3"}, cli.ExitFailure, `"code":"configuration_error"`},
		"no DSN":                {nil, nil, cli.ExitFailure, "CLICKHOUSE_URI is required"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			env := cli.Env{Args: testCase.args, Stdout: &stdout, Stderr: &stderr, Lookup: func(key string) (string, bool) {
				value, ok := testCase.env[key]
				return value, ok
			}}
			code := run(context.Background(), "upgrade", env)
			if code != testCase.code || !strings.Contains(stderr.String(), testCase.want) {
				t.Fatalf("code %d stderr %q; want %d with %q", code, stderr.String(), testCase.code, testCase.want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want nothing", stdout.String())
			}
			if testCase.code == cli.ExitFailure {
				var body map[string]map[string]string
				if err := json.Unmarshal(stderr.Bytes(), &body); err != nil || body["error"]["code"] == "" {
					t.Fatalf("stderr is not the JSON error shape: %q", stderr.String())
				}
			}
		})
	}
}
