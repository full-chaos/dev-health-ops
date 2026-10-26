// Package sharedserver_test guards the shared-server test mode
// (containers.RunWithSharedServers): in a package that opts in, a test func
// that changes server-wide state must take a private server with
// containers.StartPostgresIsolated or containers.StartClickHouseIsolated.
// The runtime leak check catches what a test leaves behind; this guard catches
// server-wide changes that are undone before the end but still reach
// neighbouring tests while they run.
package sharedserver_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	optInCall = "containers.RunWithSharedServers("
	ident     = `[A-Za-z_][A-Za-z0-9_]*`
	// name is a literal object name; a keyword there (`IF` of IF NOT EXISTS
	// followed by a built name) is not one, see literalName.
	name = `(?P<name>[A-Za-z_][A-Za-z0-9_]*)`
)

// literalName reports whether a match of pattern on line names a literal
// object: when the pattern captures a name, a keyword in that place means the
// real name is built at run time ("CREATE DATABASE IF NOT EXISTS " + db).
func literalName(pattern *regexp.Regexp, line string) bool {
	index := pattern.SubexpIndex("name")
	if index < 0 {
		return true
	}
	for _, match := range pattern.FindAllStringSubmatch(line, -1) {
		switch strings.ToUpper(match[index]) {
		case "IF", "OR", "NOT":
		default:
			return true
		}
	}
	return false
}

// serverWide lists statements whose effect is not limited to the caller's
// scratch database. A name built at run time (containers.RoleName) is unique
// per call and passes; only a literal name in the source is flagged.
var serverWide = []struct {
	name    string
	pattern *regexp.Regexp
}{
	{"ALTER SYSTEM", regexp.MustCompile(`(?i)\bALTER\s+SYSTEM\b`)},
	{"pg_reload_conf", regexp.MustCompile(`(?i)\bpg_reload_conf\s*\(`)},
	{"CREATE TABLESPACE", regexp.MustCompile(`(?i)\bCREATE\s+TABLESPACE\b`)},
	{"ALTER access entity with a literal name", regexp.MustCompile(
		`(?i)\bALTER\s+(USER|QUOTA|SETTINGS\s+PROFILE|ROW\s+POLICY|MASKING\s+POLICY|WORKLOAD|RESOURCE)\s+(IF\s+EXISTS\s+)?` + name)},
	{"ALTER ROLE or ALTER DATABASE, any target", regexp.MustCompile(`(?i)\bALTER\s+(ROLE|DATABASE)\b`)},
	{"DROP ROLE, DATABASE or SCHEMA, any target", regexp.MustCompile(`(?i)\bDROP\s+(ROLE|DATABASE|SCHEMA)\b`)},
	{"DROP TABLE in a shared database", regexp.MustCompile(
		"(?i)\\bDROP\\s+(TEMPORARY\\s+)?TABLE\\s+(IF\\s+EXISTS\\s+)?[`\"]?(default|worker_test|system|postgres|template0|template1)[`\"]?\\.")},
	{"ClickHouse workload, resource or masking policy", regexp.MustCompile(
		`(?i)\bCREATE\s+(OR\s+REPLACE\s+)?(WORKLOAD|RESOURCE|MASKING\s+POLICY)\b`)},
	{"subscription", regexp.MustCompile(`(?i)\bCREATE\s+SUBSCRIPTION\b`)},
	{"replication origin", regexp.MustCompile(`(?i)\bpg_replication_origin_create\b`)},
	{"SECURITY LABEL", regexp.MustCompile(`(?i)\bSECURITY\s+LABEL\b`)},
	{"action on another session", regexp.MustCompile(`(?i)\bpg_(terminate|cancel)_backend\b`)},
	{"cluster-wide statistics reset", regexp.MustCompile(`(?i)\bpg_stat_reset_(shared|slru|replication_slot|subscription_stats)\b`)},
	{"server WAL or log action", regexp.MustCompile(`(?i)\bpg_(switch_wal|rotate_logfile|create_restore_point)\b`)},
	{"CHECKPOINT", regexp.MustCompile("(?i)[\"`]\\s*CHECKPOINT\\s*;?\\s*[\"`]")},
	{"CREATE access entity with a literal name", regexp.MustCompile(
		`(?i)\bCREATE\s+(ROLE|USER|QUOTA|SETTINGS\s+PROFILE|ROW\s+POLICY)\s+(IF\s+NOT\s+EXISTS\s+|OR\s+REPLACE\s+)?` + name)},
	{"ClickHouse named collection", regexp.MustCompile(`(?i)\b(CREATE|ALTER)\s+NAMED\s+COLLECTION\b`)},
	{"ClickHouse SQL user-defined function", regexp.MustCompile(
		`(?i)\bCREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+(IF\s+NOT\s+EXISTS\s+)?` + ident + `\s+(ON\s+CLUSTER\s+\S+\s+)?AS\s*\(`)},
	{"GRANT <role> TO with literal names", regexp.MustCompile(
		`(?i)\bGRANT\s+` + ident + `(\s*,\s*` + ident + `)*\s+TO\s`)},
	{"REVOKE <role> FROM with literal names", regexp.MustCompile(
		`(?i)\bREVOKE\s+(ADMIN\s+OPTION\s+FOR\s+)?` + ident + `(\s*,\s*` + ident + `)*\s+FROM\s`)},
	{"CREATE EXTENSION with a literal schema", regexp.MustCompile(`(?i)\bCREATE\s+EXTENSION\b.*\bSCHEMA\s+` + ident)},
	{"server SYSTEM command", regexp.MustCompile(`(?i)\bSYSTEM\s+(DROP|RELOAD|SHUTDOWN|KILL)\b`)},
	{"SYSTEM STOP/START without a table", regexp.MustCompile(
		"\\bSYSTEM\\s+(STOP|START)\\s+(TTL MERGES|MERGES|MOVES|FETCHES|REPLICATED SENDS|DISTRIBUTED SENDS|" +
			"REPLICATION QUEUES|LISTEN)\\s*[\"`]\\s*($|[,);])")},
	{"KILL QUERY", regexp.MustCompile(`(?i)\bKILL\s+QUERY\b`)},
	{"CREATE DATABASE with a literal name", regexp.MustCompile(
		`(?i)\bCREATE\s+DATABASE\s+(IF\s+NOT\s+EXISTS\s+)?` + name)},
	{"object in a shared database (default, worker_test)", regexp.MustCompile(
		`(?i)\bCREATE\s+(OR\s+REPLACE\s+)?(TEMPORARY\s+)?(TABLE|VIEW|MATERIALIZED\s+VIEW|DICTIONARY)\s+(IF\s+NOT\s+EXISTS\s+)?` +
			"[`\"]?(default|worker_test)[`\"]?\\.")},
	{"replication slot", regexp.MustCompile(`(?i)\bpg_create_(physical|logical)_replication_slot\b`)},
	{"PREPARE TRANSACTION", regexp.MustCompile(`(?i)\bPREPARE\s+TRANSACTION\b`)},
	{"GRANT/REVOKE ON PARAMETER", regexp.MustCompile(`(?i)\b(GRANT|REVOKE)\b.*\bON\s+PARAMETER\b`)},
	{"COMMENT ON a cluster object", regexp.MustCompile(`(?i)\bCOMMENT\s+ON\s+(ROLE|DATABASE|TABLESPACE)\b`)},
	{"Instance.Container access", regexp.MustCompile(`\.Container\.`)},
}

var isolatedCall = regexp.MustCompile(`\bcontainers\.Start(Postgres|ClickHouse)Isolated\(`)

type finding struct {
	file string
	line int
	kind string
}

// scan returns the opted-in package directories under root and every
// server-wide statement in a test file of one of them that takes no private server.
func scan(t *testing.T, root string) ([]string, []finding) {
	t.Helper()
	testFiles := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			testFiles[filepath.Dir(path)] = append(testFiles[filepath.Dir(path)], path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var optedIn []string
	var findings []finding
	for dir, files := range testFiles {
		sources := map[string]string{}
		opted := false
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			sources[file] = string(data)
			opted = opted || strings.Contains(sources[file], optInCall)
		}
		if !opted {
			continue
		}
		rel, _ := filepath.Rel(root, dir)
		optedIn = append(optedIn, filepath.ToSlash(rel))
		for file, source := range sources {
			relFile, _ := filepath.Rel(root, file)
			lines := strings.Split(source, "\n")
			regions := topLevelFuncs(lines)
			isolated := map[int]bool{}
			for number, line := range lines {
				if regions[number] >= 0 && isolatedCall.MatchString(line) {
					isolated[regions[number]] = true
				}
			}
			for number, line := range lines {
				if strings.HasPrefix(strings.TrimSpace(line), "//") || (regions[number] >= 0 && isolated[regions[number]]) {
					continue
				}
				for _, statement := range serverWide {
					if statement.pattern.MatchString(line) && literalName(statement.pattern, line) {
						findings = append(findings, finding{filepath.ToSlash(relFile), number + 1, statement.name})
					}
				}
			}
		}
	}
	sort.Strings(optedIn)
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].file != findings[j].file {
			return findings[i].file < findings[j].file
		}
		return findings[i].line < findings[j].line
	})
	return optedIn, findings
}

// topLevelFuncs maps each line to the line index of the top-level func it is
// in, or -1 outside every func. The Isolated exemption holds per func: a
// server-wide statement passes only in a func that takes a private server.
func topLevelFuncs(lines []string) []int {
	regions := make([]int, len(lines))
	current := -1
	for number, line := range lines {
		if strings.HasPrefix(line, "func ") {
			current = number
		}
		regions[number] = current
		// "}" alone closes a gofmt'd top-level func; a one-line func closes on its own line.
		if line == "}" || (strings.HasPrefix(line, "func ") && strings.HasSuffix(strings.TrimSpace(line), "}")) {
			current = -1
		}
	}
	return regions
}

func TestSharedServerPackagesIsolateServerWideStatements(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	optedIn, findings := scan(t, filepath.Join(root, "internal"))
	// This package's own fixtures name the opt-in call and every planted
	// statement in string literals; it is not a package that runs on a shared server.
	const self = "cigates/sharedserver"
	kept := optedIn[:0]
	for _, dir := range optedIn {
		if dir != self {
			kept = append(kept, dir)
		}
	}
	if len(kept) == 0 {
		t.Fatal("no package calls " + optInCall + "; the guard would pass on nothing")
	}
	for _, f := range findings {
		if strings.HasPrefix(f.file, self+"/") {
			continue
		}
		t.Errorf("internal/%s:%d: %s in a package that runs on a shared server (containers.RunWithSharedServers): "+
			"use containers.StartPostgresIsolated / StartClickHouseIsolated in this func, or build the name per call "+
			"(containers.RoleName)", f.file, f.line, f.kind)
	}
}

func TestGuardFlagsPlantedServerWideStatements(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	planted := []string{
		`exec("ALTER SYSTEM SET work_mem = '7MB'")`,
		`exec("SELECT pg_reload_conf()")`,
		`exec("CREATE TABLESPACE ts LOCATION '/tmp'")`,
		`exec("ALTER ROLE app_writer SET search_path = x")`,
		`exec("CREATE ROLE venue_writer LOGIN")`,
		`exec("CREATE USER IF NOT EXISTS ch_reader")`,
		`exec("CREATE SETTINGS PROFILE tight")`,
		`exec("GRANT pg_monitor TO worker_test")`,
		`exec("GRANT reader, writer TO app")`,
		`exec("REVOKE pg_monitor FROM worker_test")`,
		`exec("CREATE EXTENSION citext SCHEMA ext_schema")`,
		`exec("SYSTEM DROP DNS CACHE")`,
		"exec(`SYSTEM STOP MERGES`)",
		`exec("KILL QUERY WHERE 1")`,
		`instance.Container.Stop(ctx, nil)`,
		`exec("CREATE NAMED COLLECTION planted_nc AS host = 'h'")`,
		`exec("ALTER NAMED COLLECTION planted_nc SET port = 1")`,
		`exec("CREATE FUNCTION planted_udf AS (x) -> x + 1")`,
		`exec("CREATE OR REPLACE FUNCTION planted_udf AS (x) -> x * 2")`,
		`exec("CREATE USER OR REPLACE ch_reader")`,
		`exec("CREATE QUOTA planted_quota FOR INTERVAL 1 hour MAX queries = 1")`,
		`exec("CREATE ROW POLICY planted_policy ON t USING 1 TO ALL")`,
		`exec("CREATE DATABASE planted_db")`,
		`exec("ALTER DATABASE worker_test SET work_mem = '7MB'")`,
		"exec(\"CREATE TABLE default.planted (id UInt8) ENGINE = Memory\")",
		"exec(\"CREATE TABLE IF NOT EXISTS `worker_test`.planted (id int)\")",
		`exec("SELECT pg_create_physical_replication_slot('planted_slot')")`,
		`exec("PREPARE TRANSACTION 'planted'")`,
		`exec("GRANT SET ON PARAMETER work_mem TO PUBLIC")`,
		`exec("COMMENT ON ROLE worker_test IS 'planted'")`,
		`exec("CREATE SUBSCRIPTION planted_sub CONNECTION 'host=x' PUBLICATION p WITH (connect = false)")`,
		`exec("SELECT pg_replication_origin_create('planted_origin')")`,
		`exec("SECURITY LABEL ON ROLE worker_test IS 'x'")`,
		`exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity")`,
		`exec("SELECT pg_cancel_backend(123)")`,
		`exec("SELECT pg_stat_reset_shared('bgwriter')")`,
		`exec("SELECT pg_stat_reset_slru()")`,
		`exec("SELECT pg_switch_wal()")`,
		`exec("CHECKPOINT")`,
		`exec("CREATE WORKLOAD planted_workload")`,
		`exec("CREATE RESOURCE planted_resource (WRITE DISK default)")`,
		`exec("CREATE MASKING POLICY planted_mp ON t UPDATE c = 0 TO ALL")`,
		`exec("ALTER QUOTA planted_quota FOR INTERVAL 1 hour MAX queries = 2")`,
	}
	allowed := []string{
		`exec("CREATE ROLE " + role + " LOGIN")`,
		`exec("GRANT " + role + " TO " + other)`,
		`exec("GRANT SELECT ON ALL TABLES IN SCHEMA public TO " + role)`,
		`exec("SYSTEM STOP MERGES " + table)`,
		`exec("SYSTEM STOP MERGES teams")`,
		`exec("SYSTEM FLUSH LOGS")`,
		`exec("CREATE OR REPLACE FUNCTION touch() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql")`,
		`exec("CREATE FUNCTION f (a int) RETURNS int AS 'select a' LANGUAGE sql")`,
		`exec(fmt.Sprintf("CREATE DATABASE %q TEMPLATE %q", goDB, sourceDB))`,
		`exec("CREATE DATABASE IF NOT EXISTS " + database)`,
		`exec("CREATE USER IF NOT EXISTS " + user + " IDENTIFIED BY 'x'")`,
		`exec("CREATE TABLE only_here (id int)")`,
		`exec("CREATE TABLE default_values (id int)")`,
		`exec("SELECT count() FROM default.events")`,
		`exec("SELECT pg_stat_reset()")`,
		`t.Log("waiting for the next CHECKPOINT of the pool")`,
		`exec("CREATE TABLE workload_runs (id int)")`,
		`// CREATE ROLE is cluster-scoped; ALTER SYSTEM too`,
	}
	inFunc := func(name string, lines []string) string {
		return "func " + name + "(t *testing.T) {\n" + strings.Join(lines, "\n") + "\n}\n"
	}
	write("internal/opted/main_test.go", "package opted\nfunc TestMain(m *testing.M) { os.Exit(containers.RunWithSharedServers(m.Run)) }\n")
	write("internal/opted/planted_test.go", "package opted\n"+inFunc("TestPlanted", planted))
	write("internal/opted/allowed_test.go", "package opted\n"+inFunc("TestAllowed", allowed))
	// One file, three funcs: the Isolated call covers its own func only, so the
	// unisolated test's statement and the helper it calls are both flagged.
	write("internal/opted/mixed_test.go", "package opted\n"+
		inFunc("TestIsolated", append([]string{"instance, _ := containers.StartPostgresIsolated(ctx)"}, planted...))+
		inFunc("TestShared", []string{`exec("ALTER SYSTEM SET work_mem = '7MB'")`, "plantSetting(t)"})+
		inFunc("plantSetting", []string{`exec("ALTER SYSTEM SET work_mem = '8MB'")`}))
	write("internal/notopted/planted_test.go", "package notopted\n"+inFunc("TestPlanted", planted))

	optedIn, findings := scan(t, filepath.Join(root, "internal"))

	if strings.Join(optedIn, ",") != "opted" {
		t.Errorf("opted-in packages = %v, want [opted]", optedIn)
	}
	flagged := map[int]bool{}
	// mixed_test.go: package line, TestIsolated (header, Isolated call,
	// planted lines, "}"), TestShared (header, statement, helper call, "}"),
	// plantSetting (header, statement).
	mixedSharedLine := 1 + 1 + 1 + len(planted) + 1 + 1 + 1
	mixedHelperLine := mixedSharedLine + 2 + 1 + 1
	mixed := map[int]bool{}
	for _, f := range findings {
		switch {
		case f.file == "opted/planted_test.go":
			flagged[f.line] = true
		case f.file == "opted/mixed_test.go" && (f.line == mixedSharedLine || f.line == mixedHelperLine):
			mixed[f.line] = true
		default:
			t.Errorf("unexpected finding %s:%d %s", f.file, f.line, f.kind)
		}
	}
	for index, line := range planted {
		if !flagged[index+3] {
			t.Errorf("planted line not flagged: %s", line)
		}
	}
	if !mixed[mixedSharedLine] {
		t.Errorf("an unisolated ALTER SYSTEM in a file whose other test takes an Isolated server was not flagged (want mixed_test.go:%d)", mixedSharedLine)
	}
	if !mixed[mixedHelperLine] {
		t.Errorf("an ALTER SYSTEM in a helper of the same file, called from the unisolated test, was not flagged (want mixed_test.go:%d)", mixedHelperLine)
	}
}

// scanFixture writes one opted-in package whose single test func holds lines,
// and returns the line numbers the guard flagged in it.
func scanFixture(t *testing.T, lines []string) map[int]bool {
	t.Helper()
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/opted/main_test.go", "package opted\nfunc TestMain(m *testing.M) { os.Exit(containers.RunWithSharedServers(m.Run)) }\n")
	write("internal/opted/fixture_test.go", "package opted\nfunc TestFixture(t *testing.T) {\n"+strings.Join(lines, "\n")+"\n}\n")
	_, findings := scan(t, filepath.Join(root, "internal"))
	flagged := map[int]bool{}
	for _, f := range findings {
		if f.file != "opted/fixture_test.go" {
			t.Errorf("unexpected finding %s:%d %s", f.file, f.line, f.kind)
			continue
		}
		flagged[f.line-3] = true
	}
	return flagged
}

// TestGuardFlagsAnAlterBuiltFromAConcatenation: an ALTER whose target is built
// at run time still changes a role or database every other test shares, so the
// keyword in any string literal is enough.
func TestGuardFlagsAnAlterBuiltFromAConcatenation(t *testing.T) {
	statements := []string{
		`exec("ALTER ROLE " + target + " NOLOGIN")`,
		`exec(fmt.Sprintf("ALTER ROLE %s NOLOGIN", target))`,
		`exec("ALTER ROLE IF EXISTS " + target + " NOLOGIN")`,
		`exec("ALTER DATABASE " + target + " SET work_mem = '7MB'")`,
		`exec("ALTER SYSTEM " + setting)`,
		`exec("ALTER USER " + target + " NOLOGIN")`,
		`exec("ALTER USER worker_test NOLOGIN")`,
		`exec("ALTER GROUP " + target + " ADD USER u")`,
		`exec("ALTER GROUP IF EXISTS g ADD USER u")`,
	}
	flagged := scanFixture(t, statements)
	for index, statement := range statements {
		if !flagged[index] {
			t.Errorf("not flagged: %s", statement)
		}
	}
}

// TestGuardFlagsDropsOfServerWideObjects: DROP ROLE, DROP DATABASE and DROP
// SCHEMA at any name, and DROP TABLE in a database the test does not own.
func TestGuardFlagsDropsOfServerWideObjects(t *testing.T) {
	flaggedStatements := []string{
		`exec("DROP ROLE worker_test")`,
		`exec("DROP USER worker_test")`,
		`exec("DROP USER IF EXISTS " + user)`,
		`exec("DROP GROUP g")`,
		`exec("DROP GROUP IF EXISTS " + group)`,
		`exec("CREATE GROUP planted_group")`,
		`exec("DROP ROLE IF EXISTS " + role)`,
		`exec("DROP DATABASE " + database)`,
		`exec("DROP DATABASE IF EXISTS worker_test")`,
		`exec("DROP SCHEMA public CASCADE")`,
		`exec("DROP SCHEMA IF EXISTS " + schema)`,
		`exec("DROP TABLE default.events")`,
		`exec("DROP TABLE IF EXISTS worker_test.events")`,
		"exec(\"DROP TABLE IF EXISTS `default`.events\")",
	}
	allowedStatements := []string{
		`exec("DROP TABLE events")`,
		`exec("CREATE GROUP " + group)`,
		`exec("DROP TABLE IF EXISTS " + table)`,
		`exec("DROP TABLE IF EXISTS public.events")`,
		`exec("DROP INDEX events_idx")`,
		`exec("DROP VIEW default_view")`,
		`exec("SELECT 1 FROM drop_rolemap")`,
		`// DROP ROLE and DROP DATABASE are cluster-scoped`,
	}
	statements := append(append([]string{}, flaggedStatements...), allowedStatements...)
	flagged := scanFixture(t, statements)
	for index, statement := range statements {
		if want := index < len(flaggedStatements); flagged[index] != want {
			t.Errorf("flagged = %v for %s, want %v", flagged[index], statement, want)
		}
	}
}

// TestGuardLetsAnIsolatedFuncDropAndAlter: the private-server exemption
// covers the new statements like every other.
func TestGuardLetsAnIsolatedFuncDropAndAlter(t *testing.T) {
	flagged := scanFixture(t, []string{
		`instance, _ := containers.StartPostgresIsolated(ctx)`,
		`exec("ALTER ROLE " + target + " NOLOGIN")`,
		`exec("DROP ROLE " + target)`,
		`exec("DROP DATABASE " + target)`,
	})
	if len(flagged) != 0 {
		t.Errorf("an Isolated func was flagged: %v", flagged)
	}
}
