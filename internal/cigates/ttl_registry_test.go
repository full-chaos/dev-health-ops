package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-10174 PR E: Go port of the schema-scanning half of tests/test_ttl_registry.py (CHAOS-3602) and
// tests/test_fixtures_ttl_horizon.py (CHAOS-3432/3544). The Python pair stays in ci/kept_python_tests.txt while the Python
// fixture generators ship (CHAOS-3165); this guard reads only the ClickHouse migrations, so it keeps the "a new TTL table
// went unnoticed" class closed after they are gone. The product_telemetry_events ceiling lives in internal/fixturesgen.

const (
	ttlShelfLifeDays = 30 // ttl_horizon.TTL_SAFETY_MARGIN
	ttlHeadroomDays  = 1  // ttl_horizon.PER_TABLE_CEILING_HEADROOM_DAYS
)

// ttlKnownTables is the hand-kept list; the sweep below must agree with it in both directions.
var ttlKnownTables = map[string]int{
	"feature_flag_event":          90,
	"telemetry_signal_bucket":     90,
	"release_impact_daily":        365,
	"product_telemetry_events":    180,
	"provider_request_usage":      395,
	"llm_categorization_attempts": 400,
	"work_unit_investment_shadow": 90,
}

var (
	ttlCreateTable = regexp.MustCompile("(?im)^\\s*CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?([A-Za-z_]\\w*)`?\\s*\\(")
	ttlPrecise     = regexp.MustCompile(`(?i)TTL\s+(?:\w+\()?([\w.]+)\)?\s*\+\s*INTERVAL\s+(\d+)\s+DAY`)
	ttlCoarse      = regexp.MustCompile(`(?is)\bTTL\b.*?\bINTERVAL\b`)
	ttlAlterModify = regexp.MustCompile("(?im)^\\s*ALTER\\s+TABLE\\s+`?([A-Za-z_]\\w*)`?\\s+MODIFY\\s+TTL\\b")
	ttlHorizonLine = regexp.MustCompile(`(?i)TTL\s+.*?\+\s*INTERVAL\s+(\d+)\s+DAY\s+DELETE`)
)

type ttlRetention struct {
	column string
	days   int
}

// ttlAttribute pairs each TTL match with the closest preceding CREATE TABLE in the same statement.
func ttlAttribute(statement string, re *regexp.Regexp) (tables []string, matches [][]string) {
	creates := ttlCreateTable.FindAllStringSubmatchIndex(statement, -1)
	if len(creates) == 0 {
		return nil, nil
	}
	for _, m := range re.FindAllStringSubmatchIndex(statement, -1) {
		owner := -1
		for i, c := range creates {
			if c[0] <= m[0] {
				owner = i
			}
		}
		if owner < 0 {
			continue
		}
		tables = append(tables, statement[creates[owner][2]:creates[owner][3]])
		var groups []string
		for g := 0; g+1 < len(m); g += 2 {
			if m[g] < 0 {
				groups = append(groups, "")
			} else {
				groups = append(groups, statement[m[g]:m[g+1]])
			}
		}
		matches = append(matches, groups)
	}
	return tables, matches
}

var (
	ttlSQLLineComment = regexp.MustCompile(`--[^\n]*`)
	ttlBlockComment   = regexp.MustCompile(`(?s)/\*.*?\*/`)
	ttlPyCommentLine  = regexp.MustCompile(`(?m)^[ \t]*#[^\n]*`)
)

// ttlStripComments blanks SQL line and block comments and Python comment lines, so a TTL clause that is only commented out
// does not count as enforced.
func ttlStripComments(text string) string {
	text = ttlBlockComment.ReplaceAllString(text, "")
	text = ttlSQLLineComment.ReplaceAllString(text, "")
	return ttlPyCommentLine.ReplaceAllString(text, "")
}

func ttlMigrationFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if ext := filepath.Ext(e.Name()); !e.IsDir() && (ext == ".sql" || ext == ".py") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

func ttlScan(t *testing.T, dir string) (precise map[string]ttlRetention, coarse map[string]bool) {
	t.Helper()
	precise, coarse = map[string]ttlRetention{}, map[string]bool{}
	for _, path := range ttlMigrationFiles(t, dir) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := ttlStripComments(string(raw))
		for _, m := range ttlAlterModify.FindAllStringSubmatch(text, -1) {
			coarse[m[1]] = true
		}
		for _, statement := range strings.Split(text, ";") {
			tables, groups := ttlAttribute(statement, ttlPrecise)
			for i, table := range tables {
				days, _ := strconv.Atoi(groups[i][2])
				precise[table] = ttlRetention{column: groups[i][1], days: days}
			}
			tables, _ = ttlAttribute(statement, ttlCoarse)
			for _, table := range tables {
				coarse[table] = true
			}
		}
	}
	return precise, coarse
}

func ttlVocabularyProblems(precise map[string]ttlRetention, coarse map[string]bool) []string {
	diff := func(a, b func(string) bool, names []string) []string {
		var out []string
		for _, n := range names {
			if a(n) && !b(n) {
				out = append(out, n)
			}
		}
		sort.Strings(out)
		return out
	}
	var all []string
	seen := map[string]bool{}
	add := func(n string) {
		if !seen[n] {
			seen[n] = true
			all = append(all, n)
		}
	}
	for n := range precise {
		add(n)
	}
	for n := range coarse {
		add(n)
	}
	for n := range ttlKnownTables {
		add(n)
	}
	inP := func(n string) bool { _, ok := precise[n]; return ok }
	inC := func(n string) bool { return coarse[n] }
	inK := func(n string) bool { _, ok := ttlKnownTables[n]; return ok }
	var problems []string
	for _, c := range []struct {
		msg  string
		a, b func(string) bool
	}{
		{"the coarse sweep found TTL tables the precise parser missed", inC, inP},
		{"the precise parser found TTL tables the coarse sweep missed", inP, inC},
		{"TTL tables not in the known list", inC, inK},
		{"known TTL tables no longer carrying a TTL clause", inK, inC},
	} {
		if d := diff(c.a, c.b, all); len(d) > 0 {
			problems = append(problems, fmt.Sprintf("%s: %v", c.msg, d))
		}
	}
	return problems
}

func ttlRealDir(t *testing.T) string {
	return filepath.Join(repoRoot(t), "src", "dev_health_ops", "migrations", "clickhouse")
}

func TestTTLVocabularyMatchesTheRealMigrations(t *testing.T) {
	precise, coarse := ttlScan(t, ttlRealDir(t))
	if problems := ttlVocabularyProblems(precise, coarse); len(problems) > 0 {
		t.Fatalf("TTL vocabulary inconsistent: %s", strings.Join(problems, "; "))
	}
	if ff := precise["feature_flag_event"]; ff.column != "event_ts" || ff.days != 90 {
		t.Fatalf("feature_flag_event TTL = %+v, want event_ts / 90 days", ff)
	}
	for table, want := range ttlKnownTables {
		if precise[table].days != want {
			t.Fatalf("%s retention = %d days, want %d: a migration changed its TTL, so every generator ceiling must follow", table, precise[table].days, want)
		}
	}
	if _, has := precise["projects"]; has {
		t.Fatal("projects has no TTL but the scan reports one")
	}
}

// Every table's generation ceiling (retention - shelf life - headroom) must stay above zero, and the tightest horizon
// binds the global ceiling.
func TestTTLCeilingsSurviveTheShelfLife(t *testing.T) {
	dir := ttlRealDir(t)
	precise, _ := ttlScan(t, dir)
	tightest := 0
	for table, r := range precise {
		if ceiling := r.days - ttlShelfLifeDays - ttlHeadroomDays; ceiling < 1 || ceiling+ttlShelfLifeDays >= r.days {
			t.Fatalf("%s: retention %d days leaves no generation ceiling inside the %d-day shelf life", table, r.days, ttlShelfLifeDays)
		}
		if tightest == 0 || r.days < tightest {
			tightest = r.days
		}
	}
	horizons := map[string]int{}
	for _, path := range ttlMigrationFiles(t, dir) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range ttlHorizonLine.FindAllStringSubmatch(ttlStripComments(string(raw)), -1) {
			days, _ := strconv.Atoi(m[1])
			if cur, ok := horizons[path]; !ok || days < cur {
				horizons[path] = days
			}
		}
	}
	if len(horizons) == 0 {
		t.Fatal("no `TTL ... INTERVAL n DAY DELETE` clause found: the horizon pattern stopped matching, which is not the risk going away")
	}
	min := 0
	for _, d := range horizons {
		if min == 0 || d < min {
			min = d
		}
	}
	if min != tightest {
		t.Fatalf("tightest DELETE horizon %d != tightest parsed retention %d", min, tightest)
	}
}

// Planted defects: one per class the Python guard exists to catch.
func TestTTLScanPlantedDefects(t *testing.T) {
	scan := func(files map[string]string) map[string]ttlRetention {
		dir := t.TempDir()
		for name, body := range files {
			writeFile(t, filepath.Join(dir, name), body)
		}
		p, _ := ttlScan(t, dir)
		return p
	}
	t.Run("wrapped and bare column forms", func(t *testing.T) {
		p := scan(map[string]string{"1.sql": "CREATE TABLE a (\n x DateTime\n) ENGINE=M TTL toDateTime(occurred_at) + INTERVAL 42 DAY DELETE;\nCREATE TABLE b (\n day Date\n) ENGINE=M TTL day + INTERVAL 99 DAY;"})
		if p["a"] != (ttlRetention{"occurred_at", 42}) || p["b"] != (ttlRetention{"day", 99}) {
			t.Fatalf("forms not parsed: %+v", p)
		}
	})
	t.Run("table without TTL is not registered", func(t *testing.T) {
		p := scan(map[string]string{"1.sql": "CREATE TABLE plain (\n x Int8\n) ENGINE=M;"})
		if _, ok := p["plain"]; ok {
			t.Fatal("a table with no TTL was registered")
		}
	})
	t.Run("a fifth TTL table in an unmatched unit form is caught", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE fifth_ttl_table (\n x Int8\n) ENGINE=M TTL occurred_at + INTERVAL 4 WEEK;")
		p, c := ttlScan(t, dir)
		if _, ok := p["fifth_ttl_table"]; ok || !c["fifth_ttl_table"] {
			t.Fatalf("WEEK form: precise=%v coarse=%v", p, c)
		}
		if len(ttlVocabularyProblems(p, c)) == 0 {
			t.Fatal("a TTL table the precise parser misses must fail the vocabulary check")
		}
	})
	t.Run("a known table that lost its TTL is caught", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE feature_flag_event (\n x Int8\n) ENGINE=M;")
		p, c := ttlScan(t, dir)
		if len(ttlVocabularyProblems(p, c)) == 0 {
			t.Fatal("known TTL tables without a TTL clause must fail")
		}
	})
	t.Run("a .py migration TTL is found", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.py"), "DDL = \"\"\"\nCREATE TABLE py_sourced_ttl_table (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 45 DAY DELETE\n\"\"\"\n")
		p, c := ttlScan(t, dir)
		if p["py_sourced_ttl_table"].days != 45 || !c["py_sourced_ttl_table"] {
			t.Fatalf("py migration TTL missed: %+v %v", p, c)
		}
	})
	t.Run("ALTER TABLE MODIFY TTL is seen by the coarse sweep", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "ALTER TABLE existing_table MODIFY TTL ts + INTERVAL 10 DAY;")
		_, c := ttlScan(t, dir)
		if !c["existing_table"] {
			t.Fatal("ALTER ... MODIFY TTL not found")
		}
	})
	t.Run("multi-table chunk without semicolons attributes to the second table", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.py"), "A = \"\"\"CREATE TABLE first_table (\n x Int8\n) ENGINE=M\n\nCREATE TABLE second_table (\n y Int8\n) ENGINE=M TTL ts + INTERVAL 20 DAY\"\"\"")
		p, c := ttlScan(t, dir)
		if p["second_table"].days != 20 || c["first_table"] || !c["second_table"] {
			t.Fatalf("misattributed: %+v %v", p, c)
		}
	})
	t.Run("coarse sweep ignores prose and identifiers containing TTL or Interval", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.py"), "\"\"\"Run SHOW CREATE TABLE to get the DDL; the table name in a CREATE TABLE statement.\"\"\"\nX = 'toIntervalDay THROTTLED'\n")
		_, c := ttlScan(t, dir)
		if len(c) != 0 {
			t.Fatalf("prose produced phantom tables: %v", c)
		}
	})
	t.Run("a commented-out TTL is not enforced", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE dropped_ttl (\n x Int8\n) ENGINE=M\n-- TTL ts + INTERVAL 30 DAY DELETE\n;")
		writeFile(t, filepath.Join(dir, "2.sql"), "CREATE TABLE block_commented (\n x Int8\n) ENGINE=M /* TTL ts + INTERVAL 30 DAY DELETE */;")
		writeFile(t, filepath.Join(dir, "3.py"), "DDL = \"\"\"CREATE TABLE py_commented (\n x Int8\n) ENGINE=M\n\"\"\"\n# TTL ts + INTERVAL 30 DAY DELETE\n")
		p, c := ttlScan(t, dir)
		if len(p) != 0 || len(c) != 0 {
			t.Fatalf("commented-out TTL counted: precise=%v coarse=%v", p, c)
		}
	})
	t.Run("changing the migration changes the registry", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "1.sql")
		writeFile(t, path, "CREATE TABLE synth (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 30 DAY;")
		p, _ := ttlScan(t, dir)
		writeFile(t, path, "CREATE TABLE synth (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 15 DAY;")
		q, _ := ttlScan(t, dir)
		if p["synth"].days != 30 || q["synth"].days != 15 {
			t.Fatalf("registry is not read from the source: %d then %d", p["synth"].days, q["synth"].days)
		}
	})
}
