package workflows_test

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
	ttlCreateTable  = regexp.MustCompile("(?im)^\\s*CREATE\\s+(?:OR\\s+REPLACE\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:`?[A-Za-z_]\\w*`?\\.)?`?([A-Za-z_]\\w*)`?(?:\\s+ON\\s+CLUSTER\\s+\\S+)?\\s*\\(")
	ttlPrecise      = regexp.MustCompile(`(?i)TTL\s+(?:\w+\()?([\w.]+)\)?\s*\+\s*INTERVAL\s+(\d+)\s+DAY`)
	ttlCoarse       = regexp.MustCompile(`(?is)\bTTL\b.*?(?:\bINTERVAL\b|\btoInterval[A-Za-z]+\()`)
	ttlAlterModify  = regexp.MustCompile("(?im)^\\s*ALTER\\s+TABLE\\s+(?:`?[A-Za-z_]\\w*`?\\.)?`?([A-Za-z_]\\w*)`?\\s+(?:ON\\s+CLUSTER\\s+\\S+\\s+)?MODIFY\\s+TTL\\b")
	ttlAlterPrecise = regexp.MustCompile("(?im)^\\s*ALTER\\s+TABLE\\s+(?:`?[A-Za-z_]\\w*`?\\.)?`?([A-Za-z_]\\w*)`?\\s+(?:ON\\s+CLUSTER\\s+\\S+\\s+)?MODIFY\\s+TTL\\s+(?:\\w+\\()?([\\w.]+)\\)?\\s*\\+\\s*INTERVAL\\s+(\\d+)\\s+DAY")
	ttlUnmodelled   = regexp.MustCompile(`(?is)\bTTL\b[^;]*?\b(?:TO\s+(?:DISK|VOLUME)|RECOMPUTE|GROUP\s+BY)\b`)
	ttlAlterRemove  = regexp.MustCompile("(?im)^\\s*ALTER\\s+TABLE\\s+(?:`?[A-Za-z_]\\w*`?\\.)?`?([A-Za-z_]\\w*)`?\\s+(?:ON\\s+CLUSTER\\s+\\S+\\s+)?REMOVE\\s+TTL\\b")
	ttlHorizonLine  = regexp.MustCompile(`(?i)TTL\s+.*?\+\s*INTERVAL\s+(\d+)\s+DAY\s+DELETE`)
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

// ttlStripSQLComments blanks SQL line and block comments outside string literals and quoted identifiers, so a TTL clause that
// is only commented out does not count as enforced and a `--` inside a string literal does not hide a real clause. The inside
// of a single-quoted string literal is blanked too, so a COMMENT or DEFAULT text that quotes a TTL clause is not a clause.
func ttlStripSQLComments(text string) string {
	var out strings.Builder
	n := len(text)
	for i := 0; i < n; {
		c := text[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < n && text[j] != c {
				if text[j] == '\\' && c != '`' {
					j++
				}
				j++
			}
			if j >= n {
				j = n - 1
			}
			if c == '\'' {
				out.WriteByte(c)
				out.WriteString(strings.Repeat(" ", j-i-1))
				out.WriteByte(c)
			} else {
				out.WriteString(text[i : j+1])
			}
			i = j + 1
		case c == '-' && i+1 < n && text[i+1] == '-':
			for i < n && text[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && text[i+1] == '*':
			end := strings.Index(text[i+2:], "*/")
			if end < 0 {
				i = n
			} else {
				i += end + 4
			}
		default:
			out.WriteByte(c)
			i++
		}
	}
	return out.String()
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func ttlMigrationFiles(t *testing.T, dirs ...string) []string {
	t.Helper()
	var out []string
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		before := len(out)
		for _, e := range entries {
			if ext := filepath.Ext(e.Name()); !e.IsDir() && ext == ".sql" {
				out = append(out, filepath.Join(dir, e.Name()))
			}
		}
		if len(out) == before {
			t.Fatalf("no migration files under %s: a TTL scan over nothing would pass empty", dir)
		}
	}
	sort.Slice(out, func(i, j int) bool { return filepath.Base(out[i]) < filepath.Base(out[j]) })
	return out
}

func ttlScan(t *testing.T, dirs ...string) (precise map[string]ttlRetention, coarse map[string]bool) {
	t.Helper()
	precise, coarse = map[string]ttlRetention{}, map[string]bool{}
	for _, path := range ttlMigrationFiles(t, dirs...) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := ttlStripSQLComments(string(raw))
		for _, m := range ttlAlterModify.FindAllStringSubmatch(text, -1) {
			coarse[m[1]] = true
		}
		for _, statement := range strings.Split(text, ";") {
			for _, m := range ttlAlterRemove.FindAllStringSubmatch(statement, -1) {
				delete(precise, m[1])
				delete(coarse, m[1])
			}
			for _, m := range ttlAlterPrecise.FindAllStringSubmatch(statement, -1) {
				days, _ := strconv.Atoi(m[3])
				precise[m[1]] = ttlRetention{column: m[2], days: days}
			} // A MODIFY TTL the precise form cannot read (function-style interval, other unit, move or group-by rule) leaves the
			// earlier retention stale: fail closed by dropping the table from the precise set while the coarse set keeps it.
			understood := map[string]bool{}
			if !ttlUnmodelled.MatchString(statement) {
				for _, m := range ttlAlterPrecise.FindAllStringSubmatch(statement, -1) {
					understood[m[1]] = true
				}
			}
			for _, m := range ttlAlterModify.FindAllStringSubmatch(statement, -1) {
				if !understood[m[1]] {
					delete(precise, m[1])
					coarse[m[1]] = true
				}
			}
			tables, groups := ttlAttribute(statement, ttlPrecise)
			for i, table := range tables {
				days, _ := strconv.Atoi(groups[i][2])
				precise[table] = ttlRetention{column: groups[i][1], days: days}
			}
			tables, _ = ttlAttribute(statement, ttlCoarse)
			for _, table := range tables {
				coarse[table] = true
			}
			// A TTL with a move, recompute or group-by rule is a form the precise scanner cannot read as a retention: keep
			// the table in the coarse set only, so the vocabulary check fails loudly instead of trusting the first clause.
			if ttlUnmodelled.MatchString(statement) {
				tables, _ = ttlAttribute(statement, ttlCoarse)
				for _, table := range tables {
					delete(precise, table)
					coarse[table] = true
				}
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

// The pre-099 text is frozen in history/, the live chain is sql/; together they are every ClickHouse migration.
func ttlRealDirs(t *testing.T) []string {
	root := filepath.Join(repoRoot(t), "internal", "chmigrate")
	return []string{filepath.Join(root, "history"), filepath.Join(root, "sql")}
}

func TestTTLVocabularyMatchesTheRealMigrations(t *testing.T) {
	precise, coarse := ttlScan(t, ttlRealDirs(t)...)
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
	dirs := ttlRealDirs(t)
	precise, _ := ttlScan(t, dirs...)
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
	for _, path := range ttlMigrationFiles(t, dirs...) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range ttlHorizonLine.FindAllStringSubmatch(ttlStripSQLComments(string(raw)), -1) {
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
		writeFile(t, filepath.Join(dir, "1.sql"), "A = \"\"\"CREATE TABLE first_table (\n x Int8\n) ENGINE=M\n\nCREATE TABLE second_table (\n y Int8\n) ENGINE=M TTL ts + INTERVAL 20 DAY\"\"\"")
		p, c := ttlScan(t, dir)
		if p["second_table"].days != 20 || c["first_table"] || !c["second_table"] {
			t.Fatalf("misattributed: %+v %v", p, c)
		}
	})
	t.Run("coarse sweep ignores prose and identifiers containing TTL or Interval", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "\"\"\"Run SHOW CREATE TABLE to get the DDL; the table name in a CREATE TABLE statement.\"\"\"\nX = 'toIntervalDay THROTTLED'\n")
		_, c := ttlScan(t, dir)
		if len(c) != 0 {
			t.Fatalf("prose produced phantom tables: %v", c)
		}
	})
	t.Run("a commented-out TTL is not enforced", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE dropped_ttl (\n x Int8\n) ENGINE=M\n-- TTL ts + INTERVAL 30 DAY DELETE\n;")
		writeFile(t, filepath.Join(dir, "2.sql"), "CREATE TABLE block_commented (\n x Int8\n) ENGINE=M /* TTL ts + INTERVAL 30 DAY DELETE */;")
		p, c := ttlScan(t, dir)
		if len(p) != 0 || len(c) != 0 {
			t.Fatalf("commented-out TTL counted: precise=%v coarse=%v", p, c)
		}
	})
	t.Run("a database-qualified CREATE TABLE is found", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE IF NOT EXISTS analytics.qualified_ttl (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 21 DAY DELETE;")
		p, c := ttlScan(t, dir)
		if p["qualified_ttl"].days != 21 || !c["qualified_ttl"] {
			t.Fatalf("qualified name missed: %+v %v", p, c)
		}
	})
	t.Run("a -- inside a string literal does not hide a TTL clause", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE string_dashes (\n note String DEFAULT 'a--b'\n) ENGINE=M TTL ts + INTERVAL 33 DAY DELETE;")
		p, c := ttlScan(t, dir)
		if p["string_dashes"].days != 33 || !c["string_dashes"] {
			t.Fatalf("TTL after a string with dashes was dropped: %+v %v", p, c)
		}
	})
	t.Run("ALTER MODIFY TTL updates the retention", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE altered_ttl (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 365 DAY DELETE;")
		writeFile(t, filepath.Join(dir, "2.sql"), "ALTER TABLE altered_ttl MODIFY TTL ts + INTERVAL 90 DAY;")
		p, _ := ttlScan(t, dir)
		if p["altered_ttl"].days != 90 {
			t.Fatalf("ALTER retention not applied: %+v", p["altered_ttl"])
		}
	})
	t.Run("CREATE TABLE variants are found", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE clustered_ttl ON CLUSTER main (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 11 DAY DELETE;\nCREATE OR REPLACE TABLE replaced_ttl (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 12 DAY DELETE;")
		p, c := ttlScan(t, dir)
		if p["clustered_ttl"].days != 11 || p["replaced_ttl"].days != 12 || !c["clustered_ttl"] || !c["replaced_ttl"] {
			t.Fatalf("CREATE variants missed: %+v %v", p, c)
		}
	})
	t.Run("toIntervalDay TTL form is seen by the coarse sweep and fails the vocabulary check", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE function_form (\n x Int8\n) ENGINE=M TTL ts + toIntervalDay(30);")
		p, c := ttlScan(t, dir)
		if _, ok := p["function_form"]; ok || !c["function_form"] {
			t.Fatalf("toIntervalDay form: precise=%v coarse=%v", p, c)
		}
		if len(ttlVocabularyProblems(p, c)) == 0 {
			t.Fatal("a TTL table in an unreadable form must fail the vocabulary check")
		}
	})
	t.Run("a TTL quoted inside a string literal is not a clause", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE quoted_text (\n note String COMMENT 'TTL ts + INTERVAL 5 DAY DELETE'\n) ENGINE=M;")
		p, c := ttlScan(t, dir)
		if len(p) != 0 || len(c) != 0 {
			t.Fatalf("quoted TTL text counted: %v %v", p, c)
		}
	})
	t.Run("a TTL with a move rule is not trusted as a retention", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE tiered (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 7 DAY TO DISK 'cold', ts + INTERVAL 90 DAY DELETE;")
		p, c := ttlScan(t, dir)
		if _, ok := p["tiered"]; ok || !c["tiered"] || len(ttlVocabularyProblems(p, c)) == 0 {
			t.Fatalf("tiered TTL trusted: %v %v", p, c)
		}
	})
	t.Run("an unreadable ALTER MODIFY TTL fails closed instead of keeping the old retention", func(t *testing.T) {
		for name, alter := range map[string]string{
			"function interval": "ALTER TABLE feature_flag_event MODIFY TTL ts + toIntervalDay(12);",
			"other unit":        "ALTER TABLE feature_flag_event MODIFY TTL ts + INTERVAL 2 WEEK;",
			"move rule":         "ALTER TABLE feature_flag_event MODIFY TTL ts + INTERVAL 7 DAY TO DISK 'cold', ts + INTERVAL 12 DAY DELETE;",
		} {
			dir := t.TempDir()
			writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE feature_flag_event (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 90 DAY DELETE;")
			writeFile(t, filepath.Join(dir, "2.sql"), alter)
			p, c := ttlScan(t, dir)
			if _, ok := p["feature_flag_event"]; ok || !c["feature_flag_event"] {
				t.Fatalf("%s: stale retention kept: precise=%v coarse=%v", name, p, c)
			}
			if len(ttlVocabularyProblems(p, c)) == 0 {
				t.Fatalf("%s: the vocabulary check passed on an unreadable ALTER", name)
			}
		}
	})
	t.Run("ALTER REMOVE TTL drops the table", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "1.sql"), "CREATE TABLE feature_flag_event (\n x Int8\n) ENGINE=M TTL ts + INTERVAL 90 DAY DELETE;")
		writeFile(t, filepath.Join(dir, "2.sql"), "ALTER TABLE feature_flag_event REMOVE TTL;")
		p, c := ttlScan(t, dir)
		if _, ok := p["feature_flag_event"]; ok || c["feature_flag_event"] {
			t.Fatalf("REMOVE TTL not applied: %v %v", p, c)
		}
		if len(ttlVocabularyProblems(p, c)) == 0 {
			t.Fatal("a listed table that lost its TTL by ALTER must fail the vocabulary check")
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
