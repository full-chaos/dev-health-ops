// Package chdatetime guards strict `<` and `>` comparisons of a ClickHouse DateTime64 column against a DateTime bound.
// ClickHouse 26.7 analyses that pair wrongly in a partition key, a sort key or a skip index, so rows in the boundary
// second are pruned away; `<=`, `>=` and a DateTime64 bound are analysed correctly (CHAOS-7290). A bound that crosses the
// driver as a positional `?` time.Time is a DateTime (toDateTime('<seconds>')), so such a bound on a keyed column is the
// failure; bind Int64 microseconds with chtime.Micros and rebuild the instant with fromUnixTimestamp64Micro(?, 'UTC').
//
// This is a best-effort text scan of literal SQL, NOT a proof that no unsafe bound exists. KNOWN MISSES, pinned by
// TestKnownMissesPassTheGuard: a WITH alias or joined projection that reuses a catalog column name, and SQL assembled from
// a run-time value or a fragment the guard cannot fold.
//
// The guard reads static text: the string constants of the tracked non-test Go files (a concatenation of literals and of
// constants of this module is folded) and the tracked .sql files, apart from the migration history, archive, rollback and
// baseline trees and testdata. The column types and keys come from internal/chmigrate/baseline/head.json. A column is
// matched by NAME, not by table or query scope, so a WITH alias or a projection that reuses a catalog name is read as
// that column; a SQL text assembled at run time from a value the guard cannot fold is not seen. The guard fails when it
// cannot measure: an unreadable head.json object or table, no Go source, no strict comparison at all.
package chdatetime_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func realCatalog(t *testing.T, extra map[string]string) catalog {
	t.Helper()
	head, err := loadHead(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := buildCatalog(head, extra)
	if err != nil {
		t.Fatalf("the catalog cannot be measured: %v", err)
	}
	return cat
}

const engine = "ENGINE = ReplacingMergeTree() "

var plantExtra = map[string]string{
	"plant_sort": "CREATE TABLE plant_sort (`org_id` String, `sort_ts` DateTime64(6, 'UTC')) " + engine + "ORDER BY (org_id, sort_ts)",
	"plant_part": "CREATE TABLE plant_part (`org_id` String, `part_ts` DateTime64(3, 'UTC')) " + engine + "PARTITION BY toYYYYMM(part_ts) ORDER BY (org_id)",
	"plant_skip": "CREATE TABLE plant_skip (`org_id` String, `skip_ts` DateTime64(3, 'UTC'), INDEX idx_skip skip_ts TYPE minmax GRANULARITY 1) " + engine + "ORDER BY (org_id)",
	"plant_none": "CREATE TABLE plant_none (`org_id` String, `plain_ts` DateTime64(3, 'UTC')) " + engine + "ORDER BY (org_id)",
	"plant_quot": "CREATE TABLE plant_quot (`org_id` String, `quoted_ts` DateTime64(3, 'UTC'), INDEX `idx ORDER BY` `quoted_ts` TYPE minmax GRANULARITY 1) " + engine + "ORDER BY (org_id) COMMENT 'ORDER BY (plain_ts) PARTITION BY plain_ts'",
}

func TestCatalogIsMeasuredFromTheBaseline(t *testing.T) {
	cat := realCatalog(t, nil)
	if cat.Tables < 100 || len(cat.Columns) < 100 {
		t.Fatalf("catalog holds %d tables and %d DateTime64 columns: head.json parsed too little", cat.Tables, len(cat.Columns))
	}
	want := map[[2]string]string{
		{"product_telemetry_events", "occurred_at"}: "PARTITION BY+ORDER BY",
		{"work_item_transitions", "occurred_at"}:    "ORDER BY",
		{"work_item_interactions", "occurred_at"}:   "ORDER BY+PRIMARY KEY",
		{"ai_attribution", "observed_at"}:           "PARTITION BY",
		{"git_pull_requests", "created_at"}:         "",
	}
	for key, keys := range want {
		col, ok := cat.has(key[0], key[1])
		if !ok {
			t.Fatalf("%s.%s is not a DateTime64 column of the catalog", key[0], key[1])
		}
		if got := strings.Join(col.Keys, "+"); got != keys {
			t.Errorf("%s.%s keys = %q, want %q", key[0], key[1], got, keys)
		}
	}
}

func TestCatalogFixturesAndRefusals(t *testing.T) {
	cat := realCatalog(t, plantExtra)
	wantKeys := map[string]map[string]string{
		"plant_sort": {"sort_ts": "ORDER BY"},
		"plant_part": {"part_ts": "PARTITION BY"},
		"plant_skip": {"skip_ts": "INDEX"},
		"plant_none": {"plain_ts": ""},
		"plant_quot": {"quoted_ts": "INDEX"},
	}
	for table, cols := range wantKeys {
		for name, want := range cols {
			col, ok := cat.has(table, name)
			if !ok {
				t.Errorf("fixture %s.%s is not in the catalog", table, name)
				continue
			}
			if got := strings.Join(col.Keys, "+"); got != want {
				t.Errorf("%s.%s keys = %q, want %q", table, name, got, want)
			}
		}
	}
	for name, create := range map[string]string{
		"no ENGINE":     "CREATE TABLE broken (`a` DateTime64(3, 'UTC'))",
		"not a table":   "CREATE VIEW broken AS SELECT 1",
		"bad column":    "CREATE TABLE broken (1a DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY a",
		"unbalanced":    "CREATE TABLE broken (`a` DateTime64(3, 'UTC') ENGINE = MergeTree",
		"empty create":  "",
		"bad index":     "CREATE TABLE broken (`a` DateTime64(3, 'UTC'), INDEX i a) ENGINE = MergeTree ORDER BY b",
		"empty element": "CREATE TABLE broken (`a` DateTime64(3, 'UTC'), ) ENGINE = MergeTree ORDER BY b",
	} {
		if _, err := parseTable("broken", create); err == nil {
			t.Errorf("%s: parseTable accepted a statement it cannot measure", name)
		}
	}
	for name, head := range map[string]string{
		"not JSON":         "{",
		"null create":      `{"objects":[{"name":"t","create":null}]}`,
		"empty create":     `{"objects":[{"name":"t","create":""}]}`,
		"unknown object":   `{"objects":[{"name":"t","create":"DROP TABLE t"}]}`,
		"nameless object":  `{"objects":[{"create":"CREATE TABLE t (a UInt8) ENGINE = Memory"}]}`,
		"unreadable table": `{"objects":[{"name":"t","create":"CREATE TABLE t (a UInt8)"}]}`,
	} {
		if _, err := buildCatalog([]byte(head), nil); err == nil {
			t.Errorf("%s: buildCatalog accepted a head.json it cannot measure", name)
		}
	}
}

func TestClassifyAndStrictHits(t *testing.T) {
	names := map[string]bool{"ts": true, "other_ts": true}
	type want struct {
		col string
		cls class
	}
	for sql, hits := range map[string][]want{
		"WHERE ts < toDateTime(1)":                         {{"ts", classFlag}},
		"WHERE a.ts > ?":                                   {{"ts", classFlag}},
		"WHERE ?\n < ts":                                   {{"ts", classFlag}},
		"WHERE {x:DateTime} < ts":                          {{"ts", classFlag}},
		"WHERE {x:DateTime64(3, 'UTC')} < ts":              {{"ts", classSafe}},
		"WHERE ts < {x:Date}":                              {{"ts", classSafe}},
		"WHERE now() < ts":                                 {{"ts", classFlag}},
		"WHERE now64(3) < ts":                              {{"ts", classSafe}},
		"WHERE now() - INTERVAL 30 DAY > ts":               {{"ts", classFlag}},
		"WHERE ts < now64(3, 'UTC') - toIntervalSecond(5)": {{"ts", classSafe}},
		"WHERE `ts` < ?":                                   {{"ts", classFlag}},
		"WHERE \"ts\" < ?":                                 {{"ts", classFlag}},
		"WHERE db.tbl.ts < ?":                              {{"ts", classFlag}},
		"WHERE (ts) < (?)":                                 {{"ts", classFlag}},
		"WHERE (a.ts < toDateTime(1))":                     {{"ts", classFlag}},
		"WHERE ts<?":                                       {{"ts", classFlag}},
		"WHERE ts <\r\n ?":                                 {{"ts", classFlag}},
		"WHERE ts < toDateTime(1) -- ts > ?\n":             {{"ts", classFlag}},
		"WHERE ts < CAST(? AS DateTime)":                   {{"ts", classFlag}},
		"WHERE ts < CAST(? AS DateTime64(3, 'UTC'))":       {{"ts", classSafe}},
		"WHERE ts < CAST(x, 'DateTime')":                   {{"ts", classFlag}},
		"WHERE ts < (toDateTime64(1, 3))":                  {{"ts", classSafe}},
		"WHERE ts < toDateTime64(1, 3)::DateTime":          {{"ts", classFlag}},
		"WHERE ts < $1::timestamptz":                       {{"ts", classSkip}},
		"WHERE $1 < ts":                                    {{"ts", classSkip}},
		"WHERE less(ts, toDateTime(1))":                    {{"ts", classFlag}},
		"WHERE greater(toDateTime(1), ts)":                 {{"ts", classFlag}},
		"WHERE ts < %s":                                    {{"ts", classUnmeasured}},
		"WHERE ts < %[1]s":                                 {{"ts", classUnmeasured}},
		"WHERE ts < (SELECT 1)":                            {{"ts", classUnmeasured}},
		"WHERE ts < other_ts":                              {{"ts", classSafe}},
		"WHERE ts > 5":                                     {{"ts", classSkip}},
		"WHERE ts < '2026-01-01'":                          {{"ts", classSafe}},
		"WHERE ts < ? AND other_ts > ?":                    {{"ts", classFlag}, {"other_ts", classFlag}},
		"WHERE ts <= toDateTime(1)":                        nil,
		"WHERE ts >= toDateTime(1)":                        nil,
		"WHERE ts <> toDateTime(1)":                        nil,
		"WHERE lessOrEquals(ts, toDateTime(1))":            nil,
		"WHERE m -> ts":                                    nil,
		"WHERE ts << 2":                                    nil,
		"WHERE ts BETWEEN 1 AND 2":                         nil,
		"WHERE other < toDateTime(1)":                      nil,
		"WHERE tsx < ?":                                    nil,
		"WHERE ts < ":                                      nil,
		"SELECT 'ts < ?' AS x":                             nil,
		"SELECT 1 -- ts < ?\n":                             nil,
		"SELECT 1 /* ts < ? */":                            nil,
		"WHERE s = 'a ts < ?' AND y = 1":                   nil,
		"SELECT \"x ts < ?\"":                              nil,
	} {
		got := strictHits(text{File: "x.go", Line: 1, Body: sql}, names)
		if len(got) != len(hits) {
			t.Errorf("%q: hits %v, want %v", sql, got, hits)
			continue
		}
		for i, h := range got {
			if h.Column != hits[i].col || h.Class != hits[i].cls {
				t.Errorf("%q: hit %d = %s %s, want %s %s", sql, i, h.Column, h.Class, hits[i].col, hits[i].cls)
			}
		}
	}
}

func TestGoConstantsAreFoldedAcrossFilesAndPackages(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, rel), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.test/m\n\ngo 1.22\n")
	write("a/a.go", "package a\n\nconst Column = \"sort_ts\"\nconst Micro = \"fromUnixTimestamp64Micro(?, 'UTC')\"\n")
	write("a/b.go", "package a\n\nconst Where = \"WHERE \" + Column + \" < \"\n")
	write("b/b.go", "package b\n\nimport \"example.test/m/a\"\n\nconst unsafe = a.Where + \"toDateTime(?)\"\nconst safe = a.Where + a.Micro\nconst cast = a.Where + string(\"?\")\nconst fragment = \"sort_ts < \"\n")
	src, err := sourcesOf(root, []string{"a/a.go", "a/b.go", "b/b.go"})
	if err != nil {
		t.Fatal(err)
	}
	cat := realCatalog(t, plantExtra)
	res := check(cat, src.texts())
	if len(res.Violations) != 2 {
		t.Fatalf("violations = %v, want the unsafe and the converted bound", res.Violations)
	}
	for _, v := range res.Violations {
		if v.File != "b/b.go" || v.Column != "sort_ts" {
			t.Errorf("unexpected violation %v", v)
		}
	}
	if _, err := sourcesOf(root, []string{"b/missing.go"}); err == nil {
		t.Error("a tracked file that cannot be read did not fail the scan")
	}
}

func TestEveryStrictUnsafeBoundOnAKeyedColumnIsFixed(t *testing.T) {
	cat := realCatalog(t, nil)
	src, err := loadSources(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	texts := src.texts()
	if len(src.pkgs) < 100 || len(texts) < 500 {
		t.Fatalf("scanned %d Go packages and %d SQL texts: the guard would scan too little", len(src.pkgs), len(texts))
	}
	res := check(cat, texts)
	if res.Strict == 0 {
		t.Fatal("found no strict comparison of a DateTime64 column: the scan measured nothing")
	}
	for _, v := range res.Violations {
		t.Errorf("%s:%d: `%s %s %s` compares a keyed DateTime64 column (%s) with a %s bound; bind chtime.Micros(t) as fromUnixTimestamp64Micro(?, 'UTC') (or a DateTime64 parameter) or use <= / >=",
			v.File, v.Line, v.Column, v.Op, v.Bound, strings.Join(cat.keyNames()[v.Column], ", "), v.Class)
	}
	t.Logf("strict comparisons of DateTime64 columns: %d, on keyed columns: %d, unsafe on columns no table keys (not a failure): %v", res.Strict, res.OnKeyed, res.Flagged)
}

func TestPlantedStrictComparisonsFailTheGuardAndTheSafeFormsDoNot(t *testing.T) {
	cat := realCatalog(t, plantExtra)
	src, err := loadSources(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	texts := src.texts()
	if base := check(cat, texts); len(base.Violations) != 0 {
		t.Fatalf("the tree without a plant is not clean: %v", base.Violations)
	}
	plant := func(body string) []hit {
		return check(cat, append(append([]text(nil), texts...), text{File: "planted.go", Line: 1, Body: body})).Violations
	}
	for name, body := range map[string]string{
		"sort-key column":       "SELECT 1 FROM plant_sort WHERE sort_ts < toDateTime('2026-01-01 00:00:00')",
		"partition column":      "SELECT 1 FROM plant_part WHERE part_ts > toDateTime('2026-01-01 00:00:00')",
		"minmax-index column":   "SELECT 1 FROM plant_skip WHERE skip_ts < toDateTime('2026-01-01 00:00:00')",
		"quoted-name index":     "SELECT 1 FROM plant_quot WHERE quoted_ts < toDateTime('2026-01-01 00:00:00')",
		"real sort-key column":  "SELECT 1 FROM work_item_transitions WHERE org_id = ? AND occurred_at < ?",
		"real partition column": "SELECT 1 FROM product_telemetry_events WHERE occurred_at > now()",
		"DateTime parameter":    "SELECT 1 FROM plant_sort WHERE sort_ts < {end:DateTime}",
		"unmeasurable bound":    "SELECT 1 FROM plant_sort WHERE sort_ts < %s",
		"bound before operator": "SELECT 1 FROM plant_sort WHERE toDateTime(1) > sort_ts",
		"positional on keyed":   "SELECT 1 FROM plant_skip WHERE ? > skip_ts",
		"backtick column":       "SELECT 1 FROM plant_sort WHERE `sort_ts` < ?",
		"cast down":             "SELECT 1 FROM plant_sort WHERE sort_ts < CAST(? AS DateTime)",
		"function spelling":     "SELECT 1 FROM plant_sort WHERE less(sort_ts, toDateTime(1))",
	} {
		if got := plant(body); len(got) != 1 || got[0].File != "planted.go" {
			t.Errorf("%s: want exactly the planted violation, got %v", name, got)
		}
	}
	for name, body := range map[string]string{
		"non-key column":       "SELECT 1 FROM plant_none WHERE plain_ts < toDateTime('2026-01-01 00:00:00')",
		"non-key positional":   "SELECT 1 FROM plant_none WHERE plain_ts < ?",
		"inclusive on key":     "SELECT 1 FROM plant_sort WHERE sort_ts <= toDateTime(1) AND sort_ts >= toDateTime(1)",
		"microsecond bound":    "SELECT 1 FROM plant_sort WHERE sort_ts < fromUnixTimestamp64Micro(?, 'UTC')",
		"DateTime64 parameter": "SELECT 1 FROM plant_part WHERE part_ts < {end:DateTime64(3, 'UTC')}",
		"toDateTime64":         "SELECT 1 FROM plant_skip WHERE skip_ts > toDateTime64('2026-01-01', 3, 'UTC')",
		"cast to DateTime64":   "SELECT 1 FROM plant_sort WHERE sort_ts < CAST(? AS DateTime64(6, 'UTC'))",
		"numbered bound":       "SELECT 1 FROM t WHERE sort_ts < $1",
		"comment text":         "SELECT 1 FROM plant_sort -- WHERE sort_ts < ?\n",
		"string text":          "SELECT 'sort_ts < ?' FROM plant_sort",
		"quoted comment key":   "SELECT 1 FROM plant_quot WHERE plain_ts < ?",
	} {
		if got := plant(body); len(got) != 0 {
			t.Errorf("%s: a safe form was flagged: %v", name, got)
		}
	}
}

// TestKnownMissesPassTheGuard pins the two named misses of the static text scan: each text below is an unsafe strict
// comparison of a keyed DateTime64 column and the guard lets it pass. A change that makes the guard catch one is good news;
// move that case to the planted cases and delete it here.
func TestKnownMissesPassTheGuard(t *testing.T) {
	cat := realCatalog(t, plantExtra)
	aliasReuse := text{File: "miss_alias.go", Line: 1, Body: "WITH toDateTime(1) AS part_ts SELECT 1 FROM plant_sort WHERE sort_ts < part_ts"}
	if got := check(cat, []text{aliasReuse}).Violations; len(got) != 0 {
		t.Errorf("WITH alias reuse is a pinned miss but the guard now flags it: %v", got)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "p"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.test/m\n\ngo 1.22\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	body := "package p\n\nvar bound = \"toDateTime(?)\"\n\nfunc q() string { return \"SELECT 1 FROM plant_sort WHERE sort_ts < \" + bound }\n"
	if err := os.WriteFile(filepath.Join(root, "p", "p.go"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := sourcesOf(root, []string{"p/p.go"})
	if err != nil {
		t.Fatal(err)
	}
	if got := check(cat, src.texts()).Violations; len(got) != 0 {
		t.Errorf("fragment assembly from a run-time value is a pinned miss but the guard now flags it: %v", got)
	}
}
