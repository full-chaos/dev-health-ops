// Package chdatetime guards strict `<` and `>` comparisons of a ClickHouse DateTime64 column against a DateTime bound.
// ClickHouse 26.7 analyses that pair wrongly in a partition key, a sort key or a skip index, so rows in the boundary
// second are pruned away; `<=`, `>=` and a DateTime64 bound are analysed correctly (CHAOS-7290). A bound that crosses the
// driver as a positional `?` time.Time is a DateTime (toDateTime('<seconds>')), so such a bound on a keyed column is the
// failure; bind Int64 microseconds with chtime.Micros and rebuild the instant with fromUnixTimestamp64Micro(?, 'UTC').
//
// The column types and keys come from internal/chmigrate/baseline/head.json. The guard fails when it cannot measure.
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

const plantTables = "ENGINE = ReplacingMergeTree() "

var plantExtra = map[string]string{
	"plant_sort": "CREATE TABLE plant_sort (`org_id` String, `sort_ts` DateTime64(6, 'UTC')) " + plantTables + "ORDER BY (org_id, sort_ts)",
	"plant_part": "CREATE TABLE plant_part (`org_id` String, `part_ts` DateTime64(3, 'UTC')) " + plantTables + "PARTITION BY toYYYYMM(part_ts) ORDER BY (org_id)",
	"plant_skip": "CREATE TABLE plant_skip (`org_id` String, `skip_ts` DateTime64(3, 'UTC'), INDEX idx_skip skip_ts TYPE minmax GRANULARITY 1) " + plantTables + "ORDER BY (org_id)",
	"plant_none": "CREATE TABLE plant_none (`org_id` String, `plain_ts` DateTime64(3, 'UTC')) " + plantTables + "ORDER BY (org_id)",
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
	for table, wantKey := range map[string]string{"plant_sort": "ORDER BY", "plant_part": "PARTITION BY", "plant_skip": "INDEX", "plant_none": ""} {
		var found bool
		for _, col := range cat.Columns {
			if col.Table != table || col.Name == "org_id" {
				continue
			}
			found = true
			if got := strings.Join(col.Keys, "+"); got != wantKey {
				t.Errorf("%s keys = %q, want %q", table, got, wantKey)
			}
		}
		if !found {
			t.Errorf("fixture table %s has no DateTime64 column in the catalog", table)
		}
	}
	for name, create := range map[string]string{
		"no ENGINE":    "CREATE TABLE broken (`a` DateTime64(3, 'UTC'))",
		"not a table":  "CREATE VIEW broken AS SELECT 1",
		"bad column":   "CREATE TABLE broken (a DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY a",
		"unbalanced":   "CREATE TABLE broken (`a` DateTime64(3, 'UTC') ENGINE = MergeTree",
		"empty create": "",
	} {
		if _, err := parseTable("broken", create); err == nil {
			t.Errorf("%s: parseTable accepted a statement it cannot measure", name)
		}
	}
	if _, err := buildCatalog([]byte("{"), nil); err == nil {
		t.Error("buildCatalog accepted a head.json that is not JSON")
	}
}

func TestClassifyBound(t *testing.T) {
	names := map[string]bool{"valid_from": true}
	for bound, want := range map[string]class{
		"?":                                       classFlag,
		"? AND x":                                 classFlag,
		"{end:DateTime}":                          classFlag,
		"{end:DateTime64(3, 'UTC')}":              classSafe,
		"{end:Date}":                              classSafe,
		"toDateTime('2026-01-01 00:00:00')":       classFlag,
		"toDateTime({end_day:Date})":              classFlag,
		"now()":                                   classFlag,
		"now() - INTERVAL 30 DAY":                 classFlag,
		"now64(3, 'UTC') - toIntervalSecond(5)":   classSafe,
		"toDateTime64({win_end:Date}, 3, 'UTC')":  classSafe,
		"fromUnixTimestamp64Micro(?, 'UTC')":      classSafe,
		"fromUnixTimestamp64Milli(toInt64(?), 1)": classSafe,
		"toDate('2026-01-01')":                    classSafe,
		"'2026-01-01'":                            classSafe,
		"valid_from":                              classSafe,
		"o.valid_from AND":                        classSafe,
		"$3":                                      classSkip,
		"0":                                       classSkip,
		"%s":                                      classUnmeasured,
		"%[1]s":                                   classUnmeasured,
		"EXCLUDED.updated_at":                     classUnmeasured,
		"(SELECT 1)":                              classUnmeasured,
		"toStartOfDay(x)":                         classUnmeasured,
		"":                                        classUnmeasured,
	} {
		if got := classifyBound(bound, names); got != want {
			t.Errorf("classifyBound(%q) = %s, want %s", bound, got, want)
		}
	}
}

func TestStrictHitsReadsBothSidesAndSkipsTheRest(t *testing.T) {
	names := map[string]bool{"ts": true}
	for sql, want := range map[string][]class{
		"WHERE ts < toDateTime(1)":    {classFlag},
		"WHERE a.ts > ?":              {classFlag},
		"WHERE ?\n < ts":              {classFlag},
		"WHERE {x:DateTime} < ts":     {classFlag},
		"WHERE now() < ts":            {classFlag},
		"WHERE now64(3) < ts":         {classSafe},
		"WHERE ts <= toDateTime(1)":   nil,
		"WHERE ts >= toDateTime(1)":   nil,
		"WHERE ts <> toDateTime(1)":   nil,
		"WHERE m -> ts":               nil,
		"WHERE ts << 2":               nil,
		"WHERE ts BETWEEN 1 AND 2":    nil,
		"WHERE other < toDateTime(1)": nil,
		"WHERE ts > ts":               {classSafe},
		"WHERE ts < 5":                {classSkip},
		"WHERE ts < $1":               {classSkip},
		"WHERE tsx < ?":               nil,
		"WHERE `ts` < ?":              nil,
		"WHERE ts < ? AND ts > ?":     {classFlag, classFlag},
	} {
		var got []class
		for _, h := range strictHits(text{File: "x.go", Line: 1, Body: sql}, names) {
			got = append(got, h.Class)
		}
		if len(got) != len(want) {
			t.Errorf("%q: classes %v, want %v", sql, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%q: classes %v, want %v", sql, got, want)
			}
		}
	}
}

func TestEveryStrictUnsafeBoundOnAKeyedColumnIsFixedOrAllowlisted(t *testing.T) {
	cat := realCatalog(t, nil)
	root := repoRoot(t)
	texts, goFiles, err := sqlTexts(root)
	if err != nil {
		t.Fatal(err)
	}
	allow, err := loadAllowlist(filepath.Join("allowlist.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	res := check(cat, texts, allow)
	if goFiles < 500 || len(texts) < 500 {
		t.Fatalf("scanned %d Go files and %d SQL texts: the guard would scan too little", goFiles, len(texts))
	}
	if res.Strict == 0 || res.OnKeyed == 0 {
		t.Fatalf("found %d strict comparisons of DateTime64 columns, %d on a keyed column: the scan measured nothing", res.Strict, res.OnKeyed)
	}
	for _, v := range res.Violations {
		t.Errorf("%s:%d: `%s %s %s` compares a keyed DateTime64 column (%s) with a %s bound; bind chtime.Micros(t) as fromUnixTimestamp64Micro(?, 'UTC') (or a DateTime64 parameter) or use <= / >=",
			v.File, v.Line, v.Column, v.Op, v.Bound, strings.Join(cat.keyNames()[v.Column], ", "), v.Class)
	}
	for _, row := range res.StaleRows {
		t.Errorf("allowlist.tsv row %s %s %q matches no comparison: delete it", row.File, row.Column, row.Bound)
	}
	t.Logf("strict comparisons of DateTime64 columns: %d, on keyed columns: %d, unsafe on columns no table keys (not a failure): %v", res.Strict, res.OnKeyed, res.Flagged)
}

func TestPlantedStrictComparisonsFailTheGuardAndTheSafeFormsDoNot(t *testing.T) {
	cat := realCatalog(t, plantExtra)
	texts, _, err := sqlTexts(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	allow, err := loadAllowlist("allowlist.tsv")
	if err != nil {
		t.Fatal(err)
	}
	if base := check(cat, texts, allow); len(base.Violations) != 0 || len(base.StaleRows) != 0 {
		t.Fatalf("the tree without a plant is not clean: %v %v", base.Violations, base.StaleRows)
	}
	plant := func(body string) result {
		return check(cat, append(append([]text(nil), texts...), text{File: "planted.go", Line: 1, Body: body}), allow)
	}
	for name, body := range map[string]string{
		"sort-key column":       "SELECT 1 FROM plant_sort WHERE sort_ts < toDateTime('2026-01-01 00:00:00')",
		"partition column":      "SELECT 1 FROM plant_part WHERE part_ts > toDateTime('2026-01-01 00:00:00')",
		"minmax-index column":   "SELECT 1 FROM plant_skip WHERE skip_ts < toDateTime('2026-01-01 00:00:00')",
		"real sort-key column":  "SELECT 1 FROM work_item_transitions WHERE org_id = ? AND occurred_at < ?",
		"real partition column": "SELECT 1 FROM product_telemetry_events WHERE occurred_at > now()",
		"DateTime parameter":    "SELECT 1 FROM plant_sort WHERE sort_ts < {end:DateTime}",
		"unmeasurable bound":    "SELECT 1 FROM plant_sort WHERE sort_ts < %s",
		"bound before operator": "SELECT 1 FROM plant_sort WHERE toDateTime(1) > sort_ts",
		"positional on keyed":   "SELECT 1 FROM plant_skip WHERE ? > skip_ts",
	} {
		got := plant(body).Violations
		if len(got) != 1 || got[0].File != "planted.go" {
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
		"Postgres bound":       "SELECT 1 FROM t WHERE sort_ts < $1",
	} {
		if got := plant(body).Violations; len(got) != 0 {
			t.Errorf("%s: a safe form was flagged: %v", name, got)
		}
	}
}
