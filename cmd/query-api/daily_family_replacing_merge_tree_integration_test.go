//go:build integration

// The daily-family output tables used to be plain MergeTree: every run
// appended a full copy, and every reader paid to scan and discard the older
// copies. These tests pin the write-time replacement against the REAL
// migration chain, from the reader's side:
//
//   - once merged, a table holds one row per reader key, so the copies no
//     longer cost the readers anything;
//   - the readers' own dedup (Python's dedup_from, whole-row argMax,
//     per-column argMax, and the Go resolvers built on them) returns the same
//     rows before and after the merge, which is what keeps the Go and Python
//     API bodies identical to each other while the engine changes under both;
//   - the one reader shape that DOES change is pinned on purpose: a
//     per-column argMax over a Nullable column skips NULL, so it shows a stale
//     older value until the merge removes that copy, then the newest NULL.
//
// It lives in package main because cmd/query-api is already an integration
// shard and can import both the query-api resolvers and the test support.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/hotspots"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reviewedges"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// dailyFamilyReaderKeys is the key each table's readers deduplicate on
// (clickhouse_dedup._APPEND_ONLY_DAILY_KEYS, api/queries/metrics.py's
// _DEDUP_BY_COMPUTED_AT, and the argMax readers of the last two). A merge
// must never collapse two rows these keys keep apart.
var dailyFamilyReaderKeys = map[string][]string{
	"repo_metrics_daily":              {"org_id", "repo_id", "day"},
	"user_metrics_daily":              {"org_id", "repo_id", "author_email", "day"},
	"commit_metrics":                  {"org_id", "repo_id", "day", "author_email", "commit_hash"},
	"team_metrics_daily":              {"org_id", "team_id", "repo_id", "day"},
	"file_metrics_daily":              {"org_id", "repo_id", "day", "path"},
	"file_hotspot_daily":              {"org_id", "repo_id", "day", "file_path"},
	"review_edges_daily":              {"org_id", "repo_id", "reviewer", "author", "day"},
	"cicd_metrics_daily":              {"org_id", "repo_id", "day"},
	"deploy_metrics_daily":            {"org_id", "repo_id", "day"},
	"incident_metrics_daily":          {"org_id", "repo_id", "day"},
	"work_item_state_durations_daily": {"org_id", "provider", "work_scope_id", "team_id", "status", "day"},
	"team_cognitive_load_daily":       {"org_id", "team_id", "day"},
	"compounding_risk_daily":          {"org_id", "scope", "scope_id", "day"},
	"testops_pipeline_metrics_daily":  {"org_id", "repo_id", "day"},
	"testops_test_metrics_daily":      {"org_id", "repo_id", "day"},
	"testops_coverage_metrics_daily":  {"org_id", "repo_id", "day"},
	"testops_release_confidence":      {"org_id", "repo_id", "day"},
	"testops_quality_drag":            {"org_id", "repo_id", "day"},
	"testops_pipeline_stability":      {"org_id", "repo_id", "day"},
}

// Three generations of every key, inserted out of order and one INSERT per
// generation so each lands in its own part. The stale key is written only by
// the oldest generation: a newer run that no longer emits a key must not make
// that key disappear.
var dailyFamilyGenerations = []string{"2026-09-10 11:00:00", "2026-09-10 12:00:00", "2026-09-10 10:00:00"}

const dailyFamilyStaleGeneration = "2026-09-10 10:00:00"

func dailyFamilyTableNames() []string {
	names := make([]string, 0, len(dailyFamilyReaderKeys))
	for name := range dailyFamilyReaderKeys {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func TestDailyFamilyTablesCollapseCopiesWithoutChangingReaderRows(t *testing.T) {
	ctx := context.Background()
	instance, client, raw := migratedClickHouse(ctx, t)
	httpDSN, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	pythonSources := dailyFamilyPythonDedupSources(ctx, t)

	type readerOutputs map[string]string
	capture := func(t *testing.T, table string) readerOutputs {
		t.Helper()
		key := strings.Join(dailyFamilyReaderKeys[table], ", ")
		out := readerOutputs{
			"whole_row_argmax": dailyFamilyHTTPQuery(ctx, t, httpDSN, fmt.Sprintf(
				"SELECT %s, argMax(tuple(*), computed_at) FROM %s GROUP BY %s ORDER BY ALL", key, table, key)),
			"per_column_argmax": dailyFamilyHTTPQuery(ctx, t, httpDSN, dailyFamilyPerColumnArgMax(ctx, t, raw, table)),
		}
		if source, ok := pythonSources[table]; ok {
			out["python_dedup_from"] = dailyFamilyHTTPQuery(ctx, t, httpDSN,
				fmt.Sprintf("SELECT * FROM %s ORDER BY ALL", source))
		}
		switch table {
		case "review_edges_daily":
			result, err := reviewedges.Resolve(ctx, client, "org-a",
				graphqldate.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)),
				graphqldate.New(time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)), nil, 1000)
			if err != nil {
				t.Fatalf("reviewedges.Resolve: %v", err)
			}
			out["go_reviewedges_resolve"] = dailyFamilyJSON(t, result)
		case "file_hotspot_daily":
			limit := 1000
			result, err := hotspots.Resolve(ctx, client, "org-a",
				time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC), nil, &limit)
			if err != nil {
				t.Fatalf("hotspots.Resolve: %v", err)
			}
			out["go_hotspots_resolve"] = dailyFamilyJSON(t, result)
		}
		for name, body := range out {
			if strings.TrimSpace(body) == "" || body == "{\"rows\":[]}" || body == "{\"edges\":[]}" {
				t.Fatalf("%s reader %s returned nothing -- the before/after comparison would be vacuous", table, name)
			}
		}
		return out
	}

	for _, table := range dailyFamilyTableNames() {
		// Merges stay stopped until the "before" reads are taken: a background
		// merge between the inserts and those reads would make both sides of
		// the comparison post-merge and prove nothing.
		if err := raw.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges on %s: %v", table, err)
		}
		keys := dailyFamilySeed(ctx, t, raw, table)
		var rowsBefore uint64
		if err := raw.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&rowsBefore); err != nil {
			t.Fatal(err)
		}
		before := capture(t, table)

		if err := raw.Exec(ctx, "SYSTEM START MERGES "+table); err != nil {
			t.Fatalf("start merges on %s: %v", table, err)
		}
		if err := raw.Exec(ctx, fmt.Sprintf("OPTIMIZE TABLE %s FINAL", table)); err != nil {
			t.Fatalf("optimize %s: %v", table, err)
		}

		t.Run(table+"/copies_collapse", func(t *testing.T) {
			var rows, distinct uint64
			key := strings.Join(dailyFamilyReaderKeys[table], ", ")
			if err := raw.QueryRow(ctx, fmt.Sprintf(
				"SELECT count(), uniqExact((%s)) FROM %s", key, table)).Scan(&rows, &distinct); err != nil {
				t.Fatal(err)
			}
			if rowsBefore <= uint64(keys) {
				t.Fatalf("%d rows before the merge for %d keys -- the seeded copies were never there, so the merge proves nothing", rowsBefore, keys)
			}
			if distinct != uint64(keys) {
				t.Fatalf("%d distinct reader keys after the merge, want the %d seeded -- a merge collapsed rows the readers keep apart", distinct, keys)
			}
			if rows != distinct {
				t.Fatalf("%d rows for %d reader keys after OPTIMIZE FINAL -- every reader still scans %d superseded copies", rows, distinct, rows-distinct)
			}
		})

		t.Run(table+"/readers_unchanged", func(t *testing.T) {
			after := capture(t, table)
			for name, want := range before {
				if got := after[name]; got != want {
					t.Errorf("%s changed across the merge\nbefore:\n%s\nafter:\n%s", name, want, got)
				}
			}
		})
	}
}

// Ruling of record: a per-column argMax over a Nullable column returns the
// newest row's NULL once the merge has removed the older non-NULL copy.
// Before the merge it still returns the stale value, because argMax skips
// NULL; the whole-row readers return NULL at both points.
func TestPerColumnArgMaxOverNullableReturnsTheNewestNullOnceMerged(t *testing.T) {
	ctx := context.Background()
	_, _, raw := migratedClickHouse(ctx, t)
	if err := raw.Exec(ctx, "SYSTEM STOP MERGES repo_metrics_daily"); err != nil {
		t.Fatal(err)
	}

	for _, row := range []string{
		"('org-a', toUUID('00000000-0000-4000-8000-000000000001'), toDate('2026-09-01'), 5.0, toDateTime64('2026-09-10 10:00:00', 6, 'UTC'))",
		"('org-a', toUUID('00000000-0000-4000-8000-000000000001'), toDate('2026-09-01'), NULL, toDateTime64('2026-09-10 11:00:00', 6, 'UTC'))",
	} {
		if err := raw.Exec(ctx, "INSERT INTO repo_metrics_daily (org_id, repo_id, day, pr_first_review_p50_hours, computed_at) VALUES "+row); err != nil {
			t.Fatalf("seed repo_metrics_daily: %v", err)
		}
	}

	read := func() (perColumn, wholeRow, limitOneBy *float64) {
		t.Helper()
		if err := raw.QueryRow(ctx, `
            SELECT
                argMax(pr_first_review_p50_hours, computed_at),
                tupleElement(argMax(tuple(pr_first_review_p50_hours), computed_at), 1),
                (SELECT pr_first_review_p50_hours FROM repo_metrics_daily
                 ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day)
            FROM repo_metrics_daily
            GROUP BY org_id, repo_id, day`).Scan(&perColumn, &wholeRow, &limitOneBy); err != nil {
			t.Fatal(err)
		}
		return perColumn, wholeRow, limitOneBy
	}

	perColumn, wholeRow, limitOneBy := read()
	if perColumn == nil || *perColumn != 5.0 {
		t.Fatalf("before the merge the per-column argMax = %v, want the stale 5.0 it skips NULL to reach", perColumn)
	}
	if wholeRow != nil || limitOneBy != nil {
		t.Fatalf("before the merge the whole-row readers = %v / %v, want the newest row's NULL", wholeRow, limitOneBy)
	}

	if err := raw.Exec(ctx, "SYSTEM START MERGES repo_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	if err := raw.Exec(ctx, "OPTIMIZE TABLE repo_metrics_daily FINAL"); err != nil {
		t.Fatal(err)
	}
	perColumn, wholeRow, limitOneBy = read()
	if perColumn != nil || wholeRow != nil || limitOneBy != nil {
		t.Fatalf("after the merge = %v / %v / %v, want NULL from every reader: the older copy must be gone", perColumn, wholeRow, limitOneBy)
	}
}

// dailyFamilySeed writes, for one table, a base key, one key per reader-key
// column that differs from the base only in that column, and a stale key.
// Varying one column at a time is what makes a sorting key that is missing
// any reader column visible: those two rows would merge into one. It returns
// the number of distinct reader keys written.
func dailyFamilySeed(ctx context.Context, t *testing.T, raw stdclickhouse.Conn, table string) int {
	t.Helper()
	readerKey := dailyFamilyReaderKeys[table]
	types := dailyFamilyColumnTypes(ctx, t, raw, table)

	variants := make([]map[string]int, 0, len(readerKey)+1)
	variants = append(variants, map[string]int{})
	for _, column := range readerKey {
		variants = append(variants, map[string]int{column: 1})
	}
	stale := map[string]int{"day": 20}

	insert := func(variant map[string]int, generation string) {
		columns := append(slices.Clone(readerKey), "computed_at")
		values := make([]string, 0, len(columns))
		for _, column := range readerKey {
			values = append(values, dailyFamilyLiteral(t, table, column, types[column], variant[column]))
		}
		values = append(values, fmt.Sprintf("toDateTime64('%s', 6, 'UTC')", generation))
		statement := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table, strings.Join(columns, ", "), strings.Join(values, ", "))
		if err := raw.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %s: %v\n%s", table, err, statement)
		}
	}
	for _, generation := range dailyFamilyGenerations {
		for _, variant := range variants {
			insert(variant, generation)
		}
		if generation == dailyFamilyStaleGeneration {
			insert(stale, generation)
		}
	}
	return len(variants) + 1
}

func dailyFamilyColumnTypes(ctx context.Context, t *testing.T, raw stdclickhouse.Conn, table string) map[string]string {
	t.Helper()
	rows, err := raw.Query(ctx,
		"SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ? "+
			"AND default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')", table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	types := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		types[name] = typ
	}
	if len(types) == 0 {
		t.Fatalf("%s has no columns in the migrated schema", table)
	}
	return types
}

func dailyFamilyLiteral(t *testing.T, table, column, typ string, variant int) string {
	t.Helper()
	base := strings.TrimSuffix(strings.TrimPrefix(typ, "LowCardinality("), ")")
	switch {
	case column == "org_id":
		return []string{"'org-a'", "'org-b'"}[variant]
	case base == "UUID":
		return fmt.Sprintf("toUUID('00000000-0000-4000-8000-%012d')", variant+1)
	case base == "Date":
		return fmt.Sprintf("toDate('2026-09-%02d')", variant+1)
	case strings.HasPrefix(base, "Enum8("):
		return []string{"'repo'", "'team'"}[variant]
	case base == "String":
		return fmt.Sprintf("'%s-%d'", column, variant)
	}
	t.Fatalf("%s.%s: no seed literal for reader-key type %q", table, column, typ)
	return ""
}

// dailyFamilyPerColumnArgMax builds the reader shape most daily readers use:
// one independent argMax(<col>, computed_at) per value column.
func dailyFamilyPerColumnArgMax(ctx context.Context, t *testing.T, raw stdclickhouse.Conn, table string) string {
	t.Helper()
	readerKey := dailyFamilyReaderKeys[table]
	types := dailyFamilyColumnTypes(ctx, t, raw, table)
	names := make([]string, 0, len(types))
	for name := range types {
		if !slices.Contains(readerKey, name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	projections := make([]string, 0, len(names))
	for _, name := range names {
		projections = append(projections, fmt.Sprintf("argMax(`%s`, computed_at)", name))
	}
	key := strings.Join(readerKey, ", ")
	return fmt.Sprintf("SELECT %s, %s FROM %s GROUP BY %s ORDER BY ALL", key, strings.Join(projections, ", "), table, key)
}

// dailyFamilyPythonDedupSources asks the real clickhouse_dedup.dedup_from for
// each table's FROM source, so the comparison runs Python's own reader SQL
// rather than a copy of it.
func dailyFamilyPythonDedupSources(ctx context.Context, t *testing.T) map[string]string {
	t.Helper()
	python, rule, err := chschema.Interpreter()
	if err != nil {
		t.Fatalf("resolve python: %v", err)
	}
	t.Logf("pyoracle: resolved interpreter %s (%s)", python, rule)
	_, file, _, _ := runtime.Caller(0)
	source := filepath.Join(filepath.Dir(file), "..", "..", "src")
	tables, err := json.Marshal(dailyFamilyTableNames())
	if err != nil {
		t.Fatal(err)
	}
	script := `
import json, sys
from dev_health_ops.clickhouse_dedup import dedup_from
tables = json.loads(sys.argv[1])
print(json.dumps({t: dedup_from(t) for t in tables if dedup_from(t) != t}))
`
	// The interpreter path comes from chschema.Interpreter (test support, a
	// developer-set override or the checked-out venv), never request data.
	// nosemgrep: go.lang.security.audit.dangerous-exec-command.dangerous-exec-command
	command := exec.CommandContext(ctx, python, "-c", script, string(tables))
	command.Env = append(os.Environ(), "PYTHONPATH="+source)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("python dedup_from: %v", err)
	}
	sources := map[string]string{}
	if err := json.Unmarshal(output, &sources); err != nil {
		t.Fatalf("decode dedup_from output %q: %v", output, err)
	}
	if len(sources) == 0 {
		t.Fatal("dedup_from registered none of the daily-family tables -- the Python reader comparison would be vacuous")
	}
	return sources
}

// dailyFamilyHTTPQuery returns the raw TSV body, so two reads compare byte for
// byte exactly as the API comparison does.
func dailyFamilyHTTPQuery(ctx context.Context, t *testing.T, dsn, query string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	scheme := "http"
	if port := parsed.Port(); port == "443" || port == "8443" {
		scheme = "https"
	}
	endpoint := url.URL{
		Scheme:   scheme,
		Host:     parsed.Host,
		RawQuery: url.Values{"database": {strings.TrimPrefix(parsed.Path, "/")}, "default_format": {"TSV"}}.Encode(),
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(query))
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	request.SetBasicAuth(parsed.User.Username(), password)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("clickhouse HTTP %d for %s: %s", response.StatusCode, query, body)
	}
	return string(body)
}

func dailyFamilyJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
