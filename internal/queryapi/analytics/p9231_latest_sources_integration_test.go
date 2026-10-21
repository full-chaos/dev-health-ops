//go:build integration

package analytics

// CHAOS-9231: the latest-row sources (LatestWorkUnitInvestmentsSource, LatestWorkUnitRepoEffortSource) are the shared
// reads of every investment statement. Whatever the measurement picks to make them cheaper, they must answer exactly as
// the frozen reference SQL did (testdata/p9231_reference_*.sql, the text main had before CHAOS-9231) on the cases the
// argMax history is made of: several generations, a newest generation that clears nullable columns (CHAOS-4547), a
// same-timestamp tie (CHAOS-4759), a superseded unit, another org, and in the repository rows a repository that only an
// older generation had.

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const p9231RepoEffortDDL = `
CREATE TABLE work_unit_repo_effort (
    work_unit_id String,
    repo_id Nullable(UUID),
    effort_metric String,
    effort_value Float64,
    allocation_weight Float64,
    allocation_source String,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC'),
    org_id String DEFAULT '',
    repo_source Nullable(String)
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(computed_at)
ORDER BY (org_id, work_unit_id, ifNull(toString(repo_id), ''))
`

const (
	p9231Org      = "p9231-org"
	p9231OtherOrg = "p9231-other-org"
	p9231RepoA    = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	p9231RepoB    = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	p9231RepoC    = "cccccccc-cccc-cccc-cccc-cccccccccccc"
)

type p9231UnitRow struct {
	org, unit, computedAt      string
	repo, provider, kind, name *string
	effort                     float64
}

func p9231Q(v *string) string {
	if v == nil {
		return "NULL"
	}
	return "'" + *v + "'"
}

func p9231QUUID(v *string) string {
	if v == nil {
		return "NULL"
	}
	return "toUUID('" + *v + "')"
}

// one INSERT per row: rows of one block with the same sort key collapse at insert time (optimize_on_insert), and
// production writes each generation in its own INSERT.
func p9231SeedUnits(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, rows []p9231UnitRow) {
	t.Helper()
	for _, r := range rows {
		insert := fmt.Sprintf(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value,
 theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band,
 categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id,
 computed_at, work_unit_type, work_unit_name, org_id) VALUES ('%s', toDateTime64('2026-01-01 00:00:00',3), toDateTime64('2026-01-05 00:00:00',3),
 %s, %s, 'fte_days', %g, map('feature', 1.0), map('feature.delivery', 1.0), '{"issues":["%s"]}', 0.5, 'moderate', 'ok', '', 'v1', 'h', 'run-%s',
 toDateTime64('%s',3), %s, %s, '%s')`,
			r.unit, p9231QUUID(r.repo), p9231Q(r.provider), r.effort, r.unit, r.computedAt, r.computedAt, p9231Q(r.kind), p9231Q(r.name), r.org)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed work_unit_investments %s@%s: %v", r.unit, r.computedAt, err)
		}
	}
}

type p9231EffortRow struct {
	org, unit, computedAt string
	repo                  *string
	effort                float64
	source                string
	// repoSource is the repo_source column: nil seeds "hierarchy" (the rows that do not care), the text "NULL" seeds SQL NULL,
	// any other text seeds itself.
	repoSource *string
}

func p9231RepoSourceSQL(r p9231EffortRow) string {
	if r.repoSource == nil {
		return "'hierarchy'"
	}
	if *r.repoSource == "NULL" {
		return "NULL"
	}
	return "'" + *r.repoSource + "'"
}

func p9231SeedEffort(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, rows []p9231EffortRow) {
	t.Helper()
	for _, r := range rows {
		insert := fmt.Sprintf(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source,
 categorization_run_id, computed_at, org_id, repo_source) VALUES ('%s', %s, 'fte_days', %g, 1.0, '%s', 'run', toDateTime64('%s',3), '%s', %s)`,
			r.unit, p9231QUUID(r.repo), r.effort, r.source, r.computedAt, r.org, p9231RepoSourceSQL(r))
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed work_unit_repo_effort %s@%s: %v", r.unit, r.computedAt, err)
		}
	}
}

// p9231Rows runs `SELECT <key>, toString(tuple(<cols>)) FROM (<source>) ORDER BY <key>` and returns the lines.
func p9231Rows(t *testing.T, ctx context.Context, client QueryClient, source, cols, order string) []string {
	t.Helper()
	statement := fmt.Sprintf("SELECT toString(tuple(%s)) AS row FROM %s AS s WHERE org_id = {org_id:String} ORDER BY %s SETTINGS max_threads = 1", cols, source, order)
	scanner, err := client.Query(ctx, statement, []dhclickhouse.Binding{{Name: "org_id", Value: p9231Org}})
	if err != nil {
		t.Fatalf("query: %v\n%s", err, statement)
	}
	defer scanner.Close()
	var out []string
	for scanner.Next() {
		var row string
		if err := scanner.Scan(&row); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func TestLatestRowSourcesEqualTheFrozenReference_SeededRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + p9231RepoEffortDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI + "?max_threads=1"})
	if err != nil {
		t.Fatalf("query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	repoA, repoB, repoC := p9231RepoA, p9231RepoB, p9231RepoC
	s := func(v string) *string { return &v }
	p9231SeedUnits(t, ctx, conn, []p9231UnitRow{
		// three generations, the newest wins
		{org: p9231Org, unit: "wu-multi", computedAt: "2026-02-01 00:00:00", repo: &repoA, provider: s("github"), kind: s("feature"), name: s("old"), effort: 1},
		{org: p9231Org, unit: "wu-multi", computedAt: "2026-02-02 00:00:00", repo: &repoA, provider: s("github"), kind: s("feature"), name: s("mid"), effort: 2},
		{org: p9231Org, unit: "wu-multi", computedAt: "2026-02-03 00:00:00", repo: &repoB, provider: s("gitlab"), kind: s("bug"), name: s("new"), effort: 3},
		// CHAOS-4547: the newest generation clears the nullable columns; the older value must NOT come back
		{org: p9231Org, unit: "wu-null", computedAt: "2026-02-01 00:00:00", repo: &repoA, provider: s("github"), kind: s("feature"), name: s("had values"), effort: 1},
		{org: p9231Org, unit: "wu-null", computedAt: "2026-02-02 00:00:00", repo: nil, provider: nil, kind: nil, name: nil, effort: 1},
		// CHAOS-4759: same timestamp, NULL row first, then the non-NULL row (and the reverse for a second unit)
		{org: p9231Org, unit: "wu-tie-null-first", computedAt: "2026-02-01 00:00:00", repo: nil, provider: nil, kind: nil, name: nil, effort: 1},
		{org: p9231Org, unit: "wu-tie-null-first", computedAt: "2026-02-01 00:00:00", repo: &repoC, provider: s("github"), kind: s("feature"), name: s("tie"), effort: 1},
		{org: p9231Org, unit: "wu-tie-value-first", computedAt: "2026-02-01 00:00:00", repo: &repoC, provider: s("github"), kind: s("feature"), name: s("tie"), effort: 1},
		{org: p9231Org, unit: "wu-tie-value-first", computedAt: "2026-02-01 00:00:00", repo: nil, provider: nil, kind: nil, name: nil, effort: 1},
		// a unit with one generation; a superseded unit; the same ids in another org
		{org: p9231Org, unit: "wu-single", computedAt: "2026-02-01 00:00:00", repo: &repoA, provider: s("github"), kind: s("feature"), name: s("single"), effort: 1},
		{org: p9231Org, unit: "wu-superseded", computedAt: "2026-02-01 00:00:00", repo: &repoA, provider: s("github"), kind: s("feature"), name: s("gone"), effort: 1},
		{org: p9231OtherOrg, unit: "wu-multi", computedAt: "2026-03-01 00:00:00", repo: &repoC, provider: s("github"), kind: s("feature"), name: s("other org"), effort: 9},
	})
	if err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO work_unit_supersessions (org_id, superseded_work_unit_id, superseded_by_run_id, superseded_at) VALUES ('%s', 'wu-superseded', 'run-x', now())", p9231Org)); err != nil {
		t.Fatalf("seed supersession: %v", err)
	}
	p9231SeedEffort(t, ctx, conn, []p9231EffortRow{
		// a repository only the older generation had must be gone
		{org: p9231Org, unit: "wu-gen", computedAt: "2026-02-01 00:00:00", repo: &repoA, effort: 2, source: "evidence"},
		{org: p9231Org, unit: "wu-gen", computedAt: "2026-02-01 00:00:00", repo: &repoB, effort: 3, source: "evidence"},
		{org: p9231Org, unit: "wu-gen", computedAt: "2026-02-02 00:00:00", repo: &repoA, effort: 5, source: "evidence"},
		// effort sum 0: weight 1/n
		{org: p9231Org, unit: "wu-zero", computedAt: "2026-02-01 00:00:00", repo: &repoA, effort: 0, source: "evidence"},
		{org: p9231Org, unit: "wu-zero", computedAt: "2026-02-01 00:00:00", repo: &repoB, effort: 0, source: "evidence"},
		{org: p9231Org, unit: "wu-one", computedAt: "2026-02-01 00:00:00", repo: &repoC, effort: 4, source: "evidence"},
		{org: p9231Org, unit: "wu-norepo", computedAt: "2026-02-01 00:00:00", repo: nil, effort: 1, source: "scalar_fallback"},
		{org: p9231OtherOrg, unit: "wu-gen", computedAt: "2026-03-01 00:00:00", repo: &repoC, effort: 7, source: "evidence"},
		// CHAOS-4547 on repo_source (Nullable): an older generation names a provenance, the newer one has none; the
		// newest NULL must win (the tuple-wrapped argMax), the older value must not come back (a bare argMax skips NULL)
		{org: p9231Org, unit: "wu-src", computedAt: "2026-02-01 00:00:00", repo: &repoA, effort: 2, source: "evidence", repoSource: s("team_fallback")},
		{org: p9231Org, unit: "wu-src", computedAt: "2026-02-02 00:00:00", repo: &repoA, effort: 2, source: "evidence", repoSource: s("NULL")},
	})

	refUnits, err := os.ReadFile("testdata/p9231_reference_latest_work_unit_investments_source.sql")
	if err != nil {
		t.Fatal(err)
	}
	refEffort, err := os.ReadFile("testdata/p9231_reference_latest_work_unit_repo_effort_source.sql")
	if err != nil {
		t.Fatal(err)
	}

	unitCols := "work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, " +
		"subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, " +
		"categorization_model_version, categorization_run_id, org_id, latest_computed_at"
	wantUnits := p9231Rows(t, ctx, client, string(refUnits), unitCols, "work_unit_id")
	gotUnits := p9231Rows(t, ctx, client, LatestWorkUnitInvestmentsSource(), unitCols, "work_unit_id")
	if len(wantUnits) != 5 {
		t.Fatalf("the reference answered %d unit rows, want 5 (multi, null, two ties, single; none for the superseded unit and the other org): the seed does not exercise the cases\n%v", len(wantUnits), wantUnits)
	}
	if !reflect.DeepEqual(wantUnits, gotUnits) {
		t.Fatalf("LatestWorkUnitInvestmentsSource answers differently from the frozen reference.\nwant %v\n got %v", wantUnits, gotUnits)
	}

	effortCols := "work_unit_id, repo_id, effort_metric, repo_effort_value, allocation_source, repo_source, org_id, latest_repo_effort_computed_at, has_allocation"
	wantEffort := p9231Rows(t, ctx, client, string(refEffort), effortCols, "work_unit_id, ifNull(toString(repo_id), '')")
	gotEffort := p9231Rows(t, ctx, client, LatestWorkUnitRepoEffortSource(), effortCols, "work_unit_id, ifNull(toString(repo_id), '')")
	if len(wantEffort) != 6 {
		t.Fatalf("the reference answered %d repository rows, want 6 (gen: A only; zero: A and B; one; norepo; src; none for the other org)\n%v", len(wantEffort), wantEffort)
	}
	foundNullSource := false
	for _, row := range wantEffort {
		if strings.Contains(row, "wu-src") && strings.Contains(row, "NULL") && !strings.Contains(row, "team_fallback") {
			foundNullSource = true
		}
	}
	if !foundNullSource {
		t.Fatalf("the reference row of wu-src does not carry the newest NULL repo_source: the seed does not exercise the tuple-wrapped argMax\n%v", wantEffort)
	}
	if !reflect.DeepEqual(wantEffort, gotEffort) {
		t.Fatalf("LatestWorkUnitRepoEffortSource answers differently from the frozen reference.\nwant %v\n got %v", wantEffort, gotEffort)
	}

	// the unit's repository-row count and effort sum, taken from either form of the source
	totals := func(source string) []string {
		statement := fmt.Sprintf("SELECT toString(tuple(work_unit_id, count(), sum(repo_effort_value))) FROM %s AS s WHERE org_id = {org_id:String} GROUP BY org_id, work_unit_id ORDER BY work_unit_id", source)
		scanner, err := client.Query(ctx, statement, []dhclickhouse.Binding{{Name: "org_id", Value: p9231Org}})
		if err != nil {
			t.Fatal(err)
		}
		defer scanner.Close()
		var out []string
		for scanner.Next() {
			var row string
			if err := scanner.Scan(&row); err != nil {
				t.Fatal(err)
			}
			out = append(out, row)
		}
		return out
	}
	wantTotals, gotTotals := totals(string(refEffort)), totals(LatestWorkUnitRepoEffortSource())
	if !sort.StringsAreSorted(wantTotals) || !reflect.DeepEqual(wantTotals, gotTotals) {
		t.Fatalf("totals differ: want %v got %v", wantTotals, gotTotals)
	}

	// the totals the weighted source carries beside every row are the unit's count and effort sum
	windowStatement := fmt.Sprintf("SELECT DISTINCT toString(tuple(work_unit_id, repo_row_count, repo_effort_sum)) AS row FROM %s AS s WHERE org_id = {org_id:String} ORDER BY row", latestWorkUnitRepoEffortWithTotalsSource())
	scanner, err := client.Query(ctx, windowStatement, []dhclickhouse.Binding{{Name: "org_id", Value: p9231Org}})
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	var gotWindow []string
	for scanner.Next() {
		var row string
		if err := scanner.Scan(&row); err != nil {
			t.Fatal(err)
		}
		gotWindow = append(gotWindow, row)
	}
	if !reflect.DeepEqual(wantTotals, gotWindow) {
		t.Fatalf("window totals of the weighted source differ from the unit totals of the reference: want %v got %v", wantTotals, gotWindow)
	}
}
