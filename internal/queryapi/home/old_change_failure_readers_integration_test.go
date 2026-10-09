//go:build integration

package home

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/operatingreview"
)

// oldReadersFixture is testdata/old_change_failure_readers_75ee6099.json: the
// statements of every reader of repo_metrics_daily.change_failure_rate (and
// the report charts of dora_metrics_daily) as they were before CHAOS-8981,
// recorded by running those readers at OpsSHA against the schema of that
// commit, with the result column types and rows they returned.
type oldReadersFixture struct {
	OpsSHA                 string   `json:"ops_sha"`
	ACRSHA                 string   `json:"acr_sha"`
	OldWriterInsertColumns []string `json:"old_writer_insert_columns"`
	Statements             []struct {
		Reader   string `json:"reader"`
		SQL      string `json:"sql"`
		Bindings []struct {
			Name  string `json:"name"`
			Type  string `json:"type"`
			Value any    `json:"value"`
		} `json:"bindings"`
		ColumnTypes []string   `json:"column_types"`
		Rows        [][]string `json:"rows"`
	} `json:"statements"`
}

// Migration 112 only adds (CHAOS-8981): a pod that still runs the readers of
// the commit before it, and a rollback to that commit, must read exactly what
// they read before. This test replays the recorded statements of those readers
// on the migrated schema over the same logical rows, stored three ways:
//
//	legacy        written by the old writer before the migration
//	roll window   written by the old writer after the migration
//	new writer    written by the current writer
//
// and requires the recorded column types and values. It then reads the same
// rows through the current revert-rate readers: no row holds a measured
// revert rate, and they must not take one from the deprecated column, whose
// values (the ported ratio, 0 in production) were never measured.
func TestReadersOfTheCommitBeforeTheMigrationReadTheSameValuesOnTheMigratedSchema(t *testing.T) {
	raw, err := os.ReadFile("testdata/old_change_failure_readers_75ee6099.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture oldReadersFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.OpsSHA != "75ee6099fc3de0a9fcc175c0270393b5de40cffb" || len(fixture.Statements) < 14 || len(fixture.OldWriterInsertColumns) != 30 {
		t.Fatalf("fixture: ops_sha %q, %d statements, %d old insert columns", fixture.OpsSHA, len(fixture.Statements), len(fixture.OldWriterInsertColumns))
	}

	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const org = "org-old-readers"
	legacy := uuid.MustParse("0a000000-0000-4000-8000-000000000001")
	rollWindow := uuid.MustParse("0a000000-0000-4000-8000-000000000002")
	newWriter := uuid.MustParse("0a000000-0000-4000-8000-000000000003")
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 8, 6, 0, 0, 0, time.UTC)

	// The deprecated column keeps its type.
	var columnType string
	if err := admin.QueryRow(ctx, `SELECT type FROM system.columns
		WHERE database = currentDatabase() AND table = 'repo_metrics_daily' AND name = 'change_failure_rate'`).Scan(&columnType); err != nil {
		t.Fatal(err)
	}
	if columnType != "Float64" {
		t.Fatalf("repo_metrics_daily.change_failure_rate is %s, want Float64 as before the migration", columnType)
	}

	// The old writer's insert: exactly its column list, so a column this
	// migration added gets its default, as it does for a pod of that commit.
	oldInsert := func(repo uuid.UUID, day time.Time, prsMerged uint32, legacyRatio float64) {
		t.Helper()
		batch, err := admin.PrepareBatch(ctx, "INSERT INTO repo_metrics_daily ("+strings.Join(fixture.OldWriterInsertColumns, ", ")+")")
		if err != nil {
			t.Fatal(err)
		}
		values := make([]any, 0, len(fixture.OldWriterInsertColumns))
		for _, column := range fixture.OldWriterInsertColumns {
			switch column {
			case "repo_id":
				values = append(values, repo)
			case "day":
				values = append(values, day)
			case "commits_count":
				values = append(values, uint32(1))
			case "prs_merged":
				values = append(values, prsMerged)
			case "change_failure_rate":
				values = append(values, legacyRatio)
			case "computed_at":
				values = append(values, computedAt)
			case "org_id":
				values = append(values, org)
			case "total_loc_touched", "prs_with_first_review", "bus_factor":
				values = append(values, uint32(0))
			case "pr_first_review_p50_hours", "pr_first_review_p90_hours", "pr_review_time_p50_hours", "pr_pickup_time_p50_hours",
				"pr_size_p50_loc", "pr_size_p90_loc", "pr_comments_per_100_loc", "pr_reviews_per_100_loc", "mttr_hours":
				values = append(values, (*float64)(nil))
			default:
				values = append(values, float64(0))
			}
		}
		if err := batch.Append(values...); err != nil {
			t.Fatalf("old insert: %v", err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	doraInsert := func(repo uuid.UUID, day time.Time, name string, value float64) {
		t.Helper()
		if err := admin.Exec(ctx, "INSERT INTO dora_metrics_daily (repo_id, day, metric_name, value, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?)",
			repo, day, name, value, computedAt, org); err != nil {
			t.Fatal(err)
		}
	}
	migrate := func() {
		t.Helper()
		sql, err := os.ReadFile("../../chmigrate/sql/112_change_failure_rate_incident_basis.sql")
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range chmigrate.SplitStatements(string(sql)) {
			if err := admin.Exec(ctx, statement); err != nil {
				t.Fatalf("migration 112: %v", err)
			}
		}
	}

	// 1. Legacy rows, then the migration over them.
	for day := 0; day < 6; day++ {
		at := start.AddDate(0, 0, day)
		oldInsert(legacy, at, 4, 0.25)
		doraInsert(legacy, at, "change_failure_rate", 0.3)
		doraInsert(legacy, at, "deployment_frequency", 4)
	}
	oldInsert(legacy, time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC), 2, 0.5)
	migrate()

	// 2. Roll-window rows: the old writer and the old DORA writer, after the
	// migration.
	for day := 0; day < 6; day++ {
		at := start.AddDate(0, 0, day)
		oldInsert(rollWindow, at, 5, 0.2)
		doraInsert(rollWindow, at, "change_failure_rate", 0.3)
		doraInsert(rollWindow, at, "deployment_frequency", 4)
	}

	// 3. Rows of the current writers: the deprecated column keeps the legacy
	// ratio, the incident-based value (0.9 here) lives in its own column, and
	// the DORA ratio has its new name.
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	incident := 0.9
	var rows []repouser.RepoMetric
	for day := 0; day < 6; day++ {
		at := start.AddDate(0, 0, day)
		row := repouser.RepoMetric{RepoID: newWriter, Day: at, CommitsCount: 1, PRsMerged: 10, ChangeFailureRate: 0.1, ChangeFailureRateIncident: &incident, ComputedAt: computedAt}
		if day == 5 {
			row.PRsMerged, row.ChangeFailureRate = 0, 0
		}
		rows = append(rows, row)
		doraInsert(newWriter, at, "deployment_failure_rate", 0.3)
		doraInsert(newWriter, at, "deployment_frequency", 4)
	}
	if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: rows}, org); err != nil {
		t.Fatal(err)
	}

	// The three ways of storing must really be present, or the replay
	// compares less than it says.
	var legacyRows, rollWindowRows, newWriterRates, withRevertRate, merged uint64
	if err := admin.QueryRow(ctx, `SELECT countIf(repo_id = ?), countIf(repo_id = ?),
		countIf(repo_id = ? AND change_failure_rate_incident = 0.9), countIf(revert_rate IS NOT NULL), countIf(prs_merged > 0 AND change_failure_rate > 0)
		FROM repo_metrics_daily WHERE org_id = ?`, legacy, rollWindow, newWriter, org).Scan(&legacyRows, &rollWindowRows, &newWriterRates, &withRevertRate, &merged); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 7 || rollWindowRows != 6 || newWriterRates != 6 || merged != 18 {
		t.Fatalf("stored shapes: %d legacy rows (want 7), %d roll-window rows (want 6), %d new-writer rows with an incident value (want 6), %d rows with merges and a deprecated ratio above 0 (want 18)", legacyRows, rollWindowRows, newWriterRates, merged)
	}
	// The migration copies nothing into revert_rate and no writer sets it.
	if withRevertRate != 0 {
		t.Fatalf("%d row(s) hold a revert_rate, want none: nothing measured one", withRevertRate)
	}

	for _, statement := range fixture.Statements {
		args := make([]any, 0, len(statement.Bindings))
		for _, binding := range statement.Bindings {
			var value any
			switch binding.Type {
			case "string":
				value = binding.Value.(string)
			case "uint32":
				value = uint32(binding.Value.(float64))
			case "uint64":
				value = uint64(binding.Value.(float64))
			case "int":
				value = int(binding.Value.(float64))
			case "strings":
				items := []string{}
				for _, item := range binding.Value.([]any) {
					items = append(items, item.(string))
				}
				value = items
			case "time":
				parsed, err := time.Parse(time.RFC3339Nano, binding.Value.(string))
				if err != nil {
					t.Fatal(err)
				}
				value = parsed
			default:
				t.Fatalf("%s: binding %s has unknown type %s", statement.Reader, binding.Name, binding.Type)
			}
			args = append(args, stdclickhouse.Named(binding.Name, value))
		}
		result, err := admin.Query(ctx, statement.SQL, args...)
		if err != nil {
			t.Errorf("%s: the statement of %s fails on the migrated schema: %v", statement.Reader, fixture.OpsSHA[:12], err)
			continue
		}
		types := result.ColumnTypes()
		gotTypes := make([]string, len(types))
		for i, columnType := range types {
			gotTypes[i] = columnType.DatabaseTypeName()
		}
		var got [][]string
		for result.Next() {
			dest := make([]any, len(types))
			for i, columnType := range types {
				dest[i] = reflect.New(columnType.ScanType()).Interface()
			}
			if err := result.Scan(dest...); err != nil {
				t.Fatalf("%s: scan: %v", statement.Reader, err)
			}
			row := make([]string, len(dest))
			for i, d := range dest {
				row[i] = formatRecorded(reflect.ValueOf(d).Elem())
			}
			got = append(got, row)
		}
		if err := result.Err(); err != nil {
			t.Fatalf("%s: rows: %v", statement.Reader, err)
		}
		result.Close()
		sort.Slice(got, func(i, j int) bool { return fmt.Sprint(got[i]) < fmt.Sprint(got[j]) })

		if !reflect.DeepEqual(gotTypes, statement.ColumnTypes) {
			t.Errorf("%s: column types %v, recorded %v", statement.Reader, gotTypes, statement.ColumnTypes)
		}
		if len(statement.Rows) == 0 {
			t.Fatalf("%s: the fixture recorded no row: nothing is compared", statement.Reader)
		}
		if len(got) != len(statement.Rows) {
			t.Errorf("%s: %d rows, recorded %d\n got      %v\n recorded %v", statement.Reader, len(got), len(statement.Rows), got, statement.Rows)
			continue
		}
		for i := range got {
			if !sameRecordedRow(got[i], statement.Rows[i], gotTypes) {
				t.Errorf("%s: row %d = %v, recorded %v", statement.Reader, i, got[i], statement.Rows[i])
			}
		}
	}

	// The current revert-rate readers over the same rows: 18 days with merged
	// pull requests and a deprecated ratio above 0, and not one measured
	// revert rate. They must show no data, never a rate built from the
	// deprecated column.
	reader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	explained, err := explain.BuildExplainResponse(ctx, reader, org, explain.Params{
		Metric: "revert_rate", StartDay: start, EndDay: start.AddDate(0, 0, 6),
		CompareStart: start.AddDate(0, 0, -6), CompareEnd: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	if explained.HasData || explained.Value != 0 || len(explained.Contributors) != 0 {
		t.Errorf("/explain revert_rate = %v (has_data %v, %d contributor(s)), want no data: no row holds a measured revert rate",
			explained.Value, explained.HasData, len(explained.Contributors))
	}
	review, err := operatingreview.Resolve(ctx, client, org, nil, graphqldate.New(time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, section := range review.Sections {
		for _, metric := range section.Metrics {
			if metric.Key != "revert_rate" {
				continue
			}
			found = true
			if metric.HasData || metric.Value != 0 {
				t.Errorf("operating review revert_rate = %v (hasData %v), want no data", metric.Value, metric.HasData)
			}
		}
	}
	if !found {
		t.Error("the operating review has no revert_rate metric")
	}
}

func formatRecorded(value reflect.Value) string {
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	switch v := value.Interface().(type) {
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// sameRecordedRow compares one row with its recording: exactly, except that a
// float may differ in its last bits (a sum over rows stored in other parts is
// taken in another order).
func sameRecordedRow(got, recorded, types []string) bool {
	if len(got) != len(recorded) {
		return false
	}
	for i := range got {
		if got[i] == recorded[i] {
			continue
		}
		if !strings.Contains(types[i], "Float") {
			return false
		}
		a, errA := strconv.ParseFloat(got[i], 64)
		b, errB := strconv.ParseFloat(recorded[i], 64)
		if errA != nil || errB != nil || math.Abs(a-b) > 1e-12 {
			return false
		}
	}
	return true
}
