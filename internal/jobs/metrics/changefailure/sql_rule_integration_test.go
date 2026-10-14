//go:build integration

package changefailure_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The rule exists twice: in Go (Rate, Evaluate) for a reader that serves the
// rate, and in ClickHouse (WindowRateSQL) for a reader that ranks or draws.
// They must give the same answer for the same stored rows. Every combination
// of "has / has not" over the seven counts is one repository, written by the
// real writer, in one day and split over two days; an older version of each
// day says something else, so the newest-version read is part of the check.
func TestTheSQLRuleAndTheGoRuleAgreeOnStoredRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer, err := repouser.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	const org = "org-sql-rule"
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	older := day.AddDate(0, 0, 3)
	newer := older.Add(time.Hour)
	want := map[uuid.UUID]changefailure.Counts{}
	var stale, current []repouser.ChangeFailureDaily
	for mask := 0; mask < 128; mask++ {
		bit := func(position int, value uint64) uint64 {
			if mask&(1<<position) != 0 {
				return value
			}
			return 0
		}
		counts := changefailure.Counts{
			Deployments: bit(0, 4), FailedNative: bit(1, 1), FailedHeuristic: bit(2, 2),
			IncidentsDirect: bit(3, 1), IncidentsViaDeployment: bit(4, 1),
			FailedStatus: bit(5, 1), StatusKnown: bit(6, 2),
		}
		// One repository holds the counts in one day, a second one holds the
		// same counts split over two days (deployments and failures on the
		// first, incident evidence on the second).
		oneDay, twoDays := uuid.New(), uuid.New()
		want[oneDay], want[twoDays] = counts, counts
		first := changefailure.Counts{Deployments: counts.Deployments, FailedNative: counts.FailedNative, FailedHeuristic: counts.FailedHeuristic, FailedStatus: counts.FailedStatus, StatusKnown: counts.StatusKnown}
		second := changefailure.Counts{IncidentsDirect: counts.IncidentsDirect, IncidentsViaDeployment: counts.IncidentsViaDeployment}
		wrong := changefailure.Counts{Deployments: 9, FailedNative: 9, IncidentsDirect: 9}
		stale = append(stale,
			repouser.ChangeFailureDaily{RepoID: oneDay, Day: day, ComputedAt: older, Counts: wrong},
			repouser.ChangeFailureDaily{RepoID: twoDays, Day: day, ComputedAt: older, Counts: wrong},
		)
		current = append(current,
			repouser.ChangeFailureDaily{RepoID: oneDay, Day: day, ComputedAt: newer, Counts: counts},
			repouser.ChangeFailureDaily{RepoID: twoDays, Day: day, ComputedAt: newer, Counts: first},
			repouser.ChangeFailureDaily{RepoID: twoDays, Day: day.AddDate(0, 0, 1), ComputedAt: newer, Counts: second},
		)
	}
	// The two versions of a key go in two inserts, so both are stored.
	for _, rows := range [][]repouser.ChangeFailureDaily{stale, current} {
		if _, err := writer.WriteChangeFailure(ctx, rows, org); err != nil {
			t.Fatal(err)
		}
	}

	query := `SELECT repo_id, ` + changefailure.WindowRateSQL + ` AS rate, ` + changefailure.ViewSumsSQL + `
FROM ` + changefailure.LatestRowsSQL("start", "end", "") + `
GROUP BY repo_id`
	rows, err := conn.Query(ctx, query,
		clickhouse.Named("org_id", org), clickhouse.Named("start", day.Format("2006-01-02")), clickhouse.Named("end", day.AddDate(0, 0, 2).Format("2006-01-02")))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	show := func(v *float64) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprint(*v)
	}
	seen, measured := 0, 0
	for rows.Next() {
		var repo uuid.UUID
		var sqlRate *float64
		var view changefailure.View
		if err := rows.Scan(append([]any{&repo, &sqlRate}, changefailure.ViewScanDest(&view)...)...); err != nil {
			t.Fatal(err)
		}
		seen++
		counts, ok := want[repo]
		if !ok {
			t.Fatalf("unexpected repository %s", repo)
		}
		if view.Counts != counts || view.StoredRows == 0 {
			t.Errorf("%+v: the view sums read %+v over %d row(s)", counts, view.Counts, view.StoredRows)
		}
		goRate := changefailure.Rate(counts)
		if show(sqlRate) != show(goRate.Value) || show(changefailure.Evaluate(view).Value) != show(goRate.Value) {
			t.Errorf("%+v: WindowRateSQL %s, Rate %s (%s), Evaluate of the view %s", counts, show(sqlRate), show(goRate.Value), goRate.State, show(changefailure.Evaluate(view).Value))
		}
		if goRate.Value != nil {
			measured++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// 128 combinations twice. 56 of the 128 are measured (a deployment, and
	// incident evidence or a deployment with a final status): the comparison is
	// not only NULL against nil.
	if seen != 256 || measured != 112 {
		t.Fatalf("compared %d repositories, %d measured; want 256 and 112", seen, measured)
	}
}

// CHAOS-9621: a window that sums a row written before migration 125 (inserted
// with the five columns that existed then, so the status columns take their
// default 0) and a recomputed row of another day, on a real store. The SQL rule
// and the Go rule agree, by value, and the answer carries D, K and the coverage.
func TestAMixedWindowOfPreAndPostMigrationRowsReadsByValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer, err := repouser.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	const org = "org-mixed-window"
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := day.AddDate(0, 0, 3)
	repoOld, repoMixed, repoMixedIncident := uuid.New(), uuid.New(), uuid.New()
	oldRow := func(repo uuid.UUID, d time.Time, deployments, heuristic, direct int) {
		t.Helper()
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO repo_change_failure_daily
(org_id, repo_id, day, deployments_count, failed_deployments_native, failed_deployments_heuristic, incidents_direct, incidents_via_deployment, computed_at)
VALUES ('%s', '%s', '%s', %d, 0, %d, %d, 0, '%s')`, org, repo, d.Format("2006-01-02"), deployments, heuristic, direct, computedAt.Format("2006-01-02 15:04:05.000"))); err != nil {
			t.Fatal(err)
		}
	}
	oldRow(repoOld, day, 3, 0, 0)
	oldRow(repoMixed, day, 3, 0, 0)
	oldRow(repoMixedIncident, day, 3, 3, 1)
	next := day.AddDate(0, 0, 1)
	if _, err := writer.WriteChangeFailure(ctx, []repouser.ChangeFailureDaily{
		{RepoID: repoMixed, Day: next, ComputedAt: computedAt, Counts: changefailure.Counts{Deployments: 3, StatusKnown: 3, FailedStatus: 1}},
		{RepoID: repoMixedIncident, Day: next, ComputedAt: computedAt, Counts: changefailure.Counts{Deployments: 3, StatusKnown: 3, FailedStatus: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
	}, org); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name     string
		repo     uuid.UUID
		value    *float64
		basis    changefailure.Basis
		reason   changefailure.UnknownReason
		d, k     uint64
		coverage float64
	}{
		{"pre-migration row only: no outcome", repoOld, nil, changefailure.BasisNone, changefailure.ReasonDeploymentsWithoutOutcome, 3, 0, 0},
		{"pre-migration and recomputed rows, no incident: 1 of K = 3", repoMixed, ptrFloat(1.0 / 3.0), changefailure.BasisStatus, changefailure.NotUnknown, 6, 3, 0.5},
		{"pre-migration and recomputed rows with an incident: 5 of D = 6", repoMixedIncident, ptrFloat(5.0 / 6.0), changefailure.BasisBoth, changefailure.NotUnknown, 6, 3, 0.5},
	} {
		query := `SELECT ` + changefailure.WindowRateSQL + `, ` + changefailure.ViewSumsSQL + `
FROM ` + changefailure.LatestRowsSQL("start", "end", "AND repo_id = {repo:UUID}")
		rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", org), clickhouse.Named("repo", tc.repo.String()),
			clickhouse.Named("start", day.Format("2006-01-02")), clickhouse.Named("end", day.AddDate(0, 0, 2).Format("2006-01-02")))
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatalf("%s: no row", tc.name)
		}
		var sqlRate *float64
		var view changefailure.View
		if err := rows.Scan(append([]any{&sqlRate}, changefailure.ViewScanDest(&view)...)...); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		got := changefailure.Evaluate(view)
		near := func(a, b *float64) bool {
			if a == nil || b == nil {
				return a == b
			}
			d := *a - *b
			return d < 1e-12 && d > -1e-12
		}
		if !near(sqlRate, tc.value) || !near(got.Value, tc.value) {
			t.Errorf("%s: SQL rate %v, Go rate %v, want %v", tc.name, sqlRate, got.Value, tc.value)
		}
		if got.Basis != tc.basis || got.UnknownReason != tc.reason || got.Deployments != tc.d || got.StatusKnown != tc.k || got.Coverage == nil || *got.Coverage != tc.coverage {
			t.Errorf("%s: basis %q reason %q D %d K %d coverage %v; want %q %q %d %d %v", tc.name, got.Basis, got.UnknownReason, got.Deployments, got.StatusKnown, got.Coverage, tc.basis, tc.reason, tc.d, tc.k, tc.coverage)
		}
	}
}

func ptrFloat(v float64) *float64 { return &v }
