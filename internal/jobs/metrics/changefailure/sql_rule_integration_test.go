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
// of "has / has not" over the five counts is one repository, written by the
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
	for mask := 0; mask < 32; mask++ {
		bit := func(position int, value uint64) uint64 {
			if mask&(1<<position) != 0 {
				return value
			}
			return 0
		}
		counts := changefailure.Counts{
			Deployments: bit(0, 4), FailedNative: bit(1, 1), FailedHeuristic: bit(2, 2),
			IncidentsDirect: bit(3, 1), IncidentsViaDeployment: bit(4, 1),
		}
		// One repository holds the counts in one day, a second one holds the
		// same counts split over two days (deployments and failures on the
		// first, incident evidence on the second).
		oneDay, twoDays := uuid.New(), uuid.New()
		want[oneDay], want[twoDays] = counts, counts
		first := changefailure.Counts{Deployments: counts.Deployments, FailedNative: counts.FailedNative, FailedHeuristic: counts.FailedHeuristic}
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
	// 32 combinations twice. 12 of the 32 are measured (a deployment and one
	// or both kinds of incident evidence): the comparison is not only NULL
	// against nil.
	if seen != 64 || measured != 24 {
		t.Fatalf("compared %d repositories, %d measured; want 64 and 24", seen, measured)
	}
}
