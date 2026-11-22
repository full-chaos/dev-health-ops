//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The daily job reads the newest generation of a repository and sums its rows
// across team ids. Two versions of ONE key (day, team, repo) that share
// computed_at are one key: the job reads one whole version of it, the version
// the served page picks (teamkeytables.TeamMetricsDailyNewestOrder), whatever
// the insert order.

const (
	tieJobOrg  = "org-14446"
	tieJobTeam = "team-14446"
	tieJobDay  = "2026-08-28"
	tieJobAt   = "2026-08-28 18:00:00"
)

var tieJobRepo = uuid.MustParse("6f1d3a0e-0000-4000-8000-000000014446")

type tieJobVersion struct {
	team                         string
	commits, afterHours, weekend float64
	at                           string
}

type tieJobEnv struct {
	ctx  context.Context
	conn driver.Conn
}

func startTieJobEnv(t *testing.T) *tieJobEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	env := &tieJobEnv{ctx: ctx, conn: conn}
	env.exec(t, "SYSTEM STOP MERGES team_metrics_daily")
	return env
}

func (env *tieJobEnv) exec(t *testing.T, statement string) {
	t.Helper()
	if err := env.conn.Exec(env.ctx, statement); err != nil {
		t.Fatalf("exec: %v\n%s", err, statement)
	}
}

// seed replaces the table with the rows, one INSERT each, in order.
func (env *tieJobEnv) seed(t *testing.T, rows ...tieJobVersion) {
	t.Helper()
	env.exec(t, "TRUNCATE TABLE team_metrics_daily")
	for _, v := range rows {
		ratio := func(n float64) float64 {
			if v.commits == 0 {
				return 0
			}
			return n / v.commits
		}
		env.exec(t, fmt.Sprintf(`INSERT INTO team_metrics_daily
			(day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
			 after_hours_commit_ratio, weekend_commit_ratio, computed_at, org_id, repo_id)
			VALUES ('%s', '%s', '%s', %g, %g, %g, %g, %g, '%s', '%s', '%s')`,
			tieJobDay, v.team, v.team, v.commits, v.afterHours, v.weekend, ratio(v.afterHours), ratio(v.weekend), v.at, tieJobOrg, tieJobRepo))
	}
}

func (env *tieJobEnv) read(t *testing.T) string {
	t.Helper()
	day := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	rows, err := loadTeamMetricsCognitiveLoadInputsForDay(env.ctx, env.conn, tieJobOrg, day)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%+v", rows)
}

// pagePick is the commit count of the version the served page's order picks
// for the key.
func (env *tieJobEnv) pagePick(t *testing.T) float64 {
	t.Helper()
	rows, err := env.conn.Query(env.ctx, `SELECT argMax(`+teamkeytables.TeamMetricsDailyPick+`, `+teamkeytables.TeamMetricsDailyNewestOrder+`).1
		FROM team_metrics_daily WHERE org_id = ? GROUP BY day, team_id, repo_id`, tieJobOrg)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var picks []float64
	for rows.Next() {
		var commits float64
		if err := rows.Scan(&commits); err != nil {
			t.Fatal(err)
		}
		picks = append(picks, commits)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(picks) != 1 {
		t.Fatalf("page pick: want one key, got %v", picks)
	}
	return picks[0]
}

// J1: a tie of two versions of one key reads one whole version, the same in both
// orders and the same as the page's pick; each version wins at least once.
func TestTeamMetricsCognitiveLoadInputsTieReadsOneWholeVersion(t *testing.T) {
	env := startTieJobEnv(t)
	wins := [2]int{}
	for k := 0; k < 12; k++ {
		versions := [2]tieJobVersion{
			{tieJobTeam, float64(10 + k), 1, 2, tieJobAt},
			{tieJobTeam, float64(20 + k), 15, 3, tieJobAt},
		}
		var alone [2]string
		for i, version := range versions {
			env.seed(t, version)
			alone[i] = env.read(t)
		}
		served := -2
		for order := 0; order < 2; order++ {
			rows := []tieJobVersion{versions[0], versions[1]}
			if order == 1 {
				rows[0], rows[1] = rows[1], rows[0]
			}
			env.seed(t, rows...)
			got := env.read(t)
			w := -1
			for i := range alone {
				if got == alone[i] {
					w = i
				}
			}
			if w < 0 {
				t.Errorf("k=%d order=%d: the job reads %s, neither whole version (%s, %s)", k, order, got, alone[0], alone[1])
				continue
			}
			if pick := env.pagePick(t); pick != versions[w].commits {
				t.Errorf("k=%d order=%d: the job reads the version of %g commits, the page picks %g", k, order, versions[w].commits, pick)
			}
			if served == -2 {
				served = w
			} else if served != w {
				t.Errorf("k=%d: the job read depends on the insert order: version %d vs %d", k, served+1, w+1)
			}
		}
		if served >= 0 {
			wins[served]++
		}
	}
	t.Logf("version 1 read %d times, version 2 %d times", wins[0], wins[1])
	if wins[0] == 0 || wins[1] == 0 {
		t.Fatalf("fixture: each version must win at least once to tell the orders apart: %v", wins)
	}
}

// J2: control. Rows of two different team ids of the repository in its newest
// generation are summed (shares of one repository), and a row of an older
// generation under another team id is left out.
func TestTeamMetricsCognitiveLoadInputsSumTeamIDsOfTheNewestGeneration(t *testing.T) {
	env := startTieJobEnv(t)
	a := tieJobVersion{tieJobTeam, 10, 1, 2, tieJobAt}
	b := tieJobVersion{"team-14446-b", 30, 9, 4, tieJobAt}
	stale := tieJobVersion{"team-14446-old", 50, 50, 50, "2026-08-28 06:00:00"}
	want := fmt.Sprintf("[{RepoID:%s AfterHoursCommitsCount:10 WeekendCommitsCount:6 CommitsCount:40}]", tieJobRepo)
	for _, rows := range [][]tieJobVersion{{a, b}, {b, a}, {a, b, stale}, {stale, b, a}} {
		env.seed(t, rows...)
		if got := env.read(t); got != want {
			t.Errorf("rows %v: got %s, want %s", rows, got, want)
		}
	}
}
