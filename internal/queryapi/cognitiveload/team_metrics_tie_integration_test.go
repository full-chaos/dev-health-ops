//go:build integration

package cognitiveload

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Two versions of one (team, repo, day) of team_metrics_daily that share
// computed_at must serve ONE whole version, whatever the insert order.

const (
	tieOrg  = "org-14426"
	tieTeam = "team-14426"
	tieDay  = "2026-08-10"
)

type tieEnv struct {
	ctx    context.Context
	client *dhclickhouse.Client
	conn   interface {
		Exec(ctx context.Context, query string, args ...any) error
	}
}

func startTieEnv(t *testing.T) *tieEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	env := &tieEnv{ctx: ctx, client: client, conn: conn}
	env.exec(t, "SYSTEM STOP MERGES team_metrics_daily")
	return env
}

func (env *tieEnv) exec(t *testing.T, statement string) {
	t.Helper()
	if err := env.conn.Exec(env.ctx, statement); err != nil {
		t.Fatalf("exec: %v\n%s", err, statement)
	}
}

type tieVersion struct {
	team                          string
	commits, afterHours, weekend  int
	afterHoursRatio, weekendRatio float64
	at                            string
}

func (v tieVersion) insert() string {
	return fmt.Sprintf(`INSERT INTO team_metrics_daily
		(day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
		 after_hours_commit_ratio, weekend_commit_ratio, computed_at, org_id, repo_id)
		VALUES ('%s', '%s', '%s', %d, %d, %d, %g, %g, '%s', '%s', 'repo-1')`,
		tieDay, v.team, v.team, v.commits, v.afterHours, v.weekend, v.afterHoursRatio, v.weekendRatio, v.at, tieOrg)
}

// tieMeasured and tieOther are two versions that differ in every measure.
func tieMeasured(k int, at string) tieVersion {
	return tieVersion{tieTeam, 10 + k, 1, 2, 0.1, 0.2, at}
}

func tieOther(k int, at string) tieVersion {
	return tieVersion{tieTeam, 20 + k, 15, 3, 0.75, 0.15, at}
}

func tieRetraction(at string) tieVersion {
	return tieVersion{tieTeam, 0, 0, 0, 0, 0, at}
}

// seed replaces the table with the rows, one INSERT each, in order.
func (env *tieEnv) seed(t *testing.T, rows ...tieVersion) {
	t.Helper()
	env.exec(t, "TRUNCATE TABLE team_metrics_daily")
	for _, row := range rows {
		env.exec(t, row.insert())
	}
}

// read is the served read of the window of the day, org-wide.
func (env *tieEnv) read(t *testing.T) string {
	t.Helper()
	got, err := fetchTeamMetrics(env.ctx, env.client, tieOrg, "2026-08-01", "2026-08-31", nil)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%+v", got)
}

const tieAt = "2026-09-01 03:00:00"

// C1: on a tie the served after-hours and weekend ratio come from one whole
// version, the same in both insert orders; each version wins at least once.
func TestTeamMetricsTieServesOneWholeVersion(t *testing.T) {
	env := startTieEnv(t)
	wins := [2]int{}
	for k := 0; k < 12; k++ {
		versions := [2]tieVersion{tieMeasured(k, tieAt), tieOther(k, tieAt)}
		var alone [2]string
		for i, version := range versions {
			env.seed(t, version)
			alone[i] = env.read(t)
		}
		var served [2]string
		for order := 0; order < 2; order++ {
			rows := []tieVersion{versions[0], versions[1]}
			if order == 1 {
				rows[0], rows[1] = rows[1], rows[0]
			}
			env.seed(t, rows...)
			served[order] = env.read(t)
			if served[order] != alone[0] && served[order] != alone[1] {
				t.Errorf("k=%d order=%d: served %s is neither whole version (%s, %s)", k, order, served[order], alone[0], alone[1])
			}
		}
		if served[0] != served[1] {
			t.Errorf("k=%d: the served value depends on the insert order: %s vs %s", k, served[0], served[1])
		}
		if served[0] == alone[0] {
			wins[0]++
		} else if served[0] == alone[1] {
			wins[1]++
		}
	}
	t.Logf("version 1 served %d times, version 2 %d times", wins[0], wins[1])
	if wins[0] == 0 || wins[1] == 0 {
		t.Fatalf("fixture: each version must win at least once to tell the orders apart: %v", wins)
	}
}

// C2: control. A version 1 s newer wins in both orders, also when it is the one
// with the smaller hash: computed_at comes before the hash.
func TestTeamMetricsNewerVersionWinsOverHash(t *testing.T) {
	env := startTieEnv(t)
	const older, newer = "2026-09-01 03:00:00", "2026-09-01 03:00:01"
	for k := 0; k < 12; k++ {
		for assignment := 0; assignment < 2; assignment++ {
			versions := [2]tieVersion{tieMeasured(k, older), tieOther(k, newer)}
			if assignment == 1 {
				versions = [2]tieVersion{tieOther(k, older), tieMeasured(k, newer)}
			}
			env.seed(t, versions[1])
			want := env.read(t)
			for order := 0; order < 2; order++ {
				rows := []tieVersion{versions[0], versions[1]}
				if order == 1 {
					rows[0], rows[1] = rows[1], rows[0]
				}
				env.seed(t, rows...)
				if got := env.read(t); got != want {
					t.Errorf("k=%d assignment=%d order=%d: served %s, the newer version alone reads %s", k, assignment, order, got, want)
				}
			}
		}
	}
}

// C3: a measurement and a retraction row of one key tie. Another team holds the
// day, so the retracted-day correction runs. The page serves the read of the one
// picked version: its counts and its liveness come from the same row.
func TestTeamMetricsTieWithRetractionRow(t *testing.T) {
	env := startTieEnv(t)
	other := tieVersion{"team-14426-b", 30, 3, 0, 0.1, 0, tieAt}
	env.seed(t, other)
	otherOnly := env.read(t)
	wins := [2]int{}
	for k := 0; k < 12; k++ {
		versions := [2]tieVersion{tieMeasured(k, tieAt), tieRetraction(tieAt)}
		var alone [2]string
		for i, version := range versions {
			env.seed(t, version, other)
			alone[i] = env.read(t)
		}
		if alone[1] != otherOnly {
			t.Errorf("k=%d: a retracted team takes part in the day: %s, the other team alone reads %s", k, alone[1], otherOnly)
		}
		var served [2]string
		for order := 0; order < 2; order++ {
			rows := []tieVersion{versions[0], versions[1], other}
			if order == 1 {
				rows[0], rows[1] = rows[1], rows[0]
			}
			env.seed(t, rows...)
			served[order] = env.read(t)
			if served[order] != alone[0] && served[order] != alone[1] {
				t.Errorf("k=%d order=%d: served %s is neither whole version (%s, %s)", k, order, served[order], alone[0], alone[1])
			}
		}
		if served[0] != served[1] {
			t.Errorf("k=%d: the served value depends on the insert order: %s vs %s", k, served[0], served[1])
		}
		if served[0] == alone[0] {
			wins[0]++
		} else if served[0] == alone[1] {
			wins[1]++
		}
	}
	t.Logf("measurement served %d times, retraction %d times", wins[0], wins[1])
	if wins[0] == 0 || wins[1] == 0 {
		t.Fatalf("fixture: each version must win at least once: %v", wins)
	}
}
