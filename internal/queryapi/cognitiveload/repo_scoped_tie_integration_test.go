//go:build integration

package cognitiveload

import (
	"fmt"
	"strings"
	"testing"
)

// The repository form of the page (fetchRepoScopedTeamMetrics) sums the rows of
// the repository's newest generation of a day across team ids. Two versions of
// ONE key (day, team, repo) that share computed_at are one key, not two team
// ids: the read takes one whole version of the key, the version the team form
// of the page (fetchTeamMetrics) takes, whatever the insert order.

const tieRepo = "6f1d3a0e-0000-4000-8000-000000014446"

func (env *tieEnv) seedRepo(t *testing.T, rows ...tieVersion) {
	t.Helper()
	env.exec(t, "TRUNCATE TABLE team_metrics_daily")
	env.exec(t, "TRUNCATE TABLE repos")
	env.exec(t, fmt.Sprintf(`INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES ('%s', 'example/repo-14446', '%s', 'github', now())`, tieRepo, tieOrg))
	for _, row := range rows {
		env.exec(t, strings.Replace(row.insert(), "'repo-1')", "'"+tieRepo+"')", 1))
	}
}

func (env *tieEnv) readRepo(t *testing.T) string {
	t.Helper()
	got, err := fetchRepoScopedTeamMetrics(env.ctx, env.client, tieOrg, "2026-08-01", "2026-08-31", tieRepo)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%+v", got)
}

func (env *tieEnv) readTeam(t *testing.T, team string) string {
	t.Helper()
	got, err := fetchTeamMetrics(env.ctx, env.client, tieOrg, "2026-08-01", "2026-08-31", &team)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%+v", got)
}

// winner is the index of the version whose read alone equals served, or -1.
func winner(served string, alone [2]string) int {
	for i := range alone {
		if served == alone[i] {
			return i
		}
	}
	return -1
}

// repoTieCase runs one tie of two versions of one key in both insert orders and
// checks the repository read serves one whole version, the same in both orders,
// and the same version as the team form. It returns the index of that version.
func repoTieCase(t *testing.T, env *tieEnv, k int, versions [2]tieVersion) int {
	t.Helper()
	var aloneRepo, aloneTeam [2]string
	for i, version := range versions {
		env.seedRepo(t, version)
		aloneRepo[i] = env.readRepo(t)
		aloneTeam[i] = env.readTeam(t, tieTeam)
	}
	if aloneRepo[0] == aloneRepo[1] || aloneTeam[0] == aloneTeam[1] {
		t.Fatalf("k=%d fixture: the two versions read the same alone: repo %v, team %v", k, aloneRepo, aloneTeam)
	}
	served := -2
	for order := 0; order < 2; order++ {
		rows := []tieVersion{versions[0], versions[1]}
		if order == 1 {
			rows[0], rows[1] = rows[1], rows[0]
		}
		env.seedRepo(t, rows...)
		repoRead := env.readRepo(t)
		repoWinner := winner(repoRead, aloneRepo)
		teamWinner := winner(env.readTeam(t, tieTeam), aloneTeam)
		if repoWinner < 0 {
			t.Errorf("k=%d order=%d: the repository read serves %s, neither whole version (%s, %s)", k, order, repoRead, aloneRepo[0], aloneRepo[1])
			continue
		}
		if repoWinner != teamWinner {
			t.Errorf("k=%d order=%d: the repository read serves version %d, the team form version %d", k, order, repoWinner+1, teamWinner+1)
		}
		if served == -2 {
			served = repoWinner
		} else if served != repoWinner {
			t.Errorf("k=%d: the repository read depends on the insert order: version %d vs %d", k, served+1, repoWinner+1)
		}
	}
	return served
}

// R1: a tie of two measurements of one key.
func TestRepoScopedTeamMetricsTieServesOneWholeVersion(t *testing.T) {
	env := startTieEnv(t)
	wins := [2]int{}
	for k := 0; k < 12; k++ {
		if w := repoTieCase(t, env, k, [2]tieVersion{tieMeasured(k, tieAt), tieOther(k, tieAt)}); w >= 0 {
			wins[w]++
		}
	}
	t.Logf("version 1 served %d times, version 2 %d times", wins[0], wins[1])
	if wins[0] == 0 || wins[1] == 0 {
		t.Fatalf("fixture: each version must win at least once to tell the orders apart: %v", wins)
	}
}

// R2: a measurement and a retraction row of one key tie. The day is kept or
// dropped by the liveness of the one picked version, the version the team form
// takes.
func TestRepoScopedTeamMetricsTieWithRetractionRow(t *testing.T) {
	env := startTieEnv(t)
	wins := [2]int{}
	for k := 0; k < 12; k++ {
		if w := repoTieCase(t, env, k, [2]tieVersion{tieMeasured(k, tieAt), tieRetraction(tieAt)}); w >= 0 {
			wins[w]++
		}
	}
	t.Logf("measurement served %d times, retraction %d times", wins[0], wins[1])
	if wins[0] == 0 || wins[1] == 0 {
		t.Fatalf("fixture: each version must win at least once: %v", wins)
	}
}

// R3: control. Rows of two different team ids of the repository in its newest
// generation are summed (shares of one repository), and a row of an older
// generation under another team id is left out.
func TestRepoScopedTeamMetricsSumsTeamIDsOfTheNewestGeneration(t *testing.T) {
	env := startTieEnv(t)
	const older = "2026-09-01 02:00:00"
	a := tieVersion{tieTeam, 10, 1, 2, 0.1, 0.2, tieAt}
	b := tieVersion{"team-14446-b", 30, 9, 4, 0.3, 0.13, tieAt}
	stale := tieVersion{"team-14446-old", 50, 50, 50, 1, 1, older}
	want := "[{day:{wall:0 ext:63921916800 loc:<nil>} afterHoursCommitRatio:0.25 weekendCommitRatio:0.15}]"
	for _, rows := range [][]tieVersion{{a, b}, {b, a}, {a, b, stale}, {stale, b, a}} {
		env.seedRepo(t, rows...)
		if got := env.readRepo(t); got != want {
			t.Errorf("rows %v: got %s, want %s (10+30 commits, 1+9 after hours, 2+4 weekend)", rows, got, want)
		}
	}
}
