//go:build integration

package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The repository list of a run of the whole organization is the one of its
// dispatch. "In no partition of the run" therefore does not say that a
// repository is gone. At its end such a run reads the repositories of the
// organization again, and in the repository-scoped tables it supersedes a key
// only when its repository is in neither set. A key of a repository the
// organization holds now is never hidden by a run that did not compute it.

var orgDayRepoLate = uuid.MustParse("00000000-0000-4000-8000-0000000007e1")

// failedRepositoryReadConn fails the read of the organization's repositories,
// as a read that is cut off does. Every other statement goes to the server.
type failedRepositoryReadConn struct{ driver.Conn }

func (conn *failedRepositoryReadConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM repos") && strings.Contains(query, "argMax(tuple(repo, settings, provider), last_synced)") {
		return nil, errors.New("the read of the repositories is cut off")
	}
	return conn.Conn.Query(ctx, query, args...)
}

// orgDayRepositoryLines runs the end of a run and returns the rows it wrote
// and the two log lines of the step (nil when a line is not written).
func orgDayRepositoryLines(
	t *testing.T, ctx context.Context, conn driver.Conn, run Run, clock time.Time,
) (written int, retracted, notInRun map[string]any) {
	t.Helper()
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	written = endStaleKeyRun(t, ctx, conn, run, clock)
	slog.SetDefault(previous)
	for _, raw := range strings.Split(logged.String(), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(raw), &entry) != nil {
			continue
		}
		switch entry["msg"] {
		case StaleKeysRetractedLogMessage:
			retracted = entry
		case StaleKeysRepositoryNotInRunLogMessage:
			notInRun = entry
		}
	}
	return written, retracted, notInRun
}

func TestARunOfTheWholeOrganizationLeavesTheKeysOfARepositoryTheOrganizationHoldsNow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := sharedScopeDay
	t0 := day.Add(-72 * time.Hour)
	stored := day.Add(30 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	float := func(query string, args ...any) float64 {
		t.Helper()
		var value float64
		if err := conn.QueryRow(ctx, query, args...).Scan(&value); err != nil {
			t.Fatalf("read: %v\n%s", err, query)
		}
		return value
	}
	const impactHeld = `SELECT toFloat64(sum(prs_total + prs_merged)) FROM ai_impact_metrics_daily FINAL WHERE org_id = ? AND day = ? AND toString(repo_id) = ?`
	const teamHeld = `SELECT toFloat64(sum(commits_count)) FROM team_metrics_daily FINAL WHERE org_id = ? AND day = ? AND repo_id = ?`
	// liveKeys is the keys of one repository whose newest row holds a measure.
	liveKeys := func(table, measure, key, org, repo string) float64 {
		t.Helper()
		return float("SELECT toFloat64(count()) FROM (SELECT "+key+" AS k, argMax("+measure+", computed_at) AS held FROM "+table+
			" WHERE org_id = ? AND day = ? AND toString(repo_id) = ? GROUP BY k) WHERE held > 0", org, day, repo)
	}
	repository := func(org string, id uuid.UUID, name string, synced time.Time) {
		t.Helper()
		exec("insert repo", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)", id, name, org, "github", synced)
	}
	// storedRows gives one repository a stored row of the day in each of the
	// two repository-scoped tables.
	storedRows := func(org string, repo uuid.UUID, at time.Time) {
		t.Helper()
		exec("stored team_metrics row", `INSERT INTO team_metrics_daily
    (org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count, computed_at)
    VALUES (?, ?, 'ENG', 'ENG', ?, 7, 1, 0, ?)`, org, day, repo.String(), at)
		exec("stored ai_impact row", `INSERT INTO ai_impact_metrics_daily
    (org_id, team_id, repo_id, work_type, day, attribution_bucket, prs_total, prs_merged, human_prs, computed_at)
    VALUES (?, 'ENG', ?, 'earlier', ?, 'human', 5, 5, 5, ?)`, org, repo, day, at)
	}
	setUp := func(org string) []RepositoryID {
		t.Helper()
		repository(org, sharedScopeRepoAPI, "acme/api", t0)
		repository(org, sharedScopeRepoWeb, "acme/web", t0)
		seedSharedScopeTeams(t, ctx, conn, org, t0,
			staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
			staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
		)
		seedSharedScopeItems(t, ctx, conn, org)
		discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := discoverer.RepositoryIDs(ctx, org)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot) != 2 {
			t.Fatalf("the discovery at the dispatch lists %v, want the two repositories: the case is not set", snapshot)
		}
		return snapshot
	}
	computeListed := func(run Run, clock time.Time) {
		t.Helper()
		for _, repo := range []uuid.UUID{sharedScopeRepoAPI, sharedScopeRepoWeb} {
			for _, family := range sharedScopeFamilies {
				runSharedScopeFamily(t, ctx, conn, family, run, repo, clock)
			}
		}
	}

	// A repository that the organization gets AFTER the dispatch of the run of
	// the whole organization, and that a run of its own computes for the day
	// before that run ends. The rows of the late repository keep their
	// measure. A repository that the organization does NOT hold (no repos
	// row) and that is in no partition is superseded, as before.
	t.Run("a repository added after the dispatch, computed by its own run", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0001"
		snapshot := setUp(org)
		storedRows(org, orgDayRepoGone, stored)

		exec("insert team", `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`,
			"github:platform", uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:github:platform")), "Team platform",
			[]string{"dev@example.com"}, []string{"acme/late"}, t0, t0, org, "github")
		repository(org, orgDayRepoLate, "acme/late", stored)
		exec("insert ownership", `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, 'github', 'github:platform', ?, 'acme/late', 'exact', 'native', 1, 10, ?, ?)`, org, orgDayRepoLate, t0, t0)
		exec("insert pull request", `INSERT INTO git_pull_requests
    (repo_id, number, title, state, author_name, author_email, created_at, merged_at, additions, deletions, changed_files, last_synced, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			orgDayRepoLate, uint32(7), "Add the thing", "merged", "Dev", "dev@example.com", day.Add(2*time.Hour), day.Add(12*time.Hour),
			uint32(10), uint32(2), uint32(1), stored, org)
		lateRun := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: false,
			DiscoveredRepoIDs: []RepositoryID{RepositoryID(orgDayRepoLate.String())}}
		impact, err := NewAIImpactExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		lateClock := stored.Add(time.Hour)
		impact.nowUTC = func() time.Time { return lateClock }
		if _, err := impact.ComputeFamily(ctx, lateRun, Partition{ID: uuid.NewString(), RunID: lateRun.ID, RepoIDs: lateRun.DiscoveredRepoIDs}); err != nil {
			t.Fatalf("ai_impact of the late repository: %v", err)
		}
		// The team_metrics_daily row of the same repository and run, in the
		// shape the wellbeing family stores.
		exec("stored team_metrics row of the late repository", `INSERT INTO team_metrics_daily
    (org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count, computed_at)
    VALUES (?, ?, 'github:platform', 'Team platform', ?, 7, 1, 0, ?)`, org, day, orgDayRepoLate.String(), lateClock)
		endStaleKeyRun(t, ctx, conn, lateRun, lateClock.Add(time.Minute))

		late, gone := orgDayRepoLate.String(), orgDayRepoGone.String()
		impactBefore, teamBefore := float(impactHeld, org, day, late), float(teamHeld, org, day, late)
		if impactBefore == 0 || teamBefore == 0 {
			t.Fatalf("the run of the late repository stored no measure (ai_impact %v, team_metrics %v): the case is not set", impactBefore, teamBefore)
		}
		if float(impactHeld, org, day, gone) != 10 || float(teamHeld, org, day, gone) != 7 {
			t.Fatalf("the repository that is gone has no stored rows: the case is not set")
		}
		impactKeys := liveKeys("ai_impact_metrics_daily", "prs_total", "tuple(team_id, work_type, attribution_bucket)", org, late)

		// The run of the whole organization, with the list of its dispatch, ends.
		orgRun := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: snapshot}
		orgClock := lateClock.Add(2 * time.Hour)
		computeListed(orgRun, orgClock)
		_, retracted, notInRun := orgDayRepositoryLines(t, ctx, conn, orgRun, orgClock)

		if got := float(impactHeld, org, day, late); got != impactBefore {
			t.Errorf("ai_impact_metrics_daily: the rows of a repository the organization holds now went from %v to %v at the end of a run that listed its repositories before it", impactBefore, got)
		}
		if got := float(teamHeld, org, day, late); got != teamBefore {
			t.Errorf("team_metrics_daily: the rows of a repository the organization holds now went from %v to %v at the end of a run that listed its repositories before it", teamBefore, got)
		}
		if impactGone, teamGone := float(impactHeld, org, day, gone), float(teamHeld, org, day, gone); impactGone != 0 || teamGone != 0 {
			t.Errorf("a repository the organization does not hold, in no partition: ai_impact holds %v and team_metrics %v, want 0 and 0", impactGone, teamGone)
		}
		// The WARN line names the repository that is in no partition and the
		// keys that are left as they are.
		if notInRun == nil {
			t.Fatalf("no %q line: a repository of the organization is in no partition of the run and nothing says so", StaleKeysRepositoryNotInRunLogMessage)
		}
		if notInRun["level"] != "WARN" || notInRun["repositories_not_in_run"] != float64(1) ||
			notInRun["team_metrics_daily_keys_left"] != float64(1) || notInRun["ai_impact_metrics_daily_keys_left"] != impactKeys {
			t.Errorf("the line of the repository in no partition is %v, want level WARN, 1 repository, 1 key of team_metrics_daily and %v key(s) of ai_impact_metrics_daily left",
				notInRun, impactKeys)
		}
		if retracted == nil || retracted["team_metrics_daily_zero_rows"] != float64(1) || retracted["ai_impact_metrics_daily_zero_rows"] != float64(1) {
			t.Errorf("the line of the retraction is %v, want 1 row of zeros in each repository-scoped table (the repository that is gone)", retracted)
		}
	})

	// The read of the repositories at the end of the run fails: the list is
	// not proven, the step fails and writes nothing, in no table.
	t.Run("the read of the repositories fails", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0002"
		snapshot := setUp(org)
		storedRows(org, orgDayRepoGone, stored)
		orgRun := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: snapshot}
		clock := stored.Add(3 * time.Hour)
		computeListed(orgRun, clock)
		retractor, err := NewRunStaleKeyRetractor(&failedRepositoryReadConn{Conn: conn})
		if err != nil {
			t.Fatal(err)
		}
		retractor.nowUTC = func() time.Time { return clock }
		written, err := retractor.RetractStaleKeys(ctx, orgRun)
		if !errors.Is(err, ErrOrganizationRepositoriesNotRead) || written != 0 {
			t.Errorf("the step wrote %d row(s) with error %v, want no row and ErrOrganizationRepositoriesNotRead", written, err)
		}
		gone := orgDayRepoGone.String()
		if impactGone, teamGone := float(impactHeld, org, day, gone), float(teamHeld, org, day, gone); impactGone != 10 || teamGone != 7 {
			t.Errorf("after the failed read ai_impact holds %v and team_metrics %v for a repository in no partition, want the stored 10 and 7: nothing may be superseded on a list that is not proven",
				impactGone, teamGone)
		}
		// The same run with the read: the step is done.
		if _, retracted, _ := orgDayRepositoryLines(t, ctx, conn, orgRun, clock); retracted == nil ||
			retracted["team_metrics_daily_zero_rows"] != float64(1) || retracted["ai_impact_metrics_daily_zero_rows"] != float64(1) {
			t.Errorf("the step with the read: the line is %v, want 1 row of zeros in each repository-scoped table", retracted)
		}
	})

	// A run of the whole organization with NO repository in its list (the
	// discovery found none) while the organization holds a repository now.
	t.Run("a run with no repository in its list", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0003"
		repository(org, sharedScopeRepoAPI, "acme/api", stored)
		storedRows(org, sharedScopeRepoAPI, stored)
		// A second key of the same repository in ai_impact_metrics_daily, so
		// that the two tables do not hold the same count of keys.
		exec("stored ai_impact row", `INSERT INTO ai_impact_metrics_daily
    (org_id, team_id, repo_id, work_type, day, attribution_bucket, prs_total, prs_merged, human_prs, computed_at)
    VALUES (?, 'ENG', ?, 'later', ?, 'human', 5, 5, 5, ?)`, org, sharedScopeRepoAPI, day, stored)
		storedRows(org, orgDayRepoGone, stored)
		orgRun := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true}
		_, _, notInRun := orgDayRepositoryLines(t, ctx, conn, orgRun, stored.Add(time.Hour))
		held, gone := sharedScopeRepoAPI.String(), orgDayRepoGone.String()
		if impactHeldNow, teamHeldNow := float(impactHeld, org, day, held), float(teamHeld, org, day, held); impactHeldNow != 20 || teamHeldNow != 7 {
			t.Errorf("a run with no repository superseded the rows of a repository the organization holds: ai_impact %v, team_metrics %v, want 20 and 7", impactHeldNow, teamHeldNow)
		}
		if impactGone, teamGone := float(impactHeld, org, day, gone), float(teamHeld, org, day, gone); impactGone != 0 || teamGone != 0 {
			t.Errorf("a repository the organization does not hold: ai_impact holds %v and team_metrics %v, want 0 and 0", impactGone, teamGone)
		}
		if notInRun == nil || notInRun["repositories_not_in_run"] != float64(1) ||
			notInRun["team_metrics_daily_keys_left"] != float64(1) || notInRun["ai_impact_metrics_daily_keys_left"] != float64(2) {
			t.Errorf("the line of the repository in no partition is %v, want 1 repository, 1 key of team_metrics_daily and 2 keys of ai_impact_metrics_daily left", notInRun)
		}
	})

	// An older run of the whole organization ends (or its end is driven
	// again) AFTER a newer run of the whole organization that listed and
	// computed one more repository. The older run's list does not hold that
	// repository; its rows of the newer run keep their measure, and the end
	// of the older run supersedes nothing.
	t.Run("an older run ends after a newer run", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0004"
		older := setUp(org)
		olderRun := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: older}
		computeListed(olderRun, stored.Add(time.Hour))

		repository(org, orgDayRepoLate, "acme/late", stored.Add(2*time.Hour))
		newerClock := stored.Add(3 * time.Hour)
		newerRun := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true,
			DiscoveredRepoIDs: append(append([]RepositoryID{}, older...), RepositoryID(orgDayRepoLate.String()))}
		computeListed(newerRun, newerClock)
		storedRows(org, orgDayRepoLate, newerClock)
		if _, _, notInRun := orgDayRepositoryLines(t, ctx, conn, newerRun, newerClock); notInRun != nil {
			t.Fatalf("the newer run lists every repository and still writes %v: the case is not set", notInRun)
		}

		const itemsHeld = `SELECT toFloat64(sum(items_started + items_completed + wip_count_end_of_day)) FROM work_item_metrics_daily FINAL WHERE org_id = ? AND day = ?`
		itemsBefore := float(itemsHeld, org, day)
		if itemsBefore == 0 {
			t.Fatalf("the newer run stored no work-item measure: the case is not set")
		}

		_, retracted, notInRun := orgDayRepositoryLines(t, ctx, conn, olderRun, newerClock.Add(time.Hour))
		late := orgDayRepoLate.String()
		if impactLate, teamLate := float(impactHeld, org, day, late), float(teamHeld, org, day, late); impactLate != 10 || teamLate != 7 {
			t.Errorf("the end of the older run superseded the rows the newer run holds for its repository: ai_impact %v, team_metrics %v, want 10 and 7", impactLate, teamLate)
		}
		// The work-item tables: the end of a run computes them from every
		// item of the organization as it is then, so the older run stores the
		// values the newer run stored, and no row of zeros in any table.
		if got := float(itemsHeld, org, day); got != itemsBefore {
			t.Errorf("work_item_metrics_daily holds %v after the end of the older run, want the %v of the newer run", got, itemsBefore)
		}
		if retracted == nil {
			t.Fatalf("the end of the older run wrote no %q line", StaleKeysRetractedLogMessage)
		}
		for field, value := range retracted {
			if strings.HasSuffix(field, "_zero_rows") && value != float64(0) {
				t.Errorf("the end of the older run wrote %v row(s) of zeros (%s) after the newer run settled the day, want none", value, field)
			}
		}
		if notInRun == nil || notInRun["repositories_not_in_run"] != float64(1) ||
			notInRun["team_metrics_daily_keys_left"] != float64(1) || notInRun["ai_impact_metrics_daily_keys_left"] != float64(1) {
			t.Errorf("the end of the older run: the line of the repository in no partition is %v, want 1 repository and 1 key left in each table", notInRun)
		}
	})
}
