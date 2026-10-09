// Package retractionseed seeds two organizations that hold the same
// measurements of the team-keyed daily tables, for the tests of the readers of
// those tables.
//
// ControlOrg holds the measurements only. RetractedOrg holds the same
// measurements and, for each day that was computed again after its teams got
// provider-keyed ids, the two things such a day holds in a real store: the
// older row under each retired team id, and the retraction row the daily
// writer stores over it (the key and computed_at, every other column its
// default: 0 in each count, NULL in each Nullable measure).
//
// A reader gives the retraction rows no weight when it answers the same for
// both organizations. A test asserts that, and it asserts the control answer
// itself, so a reader that returns nothing for both does not pass.
//
// The seed holds one team for each provider (jira, github, gitlab, linear):
// no rule here is a rule of one provider. The oldest day (Days()[0]) was not
// computed again: in both organizations it holds measured rows under the
// retired ids only, the only copy of that day. A reader must still count it.
package retractionseed

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The two organizations of the seed. The ids have the form of a UUID because
// some readers take the organization as one.
const (
	ControlOrg   = "c0c0c0c0-0000-4000-8000-000000000001"
	RetractedOrg = "c0c0c0c0-0000-4000-8000-000000000002"
)

// RetractionOnlyOrg is an organization id for a test that stores retraction
// rows only (Retract). Apply stores nothing for it.
const RetractionOnlyOrg = "c0c0c0c0-0000-4000-8000-000000000003"

// Team is one team of the seed: the id it was stored under before the carry
// (RetiredID, now inactive in the teams table) and the provider-keyed id it
// has now (KeyedID).
type Team struct {
	Provider  string
	RetiredID string
	KeyedID   string
	Name      string
	WorkScope string
	RepoID    string
}

// Teams are the teams of the seed, one for each provider.
var Teams = []Team{
	{"jira", "ENG", "jira:ENG", "Engineering", "ENGPROJ", "11111111-1111-4111-8111-111111111111"},
	{"github", "platform", "github:platform", "Platform", "acme/platform", "22222222-2222-4222-8222-222222222222"},
	{"gitlab", "ops", "gitlab:ops", "Operations", "acme/ops", "33333333-3333-4333-8333-333333333333"},
	{"linear", "core", "linear:core", "Core", "CORE", "44444444-4444-4444-8444-444444444444"},
}

// DayCount is the number of seeded days.
const DayCount = 7

// Days returns the seeded days, oldest first: the DayCount days that end
// yesterday (UTC). They are relative to now because some readers bound their
// window with the server's today().
func Days(now time.Time) []time.Time {
	today := now.UTC().Truncate(24 * time.Hour)
	days := make([]time.Time, 0, DayCount)
	for back := DayCount; back >= 1; back-- {
		days = append(days, today.AddDate(0, 0, -back))
	}
	return days
}

// Seed is what a test needs to know about the stored rows.
type Seed struct {
	// Days are the seeded days, oldest first. Days[0] was not computed again.
	Days []time.Time
	// OldComputedAt is the computed_at of the rows of the first compute.
	OldComputedAt time.Time
	// NewComputedAt is the computed_at of the second compute: the measured
	// rows under the keyed ids and the retraction rows.
	NewComputedAt time.Time
}

// Recomputed returns the days that were computed again.
func (s Seed) Recomputed() []time.Time { return s.Days[1:] }

// measure is the measured values of one (team, day). Every fraction is a
// multiple of 1/8, so a mean of them is exact and two organizations compare
// with ==.
type measure struct {
	started, completed, wip, newItems, newBugs uint32
	cycleP50, cycleP90, wipAgeP50              float64
	congestion, defectRate, bugRatio, predict  float64
	storyPoints                                float64
	blockedHours, activeHours                  float64
	commits, afterHours, weekend               uint32
	deliveryUnits, itemsDone, prsMerged        uint32
	churnLOC                                   uint64
	prsTotal, aiPRs, reworkPRs                 uint32
	aiArtifacts, declared, reviewed, scanned   uint64
	risk                                       float64
}

func measureOf(teamIndex, dayIndex int) measure {
	k, d := uint32(teamIndex), uint32(dayIndex)
	return measure{
		started: 2 + k, completed: 1 + k + d%2, wip: 3 + k, newItems: 4, newBugs: 1 + k%2,
		cycleP50: 8 * float64(k+1), cycleP90: 16 * float64(k+1), wipAgeP50: 4 * float64(k+1),
		congestion: 0.5 + 0.25*float64(k), defectRate: 0.125 * float64(k+1), bugRatio: 0.25,
		predict: 0.5, storyPoints: float64(2 * (k + 1)),
		blockedHours: 6 + 2*float64(k), activeHours: 12,
		commits: 8, afterHours: 2 * (k + 1), weekend: k % 2,
		deliveryUnits: 3 + k, itemsDone: 2 + k, prsMerged: 1 + k, churnLOC: uint64(100 * (k + 1)),
		prsTotal: 4 + k, aiPRs: 1 + k, reworkPRs: 1,
		aiArtifacts: uint64(4 + k), declared: uint64(2 + k), reviewed: 2, scanned: 1,
		risk: 0.25 + 0.125*float64(k),
	}
}

// Store is a migrated ClickHouse that holds the seed.
type Store struct {
	Seed
	// URI is the DSN of the store, for a reader that opens its own client.
	URI string
	// Conn is a raw connection to the store.
	Conn driver.Conn
}

// Start starts a ClickHouse, applies the migration chain and the seed, and
// closes everything when the test ends.
func Start(ctx context.Context, t *testing.T) Store {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("retractionseed: start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("retractionseed: close ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("retractionseed: parse DSN: %v", err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatalf("retractionseed: open connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return Store{Seed: Apply(ctx, t, conn), URI: instance.URI, Conn: conn}
}

// Apply seeds both organizations and returns the facts of the seed.
func Apply(ctx context.Context, t testing.TB, conn driver.Conn) Seed {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	seed := Seed{Days: Days(now), OldComputedAt: now.Add(-2 * time.Hour), NewComputedAt: now.Add(-time.Hour)}
	for _, org := range []string{ControlOrg, RetractedOrg} {
		seedTeams(ctx, t, conn, org, seed)
		for dayIndex, day := range seed.Days {
			for teamIndex, team := range Teams {
				m := measureOf(teamIndex, dayIndex)
				if dayIndex == 0 {
					// Not computed again: the retired id holds the only rows.
					insertMeasured(ctx, t, conn, org, day, team, team.RetiredID, m, seed.OldComputedAt)
					continue
				}
				insertMeasured(ctx, t, conn, org, day, team, team.KeyedID, m, seed.NewComputedAt)
				if org == RetractedOrg {
					// The first compute counted the day under the retired id.
					// Its values are not the values of the second compute, so
					// a reader that still serves an old row gives a different
					// answer, also where it takes a mean of equal samples.
					insertMeasured(ctx, t, conn, org, day, team, team.RetiredID,
						measureOf(teamIndex+1, dayIndex), seed.OldComputedAt)
					insertRetractions(ctx, t, conn, org, day, team, team.RetiredID, seed.NewComputedAt)
				}
			}
		}
	}
	return seed
}

// Retract stores, for one (team, day) of org, a measured row under the
// retired id at oldComputedAt and the retraction row over it at
// newComputedAt, and nothing else: a key whose only newest rows are
// retraction rows. It is for a test of a window or an organization that
// holds no measurement.
func Retract(
	ctx context.Context, t testing.TB, conn driver.Conn,
	org string, day time.Time, team Team, oldComputedAt, newComputedAt time.Time,
) {
	t.Helper()
	insertMeasured(ctx, t, conn, org, day, team, team.RetiredID, measureOf(0, 1), oldComputedAt)
	insertRetractions(ctx, t, conn, org, day, team, team.RetiredID, newComputedAt)
}

func seedTeams(ctx context.Context, t testing.TB, conn driver.Conn, org string, seed Seed) {
	t.Helper()
	const insert = `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], [], ?, ?, ?, ?)`
	for _, team := range Teams {
		exec(ctx, t, conn, insert, team.KeyedID, team.Name, seed.NewComputedAt, org, team.Provider, uint8(1))
		// The row under the old id: active at first, inactive after the carry.
		exec(ctx, t, conn, insert, team.RetiredID, team.Name, seed.OldComputedAt, org, team.Provider, uint8(1))
		exec(ctx, t, conn, insert, team.RetiredID, team.Name, seed.NewComputedAt, org, team.Provider, uint8(0))
		// The repository of the team; its name is the work scope of the
		// team's work items, as for a provider whose scope is a repository.
		exec(ctx, t, conn, `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
VALUES (?, ?, ?, ?, ?, ?)`, team.RepoID, team.WorkScope, seed.OldComputedAt, seed.NewComputedAt, org, team.Provider)
	}
}

// insertMeasured stores the measured rows of one (team id, day) in every
// seeded table.
func insertMeasured(
	ctx context.Context, t testing.TB, conn driver.Conn,
	org string, day time.Time, team Team, teamID string, m measure, computedAt time.Time,
) {
	t.Helper()
	exec(ctx, t, conn, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day,
 cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
 wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
 new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, day, team.Provider, team.WorkScope, teamID, team.Name, m.started, m.completed, m.wip,
		m.cycleP50, m.cycleP90, m.cycleP50*2, m.cycleP90*2, m.wipAgeP50, m.wipAgeP50*2, m.bugRatio, m.storyPoints,
		m.newBugs, m.newItems, m.defectRate, m.congestion, m.predict, computedAt)
	for _, state := range []struct {
		status string
		hours  float64
	}{{"blocked", m.blockedHours}, {"in_progress", m.activeHours}} {
		exec(ctx, t, conn, `INSERT INTO work_item_state_durations_daily
(org_id, day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			org, day, team.Provider, team.WorkScope, teamID, team.Name, state.status, state.hours, m.started, state.hours/24, computedAt)
	}
	exec(ctx, t, conn, `INSERT INTO team_metrics_daily
(org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count,
 after_hours_commit_ratio, weekend_commit_ratio, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, day, teamID, team.Name, team.RepoID, m.commits, m.afterHours, m.weekend,
		float64(m.afterHours)/float64(m.commits), float64(m.weekend)/float64(m.commits), computedAt)
	exec(ctx, t, conn, `INSERT INTO investment_metrics_daily
(org_id, day, repo_id, team_id, investment_area, project_stream, delivery_units, work_items_completed,
 prs_merged, churn_loc, cycle_p50_hours, computed_at)
VALUES (?, ?, ?, ?, 'feature_delivery', 'roadmap', ?, ?, ?, ?, ?, ?)`,
		org, day, team.RepoID, teamID, m.deliveryUnits, m.itemsDone, m.prsMerged, m.churnLOC, m.cycleP50, computedAt)
	exec(ctx, t, conn, `INSERT INTO issue_type_metrics_daily
(org_id, day, repo_id, provider, team_id, issue_type_norm, created_count, completed_count, active_count,
 cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at)
VALUES (?, ?, ?, ?, ?, 'bug', ?, ?, ?, ?, ?, ?, ?)`,
		org, day, team.RepoID, team.Provider, teamID, m.newItems, m.completed, m.wip, m.cycleP50, m.cycleP90, m.cycleP50*2, computedAt)
	exec(ctx, t, conn, `INSERT INTO ai_impact_metrics_daily
(org_id, team_id, repo_id, work_type, day, attribution_bucket, prs_total, prs_merged, ai_assisted_prs,
 human_prs, rework_prs, cycle_time_avg_hours, reviews_per_pr, rework_drag_rate, leverage_prs_component, computed_at)
VALUES (?, ?, ?, 'feature', ?, 'ai_assisted', ?, ?, ?, 0, ?, ?, 2, 0.25, 0.5, ?)`,
		org, teamID, team.RepoID, day, m.prsTotal, m.prsTotal, m.aiPRs, m.reworkPRs, m.cycleP50, computedAt)
	exec(ctx, t, conn, `INSERT INTO ai_governance_coverage_daily
(org_id, team_id, repo_id, day, ai_artifacts, declared_artifacts, human_reviewed_prs, security_scanned_prs,
 in_policy_artifacts, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, teamID, team.RepoID, day, m.aiArtifacts, m.declared, m.reviewed, m.scanned, m.declared, computedAt)
	exec(ctx, t, conn, `INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm, ownership_norm, review_norm,
 w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
VALUES (?, ?, 'team', ?, ?, 'low', 0.25, 0.25, 0.25, 0.25, 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, ?)`,
		org, day, teamID, m.risk, computedAt)
}

// insertRetractions stores the retraction row of each key of one (team id,
// day): the key and computed_at only, as the daily writer does, so every
// other column takes its default.
func insertRetractions(
	ctx context.Context, t testing.TB, conn driver.Conn,
	org string, day time.Time, team Team, teamID string, computedAt time.Time,
) {
	t.Helper()
	exec(ctx, t, conn, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, computed_at)
VALUES (?, ?, ?, ?, ?, ?)`, org, day, team.Provider, team.WorkScope, teamID, computedAt)
	for _, status := range []string{"blocked", "in_progress"} {
		exec(ctx, t, conn, `INSERT INTO work_item_state_durations_daily
(org_id, day, provider, work_scope_id, team_id, status, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			org, day, team.Provider, team.WorkScope, teamID, status, computedAt)
	}
	exec(ctx, t, conn, `INSERT INTO team_metrics_daily (org_id, day, team_id, repo_id, computed_at)
VALUES (?, ?, ?, ?, ?)`, org, day, teamID, team.RepoID, computedAt)
	exec(ctx, t, conn, `INSERT INTO investment_metrics_daily
(org_id, day, repo_id, team_id, investment_area, project_stream, computed_at)
VALUES (?, ?, ?, ?, 'feature_delivery', 'roadmap', ?)`, org, day, team.RepoID, teamID, computedAt)
	exec(ctx, t, conn, `INSERT INTO issue_type_metrics_daily
(org_id, day, repo_id, provider, team_id, issue_type_norm, computed_at)
VALUES (?, ?, ?, ?, ?, 'bug', ?)`, org, day, team.RepoID, team.Provider, teamID, computedAt)
	exec(ctx, t, conn, `INSERT INTO ai_impact_metrics_daily
(org_id, team_id, repo_id, work_type, day, attribution_bucket, computed_at)
VALUES (?, ?, ?, 'feature', ?, 'ai_assisted', ?)`, org, teamID, team.RepoID, day, computedAt)
	exec(ctx, t, conn, `INSERT INTO ai_governance_coverage_daily (org_id, team_id, repo_id, day, computed_at)
VALUES (?, ?, ?, ?, ?)`, org, teamID, team.RepoID, day, computedAt)
	exec(ctx, t, conn, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, computed_at)
VALUES (?, ?, 'team', ?, ?)`, org, day, teamID, computedAt)
}

func exec(ctx context.Context, t testing.TB, conn driver.Conn, statement string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, statement, args...); err != nil {
		t.Fatalf("retractionseed: %s: %v", firstLine(statement), err)
	}
}

func firstLine(statement string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(statement), "\n")
	return fmt.Sprintf("%.80s", line)
}
