//go:build integration

package throughputforecast

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The cells of the team view of the one-team throughput forecast reads (CHAOS-14376). Team P is the primary team of
// the items of scope S; team C co-owns some of them, so C has 'team' rows only. The older day holds a legacy row only
// (a day no writer computed again); the newer day holds the rows of the org writer (the legacy row retracted with a
// row of zeros, an org row and a scope row) and, in the team data set, the team rows of the same keys.

func teamViewToday() time.Time { return time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC) }

func teamViewVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

type teamViewRow struct {
	daysAgo        int
	team           string
	completed, wip uint32
	age            *float64
	at             time.Time
	view           string
}

func ageHours(v float64) *float64 { return &v }

func seedTeamViewRows(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []teamViewRow) {
	t.Helper()
	for _, row := range rows {
		var p90 *float64
		if row.age != nil {
			p90 = ageHours(*row.age * 2)
		}
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day,
			 wip_age_p50_hours, wip_age_p90_hours, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			teamViewToday().AddDate(0, 0, -row.daysAgo), row.team, row.team, row.completed, row.wip,
			row.age, p90, row.at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

// teamViewReads is every value the four work-item reads serve, and the whole served forecast (its random id blanked;
// its JSON digest is part of the compared answer, so every served field is compared).
type teamViewReads struct {
	History          []int
	AverageWIP       float64
	CurrentWIP       float64
	StaleP50         *float64
	StaleP90         *float64
	BacklogSize      int
	BacklogState     model.ThroughputBacklogState
	BacklogAsOf      string
	Forecast         *model.ThroughputForecast
	ForecastTeamID   string
	ForecastResolved bool
}

// The reads take the team selection as Resolve passes it on (a set); Resolve itself gets the selection as given.
func readTeamView(ctx context.Context, t *testing.T, client QueryClient, org string, given ...string) teamViewReads {
	t.Helper()
	var reads teamViewReads
	var err error
	teams := distinctTeamIDs(given)
	if reads.History, err = loadThroughputHistory(ctx, client, org, teams, nil, 4, teamViewToday()); err != nil {
		t.Fatalf("%v history: %v", teams, err)
	}
	if reads.CurrentWIP, reads.AverageWIP, err = loadWorkItemOverlay(ctx, client, org, teams, nil, 4, teamViewToday()); err != nil {
		t.Fatalf("%v overlay: %v", teams, err)
	}
	if reads.StaleP50, reads.StaleP90, err = loadStaleWIP(ctx, client, org, teams, nil); err != nil {
		t.Fatalf("%v stale wip: %v", teams, err)
	}
	backlog, err := loadBacklog(ctx, client, org, teams, nil, 4, teamViewToday())
	if err != nil {
		t.Fatalf("%v backlog: %v", teams, err)
	}
	reads.BacklogSize, reads.BacklogState = backlog.size, backlog.state
	if backlog.asOfDay != nil {
		reads.BacklogAsOf = backlog.asOfDay.Format("2006-01-02")
	}
	forecast, err := Resolve(ctx, client, org, model.ThroughputForecastInput{TeamIds: given, HistoryWeeks: 4}, teamViewToday())
	if err != nil {
		t.Fatalf("%v resolve: %v", teams, err)
	}
	if forecast != nil {
		forecast.ForecastID = ""
		reads.ForecastResolved = true
		if forecast.TeamID != nil {
			reads.ForecastTeamID = *forecast.TeamID
		}
	}
	reads.Forecast = forecast
	return reads
}

func (r teamViewReads) String() string {
	deref := func(v *float64) string {
		if v == nil {
			return "nil"
		}
		return fmt.Sprintf("%g", *v)
	}
	served, err := json.Marshal(r.Forecast)
	if err != nil {
		served = []byte(err.Error())
	}
	digest := sha256.Sum256(served)
	return fmt.Sprintf("history=%v currentWIP=%g averageWIP=%g stale=%s/%s backlog=%d %s asOf=%s team=%q forecast=%x",
		r.History, r.CurrentWIP, r.AverageWIP, deref(r.StaleP50), deref(r.StaleP90), r.BacklogSize, r.BacklogState,
		r.BacklogAsOf, r.ForecastTeamID, digest[:6])
}

// wantTeamViewReads compares the served fields of got with want. A want that carries " forecast=<digest>" is an answer
// pinned from main (the cell says "same as main"): the digest of the whole served forecast is then compared too. A want
// computed from the seeded rows by hand carries the fields only.
func wantTeamViewReads(t *testing.T, what string, got teamViewReads, want string) {
	t.Helper()
	gotFields, gotDigest, _ := strings.Cut(got.String(), " forecast=")
	wantFields, wantDigest, pinned := strings.Cut(want, " forecast=")
	if gotFields != wantFields || (pinned && gotDigest != wantDigest) {
		t.Errorf("%s:\n got  %s\n want %s", what, got.String(), want)
	}
}

// orgWriterDays is today's data: a legacy-only older day and an org-writer newer day of team P.
func orgWriterDays(t1, t2, t3 time.Time) []teamViewRow {
	return []teamViewRow{
		{2, "P", 2, 1, ageHours(10), t1, ""},
		{1, "P", 7, 5, ageHours(40), t1, ""}, {1, "P", 0, 0, nil, t3, ""},
		{1, "P", 4, 3, ageHours(12), t2, "org"}, {1, "P", 6, 4, ageHours(16), t2, "scope"},
	}
}

// (1) Today's data holds no team row: every served value of every read, and the whole served forecast, equal main's.
func TestThroughputTeamReadsAreANoOpOnTodaysData(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)
	t1, t2, t3 := teamViewVersions()
	const org = "org-14376-tp-noop"
	seedTeamViewRows(ctx, t, admin, org, orgWriterDays(t1, t2, t3))
	wantTeamViewReads(t, "team P", readTeamView(ctx, t, client, org, "P"), mainAnswerTeamP)
	wantTeamViewReads(t, "no team", readTeamView(ctx, t, client, org), mainAnswerNoTeam)
	wantTeamViewReads(t, "teams P and C", readTeamView(ctx, t, client, org, "P", "C"), mainAnswerTeamsPC)
}

// (2) A team row wins where one exists: team P reads its team row of the newer day, and the co-owner team C, which has
// no org row, reads its team rows.
func TestThroughputTeamReadsTakeTheTeamRow(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)
	t1, t2, t3 := teamViewVersions()
	const org = "org-14376-tp-team"
	seedTeamViewRows(ctx, t, admin, org, append(orgWriterDays(t1, t2, t3),
		teamViewRow{1, "P", 9, 6, ageHours(14), t2, "team"},
		teamViewRow{1, "C", 3, 2, ageHours(30), t2, "team"},
	))
	wantTeamViewReads(t, "team P", readTeamView(ctx, t, client, org, "P"), teamAnswerTeamP)
	wantTeamViewReads(t, "team C", readTeamView(ctx, t, client, org, "C"), teamAnswerTeamC)
	// A team set keeps the org view (each item once in the set: L2d).
	wantTeamViewReads(t, "teams P and C (a team set: org form)", readTeamView(ctx, t, client, org, "P", "C"), teamAnswerTeamsPC)
}

// (3) Live first: a team key that a later run retracted (its newest team row is a row of zeros) reads the live org
// row. The newest-day reads keep a key whose every view is retracted as its newest day, as the org form does.
func TestThroughputTeamReadsLiveFirst(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)
	t1, t2, t3 := teamViewVersions()
	const org = "org-14376-tp-live"
	seedTeamViewRows(ctx, t, admin, org, []teamViewRow{
		{2, "P", 2, 1, ageHours(10), t1, ""},
		{1, "P", 8, 7, ageHours(50), t1, "team"},
		{1, "P", 4, 3, ageHours(12), t2, "org"},
		{1, "P", 0, 0, nil, t3, "team"},
	})
	wantTeamViewReads(t, "team P", readTeamView(ctx, t, client, org, "P"), liveAnswerTeamP)
	// The newest-day reads keep a key whose every view is retracted on its newest day as that key's newest row: the
	// current WIP and the backlog read that day (0, not measured), not the older day's row.
	const retired = "org-14376-tp-live-retired"
	seedTeamViewRows(ctx, t, admin, retired, []teamViewRow{
		{2, "P", 2, 1, ageHours(10), t1, "org"},
		{1, "P", 5, 4, ageHours(20), t1, "org"}, {1, "P", 0, 0, nil, t3, "org"},
		{1, "P", 6, 5, ageHours(30), t1, "team"}, {1, "P", 0, 0, nil, t3, "team"},
	})
	wantTeamViewReads(t, "team P, newest day all retracted", readTeamView(ctx, t, client, retired, "P"), retiredAnswerTeamP)
}

// (4) Two organizations never mix: org B holds the same keys with other values, a team row of C and a day of its
// own; a read of org A returns org A's rows only, in the team form and in the org form.
func TestThroughputTeamReadsNeverMixTwoOrgs(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)
	t1, t2, t3 := teamViewVersions()
	const orgA, orgB = "org-14376-tp-two-a", "org-14376-tp-two-b"
	seedTeamViewRows(ctx, t, admin, orgA, append(orgWriterDays(t1, t2, t3), teamViewRow{1, "P", 9, 6, ageHours(14), t2, "team"}))
	seedTeamViewRows(ctx, t, admin, orgB, []teamViewRow{
		{1, "P", 90, 60, ageHours(140), t3, "team"}, {1, "P", 40, 30, ageHours(120), t3, "org"},
		{0, "P", 70, 50, ageHours(100), t3, "team"}, {1, "C", 30, 20, ageHours(300), t3, "team"},
	})
	wantTeamViewReads(t, "org A team P", readTeamView(ctx, t, client, orgA, "P"), teamAnswerTeamP)
	wantTeamViewReads(t, "org A team C (rows in org B only)", readTeamView(ctx, t, client, orgA, "C"), emptyAnswerTeamC)
	wantTeamViewReads(t, "org A no team", readTeamView(ctx, t, client, orgA), twoOrgAnswerNoTeam)
}

// (6) The team id list is a set: [P, P] is the one team P (the team view) and [P, C, P] is the team set [P, C].
func TestThroughputTeamReadsTakeTheTeamIdsAsASet(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)
	t1, t2, t3 := teamViewVersions()
	const org = "org-14376-tp-set"
	seedTeamViewRows(ctx, t, admin, org, append(orgWriterDays(t1, t2, t3),
		teamViewRow{1, "P", 9, 6, ageHours(14), t2, "team"},
		teamViewRow{1, "C", 3, 2, ageHours(30), t2, "team"},
	))
	one, twice := readTeamView(ctx, t, client, org, "P"), readTeamView(ctx, t, client, org, "P", "P")
	if !reflect.DeepEqual(one.Forecast, twice.Forecast) || one.String() != twice.String() {
		t.Errorf("[P,P] is not [P]:\n [P]   %s\n [P,P] %s", one, twice)
	}
	wantTeamViewReads(t, "[P,P] forecast reads", twice, teamAnswerTeamP)
	set, repeated := readTeamView(ctx, t, client, org, "P", "C"), readTeamView(ctx, t, client, org, "P", "C", "P")
	if !reflect.DeepEqual(set.Forecast, repeated.Forecast) || set.String() != repeated.String() {
		t.Errorf("[P,C,P] is not [P,C]:\n [P,C]   %s\n [P,C,P] %s", set, repeated)
	}
}

// The documented divergence (R2 of the plan, not a WANT): a live legacy row beside a live org row of one key. No
// writer on main stores it (the org writer stores the legacy row of zeros in the same insert block as the org row).
// The org form sums both rows; the team form reads the org row. Both answers are recorded so a writer change that makes
// the state producible shows here.
func TestThroughputTeamReadsRecordTheLiveLegacyBesideLiveOrgDivergence(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)
	t1, t2, _ := teamViewVersions()
	const org = "org-14376-tp-divergence"
	seedTeamViewRows(ctx, t, admin, org, []teamViewRow{
		{1, "P", 7, 5, ageHours(40), t1, ""},
		{1, "P", 4, 3, ageHours(12), t2, "org"},
	})
	t.Logf("DIVERGENCE (recorded, not a want) one team P: %s", readTeamView(ctx, t, client, org, "P"))
	t.Logf("DIVERGENCE (recorded, not a want) no team (org form): %s", readTeamView(ctx, t, client, org))
}

// The answers. main*, live*, empty* and retired* are pinned from main (2026-11-20, before the team view; the org form
// reads the same rows there), so those cells compare head with main field by field, and the forecast digest covers
// every served field. team* are computed by hand from the seeded rows (fields only, no digest).
const (
	mainAnswerTeamP   = "history=[2 4] currentWIP=3 averageWIP=2 stale=12/24 backlog=3 MEASURED asOf=2026-10-19 team=\"P\" forecast=82dd2e1f2a03"
	mainAnswerNoTeam  = "history=[2 4] currentWIP=3 averageWIP=2 stale=12/24 backlog=3 MEASURED asOf=2026-10-19 team=\"\" forecast=fa90e9bd55be"
	mainAnswerTeamsPC = mainAnswerNoTeam
	// Team P, by hand from the seeded rows (today 2026-10-20, window 4 weeks): one key (jira, S, P). The older day
	// (10-18) is legacy only: completed 2, WIP 1. The newer day (10-19) reads its team row: completed 9, WIP 6, ages
	// 14/28. history = [2, 9]; current WIP = the newest day's WIP = 6; average WIP = (1 + 6) / 2 = 3.5; stale ages =
	// the newest day's ages = 14/28; backlog = 6 on the newest day 10-19, inside the window: MEASURED.
	teamAnswerTeamP = "history=[2 9] currentWIP=6 averageWIP=3.5 stale=14/28 backlog=6 MEASURED asOf=2026-10-19 team=\"P\""
	// Team C, by hand: one key (jira, S, C), the newer day's team row only: completed 3, WIP 2, ages 30/60. history =
	// [3]; current WIP = 2; average WIP = 2 / 1 = 2; stale 30/60; backlog 2 on 10-19: MEASURED.
	teamAnswerTeamC    = "history=[3] currentWIP=2 averageWIP=2 stale=30/60 backlog=2 MEASURED asOf=2026-10-19 team=\"C\""
	teamAnswerTeamsPC  = mainAnswerNoTeam
	liveAnswerTeamP    = mainAnswerTeamP
	emptyAnswerTeamC   = "history=[] currentWIP=0 averageWIP=NaN stale=nil/nil backlog=0 NO_ROWS asOf= team=\"C\" forecast=db85152c4184"
	twoOrgAnswerNoTeam = mainAnswerNoTeam
	retiredAnswerTeamP = "history=[2] currentWIP=0 averageWIP=1 stale=nil/nil backlog=0 NO_ROWS asOf= team=\"P\" forecast=d59046b94c33"
)
