package operatingreview

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// CHAOS-8516: operatingReview for several teams is the review of the UNION of
// the selected teams' stored rows, by the same reducers as the one-team and the
// all-teams review. It is not made from team values.

// teamReadCalls are the per-period call indexes (fetchPeriodRows' fixed order)
// of the five reads that carry a team filter; the other five never do.
var teamReadCalls = map[int]string{0: "work_items", 1: "state_durations", 7: "investment", 8: "ai_impact", 9: "ai_governance"}

// bindingClient is fakeClient plus the bindings of each call.
type bindingClient struct {
	fakeClient
	bindings [][]clickhouse.Binding
}

func (c *bindingClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.bindings = append(c.bindings, bindings)
	return c.fakeClient.Query(ctx, statement, nil)
}

func twoStoredWeeks() *bindingClient {
	return &bindingClient{fakeClient: fakeClient{responses: append(storedZeroWeekScanners(), storedZeroWeekScanners()...)}}
}

func bindingOf(bindings []clickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

func TestTeamClauses_NoneOneAndSeveral(t *testing.T) {
	filter, group, binding := teamClauses(nil)
	if filter != "" || group != ", team_id" || binding != nil {
		t.Errorf("all teams: %q %q %v", filter, group, binding)
	}
	filter, group, binding = teamClauses(teamSelection{"team-a"})
	if filter != "AND team_id = {team_id:String}" || group != "" || binding == nil || binding.Name != "team_id" || binding.Value != "team-a" {
		t.Errorf("one team: %q %q %v (the one-team statement must not change)", filter, group, binding)
	}
	filter, group, binding = teamClauses(teamSelection{"team-a", "team-b"})
	if filter != "AND team_id IN {team_ids:Array(String)}" || group != ", team_id" || binding == nil || binding.Name != "team_ids" ||
		!reflect.DeepEqual(binding.Value, []string{"team-a", "team-b"}) {
		t.Errorf("several teams: %q %q %v", filter, group, binding)
	}
}

func TestSelectTeams_DropsBlanksAndRepeats(t *testing.T) {
	got := selectTeams([]string{"team-b", "", "  ", "team-a", "team-b"})
	if !reflect.DeepEqual([]string(got), []string{"team-b", "team-a"}) {
		t.Fatalf("selection = %v, want [team-b team-a]", got)
	}
	if selectTeams(nil) != nil || selectTeams([]string{"", " "}) != nil {
		t.Fatal("no id left must be the empty selection (all teams)")
	}
}

// One end of the rule: one id in teamIds is the one-team review, by the same
// statements, the same bindings and the same answer.
func TestResolveInput_OneIDIsTheOneTeamReview(t *testing.T) {
	week := graphqldate.New(day("2026-08-24"))
	team := "team-a"
	byArgument, byList := twoStoredWeeks(), twoStoredWeeks()
	want, err := Resolve(context.Background(), byArgument, "org-1", &team, week)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ResolveInput(context.Background(), byList, "org-1", nil, []string{"", "team-a", "team-a"}, week)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(byList.statements, byArgument.statements) || !reflect.DeepEqual(byList.bindings, byArgument.bindings) {
		t.Fatal("teamIds with one id does not issue the one-team statements and bindings")
	}
	if !reflect.DeepEqual(got, want) || got.TeamID == nil || *got.TeamID != "team-a" {
		t.Fatalf("teamIds with one id is not the one-team answer (teamId %v)", got.TeamID)
	}
}

// The other end: no id is the all-teams review.
func TestResolveInput_NoIDIsTheAllTeamsReview(t *testing.T) {
	week := graphqldate.New(day("2026-08-24"))
	all := twoStoredWeeks()
	want, err := Resolve(context.Background(), all, "org-1", nil, week)
	if err != nil {
		t.Fatal(err)
	}
	for name, ids := range map[string][]string{"null": nil, "empty": {}, "blank ids": {"", "  "}} {
		client := twoStoredWeeks()
		got, err := ResolveInput(context.Background(), client, "org-1", nil, ids, week)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(client.statements, all.statements) || !reflect.DeepEqual(client.bindings, all.bindings) || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: not the all-teams statements, bindings and answer", name)
		}
		if got.TeamID != nil {
			t.Errorf("%s: teamId = %q, want null", name, *got.TeamID)
		}
	}
}

// Several teams: the five team-scoped reads carry the selection, each team's
// rows are kept apart for the reducers, and the five other reads carry no team.
func TestResolveInput_SeveralTeamsBindTheSelectionOnTheTeamReadsOnly(t *testing.T) {
	client := twoStoredWeeks()
	got, err := ResolveInput(context.Background(), client, "org-1", nil, []string{"team-a", "team-b"}, graphqldate.New(day("2026-08-24")))
	if err != nil {
		t.Fatal(err)
	}
	if got.TeamID != nil {
		t.Errorf("teamId = %q for several teams, want null", *got.TeamID)
	}
	if client.calls != 20 {
		t.Fatalf("%d reads, want 20", client.calls)
	}
	for call := 0; call < 20; call++ {
		statement, bindings := client.statements[call], client.bindings[call]
		table, teamScoped := teamReadCalls[call%10]
		ids, bound := bindingOf(bindings, "team_ids")
		_, single := bindingOf(bindings, "team_id")
		if single {
			t.Errorf("call %d binds one team_id for a selection of two", call)
		}
		if !teamScoped {
			if strings.Contains(statement, "team_id") || bound {
				t.Errorf("call %d (a repository-level read) carries a team: its metrics would not be the organisation's", call)
			}
			continue
		}
		if !strings.Contains(statement, "AND team_id IN {team_ids:Array(String)}") || !bound || !reflect.DeepEqual(ids, []string{"team-a", "team-b"}) {
			t.Errorf("call %d (%s): the selection is not bound (team_ids = %v)", call, table, ids)
		}
		if strings.Contains(statement, "team_id = {team_id:String}") {
			t.Errorf("call %d (%s): the one-team predicate is in a several-teams read", call, table)
		}
		// Each team's rows stay apart inside the read, as in the all-teams mode.
		if groupBy, grouped := map[string]string{
			"work_items":      "GROUP BY day, provider, work_scope_id, team_id",
			"state_durations": "GROUP BY day, provider, work_scope_id, status, team_id",
			"investment":      "GROUP BY day, repo_id, investment_area, project_stream, team_id",
		}[table]; grouped && !strings.Contains(statement, groupBy) {
			t.Errorf("call %d (%s): the inner read does not group by team (%q)", call, table, groupBy)
		}
	}
}

func TestResolveInput_TeamIdAndTeamIdsTogetherIsAnError(t *testing.T) {
	client := twoStoredWeeks()
	team := "team-a"
	_, err := ResolveInput(context.Background(), client, "org-1", &team, []string{"team-b"}, graphqldate.New(day("2026-08-24")))
	if !errors.Is(err, ErrBothTeamArguments) {
		t.Fatalf("err = %v, want ErrBothTeamArguments", err)
	}
	if client.calls != 0 {
		t.Errorf("%d read(s) for a refused request", client.calls)
	}
	// Blank ids are no selection: teamId then applies.
	if _, err := ResolveInput(context.Background(), twoStoredWeeks(), "org-1", &team, []string{" "}, graphqldate.New(day("2026-08-24"))); err != nil {
		t.Errorf("teamId with blank teamIds: %v", err)
	}
}

func scopesOf(review *model.OperatingReview) (organisation, team []string) {
	for _, section := range review.Sections {
		for _, m := range section.Metrics {
			if m.Scope == model.OperatingReviewMetricScopeOrganization {
				organisation = append(organisation, m.Key)
			} else {
				team = append(team, m.Key)
			}
		}
	}
	sort.Strings(organisation)
	sort.Strings(team)
	return organisation, team
}

// A metric is ORGANIZATION exactly when its value comes from the reads that
// carry no team: rows in those five tables alone give data to the nine
// ORGANIZATION metrics and to no other, and rows in the five team-scoped tables
// alone give data to the fifteen TEAM metrics and to no other.
func TestOrganisationMetricsAreTheOnesWhoseReadsHaveNoTeam(t *testing.T) {
	week := day("2026-08-24")
	organisationRows := periodRows{
		repoMetrics: []repoMetricsRow{{prFirstReviewP50Hours: fp(1), singleOwnerFileRatio30d: fp(0.5), changeFailureRate: fp(0.1), mttrHours: fp(2), busFactor: 2, storedRows: 1}},
		hotspots:    []hotspotsAggRow{{riskScore: fp(0.4), hotspotsCount: 1}},
		complexity:  []complexityAggRow{{cyclomaticPerKloc: fp(3)}},
		deployments: []deploymentsAggRow{{deploymentsCount: 2, storedRows: 1}},
		incidents:   []incidentsAggRow{{incidentsCount: 1, mttrP50Hours: fp(1), storedRows: 1}},
	}
	teamRows := periodRows{
		workItems:      []workItemsRow{{itemsCompleted: 1, cycleTimeP50Hours: fp(1), wipAgeP90Hours: fp(1)}},
		stateDurations: []stateDurationRow{{itemsTouched: 1, durationHours: 1}},
		investment:     []investmentRow{{investmentArea: "ktlo", deliveryUnits: 1}},
		aiImpact: []aiImpactRow{{prsTotal: 2, aiAssistedPrs: 1, aiCycleTimeDeltaHours: fp(1), aiReviewAmplification: fp(1),
			reworkDragRate: fp(0.1), testGapRate: fp(0.1), incidentDragRate: fp(0.1)}},
		aiGovernance: []aiGovernanceRawRow{{aiArtifacts: 1, declaredArtifacts: 1}},
	}
	wantOrganisation := sorted("review_latency_hours", "hotspot_risk_score", "ownership_concentration", "complexity_per_kloc",
		"bus_factor", "deployments_count", "change_failure_rate", "incidents_count", "mttr_hours")

	review := computeReview("org-1", nil, week, organisationRows, periodRows{})
	organisation, team := scopesOf(review)
	if !sameKeys(organisation, wantOrganisation) || len(team) != len(allMetricKeys)-len(wantOrganisation) {
		t.Fatalf("ORGANIZATION metrics = %v, want %v (and %d TEAM metrics, got %d)", organisation, wantOrganisation, len(allMetricKeys)-len(wantOrganisation), len(team))
	}
	if got := withData(metricsByKey(t, review), false); !sameKeys(got, wantOrganisation) {
		t.Errorf("rows in the five reads with no team give data to %v, want exactly the ORGANIZATION metrics %v", got, wantOrganisation)
	}
	if got := withData(metricsByKey(t, computeReview("org-1", nil, week, teamRows, periodRows{})), false); !sameKeys(got, team) {
		t.Errorf("rows in the five team-scoped reads give data to %v, want exactly the TEAM metrics %v", got, team)
	}
	// The scope does not depend on the data or on the selection.
	if o, _ := scopesOf(computeReview("org-1", nil, week, periodRows{}, periodRows{})); !sameKeys(o, wantOrganisation) {
		t.Errorf("an empty review marks %v", o)
	}
}

func valueOf(t *testing.T, rows periodRows, key string) float64 {
	t.Helper()
	return metricsByKey(t, computeReview("org-1", nil, day("2026-08-24"), rows, periodRows{}))[key].Value
}

// What the union gives, on rows built by hand as the store returns them for a
// selection of team A and team B, against what a mean or a sum of the two team
// values gives (the web's aggregate before this change).
func TestUnionOfTwoTeams_IsNotAMeanOfTeamValues(t *testing.T) {
	// Work items per day: team A has one day, team B has two.
	a := periodRows{workItems: []workItemsRow{{itemsCompleted: 3, wipCountEndOfDay: 4, cycleTimeP50Hours: fp(10)}}}
	b := periodRows{workItems: []workItemsRow{
		{itemsCompleted: 5, wipCountEndOfDay: 7, cycleTimeP50Hours: fp(30)},
		{itemsCompleted: 2, wipCountEndOfDay: 2, cycleTimeP50Hours: fp(30)},
	}}
	// The read groups by day over both teams' rows: day 1 holds both teams, day 2 team B only.
	union := periodRows{workItems: []workItemsRow{
		{itemsCompleted: 8, wipCountEndOfDay: 7, cycleTimeP50Hours: fp(20)},
		{itemsCompleted: 2, wipCountEndOfDay: 2, cycleTimeP50Hours: fp(30)},
	}}
	if got, sum := valueOf(t, union, "throughput"), valueOf(t, a, "throughput")+valueOf(t, b, "throughput"); got != 10 || got != sum {
		t.Errorf("throughput = %v, want the sum of the team counts %v", got, sum)
	}
	if got, sum := valueOf(t, union, "wip_count"), valueOf(t, a, "wip_count")+valueOf(t, b, "wip_count"); got != 7 || got == sum {
		t.Errorf("wip_count = %v: it is the largest stored WIP (7), not the sum of the team maxima (%v)", got, sum)
	}
	if got, mean := valueOf(t, union, "cycle_time_p50_hours"), (valueOf(t, a, "cycle_time_p50_hours")+valueOf(t, b, "cycle_time_p50_hours"))/2; got != 25 || got == mean {
		t.Errorf("cycle_time_p50_hours = %v: it is the mean over the stored days (25), not the mean of the team values (%v)", got, mean)
	}

	// AI adoption: a ratio from summed numerators and denominators.
	aiA := periodRows{aiImpact: []aiImpactRow{{prsTotal: 2, aiAssistedPrs: 1}}}
	aiB := periodRows{aiImpact: []aiImpactRow{{prsTotal: 100, aiAssistedPrs: 5, agentCreatedPrs: 4}}}
	aiUnion := periodRows{aiImpact: []aiImpactRow{{prsTotal: 102, aiAssistedPrs: 6, agentCreatedPrs: 4}}}
	got := valueOf(t, aiUnion, "ai_adoption_ratio")
	mean := (valueOf(t, aiA, "ai_adoption_ratio") + valueOf(t, aiB, "ai_adoption_ratio")) / 2
	if want := 10.0 / 102.0; got != want || got == mean {
		t.Errorf("ai_adoption_ratio = %v, want the summed ratio %v, not the mean of the team ratios %v", got, want, mean)
	}

	// State duration: weighted by the items touched.
	stA := periodRows{stateDurations: []stateDurationRow{{itemsTouched: 1, durationHours: 10}}}
	stB := periodRows{stateDurations: []stateDurationRow{{itemsTouched: 9, durationHours: 20}}}
	stUnion := periodRows{stateDurations: []stateDurationRow{{itemsTouched: 1, durationHours: 10}, {itemsTouched: 9, durationHours: 20}}}
	got = valueOf(t, stUnion, "state_duration_hours")
	mean = (valueOf(t, stA, "state_duration_hours") + valueOf(t, stB, "state_duration_hours")) / 2
	if got != 19 || got == mean {
		t.Errorf("state_duration_hours = %v, want the weighted mean 19, not the mean of the team values %v", got, mean)
	}

	// Investment units: a count, summed.
	invUnion := periodRows{investment: []investmentRow{{investmentArea: "ktlo", deliveryUnits: 7}}}
	invA := periodRows{investment: []investmentRow{{investmentArea: "ktlo", deliveryUnits: 3}}}
	invB := periodRows{investment: []investmentRow{{investmentArea: "ktlo", deliveryUnits: 4}}}
	if got, sum := valueOf(t, invUnion, "ktlo_units"), valueOf(t, invA, "ktlo_units")+valueOf(t, invB, "ktlo_units"); got != 7 || got != sum {
		t.Errorf("ktlo_units = %v, want the sum %v", got, sum)
	}
}
