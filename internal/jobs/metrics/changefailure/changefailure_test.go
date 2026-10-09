package changefailure

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

var (
	repoA = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	repoB = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
)

func ptr(v float64) *float64 { return &v }

func TestRateKeepsNotApplicableUnknownAndMeasuredZeroApart(t *testing.T) {
	cases := []struct {
		name  string
		in    Counts
		value *float64
		state State
		tier  string
	}{
		{"nothing", Counts{}, nil, StateNotApplicable, ""},
		{"incidents without a deployment", Counts{IncidentsDirect: 2}, nil, StateNotApplicable, ""},
		{"deployments without incident evidence", Counts{Deployments: 4}, nil, StateUnknown, ""},
		// A failed count without incident evidence still has no value and no tier.
		{"failed deployments without incident evidence", Counts{Deployments: 4, FailedNative: 1}, nil, StateUnknown, ""},
		{"deployments with a direct incident, none failed", Counts{Deployments: 4, IncidentsDirect: 1}, ptr(0), StateMeasured, ""},
		{"deployments with a via-deployment incident", Counts{Deployments: 4, FailedHeuristic: 1, IncidentsViaDeployment: 1}, ptr(0.25), StateMeasured, TierHeuristic},
		{"native and heuristic both count", Counts{Deployments: 4, FailedNative: 1, FailedHeuristic: 1, IncidentsDirect: 1}, ptr(0.5), StateMeasured, TierHeuristic},
		{"native only", Counts{Deployments: 4, FailedNative: 1, IncidentsDirect: 1}, ptr(0.25), StateMeasured, TierNative},
	}
	for _, tc := range cases {
		got := Rate(tc.in)
		if got.State != tc.state {
			t.Errorf("%s: state %s, want %s", tc.name, got.State, tc.state)
		}
		if (got.Value == nil) != (tc.value == nil) || (got.Value != nil && *got.Value != *tc.value) {
			t.Errorf("%s: value %v, want %v", tc.name, deref(got.Value), deref(tc.value))
		}
		if got.LinkTier != tc.tier {
			t.Errorf("%s: link tier %q, want %q", tc.name, got.LinkTier, tc.tier)
		}
	}
}

func deref(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

// Evaluate is what a reader serves: a view with no stored row has no state
// (served as null), and that is not "not applicable", which needs stored
// counts with no deployment. The three empty answers stay apart.
func TestEvaluateKeepsNoStoredCountsApartFromNotApplicableAndUnknown(t *testing.T) {
	served := func(o Outcome) string {
		if o.StateOrNil() == nil {
			return "null"
		}
		return *o.StateOrNil()
	}
	for _, tc := range []struct {
		name      string
		view      View
		state     string
		measured  bool
		wantValue float64
		tier      string
	}{
		{"no stored row", View{}, "null", false, 0, "null"},
		// Counts without a stored row cannot be: the row count decides.
		{"no stored row wins over counts", View{Counts: Counts{Deployments: 3, IncidentsDirect: 1}}, "null", false, 0, "null"},
		{"stored rows of zeros (a retraction)", View{StoredRows: 2}, "not_applicable_no_deployments", false, 0, "null"},
		{"stored rows, an incident, no deployment", View{Counts: Counts{IncidentsDirect: 1}, StoredRows: 1}, "not_applicable_no_deployments", false, 0, "null"},
		{"stored rows, deployments, no incident", View{Counts: Counts{Deployments: 3}, StoredRows: 1}, "unknown_no_incident_evidence", false, 0, "null"},
		{"measured zero", View{Counts: Counts{Deployments: 3, IncidentsDirect: 1}, StoredRows: 2}, "measured", true, 0, "null"},
		{"measured", View{Counts: Counts{Deployments: 4, FailedHeuristic: 1, IncidentsDirect: 1}, StoredRows: 2}, "measured", true, 0.25, "heuristic"},
	} {
		got := Evaluate(tc.view)
		if served(got) != tc.state {
			t.Errorf("%s: state %s, want %s", tc.name, served(got), tc.state)
		}
		if (got.Value != nil) != tc.measured || (got.Value != nil && *got.Value != tc.wantValue) {
			t.Errorf("%s: value %v, want measured %v value %v", tc.name, deref(got.Value), tc.measured, tc.wantValue)
		}
		tier := "null"
		if got.LinkTierOrNil() != nil {
			tier = *got.LinkTierOrNil()
		}
		if tier != tc.tier {
			t.Errorf("%s: link tier %s, want %s", tc.name, tier, tc.tier)
		}
	}
}

// ViewSumsSQL and ViewScanDest are one contract: the columns in CountColumns
// order, then the row count.
func TestViewScanDestMatchesViewSumsSQL(t *testing.T) {
	var view View
	dest := ViewScanDest(&view)
	if len(dest) != len(CountColumns)+1 {
		t.Fatalf("%d scan destinations for %d count columns and the row count", len(dest), len(CountColumns))
	}
	for i := range dest {
		*(dest[i].(*uint64)) = uint64(i + 1)
	}
	if view != (View{Counts: Counts{Deployments: 1, FailedNative: 2, FailedHeuristic: 3, IncidentsDirect: 4, IncidentsViaDeployment: 5}, StoredRows: 6}) {
		t.Fatalf("scan order = %+v", view)
	}
	position := 0
	for _, column := range append(append([]string{}, CountColumns...), "count()") {
		at := strings.Index(ViewSumsSQL[position:], column)
		if at < 0 {
			t.Fatalf("ViewSumsSQL does not select %s after position %d: %s", column, position, ViewSumsSQL)
		}
		position += at + len(column)
	}
}

// The window rate is the ratio of the summed counts, never the average of
// daily rates: 1/1 and 0/9 is 0.1, not 0.5.
func TestRateOfSummedDaysIsNotTheAverageOfDailyRates(t *testing.T) {
	day1 := Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}
	day2 := Counts{Deployments: 9, IncidentsDirect: 1}
	got := Rate(day1.Add(day2))
	if got.State != StateMeasured || got.Value == nil || *got.Value != 0.1 {
		t.Fatalf("window rate %v (%s), want 0.1", deref(got.Value), got.State)
	}
}

func TestLowestTierNamesTheWeakestContributingLink(t *testing.T) {
	for _, tc := range []struct {
		in   Counts
		want string
	}{
		{Counts{}, ""},
		{Counts{FailedNative: 2}, TierNative},
		{Counts{FailedNative: 2, FailedHeuristic: 1}, TierHeuristic},
		{Counts{FailedHeuristic: 1}, TierHeuristic},
	} {
		if got := LowestTier(tc.in); got != tc.want {
			t.Errorf("LowestTier(%+v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCountDayTiersTiesAndDeploymentScope(t *testing.T) {
	deployments := []Deployment{
		{repoA, "d1"}, {repoA, "d2"}, {repoA, "d3"}, {repoA, "d1"}, // d1 twice: counted once
		{repoB, "b1"},
		{repoA, ""}, // no id: not a deployment
	}
	ties := []IncidentTie{
		{RepoID: repoA, IncidentID: "i-direct"},
		{RepoID: repoA, IncidentID: "i-direct"}, // repeated tie counted once
		// i-direct has a direct tie, so its via tie to B is ignored.
		{RepoID: repoB, IncidentID: "i-direct", ViaDeployment: true},
		// i-via has no direct tie anywhere: it ties to B through its deployment.
		{RepoID: repoB, IncidentID: "i-via", ViaDeployment: true},
	}
	links := []Link{
		{RepoID: repoA, DeploymentID: "d1", IncidentID: "i-direct", Source: TierHeuristic},
		{RepoID: repoA, DeploymentID: "d1", IncidentID: "i-direct", Source: TierNative},          // native wins for d1
		{RepoID: repoA, DeploymentID: "d1", IncidentID: "i-direct", Source: TierHeuristic},       // and stays, whatever the order
		{RepoID: repoA, DeploymentID: "d2", IncidentID: "i-direct", Source: "unexpected"},        // never native
		{RepoID: repoA, DeploymentID: "d-other-day", IncidentID: "i-direct", Source: TierNative}, // not deployed today
		{RepoID: repoA, DeploymentID: "d3", IncidentID: "i-untied", Source: TierNative},          // incident ties nowhere
		{RepoID: repoB, DeploymentID: "b1", IncidentID: "i-via", Source: TierHeuristic},
	}
	got := CountDay(deployments, ties, links)
	want := map[uuid.UUID]Counts{
		repoA: {Deployments: 3, FailedNative: 1, FailedHeuristic: 1, IncidentsDirect: 1},
		repoB: {Deployments: 1, FailedHeuristic: 1, IncidentsViaDeployment: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("CountDay = %+v, want %+v", got, want)
	}
	for repo, counts := range want {
		if got[repo] != counts {
			t.Errorf("repo %s: %+v, want %+v", repo, got[repo], counts)
		}
	}
}

// A repository with nothing to count is absent, never a zero-filled entry.
func TestCountDayOmitsRepositoriesWithNothingToCount(t *testing.T) {
	got := CountDay(nil, nil, []Link{{RepoID: repoA, DeploymentID: "d1", IncidentID: "i", Source: TierNative}})
	if len(got) != 0 {
		t.Fatalf("CountDay = %+v, want no repository", got)
	}
	if !(Counts{}).Empty() {
		t.Fatal("Empty must be true without a deployment and without an incident")
	}
	// Each of the three things a row is stored for, alone, makes it not empty.
	// The failed counts do not: a failed deployment is always a deployment.
	for name, counts := range map[string]Counts{
		"a deployment":              {Deployments: 1},
		"a direct incident":         {IncidentsDirect: 1},
		"a via-deployment incident": {IncidentsViaDeployment: 1},
	} {
		if counts.Empty() {
			t.Errorf("Counts with only %s is Empty: its row would not be stored", name)
		}
	}
}

// Writers and readers name the table by literal in their SQL; the constant
// must stay equal to it.
func TestTableAndCountColumnsNameTheMigratedTable(t *testing.T) {
	if Table != "repo_change_failure_daily" {
		t.Fatalf("Table = %q", Table)
	}
	want := []string{"deployments_count", "failed_deployments_native", "failed_deployments_heuristic", "incidents_direct", "incidents_via_deployment"}
	if len(CountColumns) != len(want) {
		t.Fatalf("CountColumns = %v, want %v", CountColumns, want)
	}
	for i, column := range want {
		if CountColumns[i] != column {
			t.Fatalf("CountColumns = %v, want %v", CountColumns, want)
		}
	}
}

// A named limit (CHAOS-9019), pinned so that a change to it is a decision: a
// link counts a failed deployment only on the day that holds both its
// deployment and its incident. A deployment late on one day whose incident
// starts early on the next is counted as a deployment on the first day and as
// incident evidence on the second, and as a failed deployment on neither, so
// the two-day window reads a measured 0. No producer writes such a link
// today: the daily producer links an incident to the deployments of its own
// day.
func TestALinkAcrossTwoDaysCountsNoFailedDeployment(t *testing.T) {
	deployment := Deployment{RepoID: repoA, DeploymentID: "late"}
	incident := IncidentTie{RepoID: repoA, IncidentID: "early-next-day"}
	link := Link{RepoID: repoA, DeploymentID: "late", IncidentID: "early-next-day", Source: TierNative}

	dayOne := CountDay([]Deployment{deployment}, nil, []Link{link})[repoA]
	dayTwo := CountDay(nil, []IncidentTie{incident}, []Link{link})[repoA]
	if dayOne != (Counts{Deployments: 1}) || dayTwo != (Counts{IncidentsDirect: 1}) {
		t.Fatalf("day one %+v, day two %+v; want one deployment, then one direct incident", dayOne, dayTwo)
	}
	window := Rate(dayOne.Add(dayTwo))
	if window.State != StateMeasured || window.Value == nil || *window.Value != 0 || window.LinkTier != "" {
		t.Fatalf("two-day window = %+v, want a measured 0 with no tier", window)
	}
	// The same link inside one day counts.
	sameDay := Rate(CountDay([]Deployment{deployment}, []IncidentTie{incident}, []Link{link})[repoA])
	if sameDay.Value == nil || *sameDay.Value != 1 || sameDay.LinkTier != TierNative {
		t.Fatalf("same-day control = %+v, want 1 with a native tier", sameDay)
	}
}
