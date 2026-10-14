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
		name   string
		in     Counts
		value  *float64
		state  State
		tier   string
		basis  Basis
		reason UnknownReason
	}{
		{"nothing", Counts{}, nil, StateNotApplicable, "", BasisNone, ReasonNoDeploymentsInWindow},
		{"incidents without a deployment", Counts{IncidentsDirect: 2}, nil, StateNotApplicable, "", BasisNone, ReasonNoDeploymentsInWindow},
		{"deployments without incident evidence", Counts{Deployments: 4}, nil, StateUnknown, "", BasisNone, ReasonDeploymentsWithoutOutcome},
		// A failed count without incident evidence still has no value and no tier.
		{"failed deployments without incident evidence", Counts{Deployments: 4, FailedNative: 1}, nil, StateUnknown, "", BasisNone, ReasonDeploymentsWithoutOutcome},
		{"deployments with a direct incident, none failed", Counts{Deployments: 4, IncidentsDirect: 1}, ptr(0), StateMeasured, "", BasisIncident, NotUnknown},
		{"deployments with a via-deployment incident", Counts{Deployments: 4, FailedHeuristic: 1, IncidentsViaDeployment: 1}, ptr(0.25), StateMeasured, TierHeuristic, BasisIncident, NotUnknown},
		{"native and heuristic both count", Counts{Deployments: 4, FailedNative: 1, FailedHeuristic: 1, IncidentsDirect: 1}, ptr(0.5), StateMeasured, TierHeuristic, BasisIncident, NotUnknown},
		{"native only", Counts{Deployments: 4, FailedNative: 1, IncidentsDirect: 1}, ptr(0.25), StateMeasured, TierNative, BasisIncident, NotUnknown},
		// CHAOS-9621: a deployment's own status is evidence. With statuses as
		// the only evidence the base of the rate is the deployments with a
		// final status.
		{"statuses only, all succeeded", Counts{Deployments: 4, StatusKnown: 4}, ptr(0), StateMeasured, "", BasisStatus, NotUnknown},
		{"statuses only, one failed", Counts{Deployments: 4, StatusKnown: 4, FailedStatus: 1}, ptr(0.25), StateMeasured, "", BasisStatus, NotUnknown},
		{"statuses only, the base is the final statuses", Counts{Deployments: 4, StatusKnown: 2, FailedStatus: 1}, ptr(0.5), StateMeasured, "", BasisStatus, NotUnknown},
		{"statuses only, a failed status needs no link tier", Counts{Deployments: 2, StatusKnown: 2, FailedStatus: 2}, ptr(1), StateMeasured, "", BasisStatus, NotUnknown},
		{"only unfinished statuses stay unknown", Counts{Deployments: 4}, nil, StateUnknown, "", BasisNone, ReasonDeploymentsWithoutOutcome},
		// With incident evidence the base is every deployment, as before.
		{"incident and status evidence", Counts{Deployments: 4, StatusKnown: 2, FailedNative: 1, FailedStatus: 1, IncidentsDirect: 1}, ptr(0.5), StateMeasured, TierNative, BasisBoth, NotUnknown},
		{"incident basis ignores unfinished deployments in no way", Counts{Deployments: 4, FailedNative: 1, IncidentsDirect: 1}, ptr(0.25), StateMeasured, TierNative, BasisIncident, NotUnknown},
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
		if got.Basis != tc.basis {
			t.Errorf("%s: basis %q, want %q", tc.name, got.Basis, tc.basis)
		}
		if got.UnknownReason != tc.reason {
			t.Errorf("%s: unknown reason %q, want %q", tc.name, got.UnknownReason, tc.reason)
		}
	}
}

// The three unknown answers stay apart in the rule itself (CHAOS-9621, for
// every reader and for acr): nothing collected, no deployment in the window,
// and deployments whose outcome was not collected. D, K and the coverage go
// out with every answer.
func TestApplyRuleKeepsTheThreeUnknownReasonsApart(t *testing.T) {
	for _, tc := range []struct {
		name         string
		in           RuleInputs
		reason       UnknownReason
		basis        Basis
		rate         *float64
		d, k         uint64
		coverageNil  bool
		coverageWant float64
	}{
		{"not collected: no stored row", RuleInputs{}, ReasonNotCollected, BasisNone, nil, 0, 0, true, 0},
		{"no deployments in the window: a stored row with none", RuleInputs{StoredRows: 3, IncidentEvidence: true}, ReasonNoDeploymentsInWindow, BasisNone, nil, 0, 0, true, 0},
		{"deployments without outcome", RuleInputs{StoredRows: 2, Deployments: 5}, ReasonDeploymentsWithoutOutcome, BasisNone, nil, 5, 0, false, 0},
		{"status basis divides by K", RuleInputs{StoredRows: 2, Deployments: 5, StatusKnown: 4, FailedStatusOnly: 1}, NotUnknown, BasisStatus, ptr(0.25), 5, 4, false, 0.8},
		{"incident basis divides by D", RuleInputs{StoredRows: 2, Deployments: 5, FailedIncidentLinked: 1, IncidentEvidence: true}, NotUnknown, BasisIncident, ptr(0.2), 5, 0, false, 0},
		{"both: the union divides by D", RuleInputs{StoredRows: 2, Deployments: 5, StatusKnown: 4, FailedStatusOnly: 1, FailedIncidentLinked: 1, IncidentEvidence: true}, NotUnknown, BasisBoth, ptr(0.4), 5, 4, false, 0.8},
	} {
		got := ApplyRule(tc.in)
		if got.UnknownReason != tc.reason || got.Basis != tc.basis || got.Deployments != tc.d || got.StatusKnown != tc.k {
			t.Errorf("%s: reason %q basis %q D %d K %d; want %q %q %d %d", tc.name, got.UnknownReason, got.Basis, got.Deployments, got.StatusKnown, tc.reason, tc.basis, tc.d, tc.k)
		}
		if (got.Rate == nil) != (tc.rate == nil) || (got.Rate != nil && *got.Rate != *tc.rate) {
			t.Errorf("%s: rate %v, want %v", tc.name, deref(got.Rate), deref(tc.rate))
		}
		if (got.Coverage == nil) != tc.coverageNil || (got.Coverage != nil && *got.Coverage != tc.coverageWant) {
			t.Errorf("%s: coverage %v", tc.name, deref(got.Coverage))
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
	if view != (View{Counts: Counts{Deployments: 1, FailedNative: 2, FailedHeuristic: 3, IncidentsDirect: 4, IncidentsViaDeployment: 5, FailedStatus: 6, StatusKnown: 7}, StoredRows: 8}) {
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
		{RepoID: repoA, DeploymentID: "d1"}, {RepoID: repoA, DeploymentID: "d2"}, {RepoID: repoA, DeploymentID: "d3"}, {RepoID: repoA, DeploymentID: "d1"}, // d1 twice: counted once
		{RepoID: repoB, DeploymentID: "b1"},
		{RepoID: repoA}, // no id: not a deployment
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
	want := []string{"deployments_count", "failed_deployments_native", "failed_deployments_heuristic", "incidents_direct", "incidents_via_deployment", "failed_deployments_status", "deployments_status_known"}
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

// CHAOS-9621: a deployment's own status, by value, with and without a link.
// Providers spell a failure differently (GitHub "failure", GitLab "failed"):
// every spelling is one failed meaning, a canceled deployment is not a
// deployment, and a failed deployment that an incident names counts once.
func TestCountDayStatusMeaningWithAndWithoutALink(t *testing.T) {
	link := Link{RepoID: repoA, DeploymentID: "d", IncidentID: "i", Source: TierHeuristic}
	tie := IncidentTie{RepoID: repoA, IncidentID: "i"}
	for _, tc := range []struct {
		status string
		// without a link
		bare Counts
		// with a linked incident
		linked Counts
	}{
		{"failure", Counts{Deployments: 1, FailedStatus: 1, StatusKnown: 1}, Counts{Deployments: 1, FailedHeuristic: 1, StatusKnown: 1, IncidentsDirect: 1}},
		{"failed", Counts{Deployments: 1, FailedStatus: 1, StatusKnown: 1}, Counts{Deployments: 1, FailedHeuristic: 1, StatusKnown: 1, IncidentsDirect: 1}},
		{"error", Counts{Deployments: 1, FailedStatus: 1, StatusKnown: 1}, Counts{Deployments: 1, FailedHeuristic: 1, StatusKnown: 1, IncidentsDirect: 1}},
		{"  FAILURE ", Counts{Deployments: 1, FailedStatus: 1, StatusKnown: 1}, Counts{Deployments: 1, FailedHeuristic: 1, StatusKnown: 1, IncidentsDirect: 1}},
		{"success", Counts{Deployments: 1, StatusKnown: 1}, Counts{Deployments: 1, FailedHeuristic: 1, StatusKnown: 1, IncidentsDirect: 1}},
		{"canceled", Counts{}, Counts{IncidentsDirect: 1}},
		{"cancelled", Counts{}, Counts{IncidentsDirect: 1}},
		{"inactive", Counts{Deployments: 1}, Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
		{"blocked", Counts{Deployments: 1}, Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
		{"in_progress", Counts{Deployments: 1}, Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
		{"", Counts{Deployments: 1}, Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
		{"weird", Counts{Deployments: 1}, Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
	} {
		deployments := []Deployment{{RepoID: repoA, DeploymentID: "d", Status: tc.status}}
		if got := CountDay(deployments, nil, nil)[repoA]; got != tc.bare {
			t.Errorf("status %q without a link: %+v, want %+v", tc.status, got, tc.bare)
		}
		if got := CountDay(deployments, []IncidentTie{tie}, []Link{link})[repoA]; got != tc.linked {
			t.Errorf("status %q with a linked incident: %+v, want %+v", tc.status, got, tc.linked)
		}
	}
}

// A repository with a canceled deployment only has nothing to count: no
// deployment, no incident, so no row.
func TestACanceledDeploymentAloneStoresNoRow(t *testing.T) {
	got := CountDay([]Deployment{{RepoID: repoA, DeploymentID: "d", Status: "canceled"}}, nil, nil)
	if len(got) != 0 {
		t.Fatalf("a canceled deployment alone counted %+v, want no row", got)
	}
}

// CHAOS-9621: a window that sums rows written before the status columns existed
// (both read 0 until the day is recomputed) and rows written after. K comes
// from the new rows only, and the coverage K / D shows the gap.
func TestAMixedWindowOfOldAndRecomputedRows(t *testing.T) {
	oldRow := Counts{Deployments: 3}                                  // written before migration 125
	newRow := Counts{Deployments: 3, StatusKnown: 3, FailedStatus: 1} // recomputed
	newWithIncident := Counts{Deployments: 3, StatusKnown: 3, FailedStatus: 1, FailedHeuristic: 1, IncidentsDirect: 1}
	oldWithIncident := Counts{Deployments: 3, FailedHeuristic: 3, IncidentsDirect: 1} // links only

	for _, tc := range []struct {
		name     string
		view     View
		value    *float64
		basis    Basis
		reason   UnknownReason
		d, k     uint64
		coverage *float64
	}{
		{"old rows only: nothing says how the deployments went", View{Counts: oldRow.Add(oldRow), StoredRows: 2}, nil, BasisNone, ReasonDeploymentsWithoutOutcome, 6, 0, ptr(0)},
		{"old and new rows, no incident: the rate stands on the new rows (1 of K = 3)", View{Counts: oldRow.Add(newRow), StoredRows: 2}, ptr(1.0 / 3.0), BasisStatus, NotUnknown, 6, 3, ptr(0.5)},
		{"old links and new statuses with an incident: the base is every deployment", View{Counts: oldWithIncident.Add(newWithIncident), StoredRows: 2}, ptr(5.0 / 6.0), BasisBoth, NotUnknown, 6, 3, ptr(0.5)},
		{"old rows with an incident only: as before the change", View{Counts: oldWithIncident, StoredRows: 1}, ptr(1), BasisIncident, NotUnknown, 3, 0, ptr(0)},
	} {
		got := Evaluate(tc.view)
		if (got.Value == nil) != (tc.value == nil) || (got.Value != nil && (*got.Value-*tc.value > 1e-12 || *tc.value-*got.Value > 1e-12)) {
			t.Errorf("%s: value %v, want %v", tc.name, deref(got.Value), deref(tc.value))
		}
		if got.Basis != tc.basis || got.UnknownReason != tc.reason || got.Deployments != tc.d || got.StatusKnown != tc.k {
			t.Errorf("%s: basis %q reason %q D %d K %d; want %q %q %d %d", tc.name, got.Basis, got.UnknownReason, got.Deployments, got.StatusKnown, tc.basis, tc.reason, tc.d, tc.k)
		}
		if (got.Coverage == nil) != (tc.coverage == nil) || (got.Coverage != nil && *got.Coverage != *tc.coverage) {
			t.Errorf("%s: coverage %v, want %v", tc.name, deref(got.Coverage), deref(tc.coverage))
		}
	}
}

// CHAOS-9630: the evidence a reader serves is read off the outcome of the one
// rule, whatever the state: an unknown rate keeps its reason and the counts that
// explain it, a measured one its basis.
func TestEvidenceNamesTheBasisTheCountsAndTheReasonWhateverTheState(t *testing.T) {
	str := func(v string) *string { return &v }
	cases := []struct {
		name     string
		in       Counts
		stored   uint64
		basis    *string
		d, k     uint64
		coverage *float64
		reason   *string
	}{
		{"nothing stored", Counts{}, 0, nil, 0, 0, nil, str("not_collected")},
		{"stored rows, no deployment", Counts{IncidentsDirect: 1}, 2, nil, 0, 0, nil, str("no_deployments_in_window")},
		{"deployments, no outcome", Counts{Deployments: 5}, 1, nil, 5, 0, ptr(0), str("deployments_without_outcome")},
		{"incident basis", Counts{Deployments: 5, IncidentsDirect: 1}, 1, str("incident"), 5, 0, ptr(0), nil},
		{"status basis", Counts{Deployments: 5, StatusKnown: 4, FailedStatus: 1}, 1, str("status"), 5, 4, ptr(0.8), nil},
		{"both bases", Counts{Deployments: 5, StatusKnown: 5, FailedNative: 1, IncidentsDirect: 1}, 1, str("both"), 5, 5, ptr(1), nil},
	}
	for _, tc := range cases {
		got := Evaluate(View{Counts: tc.in, StoredRows: tc.stored}).Evidence()
		if got == nil {
			t.Errorf("%s: no evidence", tc.name)
			continue
		}
		if (got.Basis == nil) != (tc.basis == nil) || (got.Basis != nil && *got.Basis != *tc.basis) {
			t.Errorf("%s: basis %v, want %v", tc.name, deref2(got.Basis), deref2(tc.basis))
		}
		if got.Deployments != tc.d || got.StatusKnown != tc.k {
			t.Errorf("%s: D %d K %d, want %d %d", tc.name, got.Deployments, got.StatusKnown, tc.d, tc.k)
		}
		if (got.Coverage == nil) != (tc.coverage == nil) || (got.Coverage != nil && *got.Coverage != *tc.coverage) {
			t.Errorf("%s: coverage %v, want %v", tc.name, deref(got.Coverage), deref(tc.coverage))
		}
		if (got.UnknownReason == nil) != (tc.reason == nil) || (got.UnknownReason != nil && *got.UnknownReason != *tc.reason) {
			t.Errorf("%s: unknown reason %v, want %v", tc.name, deref2(got.UnknownReason), deref2(tc.reason))
		}
	}
}

func deref2(v *string) string {
	if v == nil {
		return "<nil>"
	}
	return *v
}
