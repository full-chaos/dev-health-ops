package workitemmetrics

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// goldenFixture decodes tests/fixtures/daily_work_item_python_golden.json,
// produced by tests/fixtures/generate_daily_work_item_python_golden.py from
// REAL production Python (compute_work_item_metrics_daily,
// compute_estimate_coverage_metrics_daily, compute_work_item_team_attributions).
type goldenFixture struct {
	Day        string `json:"day"`
	ComputedAt string `json:"computed_at"`
	Items      []struct {
		WorkItemID  string   `json:"work_item_id"`
		Provider    string   `json:"provider"`
		Type        string   `json:"type"`
		Status      string   `json:"status"`
		WorkScopeID string   `json:"work_scope_id"`
		Assignees   []string `json:"assignees"`
		CreatedAt   string   `json:"created_at"`
		StartedAt   *string  `json:"started_at"`
		CompletedAt *string  `json:"completed_at"`
		ClosedAt    *string  `json:"closed_at"`
		StoryPoints *float64 `json:"story_points"`
	} `json:"items"`
	Transitions []struct {
		WorkItemID string `json:"work_item_id"`
		OccurredAt string `json:"occurred_at"`
		ToStatus   string `json:"to_status"`
	} `json:"transitions"`
	PrimaryAttributions []struct {
		WorkItemID string  `json:"work_item_id"`
		TeamID     *string `json:"team_id"`
		TeamName   *string `json:"team_name"`
		Source     string  `json:"source"`
	} `json:"primary_attributions"`
	MetricsDaily []struct {
		Day                      string   `json:"day"`
		Provider                 string   `json:"provider"`
		WorkScopeID              string   `json:"work_scope_id"`
		TeamID                   string   `json:"team_id"`
		TeamName                 string   `json:"team_name"`
		ItemsStarted             int      `json:"items_started"`
		ItemsCompleted           int      `json:"items_completed"`
		ItemsStartedUnassigned   int      `json:"items_started_unassigned"`
		ItemsCompletedUnassigned int      `json:"items_completed_unassigned"`
		WIPCountEndOfDay         int      `json:"wip_count_end_of_day"`
		WIPUnassignedEndOfDay    int      `json:"wip_unassigned_end_of_day"`
		CycleTimeP50Hours        *float64 `json:"cycle_time_p50_hours"`
		CycleTimeP90Hours        *float64 `json:"cycle_time_p90_hours"`
		LeadTimeP50Hours         *float64 `json:"lead_time_p50_hours"`
		LeadTimeP90Hours         *float64 `json:"lead_time_p90_hours"`
		WIPAgeP50Hours           *float64 `json:"wip_age_p50_hours"`
		WIPAgeP90Hours           *float64 `json:"wip_age_p90_hours"`
		BugCompletedRatio        float64  `json:"bug_completed_ratio"`
		StoryPointsCompleted     float64  `json:"story_points_completed"`
		NewBugsCount             int      `json:"new_bugs_count"`
		NewItemsCount            int      `json:"new_items_count"`
		DefectIntroRate          float64  `json:"defect_intro_rate"`
		WIPCongestionRatio       float64  `json:"wip_congestion_ratio"`
		PredictabilityScore      float64  `json:"predictability_score"`
	} `json:"work_item_metrics_daily"`
	UserMetricsDaily []struct {
		Day               string   `json:"day"`
		Provider          string   `json:"provider"`
		WorkScopeID       string   `json:"work_scope_id"`
		UserIdentity      string   `json:"user_identity"`
		TeamID            string   `json:"team_id"`
		TeamName          string   `json:"team_name"`
		ItemsStarted      int      `json:"items_started"`
		ItemsCompleted    int      `json:"items_completed"`
		WIPCountEndOfDay  int      `json:"wip_count_end_of_day"`
		CycleTimeP50Hours *float64 `json:"cycle_time_p50_hours"`
		CycleTimeP90Hours *float64 `json:"cycle_time_p90_hours"`
	} `json:"work_item_user_metrics_daily"`
	CycleTimes []struct {
		WorkItemID      string   `json:"work_item_id"`
		Provider        string   `json:"provider"`
		Day             string   `json:"day"`
		WorkScopeID     string   `json:"work_scope_id"`
		TeamID          string   `json:"team_id"`
		TeamName        string   `json:"team_name"`
		Assignee        *string  `json:"assignee"`
		Type            string   `json:"type"`
		Status          string   `json:"status"`
		CreatedAt       string   `json:"created_at"`
		StartedAt       *string  `json:"started_at"`
		CompletedAt     *string  `json:"completed_at"`
		CycleTimeHours  *float64 `json:"cycle_time_hours"`
		LeadTimeHours   *float64 `json:"lead_time_hours"`
		ActiveTimeHours *float64 `json:"active_time_hours"`
		WaitTimeHours   *float64 `json:"wait_time_hours"`
		FlowEfficiency  *float64 `json:"flow_efficiency"`
	} `json:"work_item_cycle_times"`
	EstimateCoverage []struct {
		Day              string   `json:"day"`
		Provider         string   `json:"provider"`
		WorkScopeID      string   `json:"work_scope_id"`
		TeamID           string   `json:"team_id"`
		TeamName         string   `json:"team_name"`
		EstimatedCount   int      `json:"estimated_count"`
		UnestimatedCount int      `json:"unestimated_count"`
		BacklogSize      int      `json:"backlog_size"`
		Ratio            *float64 `json:"ratio"`
	} `json:"estimate_coverage_metrics_daily"`
}

func goldenPath() string {
	return filepath.Join("..", "..", "..", "..", "tests", "fixtures", "daily_work_item_python_golden.json")
}

func loadGolden(t *testing.T) goldenFixture {
	t.Helper()
	data, err := os.ReadFile(goldenPath())
	if err != nil {
		t.Fatal(err)
	}
	var fixture goldenFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Items) == 0 {
		t.Fatal("golden fixture has no items")
	}
	return fixture
}

func parseGoldenTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed.UTC()
}

func parseGoldenTimePointer(t *testing.T, value *string) *time.Time {
	t.Helper()
	if value == nil {
		return nil
	}
	parsed := parseGoldenTime(t, *value)
	return &parsed
}

// goldenInputs rebuilds the compute's inputs from the fixture, INCLUDING the
// resolver -- which answers from the golden's own primary_attributions rows,
// exactly as the daily executor answers from work_item_team_attributions.
func goldenInputs(t *testing.T, fixture goldenFixture) ([]Item, []Transition, Resolver) {
	t.Helper()
	items := make([]Item, 0, len(fixture.Items))
	for index, row := range fixture.Items {
		items = append(items, Item{
			SourceIndex: index,
			WorkItemID:  row.WorkItemID,
			Provider:    row.Provider,
			Type:        row.Type,
			Status:      row.Status,
			Assignee:    FirstAssignee(row.Assignees),
			CreatedAt:   parseGoldenTime(t, row.CreatedAt),
			StartedAt:   parseGoldenTimePointer(t, row.StartedAt),
			CompletedAt: parseGoldenTimePointer(t, row.CompletedAt),
			ClosedAt:    parseGoldenTimePointer(t, row.ClosedAt),
			StoryPoints: row.StoryPoints,
		})
	}
	transitions := make([]Transition, 0, len(fixture.Transitions))
	for _, row := range fixture.Transitions {
		transitions = append(transitions, Transition{
			WorkItemID: row.WorkItemID,
			OccurredAt: parseGoldenTime(t, row.OccurredAt),
			ToStatus:   row.ToStatus,
		})
	}
	byID := make(map[string]struct{ teamID, teamName string }, len(fixture.PrimaryAttributions))
	for _, row := range fixture.PrimaryAttributions {
		byID[row.WorkItemID] = struct{ teamID, teamName string }{
			NormalizeTeamID(row.TeamID), NormalizeTeamName(row.TeamName),
		}
	}
	resolve := func(index int) Attribution {
		row := fixture.Items[index]
		team := byID[row.WorkItemID]
		if team.teamID == "" {
			team.teamID, team.teamName = UnassignedTeamID, UnassignedTeamName
		}
		return Attribution{WorkScopeID: row.WorkScopeID, TeamID: team.teamID, TeamName: team.teamName}
	}
	return items, transitions, resolve
}

// sameFloat is BIT-exact, not epsilon.
//
// An epsilon compare would hide precisely the class of defect this oracle
// exists to catch: a fused-multiply-add in the percentile lerp, or a
// compensated accumulation where Python performs a sequential one, moves the
// last bit and nothing else. Float64bits also separates -0.0 from 0.0, which
// `==` would call equal. NaN cannot appear (the generator writes with
// allow_nan=False, so a NaN would have failed generation, not comparison).
func sameFloat(left, right float64) bool {
	return math.Float64bits(left) == math.Float64bits(right)
}

func sameFloatPointer(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return sameFloat(*left, *right)
}

func sameStringPointer(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func formatFloatPointer(value *float64) string {
	if value == nil {
		return "<nil>"
	}
	return formatFloat(*value)
}

// formatFloat renders through encoding/json so a reported mismatch shows the
// shortest round-trip representation -- the same one the golden file holds --
// rather than %v's, which can print two different float64s identically.
func formatFloat(value float64) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<unencodable>"
	}
	return string(encoded)
}

// TestComputeDailyTripletMatchesPythonGolden is the differential oracle for the
// work_item family: the SAME corpus through production Python and through this
// package, compared row-exact.
func TestComputeDailyTripletMatchesPythonGolden(t *testing.T) {
	fixture := loadGolden(t)
	items, transitions, resolve := goldenInputs(t, fixture)
	day := parseGoldenTime(t, fixture.Day+"T00:00:00Z")

	triplet := ComputeDailyTriplet(day, items, transitions, resolve)

	if len(triplet.MetricsDaily) != len(fixture.MetricsDaily) {
		t.Fatalf("work_item_metrics_daily row count: got %d, python produced %d",
			len(triplet.MetricsDaily), len(fixture.MetricsDaily))
	}
	for index, want := range fixture.MetricsDaily {
		got := triplet.MetricsDaily[index]
		if got.Day.Format("2006-01-02") != want.Day || got.Provider != want.Provider ||
			got.WorkScopeID != want.WorkScopeID || got.TeamID != want.TeamID ||
			got.TeamName != want.TeamName {
			t.Errorf("metrics_daily[%d] identity: got (%s,%s,%s,%s,%s), want (%s,%s,%s,%s,%s)",
				index, got.Day.Format("2006-01-02"), got.Provider, got.WorkScopeID, got.TeamID, got.TeamName,
				want.Day, want.Provider, want.WorkScopeID, want.TeamID, want.TeamName)
			continue
		}
		label := want.WorkScopeID + "/" + want.TeamID
		checkInt(t, label, "items_started", got.ItemsStarted, want.ItemsStarted)
		checkInt(t, label, "items_completed", got.ItemsCompleted, want.ItemsCompleted)
		checkInt(t, label, "items_started_unassigned", got.ItemsStartedUnassigned, want.ItemsStartedUnassigned)
		checkInt(t, label, "items_completed_unassigned", got.ItemsCompletedUnassigned, want.ItemsCompletedUnassigned)
		checkInt(t, label, "wip_count_end_of_day", got.WIPCountEndOfDay, want.WIPCountEndOfDay)
		checkInt(t, label, "wip_unassigned_end_of_day", got.WIPUnassignedEndOfDay, want.WIPUnassignedEndOfDay)
		checkInt(t, label, "new_bugs_count", got.NewBugsCount, want.NewBugsCount)
		checkInt(t, label, "new_items_count", got.NewItemsCount, want.NewItemsCount)
		checkFloatPointer(t, label, "cycle_time_p50_hours", got.CycleTimeP50Hours, want.CycleTimeP50Hours)
		checkFloatPointer(t, label, "cycle_time_p90_hours", got.CycleTimeP90Hours, want.CycleTimeP90Hours)
		checkFloatPointer(t, label, "lead_time_p50_hours", got.LeadTimeP50Hours, want.LeadTimeP50Hours)
		checkFloatPointer(t, label, "lead_time_p90_hours", got.LeadTimeP90Hours, want.LeadTimeP90Hours)
		checkFloatPointer(t, label, "wip_age_p50_hours", got.WIPAgeP50Hours, want.WIPAgeP50Hours)
		checkFloatPointer(t, label, "wip_age_p90_hours", got.WIPAgeP90Hours, want.WIPAgeP90Hours)
		checkFloat(t, label, "bug_completed_ratio", got.BugCompletedRatio, want.BugCompletedRatio)
		checkFloat(t, label, "story_points_completed", got.StoryPointsCompleted, want.StoryPointsCompleted)
		checkFloat(t, label, "defect_intro_rate", got.DefectIntroRate, want.DefectIntroRate)
		checkFloat(t, label, "wip_congestion_ratio", got.WIPCongestionRatio, want.WIPCongestionRatio)
		checkFloat(t, label, "predictability_score", got.PredictabilityScore, want.PredictabilityScore)
	}

	if len(triplet.UserMetricsDaily) != len(fixture.UserMetricsDaily) {
		t.Fatalf("work_item_user_metrics_daily row count: got %d, python produced %d",
			len(triplet.UserMetricsDaily), len(fixture.UserMetricsDaily))
	}
	for index, want := range fixture.UserMetricsDaily {
		got := triplet.UserMetricsDaily[index]
		label := want.WorkScopeID + "/" + want.UserIdentity + "/" + want.TeamID
		if got.Day.Format("2006-01-02") != want.Day || got.Provider != want.Provider ||
			got.WorkScopeID != want.WorkScopeID || got.UserIdentity != want.UserIdentity ||
			got.TeamID != want.TeamID || got.TeamName != want.TeamName {
			t.Errorf("user_metrics_daily[%d] identity mismatch: got %+v, want %+v", index, got, want)
			continue
		}
		checkInt(t, label, "items_started", got.ItemsStarted, want.ItemsStarted)
		checkInt(t, label, "items_completed", got.ItemsCompleted, want.ItemsCompleted)
		checkInt(t, label, "wip_count_end_of_day", got.WIPCountEndOfDay, want.WIPCountEndOfDay)
		checkFloatPointer(t, label, "cycle_time_p50_hours", got.CycleTimeP50Hours, want.CycleTimeP50Hours)
		checkFloatPointer(t, label, "cycle_time_p90_hours", got.CycleTimeP90Hours, want.CycleTimeP90Hours)
	}

	if len(triplet.CycleTimes) != len(fixture.CycleTimes) {
		t.Fatalf("work_item_cycle_times row count: got %d, python produced %d",
			len(triplet.CycleTimes), len(fixture.CycleTimes))
	}
	for index, want := range fixture.CycleTimes {
		got := triplet.CycleTimes[index]
		label := want.WorkItemID
		if got.WorkItemID != want.WorkItemID || got.Provider != want.Provider ||
			got.Day.Format("2006-01-02") != want.Day || got.WorkScopeID != want.WorkScopeID ||
			got.TeamID != want.TeamID || got.TeamName != want.TeamName ||
			got.Type != want.Type || got.Status != want.Status {
			t.Errorf("cycle_times[%d] identity mismatch: got %+v, want %+v", index, got, want)
			continue
		}
		if !sameStringPointer(got.Assignee, want.Assignee) {
			t.Errorf("cycle_times[%s] assignee: got %v, want %v", label, got.Assignee, want.Assignee)
		}
		checkTime(t, label, "created_at", got.CreatedAt, parseGoldenTime(t, want.CreatedAt))
		checkTimePointer(t, label, "started_at", got.StartedAt, parseGoldenTimePointer(t, want.StartedAt))
		checkTimePointer(t, label, "completed_at", got.CompletedAt, parseGoldenTimePointer(t, want.CompletedAt))
		checkFloatPointer(t, label, "cycle_time_hours", got.CycleTimeHours, want.CycleTimeHours)
		checkFloatPointer(t, label, "lead_time_hours", got.LeadTimeHours, want.LeadTimeHours)
		// The three flow fields are computed by BOTH sides and dropped by both
		// sinks -- compared here precisely because no persisted column ever
		// would, so a divergence in _calculate_flow_breakdown is otherwise
		// invisible.
		checkFloatPointer(t, label, "active_time_hours", got.ActiveTimeHours, want.ActiveTimeHours)
		checkFloatPointer(t, label, "wait_time_hours", got.WaitTimeHours, want.WaitTimeHours)
		checkFloatPointer(t, label, "flow_efficiency", got.FlowEfficiency, want.FlowEfficiency)
	}
}

// TestComputeEstimateCoverageMatchesPythonGolden is the differential oracle for
// the work_item_estimate family.
func TestComputeEstimateCoverageMatchesPythonGolden(t *testing.T) {
	fixture := loadGolden(t)
	items, _, resolve := goldenInputs(t, fixture)
	day := parseGoldenTime(t, fixture.Day+"T00:00:00Z")

	rows := ComputeEstimateCoverage(day, items, resolve)
	if len(rows) != len(fixture.EstimateCoverage) {
		t.Fatalf("estimate_coverage_metrics_daily row count: got %d, python produced %d",
			len(rows), len(fixture.EstimateCoverage))
	}
	for index, want := range fixture.EstimateCoverage {
		got := rows[index]
		label := want.WorkScopeID + "/" + want.TeamID
		if got.Day.Format("2006-01-02") != want.Day || got.Provider != want.Provider ||
			got.WorkScopeID != want.WorkScopeID || got.TeamID != want.TeamID ||
			got.TeamName != want.TeamName {
			t.Errorf("estimate_coverage[%d] identity mismatch: got %+v, want %+v", index, got, want)
			continue
		}
		checkInt(t, label, "estimated_count", got.EstimatedCount, want.EstimatedCount)
		checkInt(t, label, "unestimated_count", got.UnestimatedCount, want.UnestimatedCount)
		checkInt(t, label, "backlog_size", got.BacklogSize, want.BacklogSize)
		checkFloatPointer(t, label, "ratio", got.Ratio, want.Ratio)
	}
}

// TestGoldenCorpusDiscriminatesCompensatedSummation is a VACUITY guard on the
// oracle above, not an independent assertion about the port.
//
// compute_work_items.py accumulates story_points_completed with a plain `+=`
// (:1226). CPython's sum() has been Neumaier-compensated since 3.12, so a Go
// port that "helpfully" compensated would be wrong -- but only on inputs where
// the two disagree. This asserts the corpus actually CONTAINS such an input:
// without it, both TestComputeDailyTripletMatchesPythonGolden and the
// production code could switch to compensated summation and stay green.
func TestGoldenCorpusDiscriminatesCompensatedSummation(t *testing.T) {
	fixture := loadGolden(t)
	for _, row := range fixture.MetricsDaily {
		var sequential, compensated, correction float64
		var summands int
		for _, item := range fixture.Items {
			if item.StoryPoints == nil || item.WorkScopeID != row.WorkScopeID {
				continue
			}
			summands++
			sequential += *item.StoryPoints
			// Neumaier
			total := compensated + *item.StoryPoints
			if math.Abs(compensated) >= math.Abs(*item.StoryPoints) {
				correction += (compensated - total) + *item.StoryPoints
			} else {
				correction += (*item.StoryPoints - total) + compensated
			}
			compensated = total
		}
		if summands >= 3 && !sameFloat(sequential, compensated+correction) {
			t.Logf("discriminating group %q: sequential=%s compensated=%s (%d summands)",
				row.WorkScopeID, formatFloat(sequential), formatFloat(compensated+correction), summands)
			return
		}
	}
	t.Fatal(
		"the golden corpus contains NO group where a Neumaier-compensated sum " +
			"of story_points disagrees with the sequential one Python performs. " +
			"Without such a group the story_points_completed assertion cannot " +
			"tell the two accumulation strategies apart, and the oracle is " +
			"vacuous for this field. Restore a group with >=3 fractional " +
			"summands chosen to disagree (see generate_daily_work_item_python_" +
			"golden.py's acme/fma case).",
	)
}

func checkInt(t *testing.T, label, field string, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("%s %s: got %d, python produced %d", label, field, got, want)
	}
}

func checkFloat(t *testing.T, label, field string, got, want float64) {
	t.Helper()
	if !sameFloat(got, want) {
		t.Errorf("%s %s: got %s, python produced %s (bit-exact compare)",
			label, field, formatFloat(got), formatFloat(want))
	}
}

func checkFloatPointer(t *testing.T, label, field string, got, want *float64) {
	t.Helper()
	if !sameFloatPointer(got, want) {
		t.Errorf("%s %s: got %s, python produced %s (bit-exact compare)",
			label, field, formatFloatPointer(got), formatFloatPointer(want))
	}
}

func checkTime(t *testing.T, label, field string, got, want time.Time) {
	t.Helper()
	if !got.Equal(want) {
		t.Errorf("%s %s: got %s, python produced %s", label, field, got, want)
	}
}

func checkTimePointer(t *testing.T, label, field string, got, want *time.Time) {
	t.Helper()
	if got == nil || want == nil {
		if got != nil || want != nil {
			t.Errorf("%s %s: got %v, python produced %v", label, field, got, want)
		}
		return
	}
	checkTime(t, label, field, *got, *want)
}

// TestAssertAlignedRejectsAReorderedProjection is the regression test for
// codex r1's P2 (cardinality-only) AND r2's P2 (id-keyed, which accepted a
// reordering between two source rows sharing an id).
//
// The projection here carries SourceIndex values that do not match their
// positions, which is what a reordering actually is.
func TestAssertAlignedRejectsAReorderedProjection(t *testing.T) {
	reordered := []Item{{WorkItemID: "B", SourceIndex: 1}, {WorkItemID: "A", SourceIndex: 0}}
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal(
				"AssertAligned accepted a REORDERED projection. Every resolver answer " +
					"would attribute one work item using another's ownership.")
		}
		if message, ok := recovered.(string); ok && !strings.Contains(message, "position 0") {
			t.Errorf("panic did not name the diverging position: %s", message)
		}
	}()
	AssertAligned(2, reordered, func(int) Attribution { return Attribution{} })
}

// TestAssertAlignedRejectsAReorderedDuplicateIDProjection is codex r2's exact
// construction, and the reason this guard keys on SourceIndex rather than on
// work_item_id.
//
// The Resolver contract chose INDEXES precisely so two source rows may share an
// id. An id-keyed check therefore compares equal at every position for
// [dup, dup] while the resolver reads the wrong row -- executed against the
// previous version as "AssertAligned duplicate reorder accepted: true". Only an
// index check distinguishes them.
func TestAssertAlignedRejectsAReorderedDuplicateIDProjection(t *testing.T) {
	swapped := []Item{{WorkItemID: "dup", SourceIndex: 1}, {WorkItemID: "dup", SourceIndex: 0}}
	defer func() {
		if recover() == nil {
			t.Fatal(
				"AssertAligned accepted a reordering between two DISTINCT source rows " +
					"sharing one work_item_id -- the exact case an id-keyed check cannot see")
		}
	}()
	AssertAligned(2, swapped, func(int) Attribution { return Attribution{} })
}

// TestAssertAlignedAcceptsAFaithfulProjection is the positive control: without
// it, a guard that panicked unconditionally would pass every test above and be
// indistinguishable from a correct one. Duplicate ids are FINE when order is
// preserved -- that is the contract.
func TestAssertAlignedAcceptsAFaithfulProjection(t *testing.T) {
	faithful := []Item{{WorkItemID: "dup", SourceIndex: 0}, {WorkItemID: "dup", SourceIndex: 1}}
	AssertAligned(2, faithful, func(int) Attribution { return Attribution{} })
}

// TestAssertAlignedRejectsALengthMismatch keeps the cardinality arm covered --
// the index check must not have replaced it.
func TestAssertAlignedRejectsALengthMismatch(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("AssertAligned accepted a projection that dropped a row")
		}
	}()
	AssertAligned(2, []Item{{WorkItemID: "A", SourceIndex: 0}}, func(int) Attribution { return Attribution{} })
}
