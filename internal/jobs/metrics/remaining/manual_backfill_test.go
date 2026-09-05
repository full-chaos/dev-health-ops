package remaining

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestManualBackfillDayScopeMatchesFixedScheduleFanoutShape(t *testing.T) {
	cases := []struct {
		family string
		want   string
	}{
		{"dora", `{"version":1,"day":"2026-08-26","backfill_days":1,"sink":"auto","interval":"daily"}`},
		{"complexity", `{"version":1,"day":"2026-08-26","backfill_days":1}`},
		{"release_impact", `{"version":1,"day":"2026-08-26","backfill_days":1,"recomputation_window_days":7}`},
	}
	for _, testCase := range cases {
		raw, err := manualBackfillDayScope(testCase.family, "2026-08-26")
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", testCase.family, err)
		}
		// validateFamilyScope must accept exactly what manualBackfillDayScope
		// builds -- a scope this function produces that the store's own
		// validator rejects would fail StartManualBackfillRun for every day
		// of that family, silently, only once an operator actually runs the
		// command.
		if _, err := validateFamilyScope(testCase.family, raw); err != nil {
			t.Fatalf("%s: scope failed validateFamilyScope: %v (scope=%s)", testCase.family, err, raw)
		}
		var got, want any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(testCase.want), &want); err != nil {
			t.Fatal(err)
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("%s: scope = %s, want %s", testCase.family, gotJSON, wantJSON)
		}
	}
}

// TestManualBackfillDayScopeWorkItemAttributionIgnoresDay proves
// manualBackfillDayScope's work_item_attribution case (CHAOS-5016) always
// builds the SAME static scope producers.go's work_item_attribution_daily_fanout
// binding does, regardless of which day is passed -- day is used only as
// the caller's ScopeKey/dedup label, never embedded in the scope itself,
// because WorkItemAttributionExecutor.ComputeOrg takes only an org id and
// derives its own affected set from its own watermark.
func TestManualBackfillDayScopeWorkItemAttributionIgnoresDay(t *testing.T) {
	want := `{"version":1,"org_wide":true}`
	for _, day := range []string{"2026-08-26", "2026-01-01", "not-even-a-real-date"} {
		raw, err := manualBackfillDayScope("work_item_attribution", day)
		if err != nil {
			t.Fatalf("day=%q: unexpected error: %v", day, err)
		}
		var got, wantValue any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
			t.Fatal(err)
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(wantValue)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("day=%q: scope = %s, want %s", day, gotJSON, wantJSON)
		}
	}
	if _, err := validateFamilyScope("work_item_attribution", json.RawMessage(want)); err != nil {
		t.Fatalf("work_item_attribution scope failed validateFamilyScope: %v", err)
	}
}

// TestManualBackfillDayScopedFamiliesExcludesWorkItemAttribution guards the
// deliberate omission documented on ManualBackfillDayScopedFamilies: `start`
// assumes a "day" column on every family's readback table
// (manualBackfillReadbackTable, cmd/dev-health-workerctl/main.go), which is
// false for work_item_attribution. Adding it back to this list without also
// fixing that hint would silently reintroduce a broken readback query.
func TestManualBackfillDayScopedFamiliesExcludesWorkItemAttribution(t *testing.T) {
	for _, family := range ManualBackfillDayScopedFamilies {
		if family == "work_item_attribution" {
			t.Fatal("work_item_attribution must not be added to ManualBackfillDayScopedFamilies without also fixing `start`'s readback hint -- use ManualBackstopTriggerFamilies instead")
		}
	}
}

// TestManualBackstopTriggerFamiliesStayInSyncWithTheSwitch mirrors
// TestManualBackfillDayScopedFamiliesListStaysInSyncWithTheSwitch: every
// family ManualBackstopTriggerFamilies advertises must actually be
// dispatchable. work_item_attribution/complexity/dora/release_impact go
// through manualBackfillDayScope (dispatchMetricsRemainingTriggerBackstop
// calls StartManualBackfillRun for them); capacity/recommendations have
// their own scope builders (StartManualCapacityTriggerRun/
// StartManualRecommendationsTriggerRun) precisely because
// manualBackfillDayScope cannot express either (see
// TestManualBackfillDayScopeRejectsNonDayScopedFamilies) -- this test
// checks each family via whichever path is actually its own.
func TestManualBackstopTriggerFamiliesStayInSyncWithTheSwitch(t *testing.T) {
	dayScopeBuildable := map[string]bool{
		"work_item_attribution": true,
		"complexity":            true,
		"dora":                  true,
		"release_impact":        true,
	}
	for _, family := range ManualBackstopTriggerFamilies {
		switch {
		case dayScopeBuildable[family]:
			if _, err := manualBackfillDayScope(family, "2026-08-26"); err != nil {
				t.Fatalf("family %q is in ManualBackstopTriggerFamilies but manualBackfillDayScope rejects it: %v", family, err)
			}
		case family == "capacity":
			teamID := "eng-core"
			if _, err := manualCapacityTriggerScope(&teamID, false); err != nil {
				t.Fatalf("family %q: manualCapacityTriggerScope rejected a well-formed request: %v", family, err)
			}
		case family == "recommendations":
			if _, err := manualRecommendationsTriggerScope(nil, 14); err != nil {
				t.Fatalf("family %q: manualRecommendationsTriggerScope rejected a well-formed request: %v", family, err)
			}
		default:
			t.Fatalf("family %q is in ManualBackstopTriggerFamilies but this test has no dispatch path registered for it -- add one to dayScopeBuildable or a case below", family)
		}
	}
}

// TestManualBackstopTriggerFamiliesAllHaveAKnownReplayMode enforces that
// every family trigger-backstop accepts has a families.json `replay` mode
// this codebase has actually reviewed (team-lead ruling, CHAOS-5055): the
// verb applies ONE uniform flag policy to every family regardless of mode
// (--day defaults to yesterday, --today must be explicit, --review-evidence
// is always required) rather than forking behavior per mode -- this test
// exists so an unreviewed mode still fails loud, naming which families are
// append/replace/marker-based for a reader, not to gate dispatch on it.
func TestManualBackstopTriggerFamiliesAllHaveAKnownReplayMode(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	replayByFamily := make(map[string]string, len(inventory.Families))
	for _, family := range inventory.Families {
		replayByFamily[family.Name] = family.Replay
	}
	knownReplayModes := map[string]bool{
		"append_latest_generation":     true,
		"generation_replace":           true,
		"completion_marker_generation": true,
	}
	for _, family := range ManualBackstopTriggerFamilies {
		mode, ok := replayByFamily[family]
		if !ok {
			t.Fatalf("ManualBackstopTriggerFamilies contains %q, which has no families.json entry", family)
		}
		if !knownReplayModes[mode] {
			t.Fatalf("family %q has replay mode %q, not yet reviewed for trigger-backstop's uniform policy -- see this test's doc comment", family, mode)
		}
	}
}

func TestManualBackfillDayScopeRejectsNonDayScopedFamilies(t *testing.T) {
	// capacity, recommendations, and membership_backfill are real
	// remaining-metrics families (families.json) but none of them scope by
	// calendar day -- capacity needs a GenerationSeed the CLI's --day/--to
	// flags have no way to supply, and the other two scope by window/repo
	// set. A silent fallthrough here would let the command build a scope
	// validateFamilyScope then rejects with an opaque ErrInvalidState,
	// instead of the actionable ErrUnsupportedManualBackfillFamily.
	for _, family := range []string{"capacity", "recommendations", "membership_backfill", "not-a-real-family"} {
		if _, err := manualBackfillDayScope(family, "2026-08-26"); !errors.Is(err, ErrUnsupportedManualBackfillFamily) {
			t.Fatalf("family %q: got err=%v, want ErrUnsupportedManualBackfillFamily", family, err)
		}
	}
}

func TestManualBackfillDayScopedFamiliesListStaysInSyncWithTheSwitch(t *testing.T) {
	// ManualBackfillDayScopedFamilies is the CLI's --family allowlist;
	// manualBackfillDayScope is what actually builds a scope. A family added
	// to one without the other is either an operator-visible option that
	// always 500s (list ahead of switch) or a working family the CLI refuses
	// to offer (switch ahead of list) -- both are silent until someone tries
	// it.
	for _, family := range ManualBackfillDayScopedFamilies {
		if _, err := manualBackfillDayScope(family, "2026-08-26"); err != nil {
			t.Fatalf("family %q is in ManualBackfillDayScopedFamilies but manualBackfillDayScope rejects it: %v", family, err)
		}
	}
	for _, family := range []string{"dora", "complexity", "release_impact"} {
		found := false
		for _, listed := range ManualBackfillDayScopedFamilies {
			if listed == family {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("manualBackfillDayScope supports %q but it is missing from ManualBackfillDayScopedFamilies", family)
		}
	}
}
