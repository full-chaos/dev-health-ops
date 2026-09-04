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
// buildable by manualBackfillDayScope, since
// dispatchMetricsRemainingTriggerBackstop calls StartManualBackfillRun
// (which calls manualBackfillDayScope internally) for whatever family it is
// given.
func TestManualBackstopTriggerFamiliesStayInSyncWithTheSwitch(t *testing.T) {
	for _, family := range ManualBackstopTriggerFamilies {
		if _, err := manualBackfillDayScope(family, "2026-08-26"); err != nil {
			t.Fatalf("family %q is in ManualBackstopTriggerFamilies but manualBackfillDayScope rejects it: %v", family, err)
		}
	}
}

// TestManualBackstopTriggerFamiliesNeverIncludesADayScopedRaceFamily is the
// executable form of ManualBackstopTriggerFamilies' own doc comment: dora,
// complexity, and release_impact must never appear here, because this list
// is what lets `metrics remaining trigger-backstop` target --today, and
// dora's automatic triggers racing a same-day manual dispatch is a real,
// previously-fixed defect (see ManualBackfillDayScopedFamilies' "start"
// today-exclusion). A family belongs in this list only after the same
// no-day-window/watermark-driven/re-entrant review documented there.
func TestManualBackstopTriggerFamiliesNeverIncludesADayScopedRaceFamily(t *testing.T) {
	for _, family := range ManualBackstopTriggerFamilies {
		for _, dayScoped := range []string{"dora", "complexity", "release_impact"} {
			if family == dayScoped {
				t.Fatalf("ManualBackstopTriggerFamilies must never include %q -- it has a proven same-day automatic-trigger race (see the today-exclusion in cmd/dev-health-workerctl's `start` verb)", dayScoped)
			}
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
