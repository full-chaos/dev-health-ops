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
