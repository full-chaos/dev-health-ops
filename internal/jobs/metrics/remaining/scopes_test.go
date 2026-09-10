package remaining

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestFamilyScopesCanonicalizeProductionDerivedInputs(t *testing.T) {
	tests := map[string]string{
		"capacity":              `{"simulations":10000,"history_days":90,"all_teams":true,"version":1}`,
		"complexity":            `{"version":1,"day":"2026-07-23","backfill_days":1}`,
		"dora":                  `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily"}`,
		"release_impact":        `{"version":1,"day":"2026-07-23","backfill_days":1,"recomputation_window_days":7}`,
		"recommendations":       `{"version":1,"window":14}`,
		"membership_backfill":   `{"version":1,"repo_ids":[]}`,
		"work_item_attribution": `{"version":1,"org_wide":true}`,
	}
	for family, raw := range tests {
		t.Run(family, func(t *testing.T) {
			canonical, err := validateFamilyScope(family, json.RawMessage(raw))
			if err != nil || !json.Valid(canonical) {
				t.Fatalf("scope = %s err=%v", canonical, err)
			}
		})
	}
}

func TestFamilyScopesRejectUnknownFieldsAndBounds(t *testing.T) {
	for _, test := range []struct{ family, raw string }{
		{"capacity", `{"version":1,"all_teams":true,"history_days":0,"simulations":10000}`},
		{"capacity", `{"version":1,"all_teams":false,"work_scope_id":"","history_days":90,"simulations":10000}`},
		{"complexity", `{"version":1,"day":"2026-07-23","backfill_days":2}`},
		{"dora", `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"mongo","interval":"daily"}`},
		{"release_impact", `{"version":1,"day":"2026-07-23","backfill_days":1,"recomputation_window_days":31}`},
		{"recommendations", `{"version":1,"window":91}`},
		{"membership_backfill", `{"version":1,"command":"bad"}`},
		{"membership_backfill", `{"version":1,"repo_ids":["AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"]}`},
		{"membership_backfill", `{"version":1,"repo_ids":["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]}`},
		// org_wide=true combined with a scoped list is a contradiction --
		// the two are mutually exclusive by design (a repo/project change
		// scopes to that repo/project, an identities/teams change is
		// org-wide, never both in one partition).
		{"work_item_attribution", `{"version":1,"org_wide":true,"repo_ids":["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]}`},
		// Neither a scope nor org_wide set: nothing for this partition to do.
		{"work_item_attribution", `{"version":1}`},
		{"work_item_attribution", `{"version":1,"repo_ids":["not-a-uuid"]}`},
	} {
		if _, err := validateFamilyScope(test.family, json.RawMessage(test.raw)); err == nil {
			t.Fatalf("%s accepted %s", test.family, test.raw)
		}
	}
}

// TestDORAScopeRejectsUnknownMetricName pins CHAOS-5395: before this fix, a
// typo'd metric name in a "dora" scope's Metrics field (e.g.
// "lead_time_for_change" instead of "lead_time_for_changes") passed
// validateFamilyScope unchanged -- boundedOptional only checked the string's
// LENGTH, never its content against defaultDORAMetrics -- and reached
// metricFilter (dora_native.go) as a `wanted` set matching nothing
// ComputeDORA ever names, silently writing 0 rows with no error surfaced
// here and no log line (logPartitionDay is gated on written>0). The
// EXPECTATION pinned below is the CORRECT one (refuse loudly at validation
// time), not the defective one a green-first test would have encoded.
func TestDORAScopeRejectsUnknownMetricName(t *testing.T) {
	raw := `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily","metrics":"deployment_frequency,lead_time_for_change"}`
	_, err := validateFamilyScope("dora", json.RawMessage(raw))
	if err == nil {
		t.Fatal("dora scope with a typo'd metric name was accepted, want a refusal")
	}
	if !errors.Is(err, ErrUnknownDORAMetricName) {
		t.Fatalf("err = %v, want errors.Is(_, ErrUnknownDORAMetricName)", err)
	}
	if !strings.Contains(err.Error(), "lead_time_for_change") {
		t.Fatalf("err = %v, want it to name the offending metric", err)
	}
}

// TestDORAScopeAcceptsEveryDefaultMetricNameAndEmptyMetrics is the paired
// green control for TestDORAScopeRejectsUnknownMetricName: every name
// defaultDORAMetrics actually contains, and an absent Metrics field (the
// production default -- "compute everything"), must both still validate.
func TestDORAScopeAcceptsEveryDefaultMetricNameAndEmptyMetrics(t *testing.T) {
	base := `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily"}`
	if _, err := validateFamilyScope("dora", json.RawMessage(base)); err != nil {
		t.Fatalf("scope with no metrics field: %v", err)
	}
	withAll := `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily","metrics":"` +
		strings.Join(defaultDORAMetrics, ",") + `"}`
	if _, err := validateFamilyScope("dora", json.RawMessage(withAll)); err != nil {
		t.Fatalf("scope naming every default metric: %v", err)
	}
}

func TestFamilyScopesMatchSharedGolden(t *testing.T) {
	type goldenCase struct {
		Family    string          `json:"family"`
		Input     json.RawMessage `json:"input"`
		Canonical string          `json:"canonical"`
	}
	var fixture struct {
		SchemaVersion int          `json:"schema_version"`
		Cases         []goldenCase `json:"cases"`
	}
	raw, err := os.ReadFile("../../../../contracts/metrics/v1/remaining-scopes.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != ScopeVersion || len(fixture.Cases) != len(expectedFamilies) {
		t.Fatalf("fixture version=%d cases=%d", fixture.SchemaVersion, len(fixture.Cases))
	}
	seen := make(map[string]struct{}, len(fixture.Cases))
	for _, test := range fixture.Cases {
		t.Run(test.Family, func(t *testing.T) {
			if _, duplicate := seen[test.Family]; duplicate {
				t.Fatalf("duplicate family %q", test.Family)
			}
			seen[test.Family] = struct{}{}
			validated, err := validateFamilyScope(test.Family, test.Input)
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := canonicalJSON(validated)
			if err != nil {
				t.Fatal(err)
			}
			if string(canonical) != test.Canonical {
				t.Fatalf("canonical scope\n got: %s\nwant: %s", canonical, test.Canonical)
			}
		})
	}
	for _, family := range expectedFamilies {
		if _, ok := seen[family]; !ok {
			t.Errorf("shared golden omits family %q", family)
		}
	}
}
