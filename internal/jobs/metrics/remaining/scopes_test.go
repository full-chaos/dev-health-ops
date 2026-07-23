package remaining

import (
	"encoding/json"
	"os"
	"testing"
)

func TestFamilyScopesCanonicalizeProductionDerivedInputs(t *testing.T) {
	tests := map[string]string{
		"capacity":            `{"simulations":10000,"history_days":90,"all_teams":true,"version":1}`,
		"complexity":          `{"version":1,"day":"2026-07-23","backfill_days":1}`,
		"dora":                `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","interval":"daily"}`,
		"release_impact":      `{"version":1,"day":"2026-07-23","backfill_days":1,"recomputation_window_days":7}`,
		"recommendations":     `{"version":1,"window":14}`,
		"membership_backfill": `{"version":1,"repo_ids":[]}`,
		"extra_metrics":       `{"version":1,"day":"2026-07-23","backfill_days":1}`,
		"team_metrics":        `{"version":1,"day":"2026-07-23","backfill_days":1}`,
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
		{"extra_metrics", `{"version":1,"day":"bad","backfill_days":1}`},
		{"extra_metrics", `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"postgres","provider":"all"}`},
		{"team_metrics", `{"version":1,"day":"2026-07-23","backfill_days":0}`},
		{"team_metrics", `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"auto","provider":"linear"}`},
	} {
		if _, err := validateFamilyScope(test.family, json.RawMessage(test.raw)); err == nil {
			t.Fatalf("%s accepted %s", test.family, test.raw)
		}
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
