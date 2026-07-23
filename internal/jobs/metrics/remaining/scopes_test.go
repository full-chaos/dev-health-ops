package remaining

import (
	"encoding/json"
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
		{"complexity", `{"version":1,"day":"2026-07-23","backfill_days":2}`},
		{"dora", `{"version":1,"day":"2026-07-23","backfill_days":1,"sink":"mongo","interval":"daily"}`},
		{"release_impact", `{"version":1,"day":"2026-07-23","backfill_days":1,"recomputation_window_days":31}`},
		{"recommendations", `{"version":1,"window":91}`},
		{"membership_backfill", `{"version":1,"command":"bad"}`},
		{"extra_metrics", `{"version":1,"day":"bad","backfill_days":1}`},
		{"team_metrics", `{"version":1,"day":"2026-07-23","backfill_days":0}`},
	} {
		if _, err := validateFamilyScope(test.family, json.RawMessage(test.raw)); err == nil {
			t.Fatalf("%s accepted %s", test.family, test.raw)
		}
	}
}
