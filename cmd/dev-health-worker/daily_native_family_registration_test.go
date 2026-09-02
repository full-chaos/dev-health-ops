package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// CHAOS-4292 rebase-gate finding (codex, 2026-09-01, two rounds): the
// pre-existing drift checks this metrics.daily cutover wave relied on --
// families_test.go's families.json validation and internal/jobruntime's
// TestDailyMetricsNativeFamiliesCoverEveryPortedFamily -- both read a
// declared source of truth, never buildDailyWorker's ACTUAL dispatch
// registration. A first fix (parsing daily.go's source with go/ast) closed
// the "assignment is missing entirely" case but codex then proved even
// that insufficient: inserting `delete(nativeFamilies, "incident")`
// immediately before SetNativeFamilies left the AST-based test green too,
// since the assignment statement was still present in source -- the AST
// walk could not see that a LATER statement undid it before the setter
// call ever ran.
//
// The real fix (team-lead ruling): dailyNativeFamilyRegistrations is now a
// PURE FUNCTION returning the native/postBridge maps, and buildDailyWorker
// passes its return values straight to the two setter calls with no
// intermediate variable a stray statement could mutate in between (see
// that function's own doc comment). This test calls it directly with a
// connection stub that makes every executor constructor succeed (each one
// only checks conn != nil at construction time; ClickHouse I/O happens
// later, when the handler executes a partition) and asserts SET EQUALITY,
// both directions, between the ACTUAL returned map keys and families.json's
// own "port":"go" set. There is no longer any source text between
// construction and assertion for an adversarial edit to hide in.
func TestDailyNativeFamilyRegistrationsMatchesFamiliesJSONPortGo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	native, postBridge := dailyNativeFamilyRegistrations(githubWorkItemsBuildExecutorConn{}, nil, logger)

	registered := make(map[string]bool, len(native)+len(postBridge))
	for family := range native {
		registered[family] = true
	}
	for family := range postBridge {
		if registered[family] {
			t.Fatalf("family %q is registered in BOTH native and postBridge maps -- "+
				"SetNativeFamilies/SetPostBridgeNativeFamilies each replace their own "+
				"map wholesale, so a family present in both is dispatched twice, not once", family)
		}
		registered[family] = true
	}

	goFamilies := readFamiliesJSONPortGoSet(t)

	var missingFromRegistration []string
	for family := range goFamilies {
		if !registered[family] {
			missingFromRegistration = append(missingFromRegistration, family)
		}
	}
	sort.Strings(missingFromRegistration)
	if len(missingFromRegistration) > 0 {
		t.Errorf(
			"families.json marks %v as port=\"go\" but dailyNativeFamilyRegistrations "+
				"(cmd/dev-health-worker/daily.go) does not return them in its native or "+
				"postBridge map -- every partition for this family silently falls "+
				"through to the Python compatibility bridge with no refusal log and no "+
				"native-family telemetry",
			missingFromRegistration,
		)
	}

	var registeredButNotGo []string
	for family := range registered {
		if !goFamilies[family] {
			registeredButNotGo = append(registeredButNotGo, family)
		}
	}
	sort.Strings(registeredButNotGo)
	if len(registeredButNotGo) > 0 {
		t.Errorf(
			"dailyNativeFamilyRegistrations returns %v but families.json does not "+
				"mark them port=\"go\" -- either families.json is stale (a family's "+
				"cutover flag was reverted or never flipped) or the registration is "+
				"dead code; both call sites must agree",
			registeredButNotGo,
		)
	}
}

// readFamiliesJSONPortGoSet reads the drift-gated families.json (the same
// file families_test.go validates) and returns the set of family names
// currently marked "port":"go".
func readFamiliesJSONPortGoSet(t *testing.T) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "jobs", "metrics", "daily", "families.json"))
	if err != nil {
		t.Fatalf("read families.json: %v", err)
	}
	var registry struct {
		Families []struct {
			Name string `json:"name"`
			Port string `json:"port"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatalf("decode families.json: %v", err)
	}
	goFamilies := make(map[string]bool, len(registry.Families))
	for _, family := range registry.Families {
		if family.Port == "go" {
			goFamilies[family.Name] = true
		}
	}
	return goFamilies
}
