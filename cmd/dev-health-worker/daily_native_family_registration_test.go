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
//
// CHAOS-4278's post_bridge phase adds a THIRD way this pair can drift,
// caught by a follow-up scoped round (codex, 2026-09-01): merging native
// and postBridge into one "is it registered anywhere" key set (as an
// earlier revision of this test did) cannot see a family registered in the
// WRONG map -- moving work_item_state from postBridge to native inside
// dailyNativeFamilyRegistrations left every drift test green, including
// this one, because "registered, and port=go" was still true. That
// mutation is a real regression: work_item_state reads
// work_item_team_attributions, written by the still-Python-bridged
// work_item_attribution family in the SAME partition, so running it
// pre_bridge means it observes stale (or absent, for a newly-attributed
// item) data -- the exact bug CHAOS-4278's own post_bridge mechanism
// exists to prevent (see WorkItemStateExecutor's doc comment). This test
// now checks PHASE too: a family registered in native must be
// families.json phase ""/"pre_bridge"; a family in postBridge must be
// families.json phase "post_bridge". Either mismatch is a drift the two
// maps' mere key-set union could never see.
func TestDailyNativeFamilyRegistrationsMatchesFamiliesJSONPortGo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	native, postBridge, finalize := dailyNativeFamilyRegistrations(githubWorkItemsBuildExecutorConn{}, nil, logger)

	registeredPhase := make(map[string]string, len(native)+len(postBridge))
	for family := range native {
		registeredPhase[family] = "pre_bridge"
	}
	for family := range postBridge {
		if _, alreadyRegistered := registeredPhase[family]; alreadyRegistered {
			t.Fatalf("family %q is registered in BOTH native and postBridge maps -- "+
				"SetNativeFamilies/SetPostBridgeNativeFamilies each replace their own "+
				"map wholesale, so a family present in both is dispatched twice, not once", family)
		}
		registeredPhase[family] = "post_bridge"
	}
	for family := range finalize {
		if _, alreadyRegistered := registeredPhase[family]; alreadyRegistered {
			t.Fatalf("family %q is registered as a finalize family AND in a partition "+
				"map -- it would run once per partition AND once per run, writing the "+
				"same rows repeatedly", family)
		}
		registeredPhase[family] = "finalize"
	}

	goFamilyPhase := readFamiliesJSONPortGoPhases(t)

	var missingFromRegistration []string
	for family := range goFamilyPhase {
		if _, ok := registeredPhase[family]; !ok {
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
	var wrongPhase []string
	for family, gotPhase := range registeredPhase {
		wantPhase, isGo := goFamilyPhase[family]
		if !isGo {
			registeredButNotGo = append(registeredButNotGo, family)
			continue
		}
		if gotPhase != wantPhase {
			wrongPhase = append(wrongPhase, family+": registered "+gotPhase+", families.json wants "+wantPhase)
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
	sort.Strings(wrongPhase)
	if len(wrongPhase) > 0 {
		t.Errorf(
			"registration phase mismatch: %v -- a family in the WRONG map (native "+
				"vs postBridge) runs at the wrong point relative to the Python "+
				"compatibility bridge call for its partition; for a family with a "+
				"real post_bridge dependency (like work_item_state's read of "+
				"work_item_team_attributions) this is silent stale-data corruption, "+
				"not a crash",
			wrongPhase,
		)
	}
}

// readFamiliesJSONPortGoPhases reads the drift-gated families.json (the
// same file families_test.go validates) and returns, for every family
// currently marked "port":"go", the phase dailyNativeFamilyRegistrations
// must register it under: "post_bridge" families.json entries map to
// "post_bridge"; everything else (an omitted phase, or the explicit
// "pre_bridge" default) maps to "pre_bridge".
func readFamiliesJSONPortGoPhases(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "jobs", "metrics", "daily", "families.json"))
	if err != nil {
		t.Fatalf("read families.json: %v", err)
	}
	var registry struct {
		Families []struct {
			Name  string `json:"name"`
			Port  string `json:"port"`
			Phase string `json:"phase"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatalf("decode families.json: %v", err)
	}
	goFamilyPhase := make(map[string]string, len(registry.Families))
	for _, family := range registry.Families {
		if family.Port != "go" {
			continue
		}
		switch family.Phase {
		case "post_bridge":
			goFamilyPhase[family.Name] = "post_bridge"
		case "finalize":
			// CHAOS-4290: a RUN-scoped family, registered through
			// FinalizeHandler.SetNativeFinalizeFamilies rather than either
			// partition map. A third bucket, not a variant of pre_bridge --
			// a finalize family in a partition map would run once per
			// PARTITION instead of once per run, writing the same rows
			// repeatedly.
			goFamilyPhase[family.Name] = "finalize"
		default:
			goFamilyPhase[family.Name] = "pre_bridge"
		}
	}
	return goFamilyPhase
}
