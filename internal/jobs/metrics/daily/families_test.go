package daily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestFamilyRegistryIsCompleteAndRoutesCorePortFirst(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("families.json"))
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		SchemaVersion int `json:"schema_version"`
		Families      []struct {
			Name      string   `json:"name"`
			Python    string   `json:"python"`
			Writes    []string `json:"writes"`
			Port      string   `json:"port"`
			Golden    string   `json:"golden"`
			Phase     string   `json:"phase"`
			PhaseNote string   `json:"phase_note"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatal(err)
	}
	if registry.SchemaVersion != 1 || len(registry.Families) != 24 {
		t.Fatalf("invalid family registry: %#v", registry)
	}
	// The port enum is closed: "pending" (still Python-only), "next_core"
	// (a Wave-1 reference-implementation family not yet cut over), and "go"
	// (a native Go executor is registered and Python's compute path for it
	// is dormant behind PartitionHandler.SetNativeFamilies -- see
	// TeamWellbeingExecutor, CHAOS-4276). A typo in this field silently
	// leaves a family stuck, so it is validated here rather than only at
	// the Go-executor construction site.
	validPorts := map[string]bool{"pending": true, "next_core": true, "go": true}
	// phase (CHAOS-4278) is additive: "" (omitted) and "pre_bridge" are the
	// SAME thing (today's dispatch order, cmd/dev-health-worker/daily.go
	// SetNativeFamilies), so every existing family needs no edit.
	// "post_bridge" (PartitionHandler.SetPostBridgeNativeFamilies) means the
	// family's native executor depends on data the SAME partition's
	// compatibility-bridge call writes, and must therefore run after it --
	// see work_item_state's phase_note for the concrete case. A
	// "post_bridge" family MUST carry a phase_note explaining the
	// cross-family dependency (this field earns its cost by being read
	// during triage, so an empty one defeats the point).
	// "finalize" (CHAOS-4290) is the third bucket: a RUN-scoped family,
	// registered through FinalizeHandler.SetNativeFinalizeFamilies rather than
	// either partition map. It is NOT a variant of pre_bridge -- a finalize
	// family in a partition map runs once per PARTITION instead of once per
	// run, rewriting the same rows for every partition of the day.
	validPhases := map[string]bool{"": true, "pre_bridge": true, "post_bridge": true, "finalize": true}
	seen := map[string]bool{}
	for _, family := range registry.Families {
		if family.Name == "" || family.Python == "" || len(family.Writes) == 0 || family.Golden != "required" || seen[family.Name] || !validPorts[family.Port] || !validPhases[family.Phase] {
			t.Fatalf("invalid family entry: %#v", family)
		}
		// Both non-default phases must justify themselves. The note earns its
		// cost by being read during triage, so an empty one defeats the point
		// -- and a note attached to a DEFAULT-phase family is either stale or
		// paired with a phase somebody forgot to set.
		if (family.Phase == "post_bridge" || family.Phase == "finalize") && family.PhaseNote == "" {
			t.Fatalf("family %q declares phase=%s with no phase_note explaining why", family.Name, family.Phase)
		}
		if family.Phase != "post_bridge" && family.Phase != "finalize" && family.PhaseNote != "" {
			t.Fatalf("family %q has a phase_note but is not phase=post_bridge or finalize -- stale note or missing phase?", family.Name)
		}
		seen[family.Name] = true
	}
	expected := []string{
		"repo_user_commit", "team_wellbeing", "file_hotspots", "file_risk_hotspots", "work_item", "work_item_estimate", "work_item_attribution", "work_item_state", "review_edges", "cicd", "testops_pipeline", "testops_test", "testops_coverage", "deploy", "incident", "ai_governance", "ai_impact", "ai_workflow", "work_graph_edges", "compounding_risk", "testops_risk", "benchmarking", "ic_finalize",
	}
	for _, core := range expected {
		if !seen[core] {
			t.Fatalf("daily family %s is absent", core)
		}
	}
	byName := make(map[string]string, len(registry.Families))
	for _, family := range registry.Families {
		byName[family.Name] = family.Port
	}
	// team_wellbeing is CHAOS-4276's reference cutover: it must be "go", not
	// "next_core" or "pending" -- catches a families.json edit that flips
	// the flag back (or forgets to) without touching the Go registration.
	if got := byName["team_wellbeing"]; got != "go" {
		t.Fatalf("team_wellbeing must be port=go, got %q", got)
	}
	// repo_user_commit is Wave 1's OTHER reference implementation
	// (lane-4275): its own PR flips this to "go", registering
	// RepoUserCommitExecutor exactly as team_wellbeing did above.
	if got := byName["repo_user_commit"]; got != "go" {
		t.Fatalf("repo_user_commit must be port=go, got %q", got)
	}
	// incident (CHAOS-4269/CHAOS-4295): registers IncidentExecutor, WITH the
	// NULL-OK valid_from guard fix -- Python's compute path for this family
	// was always zero-yield (CHAOS-4269), so this flag flipping back without
	// the Go registration would silently resurrect a permanently-broken
	// family, not just a working-but-slower one.
	if got := byName["incident"]; got != "go" {
		t.Fatalf("incident must be port=go, got %q", got)
	}
	// testops_risk (CHAOS-4294): TestopsRiskExecutor is registered in
	// cmd/dev-health-worker/daily.go the same way team_wellbeing/
	// repo_user_commit are above -- catches a families.json edit that
	// flips this back (or forgets to) without touching the registration.
	if got := byName["testops_risk"]; got != "go" {
		t.Fatalf("testops_risk must be port=go, got %q", got)
	}
	// work_item_state is CHAOS-4278's cutover: it must be "go", registering
	// WorkItemStateExecutor exactly as team_wellbeing/repo_user_commit did.
	if got := byName["work_item_state"]; got != "go" {
		t.Fatalf("work_item_state must be port=go, got %q", got)
	}
	byPhase := make(map[string]string, len(registry.Families))
	for _, family := range registry.Families {
		byPhase[family.Name] = family.Phase
	}
	// CHAOS-5078: all four work-item families are now port=go and pre_bridge
	// (phase empty). The post_bridge workaround existed only because
	// work_item_team_attributions was written by a still-Python family during
	// the same partition call; that family is native now, and families.json's
	// `after` edges sequence it ahead of its three readers WITHIN pre_bridge.
	//
	// ic_finalize is phase=finalize (CHAOS-4290), the first RUN-scoped family:
	// compute_ic_landscape_rolling reads back user_metrics_daily rows that
	// compute_ic_metrics_daily wrote for the SAME run, so it must run once
	// after every partition has landed rather than once per partition.
	if got := byPhase["ic_finalize"]; got != "finalize" {
		t.Fatalf("ic_finalize must be phase=finalize (CHAOS-4290), got %q", got)
	}
	//
	// The assertion is inverted rather than deleted. "No family declares a
	// phase" is a real, checkable property, and it is the one that catches a
	// family silently re-acquiring post_bridge -- deleting the block would
	// leave the phase field unasserted entirely, which is how a half-cutover
	// went unnoticed before (port flipped, phase not).
	for _, family := range []string{"work_item", "work_item_estimate", "work_item_state", "work_item_attribution"} {
		if got := byName[family]; got != "go" {
			t.Fatalf("%s must be port=go (CHAOS-4283/CHAOS-5078), got %q", family, got)
		}
		if got := byPhase[family]; got != "" {
			t.Fatalf("%s must be pre_bridge, i.e. NO phase declared (CHAOS-5078 moved it back), got %q", family, got)
		}
	}
	// compounding_risk must stay phase=post_bridge until a finalize-side
	// native-family hook exists (CHAOS-4287, see families.json's phase_note).
	// Its input, repo_metrics_daily, is written by repo_user_commit in the SAME
	// partition, and computeNativeFamilies walks nativeFamilyNames in SORTED
	// order -- "compounding_risk" sorts BEFORE "repo_user_commit", so a
	// pre_bridge registration reads the table before this partition's rows
	// land. Same assertion-pair discipline as work_item_state above: this is
	// the families.json half, cmd/dev-health-worker/daily.go's registration is
	// the other.
	//
	// A DIFFERENT reason from the CHAOS-4283/CHAOS-5078 four above, and
	// deliberately kept as its own block rather than folded into their loop:
	// theirs was about a stale attribution snapshot written by the bridge
	// (now retired by CHAOS-5078's native writer), this one is about
	// sorted-order execution inside computeNativeFamilies. Merging them would
	// suggest one fix retires both, and CHAOS-5078 does not touch this one.
	// It is also the ONE exception to the "every OTHER family has no phase"
	// loop below, not the whole set post_bridge was scoped to before
	// CHAOS-5078.
	if got := byPhase["compounding_risk"]; got != "post_bridge" {
		t.Fatalf("compounding_risk must be phase=post_bridge (CHAOS-4287), got %q", got)
	}
	// benchmarking (CHAOS-4288, then CHAOS-5194) used to be a THIRD post_bridge
	// family in this class -- its metric window ends on the TARGET DAY, so a
	// pre_bridge registration would benchmark day D against a window missing
	// day D. CHAOS-5194 (astra F3) relocated it to finalize scope entirely: the
	// post_bridge anchor-partition mechanism fixed cross-partition duplication
	// but not the race between the anchor partition finishing and every OTHER
	// partition for the same org/day having written its own inputs. Finalize
	// scope's ClaimFinalize barrier (every partition succeeded) closes that
	// race by construction.
	if got := byPhase["benchmarking"]; got != "finalize" {
		t.Fatalf("benchmarking must be phase=finalize (CHAOS-5194), got %q", got)
	}
	// The allow-list is kept EXPLICIT rather than relaxed to "any known phase":
	// a new non-default phase should force whoever adds it to come here and say
	// which family it belongs to and why. Widening it to accept anything in
	// validPhases would turn a deliberate acknowledgement into a silent pass.
	//
	// A name->PHASE map, not a set (CHAOS-4290): it must express that
	// ic_finalize is "finalize", not just another post_bridge family, and
	// would otherwise silently accept ic_finalize declaring post_bridge.
	// CHAOS-5078 retired work_item_state/work_item/work_item_estimate from
	// this map -- they are pre_bridge now (asserted in their own loop above),
	// leaving compounding_risk/benchmarking/ic_finalize as the only three
	// families with a deliberate non-default phase.
	nonDefaultPhase := map[string]string{
		"compounding_risk": "post_bridge",
		"benchmarking":     "finalize",
		"ic_finalize":      "finalize",
	}
	for name, phase := range byPhase {
		if phase == "" {
			continue
		}
		want, expected := nonDefaultPhase[name]
		if !expected {
			t.Fatalf("family %q declares phase=%q -- only %v are expected to be non-default today; update this test if that changes deliberately", name, phase, nonDefaultPhase)
		}
		if phase != want {
			t.Fatalf("family %q declares phase=%q, expected %q", name, phase, want)
		}
	}
	// cicd is Wave 1B's first cutover (CHAOS-4292), following repo_user_commit/
	// team_wellbeing's exact registration pattern (CICDExecutor).
	if got := byName["cicd"]; got != "go" {
		t.Fatalf("cicd must be port=go, got %q", got)
	}
	// file_hotspots and file_risk_hotspots are CHAOS-4277's cutover: both
	// must be "go", registering FileHotspotsExecutor/FileRiskHotspotsExecutor
	// exactly as the two reference implementations above did.
	if got := byName["file_hotspots"]; got != "go" {
		t.Fatalf("file_hotspots must be port=go, got %q", got)
	}
	if got := byName["file_risk_hotspots"]; got != "go" {
		t.Fatalf("file_risk_hotspots must be port=go, got %q", got)
	}
}
