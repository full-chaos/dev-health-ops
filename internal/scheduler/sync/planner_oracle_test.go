package sync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

const (
	livePythonOraclesEnv      = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir  = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	livePythonOracleProofFile = "scheduler-sync"
)

func requireLivePythonOracles(t *testing.T) {
	t.Helper()
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv(livePythonOracleProofDir) == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
}

func livePythonExecutable(t *testing.T) string {
	t.Helper()
	requireLivePythonOracles(t)
	if configured := os.Getenv("PYTHON"); configured != "" {
		return configured
	}
	if path, err := exec.LookPath("python3"); err == nil {
		return path
	}
	t.Fatal("python3 is required for the live scheduled planner oracle")
	return ""
}

type plannerOracleSource struct {
	ID         string `json:"id"`
	ExternalID string `json:"external_id"`
	Provider   string `json:"provider"`
	FullName   string `json:"full_name"`
}

type plannerOracleDataset struct {
	DatasetKey   string `json:"dataset_key"`
	InitialDepth *int   `json:"initial_depth,omitempty"`
}

type plannerOracleWatermark struct {
	SourceID   string `json:"source_id"`
	DatasetKey string `json:"dataset_key"`
	At         string `json:"at"`
}

type plannerOracleRoute struct {
	SourceID    *string  `json:"source_id"`
	SyncTargets []string `json:"sync_targets"`
}

type plannerOracleCase struct {
	ID                      string                   `json:"id"`
	OrgID                   string                   `json:"org_id"`
	IntegrationID           string                   `json:"integration_id"`
	Provider                string                   `json:"provider"`
	Mode                    string                   `json:"mode"`
	Now                     string                   `json:"now"`
	Before                  *string                  `json:"before,omitempty"`
	IntegrationDepth        *int                     `json:"integration_depth,omitempty"`
	TierCap                 *int                     `json:"tier_cap,omitempty"`
	WatermarkOverlapSeconds int                      `json:"watermark_overlap_seconds"`
	Sources                 []plannerOracleSource    `json:"sources"`
	Datasets                []plannerOracleDataset   `json:"datasets"`
	Watermarks              []plannerOracleWatermark `json:"watermarks"`
	Route                   *plannerOracleRoute      `json:"route,omitempty"`
	// RouteSwitches carries WORKER_*_ENABLED-style env var names (CHAOS-4047).
	// Present (even empty) means the durable sync.provider_unit route is a
	// River outbox route: both sides build a real route-switch mapping from
	// it (Go via completeRouteSwitchesFromCase, Python via
	// ProviderUnitRouteSwitches.from_environment). Absent (nil) means
	// off-River -- both sides plan unfiltered, unchanged from every case
	// above that predates CHAOS-4047.
	RouteSwitches map[string]string `json:"route_switches,omitempty"`
}

// completeRouteSwitchesFromCase converts the case's env-var-name-keyed map
// into the same providersync.CompleteRouteSwitches struct
// providersync.RouteSwitchesFromConfig builds in production, so the Go side
// of the oracle exercises the real struct rather than a second
// representation. The switch set here is deliberately small (only the names
// these test cases actually use) and panics on an unmapped name to catch a
// typo in a new case rather than silently ignoring it.
func completeRouteSwitchesFromCase(env map[string]string) *providersync.CompleteRouteSwitches {
	if env == nil {
		return nil
	}
	switches := providersync.CompleteRouteSwitches{}
	for name, value := range env {
		enabled := value == "true"
		switch name {
		case "WORKER_GITHUB_PRS_ENABLED":
			switches.GithubPRs = enabled
		case "WORKER_GITHUB_PR_REVIEWS_ENABLED":
			switches.GithubPRReviews = enabled
		case "WORKER_GITHUB_PR_COMMENTS_ENABLED":
			switches.GithubPRComments = enabled
		case "WORKER_GITHUB_CICD_ENABLED":
			switches.GithubCICD = enabled
		case "WORKER_GITHUB_TESTS_ENABLED":
			switches.GithubTests = enabled
		case "WORKER_GITHUB_SECURITY_ENABLED":
			switches.GithubSecurity = enabled
		case "WORKER_GITHUB_WORK_ITEMS_ENABLED":
			switches.GithubWorkItems = enabled
		case "WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED":
			switches.LaunchDarklyFeatureFlags = enabled
		default:
			panic(fmt.Sprintf("completeRouteSwitchesFromCase: unmapped env var %q", name))
		}
	}
	return &switches
}

func TestBuildScheduledPlanMatchesLivePythonPlanner(t *testing.T) {
	depth120, depth60, cap30 := 120, 60, 30
	before := "2026-07-30T11:30:00Z"
	cases := []plannerOracleCase{
		{
			ID: "github_incremental_family", OrgID: "org-a", IntegrationID: "integration-a", Provider: "github",
			Mode: SyncModeIncremental, Now: "2026-07-30T12:00:00Z", Before: &before,
			IntegrationDepth: &depth60, TierCap: &cap30, WatermarkOverlapSeconds: 300,
			Sources: []plannerOracleSource{
				{ID: "source-z", ExternalID: "owner/z", Provider: "github", FullName: "owner/z"},
				{ID: "source-a", ExternalID: "owner/a", Provider: "github", FullName: "owner/a"},
			},
			Datasets: []plannerOracleDataset{
				{DatasetKey: "repo-metadata"}, {DatasetKey: "commits", InitialDepth: &depth120},
				{DatasetKey: "prs"}, {DatasetKey: "work-item-labels"}, {DatasetKey: "work-items"},
			},
			Watermarks: []plannerOracleWatermark{
				{SourceID: "owner/a", DatasetKey: "commits", At: "2026-07-29T10:00:00Z"},
				{SourceID: "owner/a", DatasetKey: "work-items", At: "2026-07-28T10:00:00Z"},
				{SourceID: "owner/a", DatasetKey: "work-item-labels", At: "2026-07-29T10:00:00Z"},
			},
		},
		{
			ID: "pagerduty_full_resync", OrgID: "org-b", IntegrationID: "integration-b", Provider: "pagerduty",
			Mode: SyncModeFullResync, Now: "2026-07-30T12:00:00Z", IntegrationDepth: &depth60, TierCap: &cap30,
			Sources:  []plannerOracleSource{{ID: "source-pd", ExternalID: "account", Provider: "pagerduty", FullName: "account"}},
			Datasets: []plannerOracleDataset{{DatasetKey: "services"}, {DatasetKey: "incidents"}, {DatasetKey: "incident-notes"}},
		},
		{
			ID: "jira_incident_override", OrgID: "org-c", IntegrationID: "integration-c", Provider: "jira",
			Mode: SyncModeIncremental, Now: "2026-07-30T12:00:00Z", TierCap: &cap30,
			Sources:  []plannerOracleSource{{ID: "source-jira", ExternalID: "project", Provider: "jira", FullName: "project"}},
			Datasets: []plannerOracleDataset{{DatasetKey: "incidents"}},
		},
	}
	routeSource := "source-a"
	for _, routeCase := range []struct {
		id      string
		source  *string
		targets []string
	}{
		{id: "routing_parent_all_enabled", targets: []string{"work-items"}},
		{id: "routing_child_recognized", source: &routeSource, targets: []string{"work-items"}},
		{id: "routing_child_unrecognized_fallback", source: &routeSource, targets: []string{"not-a-provider-target"}},
	} {
		cases = append(cases, plannerOracleCase{
			ID: routeCase.id, OrgID: "org-routing", IntegrationID: "integration-routing", Provider: "jira",
			Mode: SyncModeIncremental, Now: "2026-07-30T12:00:00Z", TierCap: &cap30,
			Sources: []plannerOracleSource{
				{ID: "source-a", ExternalID: "project-a", Provider: "jira", FullName: "project-a"},
				{ID: "source-b", ExternalID: "project-b", Provider: "jira", FullName: "project-b"},
			},
			Datasets: []plannerOracleDataset{
				{DatasetKey: "incidents"}, {DatasetKey: "work-items"}, {DatasetKey: "work-item-labels"},
			},
			Route: &plannerOracleRoute{SourceID: routeCase.source, SyncTargets: routeCase.targets},
		})
	}
	// CHAOS-3427: the Go mirror of Python's deterministic ratchet contract
	// table (tests/test_sync_planner.py::test_ratchet_window_contract, pinned
	// verbatim on CHAOS-3412). Same pinned inputs on both sides --
	// now = 2026-06-17T12:00:00Z, initial depth 30 (default), cap 7 (env
	// unset), SYNC_WATERMARK_OVERLAP = 0, provider github, tier cap 30 (the
	// community default a non-UUID org resolves to on the Python side) -- so
	// the (since, before) pair must match EXACTLY, with no tolerance.
	//
	// These run through the live differential oracle rather than as a Go-only
	// expectation table on purpose: a hand-copied expectation table would pass
	// while the two implementations diverged, which is the entire failure mode
	// this ticket exists to prevent. The one clause the table structurally
	// cannot reach (HEAVY + WatermarkBehavior.NONE -- no such dataset is
	// registered) has its own forced case in
	// TestHeavyRatchetNeverCapsAnOpenStartWindow.
	const ratchetNow = "2026-06-17T12:00:00Z"
	ratchetBefore := "2026-03-04T12:00:00Z"
	ratchetTierCap := 30
	for _, ratchet := range []struct {
		id        string
		dataset   string
		watermark *string
		before    *string
	}{
		// HEAVY cold-start is capped at window_start + cap, NOT at now.
		{id: "ratchet_heavy_cold_start_is_capped", dataset: "commit-stats"},
		{id: "ratchet_heavy_cold_start_is_capped_for_every_heavy_key", dataset: "files"},
		// The cap is scoped to CostClass.HEAVY only.
		{id: "ratchet_medium_cold_start_is_uncapped", dataset: "commits"},
		{id: "ratchet_light_cold_start_is_uncapped", dataset: "work-item-labels"},
		{id: "ratchet_medium_behind_watermark_is_uncapped", dataset: "commits", watermark: ptr("2026-03-01T12:00:00Z")},
		// The cap applies to the behind-watermark case too.
		{id: "ratchet_heavy_behind_watermark_is_capped", dataset: "commit-stats", watermark: ptr("2026-03-01T12:00:00Z")},
		// The cap only ever moves the END in: it is a min, never an assignment.
		{id: "ratchet_heavy_watermark_inside_cap_keeps_natural_end", dataset: "commit-stats", watermark: ptr("2026-06-15T12:00:00Z")},
		{id: "ratchet_heavy_watermark_exactly_at_cap_boundary_keeps_natural_end", dataset: "commit-stats", watermark: ptr("2026-06-10T12:00:00Z")},
		{id: "ratchet_heavy_requested_before_tighter_than_cap_wins", dataset: "commit-stats", watermark: ptr("2026-03-01T12:00:00Z"), before: &ratchetBefore},
		// A NONE-watermark-behavior dataset keeps its open start.
		{id: "ratchet_none_watermark_behavior_keeps_open_start", dataset: "repo-metadata"},
	} {
		testCase := plannerOracleCase{
			ID: ratchet.id, OrgID: "org-ratchet", IntegrationID: "integration-ratchet",
			Provider: "github", Mode: SyncModeIncremental, Now: ratchetNow, Before: ratchet.before,
			TierCap: &ratchetTierCap, WatermarkOverlapSeconds: 0,
			Sources:  []plannerOracleSource{{ID: "source-r", ExternalID: "owner/r", Provider: "github", FullName: "owner/r"}},
			Datasets: []plannerOracleDataset{{DatasetKey: ratchet.dataset}},
		}
		if ratchet.watermark != nil {
			testCase.Watermarks = []plannerOracleWatermark{
				{SourceID: "owner/r", DatasetKey: ratchet.dataset, At: *ratchet.watermark},
			}
		}
		cases = append(cases, testCase)
	}
	// C10/C11: a corrupt FUTURE watermark must heal into a bounded recovery
	// window on both sides rather than planning zero units forever, and a
	// dataset already synced past the requested end must plan NO unit rather
	// than an inverted, zero-width one that fetches nothing and finalizes
	// SUCCESS. Both are window-postcondition clauses the ratchet table above
	// does not reach.
	pastBefore := "2026-02-01T12:00:00Z"
	futureBefore := "2026-09-01T12:00:00Z"
	cases = append(cases,
		// C10(a)/C11 first rule: the window END is clamped to now. A future
		// `before` would otherwise persist a FUTURE watermark on success, and
		// the next run would start in the future and silently skip everything
		// up to it.
		plannerOracleCase{
			ID: "window_future_before_is_clamped_to_now", OrgID: "org-window",
			IntegrationID: "integration-window", Provider: "github", Mode: SyncModeIncremental,
			Now: ratchetNow, Before: &futureBefore, TierCap: &ratchetTierCap,
			Sources:  []plannerOracleSource{{ID: "source-w", ExternalID: "owner/w", Provider: "github", FullName: "owner/w"}},
			Datasets: []plannerOracleDataset{{DatasetKey: "commits"}, {DatasetKey: "repo-metadata"}},
		},
		plannerOracleCase{
			ID: "window_future_watermark_heals", OrgID: "org-window", IntegrationID: "integration-window",
			Provider: "github", Mode: SyncModeIncremental, Now: ratchetNow, TierCap: &ratchetTierCap,
			Sources:  []plannerOracleSource{{ID: "source-w", ExternalID: "owner/w", Provider: "github", FullName: "owner/w"}},
			Datasets: []plannerOracleDataset{{DatasetKey: "commits"}},
			Watermarks: []plannerOracleWatermark{
				{SourceID: "owner/w", DatasetKey: "commits", At: "2027-01-01T12:00:00Z"},
			},
		},
		plannerOracleCase{
			ID: "window_synced_past_requested_before_plans_no_unit", OrgID: "org-window",
			IntegrationID: "integration-window", Provider: "github", Mode: SyncModeIncremental,
			Now: ratchetNow, Before: &pastBefore, TierCap: &ratchetTierCap,
			Sources:  []plannerOracleSource{{ID: "source-w", ExternalID: "owner/w", Provider: "github", FullName: "owner/w"}},
			Datasets: []plannerOracleDataset{{DatasetKey: "commits"}},
			Watermarks: []plannerOracleWatermark{
				{SourceID: "owner/w", DatasetKey: "commits", At: "2026-03-01T12:00:00Z"},
			},
		},
		// C9: a FULL_RESYNC HEAVY window is uncapped BY DESIGN. A one-shot
		// resync has no next tick, so a capped window would cover only the
		// cap's span and finalize SUCCESS -- claiming a resync that did not
		// happen. If the Go ratchet ever loses its incremental-mode conjunct,
		// this case diverges.
		plannerOracleCase{
			ID: "ratchet_full_resync_heavy_is_uncapped", OrgID: "org-ratchet",
			IntegrationID: "integration-ratchet", Provider: "github", Mode: SyncModeFullResync,
			Now: ratchetNow, TierCap: &ratchetTierCap,
			Sources:  []plannerOracleSource{{ID: "source-r", ExternalID: "owner/r", Provider: "github", FullName: "owner/r"}},
			Datasets: []plannerOracleDataset{{DatasetKey: "commit-stats"}},
		},
	)
	// The watermark-overlap CLAMP, measured rather than assumed. Production
	// Python clamps the overlap to zero in _watermark_overlap_seconds
	// (src/dev_health_ops/sync/watermarks.py:120) and Go clamps at every use
	// site (resolveWindowStart's max(input.WatermarkOverlap, 0),
	// effectiveHeavyMaxWindow's `overlap <= 0` guard). Before these cases the
	// table only ever fed 0 or a positive overlap, so nothing exercised that
	// clamp on either side -- and the oracle harness itself was subtracting the
	// raw case value, which for a negative overlap would have pushed the window
	// START FORWARD, behaviour neither implementation has. A negative case is
	// what turns that into a real measurement instead of an assumption.
	//
	// The MEDIUM case is the one with teeth: the clamp lands on window_start,
	// so an unclamped -1h would move the start to W+1h and the plans diverge.
	// The HEAVY case additionally pins that a negative overlap takes
	// effectiveHeavyMaxWindow's zero branch on both sides rather than computing
	// a nonsense negative min-cap.
	negativeOverlap := -3600
	negativeWideOverlap := -10 * 86_400
	positiveClampingOverlap := 9 * 86_400
	for _, overlapCase := range []struct {
		id      string
		dataset string
		overlap int
	}{
		{id: "overlap_negative_is_clamped_to_zero_medium", dataset: "commits", overlap: negativeOverlap},
		{id: "overlap_negative_is_clamped_to_zero_heavy", dataset: "commit-stats", overlap: negativeWideOverlap},
		// The other side of the same arithmetic: a POSITIVE overlap wider than
		// the 7-day default cap must trip the C8 clamp identically on both
		// sides (effective cap = floor(9)+1 = 10 days). The ratchet table above
		// pins C8 only through Go-side unit tests; this runs it differentially.
		{id: "overlap_positive_wider_than_cap_clamps_heavy", dataset: "commit-stats", overlap: positiveClampingOverlap},
	} {
		cases = append(cases, plannerOracleCase{
			ID: overlapCase.id, OrgID: "org-overlap", IntegrationID: "integration-overlap",
			Provider: "github", Mode: SyncModeIncremental, Now: ratchetNow,
			TierCap: &ratchetTierCap, WatermarkOverlapSeconds: overlapCase.overlap,
			Sources:  []plannerOracleSource{{ID: "source-o", ExternalID: "owner/o", Provider: "github", FullName: "owner/o"}},
			Datasets: []plannerOracleDataset{{DatasetKey: overlapCase.dataset}},
			Watermarks: []plannerOracleWatermark{
				{SourceID: "owner/o", DatasetKey: overlapCase.dataset, At: "2026-03-01T12:00:00Z"},
			},
		})
	}
	// CHAOS-4047: the plan-time route-switch gate. Owner decision,
	// 2026-08-21 retired the Celery fallback, so this is switch-only parity
	// -- no consumer-presence dimension to vary.
	for _, routeSwitchCase := range []struct {
		id            string
		datasets      []string
		routeSwitches map[string]string
	}{
		{
			// Regression: the exact production failure shape (CHAOS-4047/4048)
			// -- prs enabled, its pr-comments/pr-reviews aliases left off.
			id:       "route_switch_disabled_alias_with_sibling_enabled_is_not_planned",
			datasets: []string{"prs", "pr-comments", "pr-reviews"},
			routeSwitches: map[string]string{
				"WORKER_GITHUB_PRS_ENABLED":         "true",
				"WORKER_GITHUB_PR_COMMENTS_ENABLED": "false",
				"WORKER_GITHUB_PR_REVIEWS_ENABLED":  "false",
			},
		},
		{
			// A standalone dataset with its switch off and no alias sibling
			// to explain it -- the general "switch is off" exclusion.
			id:            "route_switch_disabled_with_no_fallback_is_not_planned",
			datasets:      []string{"security"},
			routeSwitches: map[string]string{"WORKER_GITHUB_SECURITY_ENABLED": "false"},
		},
		{
			id:            "route_switch_enabled_pair_is_planned",
			datasets:      []string{"security"},
			routeSwitches: map[string]string{"WORKER_GITHUB_SECURITY_ENABLED": "true"},
		},
		{
			// Codex review finding: the work-item family's CANONICAL claim
			// ("work-items") is an ordinary routable pair with its own switch,
			// unrelated to the alias-collapse machinery that deliberately
			// skips per-alias route checks. A disabled canonical switch must
			// still exclude it.
			id:            "route_switch_disabled_work_item_family_canonical_claim_is_not_planned",
			datasets:      []string{"work-items"},
			routeSwitches: map[string]string{"WORKER_GITHUB_WORK_ITEMS_ENABLED": "false"},
		},
		{
			id:            "route_switch_enabled_work_item_family_canonical_claim_is_planned",
			datasets:      []string{"work-items"},
			routeSwitches: map[string]string{"WORKER_GITHUB_WORK_ITEMS_ENABLED": "true"},
		},
	} {
		datasets := make([]plannerOracleDataset, 0, len(routeSwitchCase.datasets))
		for _, dataset := range routeSwitchCase.datasets {
			datasets = append(datasets, plannerOracleDataset{DatasetKey: dataset})
		}
		cases = append(cases, plannerOracleCase{
			ID: routeSwitchCase.id, OrgID: "org-route-switch", IntegrationID: "integration-route-switch",
			Provider: "github", Mode: SyncModeIncremental, Now: "2026-08-21T12:00:00Z", TierCap: &cap30,
			Sources:       []plannerOracleSource{{ID: "source-route", ExternalID: "owner/route", Provider: "github", FullName: "owner/route"}},
			Datasets:      datasets,
			RouteSwitches: routeSwitchCase.routeSwitches,
		})
	}
	allDatasets := []string{
		"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments",
		"cicd", "tests", "deployments", "incidents", "security", "work-items", "work-item-labels",
		"work-item-projects", "work-item-history", "work-item-comments", "feature-flags", "services",
		"business-services", "escalation-policies", "schedules", "on-calls", "users", "teams",
		"incident-alerts", "incident-log-entries", "incident-notes",
	}
	for _, provider := range []string{"github", "gitlab", "jira", "launchdarkly", "linear", "pagerduty"} {
		datasets := make([]plannerOracleDataset, 0, len(allDatasets))
		for _, dataset := range allDatasets {
			datasets = append(datasets, plannerOracleDataset{DatasetKey: dataset})
		}
		cases = append(cases, plannerOracleCase{
			ID: "provider_matrix_" + provider, OrgID: "org-matrix", IntegrationID: "integration-matrix",
			Provider: provider, Mode: SyncModeIncremental, Now: "2026-07-30T12:00:00Z", TierCap: &cap30,
			Sources:  []plannerOracleSource{{ID: "source", ExternalID: "external", Provider: provider, FullName: "source"}},
			Datasets: datasets,
		})
	}

	want := runPythonPlannerOracle(t, cases)
	for _, test := range cases {
		now, err := time.Parse(time.RFC3339, test.Now)
		if err != nil {
			t.Fatal(err)
		}
		var parsedBefore *time.Time
		if test.Before != nil {
			value, err := time.Parse(time.RFC3339, *test.Before)
			if err != nil {
				t.Fatal(err)
			}
			parsedBefore = &value
		}
		input := PlannerInput{
			OrgID: test.OrgID, IntegrationID: test.IntegrationID, Mode: test.Mode, Now: now,
			Before: parsedBefore, IntegrationDepthDays: test.IntegrationDepth,
			TierBackfillDaysCap: test.TierCap, WatermarkOverlap: time.Duration(test.WatermarkOverlapSeconds) * time.Second,
			Watermarks:    make(map[WatermarkKey]time.Time),
			RouteSwitches: completeRouteSwitchesFromCase(test.RouteSwitches),
		}
		requested := map[string]bool(nil)
		if test.Route != nil {
			requested = requestedDatasetKeys(test.Provider, test.Route.SyncTargets, test.Route.SourceID)
		}
		for _, source := range test.Sources {
			if test.Route != nil && test.Route.SourceID != nil && source.ID != *test.Route.SourceID {
				continue
			}
			input.Sources = append(input.Sources, PlanSource(source))
		}
		for _, dataset := range test.Datasets {
			if requested != nil && !requested[dataset.DatasetKey] {
				continue
			}
			input.Datasets = append(input.Datasets, PlanDataset{Key: dataset.DatasetKey, InitialDepthDays: dataset.InitialDepth})
		}
		for _, watermark := range test.Watermarks {
			value, err := time.Parse(time.RFC3339, watermark.At)
			if err != nil {
				t.Fatal(err)
			}
			input.Watermarks[WatermarkKey{SourceID: watermark.SourceID, Dataset: watermark.DatasetKey}] = value
		}
		got, err := BuildScheduledPlan(input)
		if err != nil {
			t.Fatalf("%s: BuildScheduledPlan: %v", test.ID, err)
		}
		gotJSON, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		var normalized []map[string]any
		if err := json.Unmarshal(gotJSON, &normalized); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(normalized, want[test.ID]) {
			prettyGot, _ := json.MarshalIndent(normalized, "", "  ")
			prettyWant, _ := json.MarshalIndent(want[test.ID], "", "  ")
			t.Errorf("%s Go plan:\n%s\nPython plan:\n%s", test.ID, prettyGot, prettyWant)
		}
		if strings.HasPrefix(test.ID, "provider_matrix_") && len(normalized) == 0 {
			t.Fatalf("%s produced no units; provider matrix parity did not execute", test.ID)
		}
	}
}

func ptr[T any](value T) *T { return &value }

func runPythonPlannerOracle(t *testing.T, cases []plannerOracleCase) map[string][]map[string]any {
	t.Helper()
	python := livePythonExecutable(t)
	encoded, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate Python planner oracle")
	}
	command := exec.Command(python, filepath.Join(filepath.Dir(currentFile), "testdata", "python_planner_oracle.py"))
	command.Stdin = bytes.NewReader(encoded)
	// stdout and stderr are captured SEPARATELY, not combined: the real
	// planner logs (CHAOS-3412's future-watermark clamp and heavy-cap clamp
	// warnings both fire for cases this table drives) go to stderr, and
	// folding them into stdout corrupts the JSON document the comparison
	// reads. Keeping stderr means those log lines still reach the failure
	// message, which is where they are useful.
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err = command.Run()
	output := stdout.Bytes()
	if err != nil {
		t.Fatalf("execute live Python planner oracle: %v\nstdout:\n%s\nstderr:\n%s",
			err, output, stderr.String())
	}
	proof := filepath.Join(os.Getenv(livePythonOracleProofDir), livePythonOracleProofFile)
	if err := os.WriteFile(proof, []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write live Python planner oracle proof: %v", err)
	}
	var result map[string][]map[string]any
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode live Python planner oracle: %v\n%s", err, output)
	}
	if len(result) != len(cases) {
		t.Fatalf("Python planner oracle returned %d cases for %d inputs", len(result), len(cases))
	}
	return result
}
