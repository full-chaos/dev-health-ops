package sync

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
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
	// The shell gate checks this only after the package succeeds. Recording the
	// proof here makes a missing real Python oracle invocation fail closed.
	proof := filepath.Join(os.Getenv(livePythonOracleProofDir), livePythonOracleProofFile)
	if err := os.WriteFile(proof, []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write live Python oracle proof: %v", err)
	}
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
			Watermarks: make(map[WatermarkKey]time.Time),
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
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("execute live Python planner oracle: %v\n%s", err, output)
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
