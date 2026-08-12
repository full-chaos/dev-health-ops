package synccoverage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	perturbOracleEnvironment  = "DEV_HEALTH_SYNC_COVERAGE_ORACLE_PERTURB"
	livePythonOraclesEnv      = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir  = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	livePythonOracleProofFile = "synccoverage"
)

type payloadOracleFixture struct {
	Config struct {
		ID            string  `json:"id"`
		OrgID         string  `json:"org_id"`
		Provider      string  `json:"provider"`
		IsActive      bool    `json:"is_active"`
		IntegrationID string  `json:"integration_id"`
		SourceID      *string `json:"source_id"`
	} `json:"config"`
	Scope struct {
		Sources []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			FullName string `json:"full_name"`
		} `json:"sources"`
		DatasetKeys []string `json:"dataset_keys"`
	} `json:"scope"`
	Windows []struct {
		Since      string `json:"since"`
		Before     string `json:"before"`
		SourceID   string `json:"source_id"`
		DatasetKey string `json:"dataset_key"`
		Status     string `json:"status"`
		RunTime    string `json:"run_time"`
	} `json:"windows"`
	Backfills []struct {
		Since       string   `json:"since"`
		Before      string   `json:"before"`
		SourceIDs   []string `json:"source_ids"`
		RunIDs      []string `json:"run_ids"`
		DatasetKeys []string `json:"dataset_keys"`
	} `json:"backfill_requested"`
	ActivePairs [][2]string `json:"active_pairs"`
	Schedule    struct {
		Cron      string `json:"schedule_cron"`
		NextRunAt string `json:"next_run_at"`
	} `json:"schedule"`
	HasSchedule      bool   `json:"has_schedule_row"`
	GeneratedAt      string `json:"generated_at"`
	LookbackDays     int    `json:"lookback_days"`
	LatestSuccessful string `json:"latest_successful_run_at"`
	IsTruncated      bool   `json:"is_truncated"`
}

func TestPayloadMatchesLivePythonProduction(t *testing.T) {
	requireLivePythonOracle(t)
	fixturePath, helperPath, repositoryRoot := oraclePaths(t)
	rawFixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture payloadOracleFixture
	if err := json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Windows) == 0 || len(fixture.Backfills) == 0 || len(fixture.ActivePairs) == 0 {
		t.Fatal("sync coverage oracle fixture must exercise non-empty windows, backfills, and active pairs")
	}
	if fixture.LookbackDays != HistoryLookbackDays {
		t.Fatalf("fixture lookback = %d, production Go lookback = %d", fixture.LookbackDays, HistoryLookbackDays)
	}

	pythonOutput := executePythonPayloadOracle(t, repositoryRoot, helperPath, fixturePath)
	goPayload := buildGoOraclePayload(t, fixture)
	if os.Getenv(perturbOracleEnvironment) == "1" {
		// Test-only RED proof. The normal gate never sets this variable. Setting
		// it changes one semantic leaf after the production Go builder runs and
		// must make the exact whole-payload comparison fail.
		goPayload["overall"].(map[string]any)["gap_count"] = 999
	}
	goOutput, err := json.Marshal(goPayload)
	if err != nil {
		t.Fatal(err)
	}
	pythonCanonical := canonicalJSON(t, pythonOutput)
	goCanonical := canonicalJSON(t, goOutput)
	if !bytes.Equal(goCanonical, pythonCanonical) {
		t.Fatalf("Go sync coverage payload diverges from live Python production\nGo:     %s\nPython: %s", goCanonical, pythonCanonical)
	}
}

func requireLivePythonOracle(t *testing.T) {
	t.Helper()
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracle runs only through the uncached live-oracle gate")
	}
	if os.Getenv(livePythonOracleProofDir) == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory")
	}
}

func buildGoOraclePayload(t *testing.T, fixture payloadOracleFixture) projectionPayload {
	t.Helper()
	configID := mustUUID(t, fixture.Config.ID)
	integrationID := mustUUID(t, fixture.Config.IntegrationID)
	var sourceID *uuid.UUID
	if fixture.Config.SourceID != nil {
		value := mustUUID(t, *fixture.Config.SourceID)
		sourceID = &value
	}
	config := syncConfig{
		ID: configID, OrgID: fixture.Config.OrgID, Provider: fixture.Config.Provider,
		Active: fixture.Config.IsActive, IntegrationID: &integrationID, SourceID: sourceID,
	}
	scope := effectiveScope{IntegrationID: &integrationID, DatasetKeys: fixture.Scope.DatasetKeys}
	for _, rawSource := range fixture.Scope.Sources {
		scope.Sources = append(scope.Sources, source{ID: mustUUID(t, rawSource.ID), Name: rawSource.Name, FullName: rawSource.FullName})
	}
	windows := make([]unitWindow, 0, len(fixture.Windows))
	for _, rawWindow := range fixture.Windows {
		windows = append(windows, unitWindow{
			Since: mustTime(t, rawWindow.Since), Before: mustTime(t, rawWindow.Before),
			SourceID: rawWindow.SourceID, DatasetKey: rawWindow.DatasetKey,
			Status: rawWindow.Status, RunTime: mustTime(t, rawWindow.RunTime),
		})
	}
	backfills := make([]coverageInterval, 0, len(fixture.Backfills))
	for _, rawInterval := range fixture.Backfills {
		backfills = append(backfills, coverageInterval{
			Since: mustTime(t, rawInterval.Since), Before: mustTime(t, rawInterval.Before),
			SourceIDs: rawInterval.SourceIDs, RunIDs: rawInterval.RunIDs, DatasetKeys: rawInterval.DatasetKeys,
		})
	}
	activePairs := make(map[string]struct{}, len(fixture.ActivePairs))
	for _, pair := range fixture.ActivePairs {
		activePairs[pair[0]+"\x00"+pair[1]] = struct{}{}
	}
	nextRun := mustTime(t, fixture.Schedule.NextRunAt)
	latestSuccessful := mustTime(t, fixture.LatestSuccessful)
	payload, err := buildPayload(payloadInput{
		Config: config, Scope: scope, Windows: windows, Backfills: backfills,
		ActivePairs: activePairs, Schedule: &schedule{Cron: fixture.Schedule.Cron, NextRunAt: &nextRun},
		HasSchedule: fixture.HasSchedule, Now: mustTime(t, fixture.GeneratedAt),
		LatestSuccessful: &latestSuccessful, IsTruncated: fixture.IsTruncated,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func executePythonPayloadOracle(t *testing.T, repositoryRoot, helperPath, fixturePath string) []byte {
	t.Helper()
	python := os.Getenv("PYTHON")
	if python == "" {
		python = "python3"
	}
	command := exec.Command(python, helperPath, fixturePath)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repositoryRoot, "src"))
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("execute live Python sync coverage oracle: %v: %s", err, stderr.String())
	}
	if len(bytes.TrimSpace(output)) == 0 {
		t.Fatalf("live Python sync coverage oracle returned empty output: %s", stderr.String())
	}
	proof := filepath.Join(os.Getenv(livePythonOracleProofDir), livePythonOracleProofFile)
	if err := os.WriteFile(proof, []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write live Python sync coverage oracle proof: %v", err)
	}
	return output
}

func canonicalJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode oracle JSON: %v\n%s", err, raw)
	}
	if decoder.More() {
		t.Fatal("oracle returned more than one JSON value")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func oraclePaths(t *testing.T) (string, string, string) {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate sync coverage oracle test")
	}
	packageDir := filepath.Dir(currentFile)
	repositoryRoot := filepath.Clean(filepath.Join(packageDir, "..", ".."))
	return filepath.Join(packageDir, "testdata", "payload_oracle_case.json"),
		filepath.Join(packageDir, "testdata", "python_payload_oracle.py"), repositoryRoot
}

func mustUUID(t *testing.T, raw string) uuid.UUID {
	t.Helper()
	value, err := uuid.Parse(raw)
	if err != nil {
		t.Fatalf("parse UUID %q: %v", raw, err)
	}
	return value
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("parse time %q: %v", raw, err)
	}
	return value.UTC()
}

func TestOracleFixturePinsRequiredSemantics(t *testing.T) {
	fixturePath, _, _ := oraclePaths(t)
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"planned", "success", "failed", "active_pairs", "schedule_cron", "backfill_requested"} {
		if !strings.Contains(string(raw), fmt.Sprintf("%q", token)) {
			t.Fatalf("oracle fixture does not pin %q semantics", token)
		}
	}
}
