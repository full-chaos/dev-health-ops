package synccoverage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

const perturbOracleEnvironment = "DEV_HEALTH_SYNC_COVERAGE_ORACLE_PERTURB"

// payloadOracleScript is the place of the oracle script under the repository
// root. It reads the case from the file argv[1] names; the program hands it
// the case on its input (/dev/stdin), so the case is part of the request.
const payloadOracleScript = "internal/synccoverage/testdata/python_payload_oracle.py"

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

// payloadOracleCases are the case files the Python payload builder answered,
// under testdata: the hourly schedule (the first case, with windows,
// backfills and active pairs), the same data on a daily schedule (a dataset
// that is between two and three intervals behind is stale; a third dataset,
// between one and two intervals behind, is healthy), and the first data with
// no schedule row (not scheduled).
var payloadOracleCases = []string{"payload_oracle_case.json", "payload_oracle_case_daily.json", "payload_oracle_case_unscheduled.json"}

func TestPayloadMatchesFrozenPythonProduction(t *testing.T) {
	fixturePath, helperPath, _ := oraclePaths(t)
	script, err := os.ReadFile(helperPath)
	if err != nil {
		t.Fatal(err)
	}
	fixtures := make([]payloadOracleFixture, len(payloadOracleCases))
	programs := make([]programoracle.Program, len(payloadOracleCases))
	for index, name := range payloadOracleCases {
		rawFixture, err := os.ReadFile(filepath.Join(filepath.Dir(fixturePath), name))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(rawFixture, &fixtures[index]); err != nil {
			t.Fatal(err)
		}
		fixture := fixtures[index]
		if len(fixture.Windows) == 0 || len(fixture.Backfills) == 0 || len(fixture.ActivePairs) == 0 {
			t.Fatalf("%s: the sync coverage oracle fixture must exercise non-empty windows, backfills, and active pairs", name)
		}
		if fixture.LookbackDays != HistoryLookbackDays {
			t.Fatalf("%s: fixture lookback = %d, production Go lookback = %d", name, fixture.LookbackDays, HistoryLookbackDays)
		}
		// The first case keeps the name it was recorded under.
		programName := "sync coverage payload"
		if index > 0 {
			programName += ": " + name
		}
		program := programoracle.Script(programName, payloadOracleScript, string(script), rawFixture)
		program.Text = "import sys\nsys.argv = [" + strconv.Quote(payloadOracleScript) + ", '/dev/stdin']\n" + program.Text
		programs[index] = program
	}
	outputs := frozenPython(t, "payload.golden.json", programs...)
	staleness := map[string]bool{}
	for index, name := range payloadOracleCases {
		pythonOutput := []byte(outputs[index])
		if len(bytes.TrimSpace(pythonOutput)) == 0 {
			t.Fatalf("%s: the frozen Python sync coverage payload is empty", name)
		}
		for _, status := range []string{"stale", "healthy", "not_scheduled"} {
			if bytes.Contains(pythonOutput, []byte(`"`+status+`"`)) {
				staleness[status] = true
			}
		}
		goPayload := buildGoOraclePayload(t, fixtures[index])
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
			t.Errorf("%s: Go sync coverage payload diverges from the frozen Python production payload\nGo:     %s\nPython: %s", name, goCanonical, pythonCanonical)
		}
	}
	// The cases must reach each staleness answer, or a changed grace or
	// schedule rule would be compared with nothing.
	for _, status := range []string{"stale", "healthy", "not_scheduled"} {
		if !staleness[status] {
			t.Errorf("no frozen Python payload holds the staleness %q", status)
		}
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
	_, currentFile, _, ok := moduleroot.Caller(0)
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
