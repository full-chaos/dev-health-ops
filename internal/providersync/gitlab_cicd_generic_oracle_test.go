package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var gitLabCICDOracleGoOnlyFields = map[string]string{
	"org_id":      "carried from the tenant-scoped Go claim",
	"last_synced": "stamped by the Go complete-route handler after the active Python producer returns",
}

func buildGitLabCICDRowForOracle(t *testing.T, input map[string]any) gitLabCICDPipelineRow {
	t.Helper()
	encoded, err := json.Marshal(input["raw_pipelines"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payloads []gitLabCICDPipelinePayload
	if err := decoder.Decode(&payloads); err != nil {
		t.Fatal(err)
	}
	since, err := time.Parse(time.RFC3339Nano, input["since_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	before, err := time.Parse(time.RFC3339Nano, input["before_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "cicd")
	claim.BeforeAt = &before
	rows, err := normalizeGitLabCICDPipelines(
		claim, input["repo_id"].(string), payloads,
		&since, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%+v want exactly one accepted producer row", rows)
	}
	return rows[0]
}

func TestGenericOracleMatchesActivePythonGitLabCICDProducer(t *testing.T) {
	cases := []oracleCase{
		{ID: "nullable_timestamps_and_started_fallback", Input: map[string]any{
			"repo_id":       "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
			"since_at":      "2026-07-01T00:00:00Z",
			"before_at":     "2026-07-31T23:59:59Z",
			"normalized_at": "2026-08-03T10:00:00.987654Z",
			"raw_pipelines": []any{
				map[string]any{"id": 901, "status": nil, "created_at": nil},
				map[string]any{"id": "902", "status": "running", "created_at": "2026-07-20T10:00:00.123Z", "started_at": nil, "finished_at": nil},
			},
		}},
		{ID: "since_stop_and_explicit_timestamps", Input: map[string]any{
			"repo_id":       "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
			"since_at":      "2026-07-01T00:00:00Z",
			"before_at":     "2026-07-31T23:59:59Z",
			"normalized_at": "2026-08-03T10:00:00.987654Z",
			"raw_pipelines": []any{
				map[string]any{"id": 903, "status": "success", "created_at": "2026-07-01T00:00:00Z", "started_at": "2026-07-01T00:00:01.456Z", "finished_at": "2026-07-01T00:02:03.999Z"},
				map[string]any{"id": 904, "status": "failed", "created_at": "2026-06-30T23:59:59Z"},
				map[string]any{"id": 905, "status": "success", "created_at": "2026-07-22T00:00:00Z"},
			},
		}},
		{ID: "created_within_but_started_after_before_is_filtered", Input: map[string]any{
			"repo_id":       "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
			"since_at":      "2026-07-01T00:00:00Z",
			"before_at":     "2026-07-31T23:59:59Z",
			"normalized_at": "2026-08-03T10:00:00.987654Z",
			"raw_pipelines": []any{
				map[string]any{"id": 906, "status": "running", "created_at": "2026-07-20T00:00:00Z", "started_at": "2026-08-01T00:00:00Z"},
				map[string]any{"id": 907, "status": "success", "created_at": "2026-07-19T00:00:00Z", "started_at": "2026-07-19T00:00:01Z"},
			},
		}},
		{ID: "created_after_before_but_started_within_is_retained", Input: map[string]any{
			"repo_id":       "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
			"since_at":      "2026-07-01T00:00:00Z",
			"before_at":     "2026-07-31T23:59:59Z",
			"normalized_at": "2026-08-03T10:00:00.987654Z",
			"raw_pipelines": []any{
				map[string]any{"id": 908, "status": "success", "created_at": "2026-08-01T00:00:00Z", "started_at": "2026-07-31T23:59:59Z"},
			},
		}},
	}
	compareRowsAgainstPythonOracle(
		t, "gitlab/cicd/pipeline", cases, buildGitLabCICDRowForOracle,
		gitLabCICDOracleGoOnlyFields,
	)
}

type gitLabCICDSelectionObservation struct {
	RunID []string `json:"run_id"`
}

func TestGenericOracleMatchesActivePythonGitLabCICDSelectionAtSourcePrecision(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/cicd/selection",
		[]oracleCase{{ID: "submillisecond_since_and_before_membership", Input: map[string]any{
			"repo_id":   "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
			"since_at":  "2026-07-01T00:00:00.123400Z",
			"before_at": "2026-07-31T23:59:59.123400Z",
			"raw_pipelines": []any{
				map[string]any{"id": 909, "status": "running", "created_at": "2026-07-01T00:00:00.123456Z", "started_at": "2026-07-31T23:59:59.123456Z"},
				map[string]any{"id": 910, "status": "success", "created_at": "2026-07-01T00:00:00.123456Z", "started_at": "2026-07-31T23:59:59.123400Z"},
			},
		}}},
		func(t *testing.T, input map[string]any) gitLabCICDSelectionObservation {
			encoded, err := json.Marshal(input["raw_pipelines"])
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(encoded))
			decoder.UseNumber()
			var payloads []gitLabCICDPipelinePayload
			if err := decoder.Decode(&payloads); err != nil {
				t.Fatal(err)
			}
			since, err := time.Parse(time.RFC3339Nano, input["since_at"].(string))
			if err != nil {
				t.Fatal(err)
			}
			before, err := time.Parse(time.RFC3339Nano, input["before_at"].(string))
			if err != nil {
				t.Fatal(err)
			}
			claim := nativeTestClaim("gitlab", "cicd")
			claim.BeforeAt = &before
			rows, err := normalizeGitLabCICDPipelines(
				claim, input["repo_id"].(string), payloads, &since,
				time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.RunID)
			}
			return gitLabCICDSelectionObservation{RunID: ids}
		},
		nil,
	)
}
