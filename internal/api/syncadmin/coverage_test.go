package syncadmin

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// coverageReader is faultReader with a chosen projection row.
type coverageReader struct {
	*faultReader
	row *coverageProjection
	err error
	// asked records the lookback and version the handler asked for.
	askedLookback, askedVersion int
}

func (c *coverageReader) coverageProjection(_ context.Context, _ string, _ uuid.UUID, lookback, version int) (*coverageProjection, error) {
	c.askedLookback, c.askedVersion = lookback, version
	return c.row, c.err
}

const coverageMinimal = `{"config_id": "c", "provider": "github", "generated_at": "2026-09-01T10:00:00",
"data_basis": "legacy", "history_lookback_days": "3650", "truncated_before": "2016-09-03T10:00:00+05:30",
"projection_version": 2.0, "projection_complete": 1, "is_truncated": "off", "unknown": [1, 2],
"overall": {"health": "healthy", "gap_count": true, "stale_dataset_count": 0, "failed_range_count": "0"},
"datasets": [], "sources": [],
"backfill_windows": [{"since": "2026-01-01", "before": "2026-01-02T00:00:00", "reasons": ["failed"]}]}`

const coverageMinimalWant = `{"config_id":"c","provider":"github","generated_at":"2026-09-01T10:00:00",` +
	`"data_basis":"legacy","history_lookback_days":3650,"truncated_before":"2016-09-03T10:00:00+05:30",` +
	`"coverage_since":null,"coverage_through":null,"is_truncated":false,"truncation_reason":null,` +
	`"projection_version":2,"projection_complete":true,"projection_refreshing":%s,` +
	`"overall":{"health":"healthy","latest_successful_run_at":null,"latest_covered_through":null,` +
	`"next_scheduled_run_at":null,"gap_count":1,"stale_dataset_count":0,"failed_range_count":0},` +
	`"datasets":[],"sources":[],"backfill_windows":[{"since":"2026-01-01T00:00:00Z",` +
	`"before":"2026-01-02T00:00:00Z","source_ids":[],"dataset_keys":[],"reasons":["failed"]}]}`

func serveCoverage(t *testing.T, reader *coverageReader) (*bytes.Buffer, int, string, http.Header) {
	t.Helper()
	var logs bytes.Buffer
	h := &handlers{store: reader, features: faultFeatures{}, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	recorder := serveAs(t, h, h.getCoverage, "/x", map[string]string{"config_id": faultConfigID.String()})
	return &logs, recorder.Code, recorder.Body.String(), recorder.Header()
}

// TestCoverageAnswers pins get_sync_config_coverage's decisions: the row
// asked for, the pending 503, projection_refreshing from invalidated_at
// (never the stored value), the model's lax values and defaults, and every
// refused payload a logged 500.
func TestCoverageAnswers(t *testing.T) {
	reader := &coverageReader{faultReader: &faultReader{}}
	logs, status, body, header := serveCoverage(t, reader)
	if status != http.StatusServiceUnavailable || header.Get("Retry-After") != "30" ||
		body != `{"detail":{"code":"sync_coverage_projection_pending","message":"Coverage is being prepared. Retry shortly."}}` {
		t.Errorf("no row: %d %s %v", status, body, header)
	}
	if reader.askedLookback != 3650 || reader.askedVersion != 2 {
		t.Errorf("asked lookback %d version %d, want 3650 and 2", reader.askedLookback, reader.askedVersion)
	}
	if logs.Len() != 0 {
		t.Errorf("pending logged a failure: %s", logs)
	}

	for _, tc := range []struct {
		name        string
		payload     string
		invalidated bool
		want        string
	}{
		{"fresh", coverageMinimal, false, "false"},
		{"invalidated", coverageMinimal, true, "true"},
		{"stored as pairs", `[["config_id", "c"], ["provider", "github"], ["generated_at", "2026-09-01T10:00:00"],
["data_basis", "legacy"], ["history_lookback_days", "3650"], ["truncated_before", "2016-09-03T10:00:00+05:30"],
["projection_version", 2.0], ["projection_complete", 1], ["is_truncated", "off"],
["overall", {"health": "healthy", "gap_count": true, "stale_dataset_count": 0, "failed_range_count": "0"}],
["datasets", []], ["sources", []],
["backfill_windows", [{"since": "2026-01-01", "before": "2026-01-02T00:00:00", "reasons": ["failed"]}]]]`, false, "false"},
		{"stored refreshing is overwritten", strings.Replace(coverageMinimal, `"datasets"`, `"projection_refreshing": true, "datasets"`, 1), false, "false"},
	} {
		reader := &coverageReader{faultReader: &faultReader{}, row: &coverageProjection{Payload: tc.payload, Invalidated: tc.invalidated}}
		_, status, body, _ := serveCoverage(t, reader)
		if want := strings.Replace(coverageMinimalWant, "%s", tc.want, 1); status != http.StatusOK || body != want {
			t.Errorf("%s: %d\n got  %s\n want %s", tc.name, status, body, want)
		}
	}

	for _, tc := range []struct{ name, payload, step string }{
		{"null", `null`, "coverage_payload_dict"},
		{"number", `5`, "coverage_payload_dict"},
		{"not json", `{`, "decode_coverage_payload"},
		{"empty", `{}`, "coverage_model"},
		{"null required datetime", strings.Replace(coverageMinimal, `"generated_at": "2026-09-01T10:00:00"`, `"generated_at": null`, 1), "coverage_model"},
		{"null default bool", strings.Replace(coverageMinimal, `"is_truncated": "off"`, `"is_truncated": null`, 1), "coverage_model"},
		{"null default list", strings.Replace(coverageMinimal, `"backfill_windows": [{"since": "2026-01-01", "before": "2026-01-02T00:00:00", "reasons": ["failed"]}]`, `"backfill_windows": null`, 1), "coverage_model"},
		{"bad literal", strings.Replace(coverageMinimal, `"healthy"`, `"Healthy"`, 1), "coverage_model"},
		{"unreadable window", strings.Replace(coverageMinimal, `"2026-01-01"`, `"1767225600x"`, 1), "coverage_model"},
		{"string datasets", strings.Replace(coverageMinimal, `"datasets": []`, `"datasets": "x"`, 1), "coverage_model"},
	} {
		reader := &coverageReader{faultReader: &faultReader{}, row: &coverageProjection{Payload: tc.payload}}
		logs, status, body, _ := serveCoverage(t, reader)
		if status != http.StatusInternalServerError || body != `{"detail":"Internal Server Error"}` ||
			!strings.Contains(logs.String(), "step="+tc.step) {
			t.Errorf("%s: %d %s %s", tc.name, status, body, logs)
		}
	}

	reader = &coverageReader{faultReader: &faultReader{}, err: errInjected}
	logs, status, _, _ = serveCoverage(t, reader)
	if status != http.StatusInternalServerError || !strings.Contains(logs.String(), "step=coverage_projection") {
		t.Errorf("store failure: %d %s", status, logs)
	}
	reader = &coverageReader{faultReader: &faultReader{fail: "configByID"}}
	if logs, status, _, _ := serveCoverage(t, reader); status != http.StatusInternalServerError ||
		!strings.Contains(logs.String(), "step=get_config") {
		t.Errorf("config failure: %d %s", status, logs)
	}
}
