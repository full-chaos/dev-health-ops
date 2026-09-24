package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// fakeDiagnostics records its call and answers with canned rows, or fails.
type fakeDiagnostics struct {
	calls      int
	org        string
	start, end time.Time
	fail       bool
}

func (f *fakeDiagnostics) backfillDiagnostics(_ context.Context, org string, start, end time.Time) (*pyjson.Object, error) {
	f.calls++
	f.org, f.start, f.end = org, start, end
	if f.fail {
		return nil, errInjected
	}
	return assembleDiagnostics(start, end, map[string]int64{"2026-01-02": 4}, nil,
		map[string]bucket{"2026-01-01": {risk: 2, nonNull: 1, unknown: 1, reasons: [4]int64{1, 0, 1, 0}}}), nil
}

func detailHandlers(reader *faultReader, diagnostics diagnosticsReader, logs *bytes.Buffer) *handlers {
	return &handlers{store: reader, diagnostics: diagnostics, features: faultFeatures{},
		logger: slog.New(slog.NewTextHandler(logs, nil))}
}

// TestBackfillJobDetailSteps pins get_backfill_job's order: an id that is
// not a UUID is the api's bare 500 before any read, a missing job is 404
// and opens no ClickHouse read, and a found job carries the diagnostics of
// its own date window only when the api has a ClickHouse login.
func TestBackfillJobDetailSteps(t *testing.T) {
	job := map[string]string{"job_id": faultRunID.String()}

	var logs bytes.Buffer
	reader, diagnostics := &faultReader{}, &fakeDiagnostics{}
	h := detailHandlers(reader, diagnostics, &logs)
	recorder := serveAs(t, h, h.getBackfillJob, "/x", map[string]string{"job_id": "not-a-uuid"})
	if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"detail":"Internal Server Error"}` ||
		len(reader.calls) != 0 || diagnostics.calls != 0 || !strings.Contains(logs.String(), "parse_job_id") {
		t.Errorf("bad id: %d %s, reads %v, diagnostics %d, log %s", recorder.Code, recorder.Body.String(), reader.calls, diagnostics.calls, logs.String())
	}

	reader, diagnostics = &faultReader{noBackfillJob: true}, &fakeDiagnostics{}
	h = detailHandlers(reader, diagnostics, &logs)
	recorder = serveAs(t, h, h.getBackfillJob, "/x", job)
	if recorder.Code != http.StatusNotFound || recorder.Body.String() != `{"detail":"Backfill job not found"}` || diagnostics.calls != 0 ||
		reader.calls["syncRunByID"] != 0 {
		t.Errorf("missing job: %d %s, diagnostics %d, reads %v", recorder.Code, recorder.Body.String(), diagnostics.calls, reader.calls)
	}

	h = detailHandlers(&faultReader{}, nil, &logs)
	recorder = serveAs(t, h, h.getBackfillJob, "/x", job)
	var body map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || recorder.Code != http.StatusOK {
		t.Fatalf("no ClickHouse: %d %s %v", recorder.Code, recorder.Body.String(), err)
	}
	if raw, ok := body["metrics_diagnostics"]; !ok || string(raw) != "null" {
		t.Errorf("no ClickHouse: metrics_diagnostics = %s (present %v), want null", raw, ok)
	}

	diagnostics = &fakeDiagnostics{}
	h = detailHandlers(&faultReader{}, diagnostics, &logs)
	recorder = serveAs(t, h, h.getBackfillJob, "/x", job)
	body = nil
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || recorder.Code != http.StatusOK {
		t.Fatalf("with ClickHouse: %d %s %v", recorder.Code, recorder.Body.String(), err)
	}
	var got struct {
		RangeStart string `json:"range_start"`
		RangeEnd   string `json:"range_end"`
		Aggregate  struct {
			Metrics int64 `json:"repo_metrics_rows"`
			Risk    int64 `json:"compounding_risk_rows"`
		} `json:"aggregate"`
		PerDay []struct {
			Day string `json:"day"`
		} `json:"per_day"`
	}
	if err := json.Unmarshal(body["metrics_diagnostics"], &got); err != nil {
		t.Fatal(err)
	}
	if diagnostics.calls != 1 || diagnostics.org == "" || dayKey(diagnostics.start) != "2026-01-01" || dayKey(diagnostics.end) != "2026-01-03" ||
		got.RangeStart != "2026-01-01" || got.RangeEnd != "2026-01-03" || got.Aggregate.Metrics != 4 || got.Aggregate.Risk != 2 ||
		len(got.PerDay) != 3 || got.PerDay[2].Day != "2026-01-03" {
		t.Errorf("with ClickHouse: calls %d org %q window %s..%s, body %s", diagnostics.calls, diagnostics.org,
			dayKey(diagnostics.start), dayKey(diagnostics.end), body["metrics_diagnostics"])
	}
	if key := strings.Index(recorder.Body.String(), `"metrics_diagnostics"`); key < strings.Index(recorder.Body.String(), `"updated_at"`) {
		t.Errorf("metrics_diagnostics is not the last field: %s", recorder.Body.String())
	}

	logs.Reset()
	h = detailHandlers(&faultReader{}, &fakeDiagnostics{fail: true}, &logs)
	recorder = serveAs(t, h, h.getBackfillJob, "/x", job)
	if recorder.Code != http.StatusInternalServerError || !strings.Contains(logs.String(), "metrics_diagnostics") ||
		!strings.Contains(logs.String(), errInjected.Error()) {
		t.Errorf("diagnostics failure: %d %s, log %s", recorder.Code, recorder.Body.String(), logs.String())
	}
}
