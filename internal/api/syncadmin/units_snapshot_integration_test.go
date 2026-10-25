//go:build integration

package syncadmin

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// hookedReads runs hook right after the rollup read, the instant a concurrent
// writer could split the three reads of one units response.
type hookedReads struct {
	unitsSnapshotReads
	hook func()
}

func (h hookedReads) runUnitRollupRows(ctx context.Context, org string, run uuid.UUID) ([]runUnit, error) {
	rows, err := h.unitsSnapshotReads.runUnitRollupRows(ctx, org, run)
	h.hook()
	return rows, err
}

type hookedStore struct {
	reader
	hook func()
}

func (s hookedStore) unitsSnapshot(ctx context.Context, fn func(unitsSnapshotReads) error) error {
	return s.reader.unitsSnapshot(ctx, func(reads unitsSnapshotReads) error { return fn(hookedReads{reads, s.hook}) })
}

func (v *cascadeVenue) unitsBody(h *handlers, run uuid.UUID, query string) (int, map[string]json.RawMessage) {
	v.t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/x"+query, nil)
	request.SetPathValue("run_id", run.String())
	request = request.WithContext(policy.WithUser(request.Context(), &policy.User{OrgID: v.org, Role: "admin"}))
	recorder := httptest.NewRecorder()
	h.getRunUnits(recorder, request)
	var body map[string]json.RawMessage
	_ = json.Unmarshal(recorder.Body.Bytes(), &body)
	return recorder.Code, body
}

// TestRunUnitsReadIsOneSnapshot: units are inserted and a failed unit is
// completed through a second connection right after the rollup read. The
// rollups, the filtered count and the page of that response all describe the
// rows as they were when the first read ran; the next request sees the change.
func TestRunUnitsReadIsOneSnapshot(t *testing.T) {
	v := startCascadeVenue(t, false)
	run, sources := seedUnitsRun(t, v, 1)
	insert := func(status string) uuid.UUID {
		id := uuid.New()
		v.exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
status, attempts, result, since_at, before_at, available_at, created_at, updated_at)
SELECT $1, $2, $3, integration_id, $4, 'github', 'commits', 'medium', 'incremental', $5, 1, '{}'::json,
 '2026-04-01 00:00:00+00', '2026-04-10 00:00:00+00', now(), now(), now() FROM sync_runs WHERE id = $3`, id, v.org, run, sources[0], status)
		return id
	}
	insert("success")
	insert("success")
	failed := insert("failed")

	fired := 0
	hook := func() {
		fired++
		insert("success")
		insert("success")
		v.exec(`UPDATE sync_run_units SET status = 'success' WHERE id = $1`, failed)
	}
	h := &handlers{store: hookedStore{reader: store{pool: v.pool}, hook: hook}, features: faultFeatures{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)), clock: time.Now}

	status, body := v.unitsBody(h, run, "?offset=0&limit=50&status=success")
	if status != http.StatusOK || fired != 1 {
		t.Fatalf("status %d, hook fired %d times", status, fired)
	}
	var pageUnits []json.RawMessage
	_ = json.Unmarshal(body["units"], &pageUnits)
	if string(body["unit_count"]) != "3" || string(body["filtered_unit_count"]) != "2" || len(pageUnits) != 2 ||
		string(body["by_status"]) != `{"failed":1,"success":2}` && string(body["by_status"]) != `{"success":2,"failed":1}` {
		t.Fatalf("split read: unit_count %s, filtered %s, page %d, by_status %s; want 3, 2, 2, {failed 1, success 2}",
			body["unit_count"], body["filtered_unit_count"], len(pageUnits), body["by_status"])
	}

	h.store = store{pool: v.pool}
	_, next := v.unitsBody(h, run, "?offset=0&limit=50&status=success")
	if string(next["unit_count"]) != "5" || string(next["filtered_unit_count"]) != "5" {
		t.Fatalf("after the writes: unit_count %s, filtered %s, want 5 and 5", next["unit_count"], next["filtered_unit_count"])
	}
}
