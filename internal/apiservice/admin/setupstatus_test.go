package admin

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// setup.py's _sync_status_from_job_run: the status integer maps to a state, an
// unknown integer is "running", and a result naming a partial sync overrides
// the state whatever it was.
func TestSetupRunSyncStatus(t *testing.T) {
	partial, _ := pyjson.DecodeString(`{"sync_run_status": "partial_failed"}`)
	partialShort, _ := pyjson.DecodeString(`{"sync_run_status": "partial"}`)
	other, _ := pyjson.DecodeString(`{"sync_run_status": 5}`)
	cases := []struct {
		name string
		run  setupRun
		want string
	}{
		{"pending", setupRun{Status: jobRunPending}, "pending"},
		{"running", setupRun{Status: jobRunRunning}, "running"},
		{"success", setupRun{Status: jobRunSuccess}, "complete"},
		{"failed", setupRun{Status: jobRunFailed}, "failed"},
		{"cancelled", setupRun{Status: jobRunCancelled}, "failed"},
		{"unknown integer", setupRun{Status: 9}, "running"},
		{"success with partial_failed", setupRun{Status: jobRunSuccess, Result: partial}, "partial"},
		{"failed with partial", setupRun{Status: jobRunFailed, Result: partialShort}, "partial"},
		{"a non-text sync_run_status is ignored", setupRun{Status: jobRunSuccess, Result: other}, "complete"},
		{"no result", setupRun{Status: jobRunPending, Result: nil}, "pending"},
	}
	for _, c := range cases {
		if got := c.run.syncStatus(); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// _select_primary_config sorts the parent configs by (is_active, str(created_at))
// descending and takes the first: an active config beats an inactive newer one,
// the newer of two active ones wins, and a child config is never a candidate.
func TestSelectPrimaryConfig(t *testing.T) {
	parent := uuid.New()
	older := setupConfig{ID: uuid.New(), IsActive: true, Created: "2026-09-25 03:00:00+00:00"}
	newer := setupConfig{ID: uuid.New(), IsActive: true, Created: "2026-09-25 03:00:00.5+00:00"}
	inactiveNewest := setupConfig{ID: uuid.New(), IsActive: false, Created: "2026-09-26 03:00:00+00:00"}
	child := setupConfig{ID: uuid.New(), ParentID: &parent, IsActive: true, Created: "2026-09-27 03:00:00+00:00"}
	if got := selectPrimaryConfig([]setupConfig{older, inactiveNewest, child, newer}); got == nil || got.ID != newer.ID {
		t.Fatalf("primary = %+v, want the newer active parent", got)
	}
	if got := selectPrimaryConfig([]setupConfig{inactiveNewest, older}); got == nil || got.ID != older.ID {
		t.Fatalf("primary = %+v, want the active parent over the newer inactive one", got)
	}
	if got := selectPrimaryConfig([]setupConfig{child}); got != nil {
		t.Fatalf("primary = %+v, want none (only a child config)", got)
	}
	if got := selectPrimaryConfig(nil); got != nil {
		t.Fatalf("primary = %+v, want none", got)
	}
}
