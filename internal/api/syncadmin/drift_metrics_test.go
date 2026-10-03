package syncadmin

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDriftedDatasetKeysExcludesWhatTheDeltaNames(t *testing.T) {
	// "a" was desired before and is not now: the edit itself disabled it (not drift).
	// "b" was never desired before and is disabled now: the planner was syncing it unaccounted for.
	got := driftedDatasetKeys([]string{"a", "b"}, map[string]bool{"a": true}, map[string]bool{})
	if !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("drifted = %v, want [b]", got)
	}
	if got := driftedDatasetKeys(nil, nil, nil); len(got) != 0 {
		t.Fatalf("nothing disabled, drifted = %v", got)
	}
}

func TestDriftRepairedMetricsCountPerProviderAndIgnoreZero(t *testing.T) {
	m := &DriftRepairedMetrics{repaired: map[string]uint64{}}
	m.observe("github", 2)
	m.observe("github", 1)
	m.observe("jira", 1)
	m.observe("jira", 0)
	var out bytes.Buffer
	if err := m.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"# TYPE sync_target_dataset_drift_repaired_total counter\n",
		`sync_target_dataset_drift_repaired_total{provider="github"} 3` + "\n",
		`sync_target_dataset_drift_repaired_total{provider="jira"} 1` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestRecordDatasetDriftCountsByProviderAndLogsOnlyWhenDrifted(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	read := func() string {
		var out bytes.Buffer
		if err := DriftRepairedMetricsSource().WritePrometheus(&out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	before := read()
	// "a" is the edit's own disable (named by the delta): no drift, nothing counted, nothing logged.
	recordDatasetDrift(context.Background(), logger, "org", uuid.New(), "drifttest", []string{"a"}, map[string]bool{"a": true}, map[string]bool{})
	if read() != before || logs.Len() != 0 {
		t.Fatalf("no drift must not count or log; logs=%q", logs.String())
	}
	recordDatasetDrift(context.Background(), logger, "org", uuid.New(), "drifttest", []string{"a", "b", "c"}, map[string]bool{"a": true}, map[string]bool{})
	if !strings.Contains(read(), `sync_target_dataset_drift_repaired_total{provider="drifttest"} 2`+"\n") {
		t.Fatalf("two drifted keys (b, c) must count 2:\n%s", read())
	}
	if !strings.Contains(logs.String(), "sync_target_dataset_drift_repaired") || !strings.Contains(logs.String(), "drifted_count=2") {
		t.Fatalf("the WARN log must still fire: %q", logs.String())
	}
}

// driftTx is the smallest pgx.Tx the reconcile function drives: an advisory lock
// Exec, a sibling read that returns no rows, dataset INSERT/UPDATE Execs. Every
// disabling UPDATE reports disabledAffected rows, and is counted.
type driftTx struct {
	pgx.Tx
	disabledAffected int64
	disableUpdates   int
}

func (f *driftTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "SET is_enabled = false") {
		f.disableUpdates++
		return pgconn.NewCommandTag("UPDATE " + strconv.FormatInt(f.disabledAffected, 10)), nil
	}
	return pgconn.NewCommandTag("OK"), nil
}

func (f *driftTx) Query(context.Context, string, ...any) (pgx.Rows, error) { return emptyRows{}, nil }

type emptyRows struct{ pgx.Rows }

func (emptyRows) Next() bool { return false }
func (emptyRows) Close()     {}
func (emptyRows) Err() error { return nil }

func driftCount(t *testing.T, provider string) uint64 {
	t.Helper()
	m := DriftRepairedMetricsSource()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.repaired[provider]
}

// TestReconcileCountsRealDriftThroughTheCallPath drives reconcileDatasetRowsForSyncTargets
// itself (CHAOS-8221): a call that disables rows the previous-vs-new delta never
// named moves the counter by exactly the number of drifted rows, a call that
// disables nothing leaves it alone.
func TestReconcileCountsRealDriftThroughTheCallPath(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	before := driftCount(t, "github")
	// previous targets empty: nothing was desired before, so every row the call
	// disables is drift.
	tx := &driftTx{disabledAffected: 1}
	if err := reconcileDatasetRowsForSyncTargets(context.Background(), tx, logger, "org", uuid.New(), "github", []string{"git"}, nil, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if tx.disableUpdates == 0 {
		t.Fatal("harness: the call disabled nothing, the case proves nothing")
	}
	if got := driftCount(t, "github") - before; got != uint64(tx.disableUpdates) {
		t.Fatalf("counter moved by %d, want exactly the %d drifted rows", got, tx.disableUpdates)
	}

	none := &driftTx{disabledAffected: 0}
	baseline := driftCount(t, "github")
	if err := reconcileDatasetRowsForSyncTargets(context.Background(), none, logger, "org", uuid.New(), "github", []string{"git"}, nil, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if none.disableUpdates == 0 || driftCount(t, "github") != baseline {
		t.Fatalf("rows affected 0 must not count (updates=%d)", none.disableUpdates)
	}
}

// TestReconcileEditsOwnDisablesAreNotDrift: a config edit that drops targets the
// previous config named disables their rows itself; only the rows the delta does
// not name are drift. With previous targets given, the counter moves by the
// disables minus the edit's own (CHAOS-8221, vet K2).
func TestReconcileEditsOwnDisablesAreNotDrift(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	names := []string{"git", "prs", "pull_requests", "issues", "work_items", "work-items", "deployments", "incidents", "ci", "cicd", "pipelines", "releases", "teams", "reviews", "commits"}
	previous := make([]pyjson.Value, len(names))
	for i, n := range names {
		previous[i] = n
	}
	prevKeys, _ := providersync.PlannerDatasetKeys("github", names)
	newKeys, _ := providersync.PlannerDatasetKeys("github", []string{"git"})
	isNew, controlled := map[string]bool{}, map[string]bool{}
	for _, k := range newKeys {
		isNew[k] = true
	}
	for _, k := range providersync.OperatorControlledDatasetKeys("github") {
		controlled[k] = true
	}
	own := 0
	for _, k := range prevKeys {
		if !isNew[k] && controlled[k] {
			own++
		}
	}
	if own == 0 {
		t.Fatal("harness: no previously desired controlled key is dropped; the case proves nothing")
	}
	before := driftCount(t, "github")
	tx := &driftTx{disabledAffected: 1}
	if err := reconcileDatasetRowsForSyncTargets(context.Background(), tx, logger, "org", uuid.New(), "github", []string{"git"}, previous, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if got, want := driftCount(t, "github")-before, uint64(tx.disableUpdates-own); got != want {
		t.Fatalf("counter moved by %d; want %d (%d disables, %d are the edit's own)", got, want, tx.disableUpdates, own)
	}
}

// the zero-count call must not create or move a series: a provider seen only with 0.
func TestDriftRepairedMetricsZeroCreatesNoSeries(t *testing.T) {
	m := &DriftRepairedMetrics{repaired: map[string]uint64{}}
	m.observe("onlyzero", 0)
	var out bytes.Buffer
	if err := m.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "onlyzero") {
		t.Fatalf("a zero observation must not create a series:\n%s", out.String())
	}
}
