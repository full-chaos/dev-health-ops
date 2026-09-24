package syncadmin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

var errInjected = errors.New("injected store failure")

// faultReader answers every read with enough rows to reach the next step
// of each handler, and fails the call named by fail ("method" or
// "method#n" for its n-th call).
type faultReader struct {
	fail  string
	calls map[string]int
	// noPlannerRun makes the job run carry no sync_run_id.
	noPlannerRun bool
	// noBackfillJob makes backfillJobByID find no job.
	noBackfillJob bool
}

var (
	faultConfigID      = uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	faultIntegrationID = uuid.MustParse("00000000-0000-0000-0000-00000000000d")
	faultRunID         = uuid.MustParse("00000000-0000-0000-0000-00000000000e")
)

func (f *faultReader) hit(method string) error {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[method]++
	if f.fail == method || f.fail == fmt.Sprintf("%s#%d", method, f.calls[method]) {
		return errInjected
	}
	return nil
}

func (f *faultReader) config() *syncConfig {
	integration := faultIntegrationID
	targets, options := `["git"]`, `{}`
	return &syncConfig{ID: faultConfigID, Name: "n", Provider: "github", SyncTargets: &targets, SyncOptions: &options,
		IntegrationID: &integration}
}

func (f *faultReader) listConfigs(context.Context, string, bool) ([]*syncConfig, error) {
	return []*syncConfig{f.config()}, f.hit("listConfigs")
}
func (f *faultReader) configByID(context.Context, string, uuid.UUID) (*syncConfig, error) {
	return f.config(), f.hit("configByID")
}
func (f *faultReader) childrenCounts(context.Context, []uuid.UUID) (map[uuid.UUID]int64, error) {
	return map[uuid.UUID]int64{}, f.hit("childrenCounts")
}
func (f *faultReader) credentialIDs(context.Context, string, []uuid.UUID) (map[uuid.UUID]*uuid.UUID, error) {
	return map[uuid.UUID]*uuid.UUID{}, f.hit("credentialIDs")
}
func (f *faultReader) sourcesForIntegration(context.Context, string, uuid.UUID, string) ([]plannerSource, error) {
	return nil, f.hit("sourcesForIntegration")
}
func (f *faultReader) childOptions(context.Context, string, uuid.UUID) ([]*string, error) {
	return nil, f.hit("childOptions")
}
func (f *faultReader) scheduledSyncJobIDs(context.Context, string, uuid.UUID) ([]uuid.UUID, error) {
	return []uuid.UUID{uuid.New()}, f.hit("scheduledSyncJobIDs")
}
func (f *faultReader) jobRuns(context.Context, []uuid.UUID, int64, int64) ([]jobRun, error) {
	result := `{"sync_run_id": "` + faultRunID.String() + `"}`
	if f.noPlannerRun {
		result = `{}`
	}
	return []jobRun{{ID: uuid.New(), JobID: uuid.New(), Result: &result, TriggeredBy: "manual"}}, f.hit("jobRuns")
}
func (f *faultReader) syncRunByID(context.Context, string, uuid.UUID) (*syncRun, error) {
	return &syncRun{ID: faultRunID, IntegrationID: &faultIntegrationID}, f.hit("syncRunByID")
}
func (f *faultReader) syncRunsByID(context.Context, string, []uuid.UUID) (map[uuid.UUID]*syncRun, error) {
	return map[uuid.UUID]*syncRun{}, f.hit("syncRunsByID")
}
func (f *faultReader) unitStatusCounts(context.Context, string, []uuid.UUID) (map[uuid.UUID]map[string]int64, error) {
	return map[uuid.UUID]map[string]int64{}, f.hit("unitStatusCounts")
}
func (f *faultReader) unitRanges(context.Context, string, []uuid.UUID, bool) ([]unitRange, error) {
	return nil, f.hit("unitRanges")
}
func (f *faultReader) countBackfillJobs(context.Context, string) (int64, error) {
	return 1, f.hit("countBackfillJobs")
}
func (f *faultReader) backfillJobs(context.Context, string, int64, int64) ([]backfillJob, error) {
	task := "sync_run:" + faultRunID.String()
	return []backfillJob{{ID: uuid.New(), CeleryTaskID: &task}}, f.hit("backfillJobs")
}
func (f *faultReader) unitActivity(context.Context, string, uuid.UUID) (*time.Time, *time.Time, error) {
	return nil, nil, f.hit("unitActivity")
}
func (f *faultReader) runStatusCounts(context.Context, string, uuid.UUID) (map[string]int64, error) {
	return map[string]int64{}, f.hit("runStatusCounts")
}

func (f *faultReader) runUnits(context.Context, string, uuid.UUID) ([]runUnit, error) {
	return nil, f.hit("runUnits")
}
func (f *faultReader) watermarkRows(context.Context, string, []string, []string) ([]schedsync.WatermarkRow, error) {
	return nil, f.hit("watermarkRows")
}
func (f *faultReader) backfillJobByID(context.Context, string, uuid.UUID) (*backfillJob, error) {
	if f.noBackfillJob {
		return nil, f.hit("backfillJobByID")
	}
	task := "sync_run:" + faultRunID.String()
	return &backfillJob{ID: uuid.New(), CeleryTaskID: &task,
		SinceDate:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		BeforeDate: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)}, f.hit("backfillJobByID")
}
func (f *faultReader) coverageProjection(context.Context, string, uuid.UUID, int, int) (*coverageProjection, error) {
	return &coverageProjection{Payload: "{}"}, f.hit("coverageProjection")
}

type faultFeatures struct{ fail bool }

func (f faultFeatures) Decide(context.Context, string, string) (licensing.Decision, error) {
	if f.fail {
		return licensing.Decision{}, errInjected
	}
	return licensing.Decision{}, nil
}

func serveAs(t *testing.T, h *handlers, handler http.HandlerFunc, path string, values map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	for key, value := range values {
		request.SetPathValue(key, value)
	}
	request = request.WithContext(policy.WithUser(request.Context(), &policy.User{OrgID: uuid.NewString(), Role: "admin"}))
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

// TestEveryStoreFailureIsA500 fails each read of each handler in turn,
// after every earlier read succeeded, and requires the Python api's bare
// 500 and a logged failure naming the step: no failure is swallowed into a
// 200 with partial data.
func TestEveryStoreFailureIsA500(t *testing.T) {
	config := map[string]string{"config_id": faultConfigID.String()}
	run := map[string]string{"run_id": faultRunID.String()}
	cases := []struct {
		route   string
		call    func(*handlers) http.HandlerFunc
		values  map[string]string
		methods []string
	}{
		{"list", func(h *handlers) http.HandlerFunc { return h.listSyncConfigs }, nil, []string{"listConfigs", "childrenCounts", "credentialIDs"}},
		{"get", func(h *handlers) http.HandlerFunc { return h.getSyncConfig }, config, []string{"configByID", "credentialIDs"}},
		{"repositories", func(h *handlers) http.HandlerFunc { return h.getRepositories }, config, []string{"configByID", "sourcesForIntegration", "childOptions"}},
		{"jobs", func(h *handlers) http.HandlerFunc { return h.listJobs }, config, []string{
			"configByID", "scheduledSyncJobIDs", "jobRuns", "syncRunsByID", "unitStatusCounts", "unitRanges#1", "unitRanges#2"}},
		{"backfill", func(h *handlers) http.HandlerFunc { return h.listBackfillJobs }, nil, []string{
			"countBackfillJobs", "backfillJobs", "syncRunByID", "unitActivity", "runStatusCounts"}},
		{"sync run", func(h *handlers) http.HandlerFunc { return h.getSyncRun }, run, []string{"syncRunByID"}},
		{"backfill job", func(h *handlers) http.HandlerFunc { return h.getBackfillJob }, map[string]string{"job_id": faultRunID.String()},
			[]string{"backfillJobByID", "syncRunByID", "unitActivity", "runStatusCounts"}},
	}
	for _, tc := range cases {
		for _, method := range append([]string{""}, tc.methods...) {
			var logs bytes.Buffer
			reader := &faultReader{fail: method}
			h := &handlers{store: reader, features: faultFeatures{}, logger: slog.New(slog.NewTextHandler(&logs, nil)),
				lookupEnv: func(string) (string, bool) { return "", false }}
			recorder := serveAs(t, h, tc.call(h), "/x", tc.values)
			if method == "" {
				if recorder.Code != http.StatusOK {
					t.Errorf("%s with no failure: %d %s", tc.route, recorder.Code, recorder.Body.String())
				}
				continue
			}
			if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"detail":"Internal Server Error"}` {
				t.Errorf("%s failing %s: %d %s", tc.route, method, recorder.Code, recorder.Body.String())
			}
			if !strings.Contains(logs.String(), "sync admin: request failed") || !strings.Contains(logs.String(), errInjected.Error()) {
				t.Errorf("%s failing %s: no failure log: %s", tc.route, method, logs.String())
			}
		}
	}

	// sync-targets: a feature decision that cannot be read is a 500.
	var logs bytes.Buffer
	h := &handlers{store: &faultReader{}, features: faultFeatures{fail: true}, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	if recorder := serveAs(t, h, h.syncTargets, "/x", nil); recorder.Code != http.StatusInternalServerError ||
		!strings.Contains(logs.String(), "canonical_incident_feature") {
		t.Errorf("sync targets failing the decision: %d %s", recorder.Code, logs.String())
	}
}

// TestNoSyncRunReadsWithoutASyncRunID pins that a jobs page whose runs name
// no sync run reads no sync run table at all, as the Python helpers return
// before their queries.
func TestNoSyncRunReadsWithoutASyncRunID(t *testing.T) {
	for _, method := range []string{"syncRunsByID", "unitStatusCounts", "unitRanges"} {
		reader := &faultReader{fail: method, noPlannerRun: true}
		h := &handlers{store: reader, features: faultFeatures{}, logger: quiet()}
		recorder := serveAs(t, h, h.listJobs, "/x", map[string]string{"config_id": faultConfigID.String()})
		if recorder.Code != http.StatusOK || reader.calls[method] != 0 {
			t.Errorf("%s: %d, %d calls", method, recorder.Code, reader.calls[method])
		}
	}
}

// TestEmptyIDSetsSendNoQuery pins the store's short cuts: an empty id set
// answers empty without touching the pool (a nil pool here).
func TestEmptyIDSetsSendNoQuery(t *testing.T) {
	ctx := context.Background()
	var empty store
	if counts, err := empty.childrenCounts(ctx, nil); err != nil || len(counts) != 0 {
		t.Fatal(counts, err)
	}
	if found, err := empty.credentialIDs(ctx, "org", nil); err != nil || len(found) != 0 {
		t.Fatal(found, err)
	}
	if runs, err := empty.syncRunsByID(ctx, "org", nil); err != nil || len(runs) != 0 {
		t.Fatal(runs, err)
	}
}
