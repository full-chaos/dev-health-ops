package syncadmin

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// activeUsers is a policy.Store where every user exists and is active.
type activeUsers struct{}

func (activeUsers) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true}, true, nil
}
func (activeUsers) IsMember(context.Context, uuid.UUID, uuid.UUID) (bool, error) { return true, nil }
func (activeUsers) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

const unitKey = "sync-admin-unit-test-key-sync-admin-unit-test-key"

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testGuard(t *testing.T) *policy.Guard {
	t.Helper()
	verifier, err := edgetoken.New(unitKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, activeUsers{}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	return policy.NewGuard(auth, quiet())
}

func sign(t *testing.T, role, org string) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uuid.NewString(), "type": "access", "org_id": org, "role": role, "is_superuser": false,
		"iss": "dev-health-ops", "aud": "dev-health-api", "exp": time.Now().Add(time.Hour).Unix(), "tv": 0,
	}).SignedString([]byte(unitKey))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// TestEveryRouteIsGuardedAtAdminOrg drives each mounted route through its
// real handler chain with a refused caller. The store has no pool, so a
// handler reached past the guard would panic: a 401/403 here proves the
// guard answered first, per route (an admin route mounted Public or at a
// weaker level fails the member or no-org row).
func TestEveryRouteIsGuardedAtAdminOrg(t *testing.T) {
	routes := Routes(Deps{Guard: testGuard(t), Logger: quiet()})
	want := []string{
		"GET /api/v1/admin/sync-configs/auto-import-capabilities",
		"GET /api/v1/admin/sync-targets",
		"GET /api/v1/admin/sync-configs",
		"GET /api/v1/admin/sync-configs/{config_id}",
		"DELETE /api/v1/admin/sync-configs/{config_id}",
		"GET /api/v1/admin/sync-configs/{config_id}/repositories",
		"PUT /api/v1/admin/sync-configs/{config_id}/repositories",
		"GET /api/v1/admin/sync-configs/{config_id}/jobs",
		"GET /api/v1/admin/sync-configs/{config_id}/coverage",
		"GET /api/v1/admin/backfill-jobs",
		"GET /api/v1/admin/backfill-jobs/{job_id}",
		"GET /api/v1/admin/sync-runs/{run_id}",
		"GET /api/v1/admin/sync-runs/{run_id}/units",
	}
	if len(routes) != len(want) {
		t.Fatalf("%d routes, want %d", len(routes), len(want))
	}
	cases := []struct {
		name, authorization string
		status              int
		body                string
	}{
		{"no token", "", http.StatusUnauthorized, `{"detail":{"message":"Not authenticated"}}`},
		{"member", "Bearer " + sign(t, "member", uuid.NewString()), http.StatusForbidden, `{"detail":"Admin access required"}`},
		{"admin without org", "Bearer " + sign(t, "admin", ""), http.StatusForbidden, `{"detail":"Organization context required"}`},
	}
	for index, route := range routes {
		if got := route.Method + " " + route.Pattern; got != want[index] {
			t.Fatalf("route %d = %q, want %q", index, got, want[index])
		}
		path := strings.NewReplacer("{config_id}", uuid.NewString(), "{run_id}", uuid.NewString(), "{job_id}", uuid.NewString()).Replace(route.Pattern)
		for _, tc := range cases {
			request := httptest.NewRequest(route.Method, path+"?limit=bad", nil)
			if tc.authorization != "" {
				request.Header.Set("Authorization", tc.authorization)
			}
			recorder := httptest.NewRecorder()
			route.Handler.ServeHTTP(recorder, request)
			if recorder.Code != tc.status || recorder.Body.String() != tc.body {
				t.Errorf("%s %s: %d %s, want %d %s", route.Pattern, tc.name, recorder.Code, recorder.Body.String(), tc.status, tc.body)
			}
		}
	}
}

func TestAutoImportCapabilityTable(t *testing.T) {
	body, err := pyjson.Marshal(autoImportCapabilityTable())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"github":{"teams":true,"projects":false,"members":true,"reasons":{"projects":"GitHub attributes ownership via repos, not projects."}},` +
		`"gitlab":{"teams":true,"projects":true,"members":true,"reasons":{}},` +
		`"jira":{"teams":true,"projects":true,"members":true,"reasons":{}},` +
		`"linear":{"teams":true,"projects":true,"members":true,"reasons":{}}}`
	if string(body) != want {
		t.Fatalf("got %s", body)
	}
}

func TestHideMigratedChildConfigsReadsTheTruthySet(t *testing.T) {
	for raw, want := range map[string]bool{
		"": false, "1": true, "true": true, " TRUE ": true, "Yes": true, "on": true, "\ton\n": true,
		"0": false, "false": false, "no": false, "y": false, "2": false, "t": false,
	} {
		h := &handlers{lookupEnv: func(string) (string, bool) { return raw, true }}
		if got := h.hideMigratedChildConfigs(); got != want {
			t.Errorf("%q: %v, want %v", raw, got, want)
		}
	}
}

func decode(t *testing.T, text string) pyjson.Value {
	t.Helper()
	value, err := pyjson.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func marshal(t *testing.T, value pyjson.Value) string {
	t.Helper()
	out, err := pyjson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestPyStringListIsListThenListStr(t *testing.T) {
	for stored, want := range map[string]string{
		`null`: `[]`, `[]`: `[]`, `{}`: `[]`, `""`: `[]`, `0`: `[]`, `false`: `[]`, `0.0`: `[]`,
		`["a","b"]`: `["a","b"]`, `"ab"`: `["a","b"]`, `{"z":1,"y":2}`: `["z","y"]`,
		`["a",1]`: "error", `5`: "error", `true`: "error", `1.5`: "error", `[null]`: "error", `[["a"]]`: "error",
	} {
		got, err := pyStringList(decode(t, stored))
		if want == "error" {
			if err == nil {
				t.Errorf("%s: %v, want an error", stored, got)
			}
			continue
		}
		if err != nil || marshal(t, got) != want {
			t.Errorf("%s: %v %v, want %s", stored, got, err, want)
		}
	}
}

func TestDictConversionsFollowDictThenPydantic(t *testing.T) {
	cases := []struct{ stored, pydantic, plain string }{
		{`null`, `{}`, `{}`},
		{`[]`, `{}`, `{}`},
		{`{"b":1,"a":2}`, `{"b":1,"a":2}`, `{"b":1,"a":2}`},
		{`[["k","v"],"xy",{"p":1,"q":2}]`, `{"k":"v","x":"y","p":"q"}`, `{"k":"v","x":"y","p":"q"}`},
		{`[["k",1],["k",2],["j",3]]`, `{"k":2,"j":3}`, `{"k":2,"j":3}`},
		{`[[1,2],["repo","r"]]`, "error", `{"repo":"r"}`},
		{`[[null,2]]`, "error", `{}`},
		{`[[[1],2]]`, "error", "error"},
		{`[[{"a":1},2]]`, "error", "error"},
		{`[["a"]]`, "error", "error"},
		{`["abc"]`, "error", "error"},
		{`[{"a":1}]`, "error", "error"},
		{`[1]`, "error", "error"},
		{`[["a","b","c"]]`, "error", "error"},
		{`[{"a":1,"b":2,"c":3}]`, "error", "error"},
		{`[{"a":1}]`, "error", "error"},
		{`"abc"`, "error", "error"},
		{`true`, "error", "error"},
		{`3`, "error", "error"},
	}
	for _, tc := range cases {
		for _, variant := range []struct {
			want    string
			convert func(pyjson.Value) (*pyjson.Object, error)
		}{{tc.pydantic, pyDict}, {tc.plain, plainDict}} {
			got, err := variant.convert(decode(t, tc.stored))
			if variant.want == "error" {
				if err == nil {
					t.Errorf("%s: %s, want an error", tc.stored, marshal(t, got))
				}
				continue
			}
			if err != nil || marshal(t, got) != variant.want {
				t.Errorf("%s: %v, want %s", tc.stored, err, variant.want)
			}
		}
	}
}

func TestItemsSyncedIsIntOfTheFirstPresentKey(t *testing.T) {
	for stored, want := range map[string]string{
		`null`: "0", `[1]`: "0", `{}`: "0", `{"other":5}`: "0",
		`{"items_synced":"12","rows":99}`: "12", `{"rows":3.9}`: "3", `{"rows":-3.9}`: "-3", `{"count":true}`: "1",
		`{"count":false}`: "0", `{"items":"1.5"}`: "0", `{"rows_ingested":" 1_000 "}`: "1000", `{"items":1e20}`: "100000000000000000000",
		`{"items_synced":[]}`: "0", `{"items_synced":[1]}`: "0", `{"items_synced":{"a":1}}`: "0", `{"items_synced":null,"rows":7}`: "0",
		`{"items_synced":"٣"}`: "3", `{"items_synced":123456789012345678901234567890}`: "123456789012345678901234567890",
		`{"items_synced":1e400}`: "error", `{"items_synced":-1e400}`: "error", `{"items_synced":NaN}`: "0",
	} {
		got, err := itemsSynced(decode(t, stored))
		if want == "error" {
			if err == nil {
				t.Errorf("%s: %v, want an error", stored, got)
			}
			continue
		}
		if err != nil || got.String() != want {
			t.Errorf("%s: %v %v, want %s", stored, got, err, want)
		}
	}
}

func TestPlannerSyncRunIDReadsUUIDOfStr(t *testing.T) {
	id := uuid.New()
	hex := strings.ReplaceAll(id.String(), "-", "")
	for stored, want := range map[string]bool{
		`{"sync_run_id":"` + id.String() + `"}`: true, `{"sync_run_id":"{` + strings.ToUpper(id.String()) + `}"}`: true,
		`{"sync_run_id":"urn:uuid:` + id.String() + `"}`: true, `{"sync_run_id":"` + hex + `"}`: true,
		`{"sync_run_id":null}`: false, `{}`: false, `[]`: false, `null`: false, `{"sync_run_id":"x"}`: false,
		`{"sync_run_id":1}`: false, `{"sync_run_id":["` + id.String() + `"]}`: false,
	} {
		got := plannerSyncRunID(decode(t, stored))
		if (got != nil) != want || (got != nil && *got != id) {
			t.Errorf("%s: %v, want found=%v", stored, got, want)
		}
	}
	// str() of a 32-digit int is a valid UUID hex string.
	if got := plannerSyncRunID(decode(t, `{"sync_run_id":12345678901234567890123456789012}`)); got == nil {
		t.Error("a 32-digit int is uuid.UUID(str(value))")
	}
}

func TestEffectiveRunStatusTable(t *testing.T) {
	cases := []struct {
		counts map[string]int64
		total  int64
		want   string
	}{
		{map[string]int64{"success": 2}, 2, "success"},
		{map[string]int64{"success": 2, "failed": 1}, 3, "partial_failed"},
		{map[string]int64{"failed": 3}, 3, "failed"},
		{map[string]int64{"success": 1, "failed": 1}, 2, "partial_failed"},
		{map[string]int64{"failed": 3}, 0, "running"},
		{map[string]int64{"success": 1}, 2, "running"},
		{map[string]int64{"retrying": 1}, 2, "running"},
		{map[string]int64{"running": 1, "planned": 1}, 2, "running"},
		{map[string]int64{"dispatching": 1, "planned": 1}, 2, "dispatching"},
		{map[string]int64{"planned": 1}, 2, "planned"},
		{map[string]int64{"weird": 1}, 2, "RUN"},
		{map[string]int64{}, 0, "RUN"},
	}
	for _, tc := range cases {
		if got := effectiveRunStatus("RUN", tc.counts, tc.total); got != tc.want {
			t.Errorf("%v total=%d: %s, want %s", tc.counts, tc.total, got, tc.want)
		}
	}
	for status, want := range map[string]int64{
		"planned": 0, "dispatching": 1, "running": 1, "success": 2, "partial_failed": 3, "failed": 3, "other": 1,
	} {
		if got := plannerJobRunStatus(status); got != want {
			t.Errorf("%s: %d, want %d", status, got, want)
		}
	}
}

func TestElapsedSecondsTruncatesAndClamps(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for delta, want := range map[time.Duration]int64{
		0: 0, 9999999 * time.Microsecond: 9, 10 * time.Second: 10, -time.Second: 0, -500 * time.Millisecond: 0,
	} {
		end := start.Add(delta)
		if got := elapsedSeconds(&start, &end); got != want {
			t.Errorf("%v: %v, want %d", delta, got, want)
		}
	}
	if elapsedSeconds(nil, &start) != nil || elapsedSeconds(&start, nil) != nil {
		t.Error("a missing end is None")
	}
}

func TestBackfillSyncRunIDTakesTheTextAfterTheLastMarker(t *testing.T) {
	text := func(value string) *string { return &value }
	for _, tc := range []struct {
		task *string
		want string
	}{
		{nil, ""}, {text(""), ""}, {text("abc"), ""}, {text("sync_run:"), ""}, {text("sync_run:x"), "x"},
		{text("a sync_run:b sync_run:c"), "c"}, {text("SYNC_RUN:x"), ""},
	} {
		if got := backfillSyncRunID(&backfillJob{CeleryTaskID: tc.task}); got != tc.want {
			t.Errorf("%v: %q, want %q", tc.task, got, tc.want)
		}
	}
}

func TestJobRunResponseMergesThePlannerRun(t *testing.T) {
	started := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	completed := started.Add(9999999 * time.Microsecond)
	emptyError, jobError := "", "jerr"
	runResult := `{"keep":"b","extra":1}`
	planner := &syncRun{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Mode: "incremental", TriggeredBy: "manual",
		Status: "running", TotalUnits: 5, StartedAt: &started, CompletedAt: &completed, Result: &runResult, Error: &emptyError}
	job := &jobRun{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), JobID: uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		Error: &jobError, TriggeredBy: "manual", CreatedAt: started}
	rollup := &unitRollup{StatusCounts: map[string]int64{"success": 2, "failed": 1, "planned": 1}}
	body, err := jobRunResponse(job, decode(t, `{"sync_run_id":"x","keep":"a","items_synced":3}`), planner, rollup)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"00000000-0000-0000-0000-000000000002","job_id":"00000000-0000-0000-0000-000000000003","status":"running",` +
		`"started_at":"2026-05-01T10:00:00Z","completed_at":"2026-05-01T10:00:09.999999Z","duration_seconds":9,"items_synced":2,` +
		`"result":{"sync_run_id":"x","keep":"b","items_synced":3,"extra":1,"sync_run_status":"running","total_units":5,"completed_units":2,"failed_units":1},` +
		`"error":"jerr","triggered_by":"manual","sync_run":{"mode":"incremental","triggered_by":"manual","requested_range":null,` +
		`"covered_range":null,"total_units":5,"completed_units":2,"failed_units":1,"sync_run_id":"00000000-0000-0000-0000-000000000001"},` +
		`"created_at":"2026-05-01T10:00:00Z"}`
	if got := marshal(t, body); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	// A plain run keeps its own status label, and an unknown status is
	// "failed"; a non-dict result cannot be rendered.
	job.Status = 9
	body, err = jobRunResponse(job, nil, nil, nil)
	if err != nil || !strings.Contains(marshal(t, body), `"status":"failed"`) {
		t.Fatalf("%v %v", err, body)
	}
	if _, err := jobRunResponse(job, decode(t, `[1]`), nil, nil); err == nil {
		t.Fatal("a list result must not render")
	}
}

func TestBackfillJobResponseProgressAndRunCounts(t *testing.T) {
	updated := time.Date(2026, 7, 1, 0, 0, 1, 0, time.UTC)
	later := updated.Add(time.Hour)
	message := "old"
	job := &backfillJob{ID: uuid.Nil, SyncConfigID: uuid.Nil, Status: "running", SinceDate: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		BeforeDate: time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), TotalChunks: 3, CompletedChunks: 1, ErrorMessage: &message,
		CreatedAt: updated, UpdatedAt: updated}
	body := marshal(t, backfillJobResponse(job, nil))
	if !strings.Contains(body, `"progress_pct":33.33333333333333,`) || !strings.Contains(body, `"error_message":"old"`) ||
		!strings.Contains(body, `"since_date":"2026-01-01","before_date":"2026-01-31"`) {
		t.Fatalf("got %s", body)
	}
	body = marshal(t, backfillJobResponse(job, &backfillCounts{Status: "success", Total: 0, UpdatedAt: &later}))
	if !strings.Contains(body, `"progress_pct":0.0,`) || !strings.Contains(body, `"error_message":null`) ||
		!strings.Contains(body, `"updated_at":"2026-07-01T01:00:01Z"`) || !strings.Contains(body, `"status":"success"`) {
		t.Fatalf("got %s", body)
	}
}

func TestSelectionOwnerIsStrOfOwnerOrGroup(t *testing.T) {
	for stored, want := range map[string]string{
		`{}`: "", `{"owner":"acme"}`: "acme", `{"owner":"","group":"g"}`: "g", `{"owner":0,"group":0}`: "",
		`{"group":1.5}`: "1.5", `{"owner":["x",1]}`: "['x', 1]", `{"owner":true}`: "True", `{"owner":{"a":null}}`: "{'a': None}",
	} {
		options, err := plainDict(decode(t, stored))
		if err != nil {
			t.Fatal(err)
		}
		if got := selectionOwner(options); got != want {
			t.Errorf("%s: %q, want %q", stored, got, want)
		}
	}
}

func TestPageLimitsAreTheQueryBounds(t *testing.T) {
	if pageLimitMin != 1 || pageLimitMax != 200 || pageOffsetGe != 0 {
		t.Fatal("limit ge=1 le=200, offset ge=0")
	}
	if _, ok := offsetValue(new(big.Int).Lsh(big.NewInt(1), 63)); ok {
		t.Fatal("an offset past int8 cannot be bound")
	}
}

func TestDecodeStoredReadsNullAndRefusesMalformedText(t *testing.T) {
	if value, err := decodeStored(nil); value != nil || err != nil {
		t.Fatal(value, err)
	}
	broken := "{"
	if _, err := decodeStored(&broken); err == nil {
		t.Fatal("malformed stored text must not decode")
	}
	null := "null"
	if value, err := decodeStored(&null); value != nil || err != nil {
		t.Fatal(value, err)
	}
}
