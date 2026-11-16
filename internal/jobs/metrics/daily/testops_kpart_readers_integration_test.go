//go:build integration

package daily

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The daily suite reader reads the newest-attempt rule of the legacy-id form: a legacy (slotless) suite row drops only when a
// kept slotted row of its run lists the legacy row's exact id. Slotted rows come from the REAL GitHub route (the writer lists the
// legacy ids), legacy rows go through the real sink; the real daily writer then stores the day's p50/p95, which is what is read.

type kpEnv struct {
	t    *testing.T
	ctx  context.Context
	conn driver.Conn
	n    int
}

func startKpEnv(t *testing.T) *kpEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &kpEnv{t: t, ctx: ctx, conn: conn}
}

func (e *kpEnv) org() string { e.n++; return fmt.Sprintf("org-kp-%d", e.n) }

type kpDoer struct {
	t         *testing.T
	runID     int
	attempt   int
	started   string
	artifacts []string
	zips      map[string][]string
	names     map[string][]string
}

func kpArtifact(id int, name, created string) string {
	return fmt.Sprintf(`{"id":%d,"name":%q,"expired":false,"created_at":%q}`, id, name, created)
}

func (d *kpDoer) archive(id string) []byte {
	var body strings.Builder
	body.WriteString("<testsuites>")
	for i, value := range d.zips[id] {
		name := fmt.Sprintf("s%d", i)
		if names := d.names[id]; i < len(names) {
			name = names[i]
		}
		fmt.Fprintf(&body, `<testsuite name=%q time=%q><testcase name="c" classname="x" time="1"/></testsuite>`, name, value)
	}
	body.WriteString("</testsuites>")
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	member, _ := writer.Create("junit.xml")
	_, _ = member.Write([]byte(body.String()))
	_ = writer.Close()
	return archive.Bytes()
}

func (d *kpDoer) Do(request *http.Request) (*http.Response, error) {
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	path := request.URL.Path
	const root = "/repos/acme/r"
	switch {
	case path == root:
		body = `{"id":9100,"name":"r","full_name":"acme/r","html_url":"https://github.com/acme/r","default_branch":"main","archived":false,"updated_at":"2026-01-20T10:00:00Z","pushed_at":"2026-01-21T10:00:00Z"}`
	case path == root+"/actions/runs":
		body = fmt.Sprintf(`{"workflow_runs":[{"id":%d,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-01-02T09:59:00Z","run_started_at":%q,"updated_at":"2026-01-02T12:30:00Z","run_attempt":%d,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/r/actions/runs/%d","pull_requests":[]}]}`, d.runID, d.started, d.attempt, d.runID)
	case strings.HasSuffix(path, "/jobs"):
		body = `{"jobs":[]}`
	case strings.HasSuffix(path, "/artifacts"):
		body = `{"artifacts":[` + strings.Join(d.artifacts, ",") + `]}`
	case strings.HasSuffix(path, "/zip"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, root+"/actions/artifacts/"), "/zip")
		status = http.StatusFound
		header.Set("Location", "https://blob.example/report-"+id+".zip")
		body = ""
	case strings.HasPrefix(path, "/report-"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/report-"), ".zip")
		header.Set("Content-Type", "application/zip")
		body = string(d.archive(id))
	default:
		d.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func kpClaim(org string) providersync.Claim {
	capability, _ := providersync.Capability("github", "tests")
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 1, 31, 23, 59, 59, 0, time.UTC)
	return providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org, IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "acme/r", SourceName: "acme/r", Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before, CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (e *kpEnv) collect(org string, doer *kpDoer, at time.Time) string {
	e.t.Helper()
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, lease)
	if err != nil {
		e.t.Fatal(err)
	}
	claim := kpClaim(org)
	batch, err := (providersync.GitHubTestsRouteHandler{}).Collect(e.ctx, claim, providerfoundation.Credential{}, client, at)
	if err != nil {
		e.t.Fatalf("collect: %v", err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: lease}
	repo := ""
	for _, effect := range batch.Effects {
		if err := sink.WriteEffect(e.ctx, claim, effect); err != nil {
			e.t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if effect.Destination == "ci_pipeline_runs" && len(effect.Rows) > 0 {
			var row struct {
				RepoID string `json:"repo_id"`
			}
			_ = json.Unmarshal(effect.Rows[0], &row)
			repo = row.RepoID
		}
	}
	return repo
}

func (e *kpEnv) startRun(org string, run int, started string) string {
	return e.collect(org, &kpDoer{t: e.t, runID: run, attempt: 1, started: started}, time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC))
}

func (e *kpEnv) slotted(org string, d *kpDoer) {
	e.collect(org, d, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
}

func (e *kpEnv) legacyIDs(org, run string) map[string][]string {
	e.t.Helper()
	rows, err := e.conn.Query(e.ctx, `SELECT suite_name, legacy_suite_ids FROM test_suite_results FINAL WHERE org_id = ? AND run_id = ? AND report_slot != '' ORDER BY suite_name, suite_id`, org, run)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var name string
		var ids []string
		if err := rows.Scan(&name, &ids); err != nil {
			e.t.Fatal(err)
		}
		out[name] = append(out[name], ids...)
	}
	return out
}

func (e *kpEnv) legacy(org, repo, run, id, name string, seconds float64) {
	e.t.Helper()
	raw, err := json.Marshal(map[string]any{"org_id": org, "repo_id": repo, "run_id": run, "suite_id": id, "suite_name": name, "total_count": 1, "passed_count": 1,
		"duration_seconds": seconds, "started_at": "2026-01-02T10:00:00Z", "last_synced": "2026-01-09T00:00:00Z"})
	if err != nil {
		e.t.Fatal(err)
	}
	effect, err := providersync.BuildEffectBatch("test_suite_results", providersync.EffectReadbackRequired, []json.RawMessage{raw})
	if err != nil {
		e.t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	if err := sink.WriteEffect(e.ctx, kpClaim(org), effect); err != nil {
		e.t.Fatal(err)
	}
}

// stored runs the real daily writers and returns the stored day's suite p50 and p95 (NaN when nothing is stored).
func (e *kpEnv) stored(org, repo string) (p50, p95 float64) {
	e.t.Helper()
	pipeline, err := NewTestopsPipelineExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	tests, err := NewTestopsTestExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	run := Run{ID: "run-" + org, OrganizationID: org, TargetDay: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}
	part := Partition{ID: "part-" + org, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo)}}
	if _, err := pipeline.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	if _, err := tests.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	var a, b *float64
	if err := e.conn.QueryRow(e.ctx, `SELECT suite_duration_p50_seconds, suite_duration_p95_seconds FROM testops_test_metrics_daily FINAL WHERE org_id = ?`, org).Scan(&a, &b); err != nil || a == nil || b == nil {
		e.t.Fatalf("stored suite quantiles: %v %v %v", a, b, err)
	}
	return *a, *b
}

func kpInclusive(values []float64, p float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := float64(len(sorted)-1) * p
	lo := int(math.Floor(rank))
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	return sorted[lo]*(1-(rank-float64(lo))) + sorted[hi]*(rank-float64(lo))
}

func (e *kpEnv) want(org, repo string, want ...float64) {
	e.t.Helper()
	p50, p95 := e.stored(org, repo)
	if math.Abs(p50-kpInclusive(want, 0.5)) > 1e-9 || math.Abs(p95-kpInclusive(want, 0.95)) > 1e-9 {
		e.t.Errorf("stored suite p50 %v p95 %v, want %v / %v over %v", p50, p95, kpInclusive(want, 0.5), kpInclusive(want, 0.95), want)
	}
}

func kpNow() time.Time { return time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC) }

// KP-1: an unlisted legacy suite stays next to a slotted suite (RED on the run-level arm: it dropped the unlisted one too).
func TestDailySuiteReaderKP1UnlistedLegacySuiteStays(t *testing.T) {
	e := startKpEnv(t)
	org := e.org()
	repo := e.startRun(org, 9601, "2026-01-02T10:00:00Z")
	e.slotted(org, &kpDoer{t: t, runID: 9601, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{kpArtifact(91, "report", "2026-01-02T10:05:00Z")}, zips: map[string][]string{"91": {"100"}}})
	ids := e.legacyIDs(org, "9601")["s0"]
	if len(ids) != 1 {
		t.Fatalf("the slotted s0 lists %v legacy ids, want one", ids)
	}
	e.legacy(org, repo, "9601", ids[0], "s0", 10)
	e.legacy(org, repo, "9601", "legacy-unlisted-s1", "s1", 30)
	e.want(org, repo, 100, 30)
}

// KP-2: a re-run double: the slotted newest lists both artifacts' legacy ids; only the slotted row counts.
func TestDailySuiteReaderKP2ReRunDouble(t *testing.T) {
	e := startKpEnv(t)
	org := e.org()
	repo := e.startRun(org, 9602, "2026-01-02T12:00:00Z")
	e.slotted(org, &kpDoer{t: t, runID: 9602, attempt: 2, started: "2026-01-02T12:00:00Z",
		artifacts: []string{kpArtifact(77, "report", "2026-01-02T10:05:00Z"), kpArtifact(78, "report", "2026-01-02T12:05:00Z")},
		zips:      map[string][]string{"77": {"10"}, "78": {"100"}}})
	ids := e.legacyIDs(org, "9602")["s0"]
	if len(ids) != 2 {
		t.Fatalf("the slotted s0 lists %v legacy ids, want two", ids)
	}
	e.legacy(org, repo, "9602", ids[0], "s0", 100)
	e.legacy(org, repo, "9602", ids[1], "s0", 10)
	e.want(org, repo, 100)
}

// KP-3 SHARD: shards share a suite name across slots; only shard 1 is re-written; shard 2's legacy row stays (RED on the run-level arm).
func TestDailySuiteReaderKP3ShardNotReWrittenStays(t *testing.T) {
	e := startKpEnv(t)
	org := e.org()
	repo := e.startRun(org, 9603, "2026-01-02T10:00:00Z")
	e.slotted(org, &kpDoer{t: t, runID: 9603, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{kpArtifact(91, "shard1", "2026-01-02T10:05:00Z")}, zips: map[string][]string{"91": {"100"}}})
	ids := e.legacyIDs(org, "9603")["s0"]
	e.legacy(org, repo, "9603", ids[0], "s0", 100)
	e.legacy(org, repo, "9603", "legacy-shard2", "s0", 40)
	e.want(org, repo, 100, 40)
}

// KP-4: a legacy-only run keeps every legacy row, same-content duplicates included.
func TestDailySuiteReaderKP4LegacyOnlyRun(t *testing.T) {
	e := startKpEnv(t)
	org := e.org()
	repo := e.startRun(org, 9604, "2026-01-02T10:00:00Z")
	e.legacy(org, repo, "9604", "legacy-a", "s0", 7)
	e.legacy(org, repo, "9604", "legacy-b", "s0", 7)
	e.legacy(org, repo, "9604", "legacy-c", "s1", 9)
	e.want(org, repo, 7, 7, 9)
}

// KP-8: the same suite name twice in ONE artifact: the ordinal fold gives one legacy id per sibling; both legacy rows drop.
func TestDailySuiteReaderKP8SiblingsDropOneToOne(t *testing.T) {
	e := startKpEnv(t)
	org := e.org()
	repo := e.startRun(org, 9608, "2026-01-02T10:00:00Z")
	e.slotted(org, &kpDoer{t: t, runID: 9608, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{kpArtifact(95, "report", "2026-01-02T10:05:00Z")},
		zips:      map[string][]string{"95": {"10", "20"}}, names: map[string][]string{"95": {"dup", "dup"}}})
	ids := e.legacyIDs(org, "9608")["dup"]
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("the two siblings list %v, want two different ids", ids)
	}
	e.legacy(org, repo, "9608", ids[0], "dup", 11)
	e.legacy(org, repo, "9608", ids[1], "dup", 21)
	e.want(org, repo, 10, 20)
}

// R-ID2 (named residual): a suite only attempt 1 had has a legacy row no later slotted row lists: it stays counted.
func TestDailySuiteReaderRID2SuiteOnlyAttemptOneHadStaysCounted(t *testing.T) {
	e := startKpEnv(t)
	org := e.org()
	repo := e.startRun(org, 9609, "2026-01-02T12:00:00Z")
	e.slotted(org, &kpDoer{t: t, runID: 9609, attempt: 2, started: "2026-01-02T12:00:00Z",
		artifacts: []string{kpArtifact(98, "report", "2026-01-02T12:05:00Z")}, zips: map[string][]string{"98": {"100"}}})
	ids := e.legacyIDs(org, "9609")["s0"]
	e.legacy(org, repo, "9609", ids[0], "s0", 10)
	e.legacy(org, repo, "9609", "legacy-attempt1-only", "gone", 55)
	e.want(org, repo, 100, 55)
}
