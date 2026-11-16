//go:build integration

package report

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// Form (b): a legacy (slotless) suite or coverage row drops only when a KEPT slotted row of its run lists the legacy row's exact
// id (legacy_suite_ids / legacy_snapshot_ids, written by the slotted writer). Slotted rows come from the REAL GitHub route; the
// legacy rows go through the real sink with BuildEffectBatch (neither route writes a slotless row any more). Seed recipe: the
// slotted pass runs first, its listed legacy ids are read back, and the legacy rows are written with exactly those ids (the
// "re-read" rows) or with unrelated ids (rows nothing replaces).

func (e *pooledEnv) legacySuite(t *testing.T, org, repo, run, suiteID, name string, seconds float64, started string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"org_id": org, "repo_id": repo, "run_id": run, "suite_id": suiteID, "suite_name": name, "total_count": 1, "passed_count": 1,
		"duration_seconds": seconds, "started_at": started, "last_synced": "2026-01-09T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	effect, err := providersync.BuildEffectBatch("test_suite_results", providersync.EffectReadbackRequired, []json.RawMessage{raw})
	if err != nil {
		t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	if err := sink.WriteEffect(e.ctx, pooledClaim(org, "acme/r"), effect); err != nil {
		t.Fatal(err)
	}
}

func (e *pooledEnv) slottedSuites(t *testing.T, org string, run, attempt int, started string, secs []string) {
	t.Helper()
	e.collectAttempt(org, &attemptDoer{t: t, runID: run, attempt: attempt, started: started,
		artifacts: []string{attemptArtifact(run, "2026-01-02T12:05:00Z")}, zips: map[string][]string{fmt.Sprint(run): secs}},
		time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
}

func (e *pooledEnv) suiteQuantiles(org string) (p50, p95 float64) {
	e.t.Helper()
	a := e.value(org, "suite_duration_p50_seconds", "", "2026-01-01", "2026-01-06")
	b := e.value(org, "suite_duration_p95_seconds", "", "2026-01-01", "2026-01-06")
	return a["total"], b["total"]
}

func (e *pooledEnv) suiteLegacyIDs(t *testing.T, org, run string) map[string][]string {
	t.Helper()
	rows, err := e.conn.Query(e.ctx, `SELECT suite_name, legacy_suite_ids FROM test_suite_results FINAL WHERE org_id = ? AND run_id = ? AND report_slot != '' ORDER BY suite_name, suite_id`, org, run)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var name string
		var ids []string
		if err := rows.Scan(&name, &ids); err != nil {
			t.Fatal(err)
		}
		out[name] = append(out[name], ids...)
	}
	return out
}

func (e *pooledEnv) legacyRow(t *testing.T, org, repo, run, id, name string, seconds float64) {
	t.Helper()
	e.legacySuite(t, org, repo, run, id, name, seconds, "2026-01-02T10:00:00Z")
}

func wantQuantiles(t *testing.T, e *pooledEnv, org string, want []float64) {
	t.Helper()
	p50, p95 := e.suiteQuantiles(org)
	if !near(p50, inclusive(want, 0.5)) || !near(p95, inclusive(want, 0.95)) {
		t.Errorf("suite p50 %v p95 %v, want %v / %v over %v", p50, p95, inclusive(want, 0.5), inclusive(want, 0.95), want)
	}
}

func (e *pooledEnv) startRun(t *testing.T, org string, run int, started string) string {
	t.Helper()
	return e.collectAttempt(org, &attemptDoer{t: t, runID: run, attempt: 1, started: started}, time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC))
}

func (e *pooledEnv) finish(org, repo string) {
	e.own(org, "team-k", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02")
}

// KP-1: legacy s0 (listed by the slotted row) and legacy s1 (listed by nothing) next to a slotted s0: the slotted s0 and the
// legacy s1 count. RED on the run-level rule (it dropped s1 too).
func TestRegistryTestopsKP1LegacySuiteNotListedStays(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9501, "2026-01-02T10:00:00Z")
	e.slottedSuites(t, org, 9501, 1, "2026-01-02T10:00:00Z", []string{"100"})
	ids := e.suiteLegacyIDs(t, org, "9501")["s0"]
	if len(ids) != 1 {
		t.Fatalf("the slotted s0 lists %v legacy ids, want exactly one", ids)
	}
	e.legacyRow(t, org, repo, "9501", ids[0], "s0", 10)
	e.legacyRow(t, org, repo, "9501", "legacy-unlisted-s1", "s1", 30)
	e.finish(org, repo)
	wantQuantiles(t, e, org, floats(100, 30))
}

// KP-2: a re-run double. The slotted newest lists the legacy ids of BOTH artifacts of its slot (the superseded one too): only
// the slotted row counts.
func TestRegistryTestopsKP2ReRunDoubleListsBothLegacyIDs(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9502, "2026-01-02T12:00:00Z")
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9502, attempt: 2, started: "2026-01-02T12:00:00Z",
		artifacts: []string{attemptArtifact(77, "2026-01-02T10:05:00Z"), attemptArtifact(78, "2026-01-02T12:05:00Z")},
		zips:      map[string][]string{"77": {"10"}, "78": {"100"}}}, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	ids := e.suiteLegacyIDs(t, org, "9502")["s0"]
	if len(ids) != 2 {
		t.Fatalf("the slotted s0 lists %v legacy ids, want two (its artifact and the superseded one)", ids)
	}
	e.legacyRow(t, org, repo, "9502", ids[0], "s0", 100)
	e.legacyRow(t, org, repo, "9502", ids[1], "s0", 10)
	e.finish(org, repo)
	wantQuantiles(t, e, org, floats(100))
}

// KP-3 SHARD (the main prod shape): shards share a suite name across slots. Legacy rows of both shards, a slotted row of shard 1
// only: shard 2's legacy row stays. RED on the run-level rule and on a same-name match.
func TestRegistryTestopsKP3ShardNotReWrittenStays(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9503, "2026-01-02T10:00:00Z")
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9503, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{attemptNamedArtifact(91, "shard1", "2026-01-02T10:05:00Z")},
		zips:      map[string][]string{"91": {"100"}}}, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	ids := e.suiteLegacyIDs(t, org, "9503")["s0"]
	if len(ids) != 1 {
		t.Fatalf("shard 1 lists %v legacy ids, want one", ids)
	}
	e.legacyRow(t, org, repo, "9503", ids[0], "s0", 100)
	e.legacyRow(t, org, repo, "9503", "legacy-shard2", "s0", 40)
	e.finish(org, repo)
	wantQuantiles(t, e, org, floats(100, 40))
}

// KP-4: a legacy-only run counts every legacy row, same-content duplicates included (counted today and after: no worse).
func TestRegistryTestopsKP4LegacyOnlyRunCountsEveryRow(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9504, "2026-01-02T10:00:00Z")
	e.legacyRow(t, org, repo, "9504", "legacy-a", "s0", 7)
	e.legacyRow(t, org, repo, "9504", "legacy-b", "s0", 7)
	e.legacyRow(t, org, repo, "9504", "legacy-c", "s1", 9)
	e.finish(org, repo)
	wantQuantiles(t, e, org, floats(7, 7, 9))
}

// KP-8: CHAOS-4508 siblings, the same suite name twice in ONE artifact. The ordinal fold gives one legacy id per sibling; the
// slotted rows list both, so both legacy rows drop and only the two slotted rows count.
func TestRegistryTestopsKP8SiblingsOfOneNameDropOneToOne(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9508, "2026-01-02T10:00:00Z")
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9508, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{attemptArtifact(95, "2026-01-02T10:05:00Z")},
		zips:      map[string][]string{"95": {"10", "20"}}, names: map[string][]string{"95": {"dup", "dup"}}}, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	ids := e.suiteLegacyIDs(t, org, "9508")["dup"]
	seen := map[string]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if len(ids) != 2 || len(seen) != 2 {
		t.Fatalf("the two sibling suites list %v legacy ids, want two DIFFERENT ids (the ordinal fold)", ids)
	}
	e.legacyRow(t, org, repo, "9508", ids[0], "dup", 11)
	e.legacyRow(t, org, repo, "9508", ids[1], "dup", 21)
	e.finish(org, repo)
	wantQuantiles(t, e, org, floats(10, 20))
}

// KP-9: the legacy ids count only when a KEPT slotted row lists them. Attempt 1's slotted row lists the legacy id, attempt 2
// (the kept one) does not: the legacy row stays counted (the named residual: a suite only the first attempt had).
func TestRegistryTestopsKP9IdsListedOnlyByANonKeptAttemptDoNotDrop(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9510, "2026-01-02T10:00:00Z")
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9510, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{attemptArtifact(71, "2026-01-02T10:05:00Z")}, zips: map[string][]string{"71": {"10"}}}, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	attempt1 := e.suiteLegacyIDs(t, org, "9510")["s0"]
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9510, attempt: 2, started: "2026-01-02T12:00:00Z",
		artifacts: []string{attemptArtifact(72, "2026-01-02T12:05:00Z")}, zips: map[string][]string{"72": {"100"}}}, time.Date(2026, 1, 11, 0, 0, 0, 0, time.UTC))
	if len(attempt1) != 1 {
		t.Fatalf("attempt 1 lists %v legacy ids, want one", attempt1)
	}
	e.legacyRow(t, org, repo, "9510", attempt1[0], "s0", 55)
	e.finish(org, repo)
	// kept: the attempt-2 slotted row (100) and the legacy row (55): attempt 1's slotted row is not kept, so its list drops nothing.
	wantQuantiles(t, e, org, floats(100, 55))
}

// KP-10: a legacy row of ANOTHER run never drops, even with the same id string as a listed one.
func TestRegistryTestopsKP10ListedIdOfAnotherRunDoesNotDrop(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9511, "2026-01-02T10:00:00Z")
	e.startRun(t, org, 9512, "2026-01-02T10:00:00Z")
	e.slottedSuites(t, org, 9511, 1, "2026-01-02T10:00:00Z", []string{"100"})
	ids := e.suiteLegacyIDs(t, org, "9511")["s0"]
	e.legacyRow(t, org, repo, "9512", ids[0], "s0", 40) // run 9512 has no slotted row: its legacy row stays
	e.finish(org, repo)
	wantQuantiles(t, e, org, floats(100, 40))
}

// R-ID2 (named residual): a suite only attempt 1 had has a legacy row no later slotted row lists: it stays counted (not a loss).
func TestRegistryTestopsRID2SuiteOnlyTheFirstAttemptHadStaysCounted(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9509, "2026-01-02T12:00:00Z")
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9509, attempt: 2, started: "2026-01-02T12:00:00Z",
		artifacts: []string{attemptArtifact(98, "2026-01-02T12:05:00Z")}, zips: map[string][]string{"98": {"100"}}}, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	ids := e.suiteLegacyIDs(t, org, "9509")["s0"]
	e.legacyRow(t, org, repo, "9509", ids[0], "s0", 10)
	e.legacyRow(t, org, repo, "9509", "legacy-attempt1-only", "gone", 55)
	e.finish(org, repo)
	wantQuantiles(t, e, org, floats(100, 55))
}

// KP-5 coverage: the coverage fragment keeps the same four shapes on snapshot_id / legacy_snapshot_ids. Legacy coverage rows go
// through the real sink; the slotted coverage row comes from the real route (an LCOV report in the artifact).
func TestRegistryTestopsKP5CoverageFragment(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.startRun(t, org, 9505, "2026-01-02T10:00:00Z")
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9505, attempt: 1, started: "2026-01-02T10:00:00Z", lcov: true,
		artifacts: []string{attemptArtifact(96, "2026-01-02T10:05:00Z")}, zips: map[string][]string{"96": {"10"}}}, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	var listed []string
	if err := e.conn.QueryRow(e.ctx, `SELECT legacy_snapshot_ids FROM coverage_snapshots FINAL WHERE org_id = ? AND run_id = '9505' AND report_slot != ''`, org).Scan(&listed); err != nil || len(listed) != 1 {
		t.Fatalf("the slotted coverage row lists %v legacy snapshot ids: %v", listed, err)
	}
	legacy := func(run, id string) {
		raw, err := json.Marshal(map[string]any{"org_id": org, "repo_id": repo, "run_id": run, "snapshot_id": id, "report_format": "lcov", "lines_total": 2, "lines_covered": 1, "line_coverage_pct": 50.0, "last_synced": "2026-01-09T00:00:00Z"})
		if err != nil {
			t.Fatal(err)
		}
		effect, err := providersync.BuildEffectBatch("coverage_snapshots", providersync.EffectReadbackRequired, []json.RawMessage{raw})
		if err != nil {
			t.Fatal(err)
		}
		sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
		if err := sink.WriteEffect(e.ctx, pooledClaim(org, "acme/r"), effect); err != nil {
			t.Fatal(err)
		}
	}
	legacy("9505", listed[0])         // the re-read legacy snapshot: replaced
	legacy("9505", "legacy-unlisted") // nothing replaces it: stays
	legacy("9506", "legacy-only-run") // a legacy-only run: stays
	got := map[string]bool{}
	rows, err := e.conn.Query(e.ctx, `SELECT snapshot_id FROM coverage_snapshots AS c FINAL WHERE c.org_id = {org_id:String} AND `+testopspooled.CoverageRowsNewestAttempt("c", testopspooled.Scope{}), driver.NamedValue{Name: "org_id", Value: org})
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got[id] = true
	}
	rows.Close()
	if got[listed[0]] {
		t.Errorf("the legacy snapshot the slotted row lists was kept: %v", got)
	}
	if !got["legacy-unlisted"] || !got["legacy-only-run"] {
		t.Errorf("an unlisted or legacy-only snapshot was dropped: %v", got)
	}
	if len(got) != 3 { // slotted + 2 legacy
		t.Errorf("kept %d snapshots %v, want 3 (the slotted one and the two unreplaced legacy rows)", len(got), got)
	}
}

// GitLab mixed run (retry after the roll): the pre-change writer stored the legacy suite row of job 11 (no slot); a pass of the
// current writer then sees job 11 retried and job 12 as the latest and writes the slotted row at attempt 2 under a NEW key. The
// slotted GitLab row lists the legacy id, so the legacy row drops: no double. Seed shape from the writer-side cell of the PR-0 lane
// (GitLab retried-job shape through the production chunked route); the legacy row goes through the real sink with the id the
// slotted row lists (read back).
type glDoer struct {
	t *testing.T
}

func glLcovArchive() []byte {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, _ := writer.Create("coverage.info")
	_, _ = member.Write([]byte("SF:a.go\nDA:1,1\nDA:2,0\nend_of_record\n"))
	_ = writer.Close()
	return buffer.Bytes()
}

func (d glDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	const root = "/api/v4/projects/123"
	switch {
	case path == root:
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main"}`
	case path == root+"/pipelines":
		body = `[{"id":9001,"name":"CI","ref":"main","status":"success","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T11:40:00Z","updated_at":"2026-07-22T11:40:00Z","source":"push","sha":"abc","web_url":"https://gitlab.example/acme/api/-/pipelines/9001"}]`
	case path == root+"/pipelines/9001/jobs":
		items := []string{}
		for _, job := range []struct {
			id      int
			retried bool
		}{{11, true}, {12, false}} {
			if job.retried && request.URL.Query().Get("include_retried") != "true" {
				continue
			}
			items = append(items, fmt.Sprintf(`{"id":%d,"name":"unit","stage":"test","status":"success","allow_failure":false,"started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T11:40:00Z","retried":%t,"artifacts_file":{"filename":"reports.zip"}}`, job.id, job.retried))
		}
		body = "[" + strings.Join(items, ",") + "]"
	case path == root+"/pipelines/9001/test_report":
		body = `{"test_suites":[{"name":"unit","total_time":"100","test_cases":[{"name":"c","classname":"tests.T","status":"success","execution_time":"0.5"}]}]}`
	case path == root+"/pipelines/9001/test_report_summary":
		body = `{"total":{"time":100,"count":1,"success":1,"failed":0,"skipped":0,"error":0,"suite_error":null},"test_suites":[{"name":"unit","total_time":100,"total_count":1,"build_ids":[12],"suite_error":null}]}`
	case strings.HasPrefix(path, root+"/jobs/") && strings.HasSuffix(path, "/artifacts"):
		header.Set("Content-Type", "application/zip")
		body = string(glLcovArchive())
	default:
		d.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestRegistryTestopsGitLabMixedRunHasNoDouble(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	capability, _ := providersync.Capability("gitlab", "tests")
	since := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	before := since.Add(time.Hour)
	claim := pooledClaim(org, "123")
	claim.Provider, claim.CostClass, claim.SinceAt, claim.BeforeAt = "gitlab", capability.CostClass, &since, &before
	claim.LeaseExpiresAt = before.Add(time.Hour)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	client, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example", fakehttp.Client(glDoer{t}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, lease)
	if err != nil {
		t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: lease}
	err = providersync.GitLabTestsRouteHandler{}.CollectChunks(e.ctx, claim, providerfoundation.Credential{}, client, before.Add(5*time.Minute), "",
		func(emission providersync.ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				if len(effect.Rows) == 0 {
					continue
				}
				if err := sink.WriteEffect(e.ctx, claim, effect); err != nil {
					return fmt.Errorf("write %s: %w", effect.Destination, err)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	var repo string
	if err := e.conn.QueryRow(e.ctx, `SELECT toString(repo_id) FROM ci_pipeline_runs FINAL WHERE org_id = ? LIMIT 1`, org).Scan(&repo); err != nil {
		t.Fatal(err)
	}
	ids := e.suiteLegacyIDs(t, org, "9001")["unit"]
	if len(ids) != 1 {
		t.Fatalf("the slotted GitLab suite lists %v legacy ids, want the one the pre-change writer stored (attempt 2 of a retried job)", ids)
	}
	// The legacy row of job 11 as the writer before the slot columns stored it.
	e.legacySuite(t, org, repo, "9001", ids[0], "unit", 10, "2026-07-22T10:01:00Z")
	e.own(org, "team-k", repo)
	e.computeDaily(org, []string{repo}, "2026-07-22")
	p50 := e.value(org, "suite_duration_p50_seconds", "", "2026-07-21", "2026-07-23")
	p95 := e.value(org, "suite_duration_p95_seconds", "", "2026-07-21", "2026-07-23")
	if !near(p50["total"], 100) || !near(p95["total"], 100) {
		t.Errorf("GitLab mixed run double-counts: p50 %v p95 %v, want 100 / 100", p50, p95)
	}
}
