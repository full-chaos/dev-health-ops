//go:build integration

package workersctl

// `metrics testops-slot-recompute` end to end on real PostgreSQL and ClickHouse. The ClickHouse rows are the production route's
// emissions (testdata/chaos14544_retired_slot_emission.json, recorded by TestRecordChaos14544VerbFixture in
// internal/jobs/metrics/daily): run R on D at attempt 1 (slot total-b fails case a) and attempt 2 (total-b retired), run S on
// D+1 (case a fails). STALE stores D and D+1 before attempt 2 and recomputes only D after it, as the touched day does; CONTROL
// computes every day after every sync. The cell runs the verb, executes the daily partitions it starts (the worker's part, with
// the production executors), and wants the stored days D .. D+29 of STALE equal to CONTROL's.

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type slotRecomputePass struct {
	Name    string `json:"name"`
	Effects []struct {
		Destination string            `json:"destination"`
		Rows        []json.RawMessage `json:"rows"`
	} `json:"effects"`
}

// slotRecomputeLease is a lease that always holds.
type slotRecomputeLease struct{}

func (slotRecomputeLease) Assert(context.Context) error { return nil }

func TestTestopsSlotRecomputeApplyStoresTheControlDays(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
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
	runtime := triggerIntegrationRuntimeWithAuditor(t, ctx, commandAuthorizer{}, migratedPostgresAuditor)
	runtime.lookup = func(key string) (string, bool) {
		if key == "CLICKHOUSE_URI" {
			return instance.URI, true
		}
		return "", false
	}
	pool := runtime.pools.Domain

	raw, err := os.ReadFile(filepath.Join("testdata", "chaos14544_retired_slot_emission.json"))
	if err != nil {
		t.Fatal(err)
	}
	var passes []slotRecomputePass
	if err := json.Unmarshal(raw, &passes); err != nil {
		t.Fatal(err)
	}
	replay := func(org, name string) {
		t.Helper()
		capability, _ := providersync.Capability("github", "tests")
		since := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		before := since.Add(time.Hour)
		claim := providersync.Claim{
			Unit: providersync.Unit{
				ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org, IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
				SourceExternalID: "acme/api", SourceName: "acme/api", Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
				Mode: "incremental", SinceAt: &since, BeforeAt: &before, CredentialID: uuid.NewString(), AuthSource: "integration_credential",
			},
			Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.Add(time.Hour),
		}
		sink := providersync.TestOpsClickHouseEffects{Conn: conn, Lease: slotRecomputeLease{}}
		for _, pass := range passes {
			if pass.Name != name {
				continue
			}
			for _, effect := range pass.Effects {
				rows := make([]json.RawMessage, 0, len(effect.Rows))
				for _, row := range effect.Rows {
					var fields map[string]any
					if err := json.Unmarshal(row, &fields); err != nil {
						t.Fatal(err)
					}
					fields["org_id"] = org
					encoded, err := json.Marshal(fields)
					if err != nil {
						t.Fatal(err)
					}
					rows = append(rows, encoded)
				}
				if err := sink.WriteEffect(ctx, claim, providersync.EffectBatch{Destination: effect.Destination, Rows: rows}); err != nil {
					t.Fatalf("replay %s %s: %v", name, effect.Destination, err)
				}
			}
			return
		}
		t.Fatalf("the fixture has no pass %s", name)
	}
	compute := func(org string, day time.Time, repos []daily.RepositoryID) {
		t.Helper()
		run := daily.Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
		part := daily.Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: repos}
		pipeline, err := daily.NewTestopsPipelineExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		tests, err := daily.NewTestopsTestExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		risk, err := daily.NewTestopsRiskExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range []daily.NativeFamilyExecutor{pipeline, tests, risk} {
			if _, err := family.ComputeFamily(ctx, run, part); err != nil {
				t.Fatal(err)
			}
		}
	}
	repoOf := func(org string) []daily.RepositoryID {
		t.Helper()
		var repo string
		if err := conn.QueryRow(ctx, "SELECT toString(any(repo_id)) FROM test_suite_results WHERE org_id = ?", org).Scan(&repo); err != nil {
			t.Fatal(err)
		}
		return []daily.RepositoryID{daily.RepositoryID(repo)}
	}
	// stored is every stored day row of the org: the live testops_test_metrics_daily row and the release confidence row.
	stored := func(org string) map[string]string {
		t.Helper()
		out := map[string]string{}
		for _, query := range []string{
			`SELECT concat('test ', toString(day)), formatRow('JSONEachRow', total_cases, passed_count, failed_count, skipped_count, pass_rate,
  failure_rate, total_suites, flake_cases, distinct_cases, flake_rate, failure_recurrence_score)
FROM (SELECT * FROM testops_test_metrics_daily FINAL WHERE org_id = ?) WHERE retracted = 0`,
			`SELECT concat('risk ', toString(day)), formatRow('JSONEachRow', confidence_score, regression_penalty, factors_json)
FROM testops_release_confidence FINAL WHERE org_id = ?`,
		} {
			rows, err := conn.Query(ctx, query, org)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var key, value string
				if err := rows.Scan(&key, &value); err != nil {
					t.Fatal(err)
				}
				out[key] = strings.TrimSpace(value)
			}
			rows.Close()
		}
		return out
	}
	recurrence := func(org string, day time.Time) float64 {
		t.Helper()
		var value float64
		if err := conn.QueryRow(ctx, `SELECT failure_recurrence_score FROM (SELECT * FROM testops_test_metrics_daily FINAL WHERE org_id = ? AND day = ?)
WHERE retracted = 0`, org, day).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}

	const control, stale = "00000000-0000-4000-8000-000000145441", "00000000-0000-4000-8000-000000145442"
	dayD := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	dayX, through := dayD.AddDate(0, 0, 1), dayD.AddDate(0, 0, daily.RetiredSlotHistoryDays)

	replay(control, "R1")
	replay(control, "R2")
	replay(control, "S")
	compute(control, dayD, repoOf(control))
	compute(control, dayX, repoOf(control))
	replay(stale, "R1")
	replay(stale, "S")
	compute(stale, dayD, repoOf(stale))
	compute(stale, dayX, repoOf(stale))
	if got := recurrence(stale, dayX); got != 1 {
		t.Fatalf("STALE D+1 before the retirement: recurrence %v, want 1 (case a failed on D in total-b)", got)
	}
	replay(stale, "R2")
	time.Sleep(2100 * time.Millisecond)
	compute(stale, dayD, repoOf(stale))
	want := stored(control)
	if got := recurrence(control, dayX); got != 0 {
		t.Fatalf("CONTROL D+1: recurrence %v, want 0", got)
	}
	if got := stored(stale); reflect.DeepEqual(got, want) {
		t.Fatalf("control: STALE already stores CONTROL's days before the verb, so the cell proves nothing: %v", got)
	}

	runs := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM daily_metrics_runs WHERE org_id = $1::uuid", stale).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	call := func(label string, extra ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		argv := append([]string{"metrics", "testops-slot-recompute", "--org", stale, "--from", dayD.Format(time.DateOnly), "--to",
			dayD.Format(time.DateOnly), "--through", through.Format(time.DateOnly), "--rerun-tag", "slots-1"}, extra...)
		code := dispatch(ctx, runtime, argv, &stdout, &stderr)
		t.Logf("[%s] code=%d stdout=%s stderr=%s", label, code, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
		return code, stdout.String(), stderr.String()
	}
	apply := []string{"--apply", "--reason", "operator_test", "--correlation-id", "corr-14544"}

	// Preview: the plan, no run.
	code, out, _ := call("preview")
	if code != 0 || !strings.Contains(out, `"preview":true`) || !strings.Contains(out, `"repo_days":30`) || runs() != 0 {
		t.Fatalf("preview: code=%d out=%s runs=%d; want the plan of 30 repository-days (D .. D+29) and no run", code, out, runs())
	}
	// Over the cap: a stop with the count, no run.
	code, _, errText := call("cap", append([]string{"--max-repo-days", "29"}, apply...)...)
	if code != 1 || !strings.Contains(errText, `"code":"repo_day_cap"`) || !strings.Contains(errText, `"repo_days":30`) || runs() != 0 {
		t.Fatalf("cap: code=%d stderr=%s runs=%d; want repo_day_cap with the count and no run", code, errText, runs())
	}
	// Apply: one tagged run per day; the worker's part runs each started partition with the production executors.
	code, out, _ = call("apply", apply...)
	if code != 0 || runs() != 30 || strings.Contains(out, `"started":false`) {
		t.Fatalf("apply: code=%d runs=%d out=%s; want 30 started runs", code, runs(), out)
	}
	time.Sleep(2100 * time.Millisecond)
	executePartitions := func() int {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT r.target_day::text, p.repo_ids::text FROM daily_metrics_runs r
JOIN daily_metrics_partitions p ON p.run_id = r.id WHERE r.org_id = $1::uuid ORDER BY r.target_day`, stale)
		if err != nil {
			t.Fatal(err)
		}
		type work struct {
			day   time.Time
			repos []daily.RepositoryID
		}
		var items []work
		for rows.Next() {
			var day, ids string
			if err := rows.Scan(&day, &ids); err != nil {
				t.Fatal(err)
			}
			parsed, err := time.Parse(time.DateOnly, day)
			if err != nil {
				t.Fatal(err)
			}
			var repos []daily.RepositoryID
			for _, id := range strings.Split(strings.Trim(ids, "{}[]\" "), ",") {
				if id = strings.Trim(id, "\" "); id != "" {
					repos = append(repos, daily.RepositoryID(id))
				}
			}
			items = append(items, work{parsed, repos})
		}
		rows.Close()
		for _, item := range items {
			compute(stale, item.day, item.repos)
		}
		return len(items)
	}
	if n := executePartitions(); n != 30 {
		t.Fatalf("the started runs hold %d partitions, want 30 (one repository per day)", n)
	}
	if got := stored(stale); !reflect.DeepEqual(got, want) {
		t.Fatalf("after the verb STALE stores\n %v\nwant CONTROL\n %v", got, want)
	}

	// A second apply with the same tag starts nothing and the stored days stay CONTROL's.
	code, out, _ = call("apply again", apply...)
	if code != 0 || runs() != 30 || strings.Contains(out, `"started":true`) {
		t.Fatalf("second apply: code=%d runs=%d out=%s; want every day started=false and no new run", code, runs(), out)
	}
	if got := stored(stale); !reflect.DeepEqual(got, want) {
		t.Fatalf("after the second apply STALE stores %v, want CONTROL %v", got, want)
	}
}
