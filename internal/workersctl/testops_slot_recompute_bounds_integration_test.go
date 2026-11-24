//go:build integration

package workersctl

// `metrics testops-slot-recompute` plan bounds on real PostgreSQL and ClickHouse, on planted test_suite_results rows (only the
// selection reads them): --through bounds the selected repository-days before the row cap, an apply over a cap writes no audit
// row and starts no run, and no planned day holds more repositories than one daily run admits. Every helper takes the subtest's
// t, so one failed case never stops the others.

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestTestopsSlotRecomputePlanBounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
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
	day := func(text string) time.Time {
		parsed, err := time.Parse(time.DateOnly, text)
		if err != nil {
			panic(err)
		}
		return parsed.Add(10 * time.Hour)
	}
	synced := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	// plant writes one suite key of run in repo on the day; retracted marks it retired.
	plant := func(t *testing.T, org, repo, run, suite, on string, retracted uint8) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count,
  failed_count, skipped_count, started_at, last_synced, retracted) VALUES (?, ?, ?, ?, 's', 1, 1, 0, 0, ?, ?, ?)`,
			org, repo, run, suite, day(on), synced, retracted); err != nil {
			t.Fatal(err)
		}
	}
	plantRepos := func(t *testing.T, org, on string, repos int) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count,
  failed_count, skipped_count, started_at, last_synced, retracted) SELECT ?, generateUUIDv4(), 'r', 's', 's', 1, 1, 0, 0, ?, ?, 1 FROM numbers(?)`,
			org, day(on), synced, repos); err != nil {
			t.Fatal(err)
		}
	}
	call := func(t *testing.T, org string, args ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		argv := append([]string{"metrics", "testops-slot-recompute", "--org", org, "--rerun-tag", "bounds-1"}, args...)
		code := dispatch(ctx, runtime, argv, &stdout, &stderr)
		t.Logf("[%s] code=%d stdout=%.300s stderr=%s", strings.Join(args, " "), code, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
		return code, stdout.String(), stderr.String()
	}
	type plan struct {
		RepoDays int `json:"repo_days"`
		Days     []struct {
			Day string `json:"day"`
		} `json:"days"`
	}
	planOf := func(t *testing.T, out string) plan {
		t.Helper()
		var answer map[string]plan
		if err := json.Unmarshal([]byte(out), &answer); err != nil {
			t.Fatalf("decode %q: %v", out, err)
		}
		return answer["testops_slot_recompute"]
	}
	count := func(t *testing.T, query, org string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, query, org).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	newOrg := func() string { return uuid.NewString() }
	repoA, repoB := uuid.NewString(), uuid.NewString()

	// --through bounds the selected repository-days before the cap.
	t.Run("through: one eligible day", func(t *testing.T) {
		org := newOrg()
		plant(t, org, repoA, "r1", "s1", "2026-07-01", 1)
		code, out, _ := call(t, org, "--from", "2026-07-01", "--to", "2026-07-01", "--through", "2026-07-01", "--max-repo-days", "1")
		if code != 0 || planOf(t, out).RepoDays != 1 {
			t.Fatalf("code=%d out=%s; want one planned repository-day", code, out)
		}
	})
	t.Run("through: later retirement", func(t *testing.T) {
		org := newOrg()
		plant(t, org, repoA, "r1", "s1", "2026-07-01", 1)
		plant(t, org, repoB, "r2", "s2", "2026-07-02", 1)
		code, out, _ := call(t, org, "--from", "2026-07-01", "--to", "2026-07-02", "--through", "2026-07-01", "--max-repo-days", "1")
		if code != 0 || planOf(t, out).RepoDays != 1 {
			t.Fatalf("code=%d out=%s; want the July 1 repository-day only (July 2 is after --through)", code, out)
		}
	})
	t.Run("through: later day of the same run", func(t *testing.T) {
		org := newOrg()
		plant(t, org, repoA, "r1", "s1", "2026-07-01", 1)
		plant(t, org, repoA, "r1", "s2", "2026-07-02", 0)
		code, out, _ := call(t, org, "--from", "2026-07-01", "--to", "2026-07-01", "--through", "2026-07-01", "--max-repo-days", "1", "--repo-id", repoA)
		if code != 0 || planOf(t, out).RepoDays != 1 {
			t.Fatalf("code=%d out=%s; want the July 1 repository-day only", code, out)
		}
	})
	t.Run("through: earlier day of the same run", func(t *testing.T) {
		org := newOrg()
		plant(t, org, repoA, "r1", "s1", "2026-07-02", 1)
		plant(t, org, repoA, "r1", "s2", "2026-07-01", 0)
		code, out, _ := call(t, org, "--from", "2026-07-02", "--to", "2026-07-02", "--through", "2026-07-01", "--max-repo-days", "1")
		if got := planOf(t, out); code != 0 || got.RepoDays != 1 || len(got.Days) != 1 || got.Days[0].Day != "2026-07-01" {
			t.Fatalf("code=%d out=%s; want the run's July 1 repository-day", code, out)
		}
	})
	t.Run("through: expanded work exceeds the cap", func(t *testing.T) {
		org := newOrg()
		plant(t, org, repoA, "r1", "s1", "2026-07-01", 1)
		code, _, stderr := call(t, org, "--from", "2026-07-01", "--to", "2026-07-01", "--through", "2026-07-02", "--max-repo-days", "1")
		if code != 1 || !strings.Contains(stderr, `"code":"repo_day_cap"`) || !strings.Contains(stderr, `"repo_days":2`) {
			t.Fatalf("code=%d stderr=%s; want repo_day_cap with 2 repository-days", code, stderr)
		}
	})

	// An apply over the cap writes no audit row and starts no run.
	t.Run("cap: apply over the cap writes nothing", func(t *testing.T) {
		org := newOrg()
		plant(t, org, repoA, "r1", "s1", "2026-07-01", 1)
		code, _, stderr := call(t, org, "--from", "2026-07-01", "--to", "2026-07-01", "--through", "2026-07-02", "--max-repo-days", "1",
			"--apply", "--reason", "operator_test", "--correlation-id", "cap-check")
		audits := count(t, "SELECT count(*) FROM public.worker_operator_audits WHERE resource_id = $1", org)
		runs := count(t, "SELECT count(*) FROM daily_metrics_runs WHERE org_id = $1::uuid", org)
		if code != 1 || !strings.Contains(stderr, `"code":"repo_day_cap"`) || audits != 0 || runs != 0 {
			t.Fatalf("code=%d stderr=%s audit rows %d runs %d; want repo_day_cap, 0 audit rows and 0 runs", code, stderr, audits, runs)
		}
	})

	// A day of more repositories than one daily run admits stops in the preview, before any start.
	for _, repos := range []int{daily.MaxRepositoriesPerRun, daily.MaxRepositoriesPerRun + 1} {
		t.Run("day size: one day of "+strconv.Itoa(repos)+" repositories", func(t *testing.T) {
			org := newOrg()
			plantRepos(t, org, "2026-07-01", repos)
			code, out, stderr := call(t, org, "--from", "2026-07-01", "--to", "2026-07-01", "--through", "2026-07-01", "--max-repo-days", "2000")
			if repos <= daily.MaxRepositoriesPerRun {
				if code != 0 || planOf(t, out).RepoDays != repos {
					t.Fatalf("code=%d stderr=%s; want a plan of %d repository-days", code, stderr, repos)
				}
				return
			}
			if code != 1 || !strings.Contains(stderr, `"code":"day_repo_cap"`) || !strings.Contains(stderr, `"day":"2026-07-01"`) ||
				!strings.Contains(stderr, `"repos":`+strconv.Itoa(repos)) {
				t.Fatalf("code=%d stderr=%s; want day_repo_cap naming 2026-07-01 and %d repositories", code, stderr, repos)
			}
			code, _, stderr = call(t, org, "--from", "2026-07-01", "--to", "2026-07-01", "--through", "2026-07-01", "--max-repo-days", "2000",
				"--apply", "--reason", "operator_test", "--correlation-id", "day-cap")
			if runs := count(t, "SELECT count(*) FROM daily_metrics_runs WHERE org_id = $1::uuid", org); code != 1 || runs != 0 {
				t.Fatalf("apply: code=%d stderr=%s runs %d; want the stop and no run", code, stderr, runs)
			}
		})
	}
}
