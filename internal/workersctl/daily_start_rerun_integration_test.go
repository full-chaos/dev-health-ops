//go:build integration

package workersctl

// End-to-end dispatch of `metrics daily-start --rerun-tag` on real PostgreSQL:
// the real dispatch, the real auditor, --org-stdin. It pins the wiring from the
// flag to the generation and to the output (a wiring fault exits 0 and starts
// nothing), and the production case: a repository-scoped tagged call on a day
// that already has a succeeded scheduled run must start.

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestDailyStartDispatchWithARerunTagOnRealPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	runtime := triggerIntegrationRuntimeWithAuditor(t, ctx, commandAuthorizer{}, migratedPostgresAuditor)
	pool := runtime.pools.Domain
	const org = "00000000-0000-4000-8000-0000000090aa"
	repoA, repoB := "11111111-1111-4111-8111-111111111111", "00000000-0000-0000-0000-000000000000"
	call := func(label string, args ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		runtime.stdin = strings.NewReader(org + "\n")
		a := append([]string{"metrics", "daily-start"}, args...)
		a = append(a, "--org-stdin", "--reason", "operator_test", "--correlation-id", "corr-"+label)
		code := dispatch(ctx, runtime, a, &stdout, &stderr)
		t.Logf("[%s] code=%d stdout=%s stderr=%s", label, code, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
		return code, stdout.String(), stderr.String()
	}
	runs := func(day string) int {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM daily_metrics_runs WHERE org_id=$1::uuid AND target_day=$2::date", org, day).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	repos := []string{"--repo-id", repoA, "--repo-id", repoB}
	day := "2026-08-01"
	with := func(extra ...string) []string { return append(append([]string{"--day", day}, extra...), repos...) }

	call("plain1", with()...)
	call("plain2", with()...)
	if runs(day) != 1 {
		t.Fatalf("plain calls: runs=%d want 1", runs(day))
	}
	_, out, _ := call("tagA1", with("--rerun-tag", "fix-1")...)
	if runs(day) != 2 || !strings.Contains(out, `"started":true`) || !strings.Contains(out, `"rerun_tag":"fix-1"`) {
		t.Fatalf("tagA1 runs=%d out=%s", runs(day), out)
	}
	_, out, _ = call("tagA2", with("--rerun-tag", "fix-1")...)
	if runs(day) != 2 || !strings.Contains(out, `"started":false`) {
		t.Fatalf("tagA2 runs=%d out=%s", runs(day), out)
	}
	call("tagCase", with("--rerun-tag", "FIX-1")...)
	if runs(day) != 3 {
		t.Fatalf("tag differing by case must be a new run: runs=%d", runs(day))
	}
	call("tagOrder", "--day", day, "--rerun-tag", "fix-1", "--repo-id", repoB, "--repo-id", repoA)
	if runs(day) != 3 {
		t.Fatalf("repo order must not matter: runs=%d", runs(day))
	}
	call("tagSubset", "--day", day, "--rerun-tag", "fix-1", "--repo-id", repoA)
	if runs(day) != 4 {
		t.Fatalf("a different repo set is a new request: runs=%d", runs(day))
	}
	c1, _, _ := call("len1", with("--rerun-tag", "x")...)
	c32, _, _ := call("len32", with("--rerun-tag", strings.Repeat("y", 32))...)
	c33, _, _ := call("len33", with("--rerun-tag", strings.Repeat("y", 33))...)
	if c1 != 0 || c32 != 0 || c33 == 0 {
		t.Fatalf("length codes %d %d %d", c1, c32, c33)
	}
	call("win", "--day", "2026-08-01", "--to", "2026-08-03", "--rerun-tag", "fix-1", "--repo-id", repoA, "--repo-id", repoB)
	if runs("2026-08-02") != 1 || runs("2026-08-03") != 1 {
		t.Fatalf("partial window: 08-02=%d 08-03=%d", runs("2026-08-02"), runs("2026-08-03"))
	}
	c31, _, _ := call("w31", "--day", "2026-09-01", "--to", "2026-10-01", "--rerun-tag", "w", "--repo-id", repoA)
	c32d, _, _ := call("w32", "--day", "2026-09-01", "--to", "2026-10-02", "--rerun-tag", "w", "--repo-id", repoA)
	if c31 != 0 || c32d == 0 {
		t.Fatalf("window codes %d %d", c31, c32d)
	}
	// production shape: the day has a SUCCEEDED scheduled-fanout run
	if _, err := pool.Exec(ctx, "INSERT INTO daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org) VALUES (gen_random_uuid(), $1::uuid, '2026-06-01', 'fixed-schedule:daily_metrics_fanout:2026-06-02T02:00:00Z', 'succeeded', 'succeeded', now(), now(), true)", org); err != nil {
		t.Fatal(err)
	}
	_, out, _ = call("schedcovered-repo", "--day", "2026-06-01", "--rerun-tag", "fix-9", "--repo-id", repoA, "--repo-id", repoB)
	if !strings.Contains(out, `"started":true`) {
		t.Fatalf("repo-scoped tagged call on a schedule-covered day did not start: %s", out)
	}
	c, _, e := call("schedcovered-plain", "--day", "2026-06-01")
	if c == 0 || !strings.Contains(e, "already_covered") {
		t.Fatalf("an untagged no-repo call on a schedule-covered day must stay already_covered: %d %s", c, e)
	}
	c, out, _ = call("schedcovered-norepo", "--day", "2026-06-01", "--rerun-tag", "fix-9")
	if c != 0 || !strings.Contains(out, `"started":true`) || !strings.Contains(out, `"covered_day_overridden_by"`) {
		t.Fatalf("a tagged no-repo call on a schedule-covered day must start and name the run it overrides: %d %s", c, out)
	}
	c, out, _ = call("schedcovered-norepo-again", "--day", "2026-06-01", "--rerun-tag", "fix-9")
	if c != 0 || !strings.Contains(out, `"started":false`) || strings.Contains(out, "covered_day_overridden_by") {
		t.Fatalf("the same tag again must start nothing and override nothing: %d %s", c, out)
	}
}
