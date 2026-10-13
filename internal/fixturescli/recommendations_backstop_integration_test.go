//go:build integration

package fixturescli

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The fixtures load with --backstop-database starts the same recommendations run the nightly
// schedule and the operator verb start (all teams, window 14), once per day for the org, and a
// load without the flag starts none.
func TestGenerateVerbStartsTheRecommendationsBackstop(t *testing.T) {
	ctx := context.Background()
	prior := jobsContractRoot
	jobsContractRoot = "../../contracts/jobs/v1"
	t.Cleanup(func() { jobsContractRoot = prior })

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	ch := startClickHouse(t)
	stopMerges(t, ch.httpDSN)
	const org = "99999999-8888-4777-8666-555555555556"
	env := map[string]string{"ORG_ID": org}

	runs := func() int {
		var count int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM public.remaining_metric_runs WHERE org_id = $1 AND family = 'recommendations'`, org).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	code, _, stderr := runGenerateVerb(t, env, acrArgs(ch.instance.URI)...)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if got := runs(); got != 0 {
		t.Fatalf("a load without --backstop-database started %d recommendations runs", got)
	}

	var statuses []string
	for range 2 {
		code, stdout, stderr := runGenerateVerb(t, env, acrArgs(ch.instance.URI, "--allow-mixed-org", "--backstop-database", instance.URI)...)
		if code != cli.ExitOK {
			t.Fatalf("exit %d: %s", code, stderr)
		}
		var result struct {
			Backstop RecommendationsBackstopResult `json:"recommendations_backstop"`
		}
		if err := json.Unmarshal([]byte(stdout), &result); err != nil {
			t.Fatalf("summary %q: %v", stdout, err)
		}
		if result.Backstop.RunID == "" {
			t.Fatalf("no run id in %q", stdout)
		}
		statuses = append(statuses, result.Backstop.Status)
	}
	if statuses[0] != "started" || statuses[1] == "started" {
		t.Fatalf("statuses = %v, want the first load to start the run and the second not to start another", statuses)
	}
	if got := runs(); got != 1 {
		t.Fatalf("recommendations runs = %d, want 1", got)
	}
	var window, partitions int
	if err := pool.QueryRow(ctx, `
SELECT count(*), coalesce(max((p.scope->>'window')::int), 0)
FROM public.remaining_metric_partitions p JOIN public.remaining_metric_runs r ON r.id = p.run_id
WHERE r.org_id = $1 AND r.family = 'recommendations' AND p.scope->>'team_id' IS NULL`, org).Scan(&partitions, &window); err != nil {
		t.Fatal(err)
	}
	if partitions != 1 || window != recommendationsWindowDays {
		t.Fatalf("partitions = %d, window = %d, want one all-teams partition with window %d", partitions, window, recommendationsWindowDays)
	}
}
