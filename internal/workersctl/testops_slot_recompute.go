package workersctl

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// testopsSlotRecomputeDefaultCap bounds the repository-days one call may start (the repair's records bound).
const testopsSlotRecomputeDefaultCap = 200

// testopsSlotRecomputeToday is the clock of the default --through; tests fix it.
var testopsSlotRecomputeToday = time.Now

// dispatchMetricsTestopsSlotRecompute handles `metrics testops-slot-recompute`: it starts the daily runs again for the days
// whose stored recurrence a retired report slot left stale. It selects the repository-days of every run with a retired slot
// (daily.RetiredSlotRepoDaysSQL; the retraction's day in --from .. --to), expands each day D to D .. D+29 up to --through, and
// starts one tagged daily run per day for its repositories, through the `metrics daily-start --rerun-tag` path (the same
// generation, so a second call with the same tag starts nothing). It previews by default and writes only with --apply. Over
// --max-repo-days it stops before any write.
func dispatchMetricsTestopsSlotRecompute(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("metrics testops-slot-recompute")
	org := flags.String("org", "", "organization id (uuid)")
	orgStdin := flags.Bool("org-stdin", false, orgStdinUsage)
	from := flags.String("from", "", "first day of a retired slot, inclusive (YYYY-MM-DD, UTC)")
	to := flags.String("to", "", "last day of a retired slot, inclusive (YYYY-MM-DD, UTC)")
	through := flags.String("through", "", "last day to compute again, inclusive (YYYY-MM-DD, UTC); default today")
	var repoIDs stringList
	flags.Var(&repoIDs, "repo-id", "limit the selection to this repository (uuid, repeatable)")
	rerunTag := flags.String("rerun-tag", "", "the rerun tag of the daily runs (required): the same tag starts nothing a second time")
	maxRepoDays := flags.Int("max-repo-days", testopsSlotRecomputeDefaultCap, "stop before any write when more repository-days are selected")
	apply := flags.Bool("apply", false, "start the daily runs; without it the verb only previews")
	mutation := addMutationFlags(flags)
	if flags.Parse(args) != nil || flags.NArg() != 0 || !mutation.valid(!*apply) || !daily.ValidRerunTag(*rerunTag) || *maxRepoDays < 1 {
		return writeError(stderr, "invalid_request")
	}
	fromDay, fromErr := time.Parse(time.DateOnly, *from)
	toDay, toErr := time.Parse(time.DateOnly, *to)
	if fromErr != nil || toErr != nil || toDay.Before(fromDay) || int(toDay.Sub(fromDay).Hours()/24)+1 > manualBackfillMaxDays {
		return writeError(stderr, "invalid_request")
	}
	now := testopsSlotRecomputeToday().UTC()
	throughDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if *through != "" {
		parsed, err := time.Parse(time.DateOnly, *through)
		if err != nil || parsed.Before(fromDay) {
			return writeError(stderr, "invalid_request")
		}
		throughDay = parsed
	}
	repositoryIDs, err := canonicalDedupedRepositoryIDs(repoIDs)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	canonicalOrg, code := resolveOrgFlag(runtime, *org, *orgStdin, stderr)
	if code != 0 {
		return code
	}
	if runtime.service == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	if !*apply {
		if err := runtime.service.Authorize(ctx, runtime.principal, joboperator.ActionMetricsDailyStart, "organization", canonicalOrg); err != nil {
			return writeServiceError(stderr, err)
		}
		days, code := selectTestopsSlotRecomputeDays(ctx, runtime, stderr, canonicalOrg, fromDay, toDay, throughDay, repositoryIDs, *maxRepoDays)
		if code != 0 {
			return code
		}
		plan := testopsSlotRecomputePlan(*rerunTag, fromDay, toDay, throughDay, days)
		plan["preview"] = true
		return writeResult(stdout, stderr, map[string]any{"testops_slot_recompute": plan})
	}
	// The write is audited before it reads: the selection, the cap stop and the starts all run inside the audited write, so a
	// stop over the cap leaves a failed audit row and starts no run.
	perform := func(ctx context.Context) int {
		days, code := selectTestopsSlotRecomputeDays(ctx, runtime, stderr, canonicalOrg, fromDay, toDay, throughDay, repositoryIDs, *maxRepoDays)
		if code != 0 {
			return code
		}
		plan := testopsSlotRecomputePlan(*rerunTag, fromDay, toDay, throughDay, days)
		if runtime.pools == nil || runtime.registry == nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		store, err := daily.NewPostgresStore(runtime.pools.Domain)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		publisher, err := daily.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		started := make([]map[string]any, 0, len(days))
		for _, day := range days {
			outcomes, err := startDailyDays(ctx, store, publisher, canonicalOrg, day.Day, day.Day, day.RepoIDs, *rerunTag, true)
			if err != nil {
				return writeServiceError(stderr, err)
			}
			for _, outcome := range outcomes {
				started = append(started, map[string]any{"day": outcome.Day, "run_id": outcome.RunID, "started": !outcome.AlreadyStarted})
			}
		}
		plan["runs"] = started
		return writeResult(stdout, stderr, map[string]any{"testops_slot_recompute": plan})
	}
	return auditedWriteWith(ctx, runtime, stderr, mutation, joboperator.ActionMetricsDailyStart, "organization", canonicalOrg, !*orgStdin, perform)
}

// selectTestopsSlotRecomputeDays reads the retired-slot repository-days from ClickHouse (CLICKHOUSE_URI, resolved only here) and
// expands them. It reads at most maxRepoDays+1 rows; more repository-days than maxRepoDays is a stop with the count.
func selectTestopsSlotRecomputeDays(
	ctx context.Context, runtime *operatorRuntime, stderr io.Writer, org string, fromDay, toDay, throughDay time.Time,
	repositoryIDs []daily.RepositoryID, maxRepoDays int,
) ([]daily.RetiredSlotRecomputeDay, int) {
	if runtime.lookup == nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	dsn, err := resolveDSNRequired("CLICKHOUSE_URI", platformconfig.ClickHouseSpec, runtime.lookup)
	if err != nil {
		return nil, writeConfigError(stderr, err)
	}
	logResolvedDatabase(stderr, runtime.lookup, platformconfig.ClickHouseSpec, "clickhouse", dsn)
	conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(dsn.Reveal()))
	if err != nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	defer func() { _ = conn.Close() }()
	repos := make([]string, 0, len(repositoryIDs))
	for _, id := range repositoryIDs {
		repos = append(repos, string(id))
	}
	pairs, err := daily.LoadRetiredSlotRepoDays(ctx, conn, org, fromDay, toDay, repos, maxRepoDays+1)
	if err != nil {
		return nil, writeServiceError(stderr, err)
	}
	days := daily.RetiredSlotRecomputeDays(pairs, throughDay)
	if count := daily.RetiredSlotRepoDayCount(days); len(pairs) > maxRepoDays || count > maxRepoDays {
		_, _ = fmt.Fprintf(stderr, "{\"error\":{\"code\":\"repo_day_cap\",\"retired_repo_days\":%d,\"repo_days\":%d,\"max_repo_days\":%d}}\n",
			len(pairs), count, maxRepoDays)
		return nil, 1
	}
	return days, 0
}

// testopsSlotRecomputePlan is the plan the verb prints: per day the repository count and the first 8 characters of each
// repository id.
func testopsSlotRecomputePlan(tag string, fromDay, toDay, throughDay time.Time, days []daily.RetiredSlotRecomputeDay) map[string]any {
	repos := map[daily.RepositoryID]struct{}{}
	planned := make([]map[string]any, 0, len(days))
	for _, day := range days {
		cuts := make([]string, 0, len(day.RepoIDs))
		for _, id := range day.RepoIDs {
			repos[id] = struct{}{}
			cuts = append(cuts, string(id)[:min(8, len(id))])
		}
		planned = append(planned, map[string]any{"day": day.Day.Format(time.DateOnly), "repos": len(day.RepoIDs), "repo_id_prefixes": cuts})
	}
	return map[string]any{
		"rerun_tag": tag, "from": fromDay.Format(time.DateOnly), "to": toDay.Format(time.DateOnly), "through": throughDay.Format(time.DateOnly),
		"repos": len(repos), "repo_days": daily.RetiredSlotRepoDayCount(days), "days": planned,
	}
}
