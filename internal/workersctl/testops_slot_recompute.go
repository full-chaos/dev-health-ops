package workersctl

import (
	"context"
	"encoding/json"
	"errors"
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

// testopsSlotRecomputeToday is the clock of a preview's default --through; tests fix it.
var testopsSlotRecomputeToday = time.Now

// dispatchMetricsTestopsSlotRecompute handles `metrics testops-slot-recompute`: it starts the daily runs again for the days
// whose stored recurrence a retired report slot left stale. It selects the repository-days of every run with a retired slot
// (daily.RetiredSlotRepoDaysSQL; the retraction's day in --from .. --to, the run's days up to --through), expands each day D to
// D .. D+29 up to --through, and starts one tagged daily run per day for its repositories, through the `metrics daily-start
// --rerun-tag` path (the same generation, so a second call with the same plan and tag starts nothing). It previews by default
// and writes only with --apply, which needs an explicit --through. The plan is read and bounded (--max-repo-days, and
// daily.MaxRepositoriesPerRun per day) before the audited write opens: a stop writes nothing, not even an audit row.
func dispatchMetricsTestopsSlotRecompute(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("metrics testops-slot-recompute")
	org := flags.String("org", "", "organization id (uuid)")
	orgStdin := flags.Bool("org-stdin", false, orgStdinUsage)
	from := flags.String("from", "", "first day of a retired slot, inclusive (YYYY-MM-DD, UTC)")
	to := flags.String("to", "", "last day of a retired slot, inclusive (YYYY-MM-DD, UTC)")
	through := flags.String("through", "", "last day to compute again, inclusive (YYYY-MM-DD, UTC); required with --apply, a preview defaults to today")
	var repoIDs stringList
	flags.Var(&repoIDs, "repo-id", "limit the selection to this repository (uuid, repeatable)")
	rerunTag := flags.String("rerun-tag", "", "the rerun tag of the daily runs (required): the same plan and tag start nothing a second time")
	maxRepoDays := flags.Int("max-repo-days", testopsSlotRecomputeDefaultCap, "stop before any write when more repository-days are selected (at most 10000)")
	apply := flags.Bool("apply", false, "start the daily runs; without it the verb only previews")
	mutation := addMutationFlags(flags)
	if flags.Parse(args) != nil || flags.NArg() != 0 || !mutation.valid(!*apply) || !daily.ValidRerunTag(*rerunTag) ||
		*maxRepoDays < 1 || *maxRepoDays > daily.RetiredSlotMaxRepoDays || (*apply && *through == "") {
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
		if err != nil {
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
	if err := runtime.service.Authorize(ctx, runtime.principal, joboperator.ActionMetricsDailyStart, "organization", canonicalOrg); err != nil {
		return writeServiceError(stderr, err)
	}
	repos := make([]string, 0, len(repositoryIDs))
	for _, id := range repositoryIDs {
		repos = append(repos, string(id))
	}
	request := daily.RetiredSlotPlanRequest{From: fromDay, To: toDay, Through: throughDay, RepoIDs: repos, MaxRepoDays: *maxRepoDays}
	if !*apply {
		plan, code := planTestopsSlotRecompute(ctx, runtime, stderr, canonicalOrg, request)
		if code != 0 {
			return code
		}
		printed := testopsSlotRecomputePlan(*rerunTag, fromDay, toDay, throughDay, plan.Days)
		printed["preview"] = true
		return writeResult(stdout, stderr, map[string]any{"testops_slot_recompute": printed})
	}
	perform := func(ctx context.Context) int {
		plan, code := planTestopsSlotRecompute(ctx, runtime, stderr, canonicalOrg, request)
		if code != 0 {
			return code
		}
		printed := testopsSlotRecomputePlan(*rerunTag, fromDay, toDay, throughDay, plan.Days)
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
		started := make([]map[string]any, 0, len(plan.Days))
		for _, day := range plan.Days {
			outcomes, err := startDailyDays(ctx, store, publisher, canonicalOrg, day.Day, day.Day, day.RepoIDs, *rerunTag, true)
			if err != nil {
				return writeServiceError(stderr, err)
			}
			for _, outcome := range outcomes {
				started = append(started, map[string]any{"day": outcome.Day, "run_id": outcome.RunID, "started": !outcome.AlreadyStarted})
			}
		}
		printed["runs"] = started
		return writeResult(stdout, stderr, map[string]any{"testops_slot_recompute": printed})
	}
	return auditedWriteWith(ctx, runtime, stderr, mutation, joboperator.ActionMetricsDailyStart, "organization", canonicalOrg, !*orgStdin, perform)
}

// planTestopsSlotRecompute reads and bounds the plan: runtime.retiredSlotPlan when set (tests), else ClickHouse through
// CLICKHOUSE_URI, resolved only here. A stop prints its code and counts; any other failure prints a fixed code.
func planTestopsSlotRecompute(
	ctx context.Context, runtime *operatorRuntime, stderr io.Writer, org string, request daily.RetiredSlotPlanRequest,
) (daily.RetiredSlotPlan, int) {
	read := runtime.retiredSlotPlan
	if read == nil {
		if runtime.lookup == nil {
			return daily.RetiredSlotPlan{}, writeError(stderr, "operator_backend_unavailable")
		}
		dsn, err := resolveDSNRequired("CLICKHOUSE_URI", platformconfig.ClickHouseSpec, runtime.lookup)
		if err != nil {
			return daily.RetiredSlotPlan{}, writeConfigError(stderr, err)
		}
		logResolvedDatabase(stderr, runtime.lookup, platformconfig.ClickHouseSpec, "clickhouse", dsn)
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(dsn.Reveal()))
		if err != nil {
			return daily.RetiredSlotPlan{}, writeError(stderr, "operator_backend_unavailable")
		}
		defer func() { _ = conn.Close() }()
		read = func(ctx context.Context, org string, request daily.RetiredSlotPlanRequest) (daily.RetiredSlotPlan, error) {
			return daily.PlanRetiredSlotRecompute(ctx, conn, org, request)
		}
	}
	plan, err := read(ctx, org, request)
	var stop daily.RetiredSlotPlanStop
	if errors.As(err, &stop) {
		detail := map[string]any{"code": stop.Code, "retired_repo_days": stop.RetiredRepoDays, "repo_days": stop.RepoDays, "max_repo_days": stop.MaxRepoDays}
		if stop.Day != "" {
			detail["day"], detail["repos"], detail["max_repos_per_day"] = stop.Day, stop.DayRepos, daily.MaxRepositoriesPerRun
		}
		encoded, _ := json.Marshal(map[string]any{"error": detail})
		_, _ = fmt.Fprintf(stderr, "%s\n", encoded)
		return daily.RetiredSlotPlan{}, 1
	}
	if err != nil {
		return daily.RetiredSlotPlan{}, writeError(stderr, "operator_request_failed")
	}
	return plan, 0
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
