package externalrecompute

// plan.go is a verbatim Go port of src/dev_health_ops/external_ingest/
// recompute.py's plan_recompute (CHAOS-5296, absorbing CHAOS-4057).
//
// It is deliberately PURE -- no I/O, no clock of its own, no enqueue -- for the
// same reason the Python original is: the decisions D4/D5/D7/D8 encoded below
// each came out of a real incident, and keeping them in a function that can be
// tested by value is what let those incidents be pinned by unit tests rather
// than by an integration run. tests in plan_test.go are the 1:1 port of
// tests/test_external_ingest_recompute_planner.py.
//
// Design decisions D1-D15 are recorded in
// docs/architecture/external-ingest-bounded-recompute.md.

import (
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// D7: record-kind -> job-category routing, mirroring the Python module's
// _GIT_KINDS/_WORK_ITEM_KINDS/_TEAM_KINDS/_OPERATIONAL_KINDS/_REPO_ONLY_KINDS
// frozensets for the v1 customer-push record kinds. repository.v1 is
// deliberately absent from every set below: a repository-only batch is the
// "nothing to recompute" case (Trigger=false), not an unclassified one.
var (
	gitKinds         = []string{"pull_request.v1", "review.v1", "commit.v1"}
	workItemKinds    = []string{"work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"}
	teamKinds        = []string{"identity.v1", "team.v1"}
	operationalKinds = []string{
		"operational_service.v1",
		"operational_incident.v1",
		"operational_alert.v1",
		"incident_timeline_event.v1",
		"incident_note.v1",
		"incident_responder.v1",
		"escalation_policy.v1",
		"on_call_schedule.v1",
		"on_call_assignment.v1",
		"operational_team.v1",
		"operational_user.v1",
		"service_repository_mapping.v1",
	}
)

const (
	// defaultMaxBackfillDays / defaultMaxFanoutRepos are the Python module's
	// _DEFAULT_MAX_BACKFILL_DAYS / _DEFAULT_MAX_FANOUT_REPOS. They are the
	// bounded-recompute contract (master-spec CC21: never a full-org recompute
	// by default), not tuning knobs -- a wider window or fan-out is a decision,
	// which is why both are surfaced as an operator env override rather than
	// silently widened here.
	defaultMaxBackfillDays = 14
	defaultMaxFanoutRepos  = 25

	envMaxBackfillDays = "EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS"
	envMaxFanoutRepos  = "EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS"
)

// PlanScope is one bounded-recompute decision's input: the coalesced scope a
// finished external-ingest batch (or several, merged across a debounce window)
// affected.
type PlanScope struct {
	OrgID       string
	RepoIDs     []string
	TeamIDs     []string
	RecordKinds []string
	WindowStart *time.Time
	WindowEnd   *time.Time
}

// Plan is the pure output of PlanRecompute: a bounded, capped plan.
//
// Trigger=false means nothing to dispatch (record kinds are repository.v1-only
// or empty -- the D7 _REPO_ONLY_KINDS row).
//
// FromDate/ToDate are times rather than the Python original's ISO strings on
// purpose. Python's strings were Celery kwargs; the native investment.materialize
// scope this plan now feeds parses from_date/to_date with time.DateOnly
// (internal/jobs/investment/nativeexecutor.go), so an ISO datetime string would
// be REFUSED there. Carrying times keeps the formatting decision at the
// enqueue seam where the consumer's contract is known, instead of baking one
// consumer's wire format into the planner.
type Plan struct {
	OrgID         string
	Trigger       bool
	DispatchDaily bool
	RepoIDs       []string
	TeamIDs       []string
	// Day is the target day (window end's UTC calendar date). Empty when
	// Trigger is false.
	Day time.Time
	// BackfillDays is the inclusive day count of the clamped window, >= 1 when
	// Trigger is true and 0 otherwise.
	BackfillDays int
	// FromDate is midnight UTC of the clamped window start; ToDate is the
	// window end verbatim (Python: to_date = window_end.isoformat()).
	FromDate time.Time
	ToDate   time.Time
	// CappedDays / CappedRepos record that this recompute was TRUNCATED. They
	// are the existing operator signal for "the window/fan-out you asked for
	// was wider than the bound", and must reach the logs -- dropping them is
	// how a silently narrowed recompute becomes indistinguishable from a
	// complete one.
	CappedDays  bool
	CappedRepos bool
	// FallbackOrgWideDaily is D8: a day-bounded, all-repository daily run for a
	// work-item batch that carries no repository linkage (Jira-native work
	// items). Git kinds never get it -- see PlanRecompute's body.
	FallbackOrgWideDaily bool
	// SkipInvestmentNoScope is the D4 hard invariant: investment materialize is
	// never dispatched with both repo and team scope empty.
	SkipInvestmentNoScope bool
}

// PlanRecompute maps a scope to a bounded plan. now is injected (Python calls
// datetime.now(timezone.utc) inline) so the missing-window default is testable.
func PlanRecompute(scope PlanScope, now time.Time) Plan {
	hasGit := containsAny(scope.RecordKinds, gitKinds)
	hasWorkItems := containsAny(scope.RecordKinds, workItemKinds)
	hasTeam := containsAny(scope.RecordKinds, teamKinds)
	hasOperational := containsAny(scope.RecordKinds, operationalKinds)

	if !(hasGit || hasWorkItems || hasTeam || hasOperational) {
		return Plan{OrgID: scope.OrgID}
	}

	maxBackfillDays := envIntAtLeastOne(envMaxBackfillDays, defaultMaxBackfillDays)
	maxFanout := envIntAtLeastOne(envMaxFanoutRepos, defaultMaxFanoutRepos)

	windowEnd := now.UTC()
	switch {
	case scope.WindowEnd != nil:
		windowEnd = scope.WindowEnd.UTC()
	case scope.WindowStart != nil:
		windowEnd = scope.WindowStart.UTC()
	}
	windowStart := windowEnd
	if scope.WindowStart != nil {
		windowStart = scope.WindowStart.UTC()
	}

	endDay := truncateToUTCDay(windowEnd)
	startDay := truncateToUTCDay(windowStart)
	requestedDays := int(endDay.Sub(startDay)/(24*time.Hour)) + 1
	if requestedDays < 1 {
		requestedDays = 1
	}
	cappedDays := requestedDays > maxBackfillDays
	backfillDays := min(requestedDays, maxBackfillDays)
	clampedStartDay := endDay.AddDate(0, 0, -(backfillDays - 1))

	sortedRepoIDs := sortedUnique(scope.RepoIDs)
	cappedRepos := len(sortedRepoIDs) > maxFanout
	repoIDs := sortedRepoIDs
	if cappedRepos {
		repoIDs = sortedRepoIDs[:maxFanout]
	}
	teamIDs := sortedUnique(scope.TeamIDs)

	dispatchDaily := hasGit || hasWorkItems
	// D8: the org-wide day-bounded fallback is ONLY for work-item kinds with an
	// empty repository scope (Jira-native work items may carry no repo linkage).
	// Git kinds always carry a repository (pull_request/review/commit); a
	// git-only batch with empty repo_ids is structurally impossible but is
	// handled defensively here by simply not dispatching daily at all, rather
	// than falling back org-wide for a kind category that was never meant to
	// trigger the fallback.
	fallbackOrgWideDaily := dispatchDaily && len(repoIDs) == 0 && hasWorkItems

	return Plan{
		OrgID:                scope.OrgID,
		Trigger:              true,
		DispatchDaily:        dispatchDaily,
		RepoIDs:              repoIDs,
		TeamIDs:              teamIDs,
		Day:                  endDay,
		BackfillDays:         backfillDays,
		FromDate:             clampedStartDay,
		ToDate:               windowEnd,
		CappedDays:           cappedDays,
		CappedRepos:          cappedRepos,
		FallbackOrgWideDaily: fallbackOrgWideDaily,
		// D4 hard invariant: never materialize investment with both repo_ids
		// and team_ids empty -- an unscoped materialize fuses every tenant's
		// work graph into one component (see internal/jobs/investment/scope.go).
		SkipInvestmentNoScope: len(repoIDs) == 0 && len(teamIDs) == 0,
	}
}

// DailyTargetDays enumerates the calendar days one plan's daily recompute
// covers, newest first: Day, Day-1, ... Day-(BackfillDays-1).
//
// This is where the Python and native shapes genuinely differ, and the
// difference is structural rather than a porting choice. Python passed
// backfill_days as a KWARG to a single run_daily_metrics task, which looped the
// days inside one Celery job. The native daily contract carries only a durable
// run identity (jobcontract.DailyMetricsDispatchPayload is just run_id) and one
// daily_metrics_runs row is scoped to exactly one target_day, so the same
// window is expressed as BackfillDays separate runs. maxBackfillDays (14)
// already bounds this, and it is the same bound cmd/dev-health-worker/
// sync_dispatch.go's maxPostSyncDailyBackfillDays picked independently for the
// identical "one sync must not burst dozens of day-pipelines" reason.
func (plan Plan) DailyTargetDays() []time.Time {
	if !plan.Trigger || !plan.DispatchDaily || plan.BackfillDays < 1 {
		return nil
	}
	days := make([]time.Time, 0, plan.BackfillDays)
	for offset := range plan.BackfillDays {
		days = append(days, plan.Day.AddDate(0, 0, -offset))
	}
	return days
}

func truncateToUTCDay(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func containsAny(values, candidates []string) bool {
	for _, value := range values {
		if slices.Contains(candidates, value) {
			return true
		}
	}
	return false
}

// envIntAtLeastOne mirrors the Python module's _env_int + max(1, ...) pairing:
// an unset, blank or unparseable value falls back to the default, and the
// result is never below 1 (a zero cap would mean "recompute nothing", which is
// never what an operator setting a bound intends).
func envIntAtLeastOne(name string, fallback int) int {
	parsed, ok := pythonInt(os.Getenv(name))
	if !ok {
		return fallback
	}
	return max(1, parsed)
}

// pythonInt accepts exactly what CPython's int(str) accepts, because this is a
// PARITY seam: the same environment variable configures the Python planner and
// this one, and a value one of them honours and the other silently ignores is
// worse than a value neither honours.
//
// strconv.Atoi is NOT that function. It rejects two forms CPython accepts:
//
//	int(" 3 ")  == 3    strconv.Atoi(" 3 ")  -> error
//	int("1_0")  == 10   strconv.Atoi("1_0")  -> error
//
// Both then fell through to the DEFAULT, which is the widening direction: an
// operator who set EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS=" 3 " (a
// trailing space in a .env file, or a YAML block scalar) got 14 days of
// recompute instead of 3, silently, from a cap that exists to bound fan-out.
// Surrounding whitespace is the realistic case; underscores are here because
// CPython accepts them and matching the accepted syntax exactly is cheaper to
// reason about than matching a subset of it.
//
// CPython's underscore rule is "single underscores between digits only" -- not
// leading, not trailing, not doubled, and not adjacent to a sign -- so those
// are rejected here rather than stripped.
func pythonInt(raw string) (int, bool) {
	// CPython's int() strips str.strip()'s whitespace set. Go's TrimSpace is
	// narrower (unicode.IsSpace omits 0x1c-0x1f), so those four are added
	// explicitly -- the same divergence internal/jobs/investment/scope.go
	// documents for its own Python-parity strip.
	trimmed := strings.Trim(raw, " \t\n\v\f\r\x1c\x1d\x1e\x1f\u0085\u00a0")
	if trimmed == "" {
		return 0, false
	}
	digits := trimmed
	if digits[0] == '+' || digits[0] == '-' {
		digits = digits[1:]
	}
	if digits == "" {
		return 0, false
	}
	var builder strings.Builder
	for index := 0; index < len(digits); index++ {
		character := digits[index]
		if character == '_' {
			// Between digits only: an underscore at either end, or next to
			// another underscore, is a syntax error in CPython too.
			if index == 0 || index == len(digits)-1 || digits[index+1] == '_' {
				return 0, false
			}
			continue
		}
		if character < '0' || character > '9' {
			return 0, false
		}
		builder.WriteByte(character)
	}
	sign := ""
	if trimmed[0] == '-' {
		sign = "-"
	}
	parsed, err := strconv.Atoi(sign + builder.String())
	if err != nil {
		return 0, false
	}
	return parsed, true
}
