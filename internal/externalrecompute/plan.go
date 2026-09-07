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
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
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

// ErrInvalidCapEnv is returned by ValidateCapEnv for a bound the operator set
// but that this process cannot honour.
var ErrInvalidCapEnv = errors.New("external recompute: invalid cap environment variable")

// ValidateCapEnv refuses, at startup, any bounded-recompute cap that is set but
// unusable. It is the loud half of the pair whose quiet half is envIntAtLeastOne.
//
// WHY REFUSING BEATS DEFAULTING. Both cap variables exist to BOUND fan-out. A
// value this process cannot parse -- or one that parses below 1 -- means the
// operator asked for a bound and we do not know which. Falling back to the
// default then silently recomputes MORE than they asked for, in the widening
// direction, with no signal: exactly how "EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS
// = ' 3 '" quietly became 14 days before the r1 fix. Refusing at startup turns
// an operator typo into an attributable failure at deploy time instead of a
// silently over-wide recompute discovered weeks later, which is the same class
// of invisible failure this whole ticket exists to end.
//
// The message names the VARIABLE and the rule, never the value -- the same
// convention jobcontract's validation messages follow, so a malformed value can
// never be echoed into a log.
func ValidateCapEnv() error {
	for _, name := range []string{envMaxBackfillDays, envMaxFanoutRepos} {
		raw, present := os.LookupEnv(name)
		if !present {
			// ABSENT is not an error: the checked-in default is the intended
			// bound when an operator expresses no preference.
			//
			// PRESENT-BUT-EMPTY is a different thing entirely and is refused
			// below. os.Getenv cannot tell the two apart -- it returns "" for
			// both -- and this loop used to skip on that empty string, so
			// `EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS=` in a .env file, a
			// k8s env entry with no value, or a --set that resolved to nothing
			// sailed past the refusal and silently took the default. That is
			// the single most common way an operator gets a variable wrong,
			// and it defeated the exact contract this function exists to
			// enforce (r3 P1).
			continue
		}
		if raw == "" {
			return fmt.Errorf("%w: %s is set to an empty value", ErrInvalidCapEnv, name)
		}
		parsed, ok := pythonInt(raw)
		if !ok {
			return fmt.Errorf("%w: %s is set but is not an integer", ErrInvalidCapEnv, name)
		}
		if parsed < 1 {
			return fmt.Errorf("%w: %s must be at least 1", ErrInvalidCapEnv, name)
		}
	}
	return nil
}

// envIntAtLeastOne mirrors the Python module's _env_int + max(1, ...) pairing:
// an unset, blank or unparseable value falls back to the default, and the
// result is never below 1 (a zero cap would mean "recompute nothing", which is
// never what an operator setting a bound intends).
//
// The fallback is kept so this function stays total and the planner stays pure
// and testable by value. It is NOT the production tolerance for a bad value:
// ValidateCapEnv above refuses such a value at startup, so no live process can
// reach this fallback with a cap the operator set and we could not read.
//
// os.Getenv, not os.LookupEnv, is deliberate here. Python's _env_int does
// `raw = os.getenv(name); if not raw: return default`, which treats set-empty
// and unset identically -- so matching it keeps the two planners agreeing on
// every input. The stricter present-but-empty rule lives ONLY in
// ValidateCapEnv, which is a Go-side safety layer with no Python counterpart:
// refusing at startup is an addition to the contract, not a reinterpretation
// of it, so it must not leak into the parity-faithful path.
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
//
// TWO DELIBERATE NON-MATCHES, both in the safe direction now that ValidateCapEnv
// refuses at startup rather than defaulting (r2 measured both):
//
//   - NON-ASCII DECIMAL DIGITS. int("\u0663") is 3 in CPython; this returns
//     false. Go's strconv has no notion of Unicode decimal digits, and an
//     Arabic-Indic numeral in a deployment environment variable is not a case
//     worth hand-rolling a Nd-category decoder for.
//   - VALUES OUTSIDE machine int. int() is arbitrary-precision, so a
//     1000-digit cap parses there; strconv.Atoi refuses it here.
//
// Both used to matter because an unparsed value silently became the DEFAULT --
// the widening direction, invisible. They no longer do: ValidateCapEnv turns
// either into a named startup refusal, so the operator is told their value was
// not understood instead of quietly getting a wider bound than they asked for.
func pythonInt(raw string) (int, bool) {
	// int()'s whitespace set is Py_UNICODE_ISSPACE, which is what Go's
	// unicode.IsSpace implements -- NOT str.strip()'s set.
	//
	// r1's fix trimmed 0x1c-0x1f as well, borrowing the note in
	// internal/jobs/investment/scope.go. That note is correct for str.strip()
	// and WRONG here, and r2 caught the difference: measured against a real
	// python3, int("\x1c3") REJECTS while int("\xa03") and int("\x853")
	// return 3. Trimming the separators made this function accept a value
	// CPython refuses. unicode.IsSpace draws exactly the right line -- it
	// includes NBSP and NEL (both realistic copy-paste damage in a .env file)
	// and excludes the four file/group/record/unit separators.
	trimmed := strings.TrimFunc(raw, unicode.IsSpace)
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
