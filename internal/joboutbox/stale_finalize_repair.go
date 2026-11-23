package joboutbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The stale-finalize stage (CHAOS-14090, shape D of CHAOS-14072) redrives a
// daily-metrics run whose finalization stays 'pending' after its River job was
// cleaned and its outbox row pruned. No strand shape reaches such a run: every
// shape starts from an outbox row joined to a River job, and this run has
// neither. The stage reads on the QUEUE pool (it needs river.river_job) and
// redrives on the DOMAIN pool through StaleFinalizeRedriver (the queue role
// holds no outbox INSERT and no runs UPDATE); neither half may move.
const (
	// StaleFinalizeHorizon is how long a run, its partitions and its last
	// finalize row or job must stay unwritten before the stage treats the
	// finalize as lost. It stays far above the metrics.daily_finalize retry
	// budget (TestStaleFinalizeHorizonExceedsTheRetryBudget), so a job River
	// can still run is never redriven beside.
	StaleFinalizeHorizon = 24 * time.Hour

	staleFinalizeInterval     = 10 * time.Minute
	staleFinalizeBudget       = 2 * time.Second
	staleFinalizeSurveyBudget = 850 * time.Millisecond
	staleFinalizeRunDeadline  = 200 * time.Millisecond
	staleFinalizeRunReserve   = 50 * time.Millisecond
	// staleFinalizeSurveyRollback bounds the survey's rollback on its own: a
	// connection that stalls at that instant must not hold the relay step.
	// 850 ms of survey plus 250 ms of rollback stays inside the pass budget.
	staleFinalizeSurveyRollback = 250 * time.Millisecond
	// Every per-run deadline closes one domain-pool connection, and pgxpool
	// frees its slot only when the close ends; the reconciler's domain pool
	// has four. Two deadlines end the pass.
	staleFinalizeMaxDeadlines = 2
	staleFinalizeLevelCap     = 1000

	staleFinalizeBaseStatementTimeout       = "SET LOCAL statement_timeout = '150ms'"
	staleFinalizeCandidatesStatementTimeout = "SET LOCAL statement_timeout = '400ms'"
	staleFinalizeLevelsStatementTimeout     = "SET LOCAL statement_timeout = '250ms'"
	staleFinalizeLockTimeout                = "SET LOCAL lock_timeout = '100ms'"
	// JIT off for the whole survey: A and B price above jit_above_cost at prod
	// sizes, and JIT compilation alone cost 543 ms of a 781 ms statement A.
	staleFinalizeJITOff = "SET LOCAL jit = off"
)

// Failure steps and classes of the stage. A failure is counted, never returned:
// the relay step that carries the stage goes on.
const (
	StaleFinalizeStepSurvey  = "survey"
	StaleFinalizeStepLevels  = "levels"
	StaleFinalizeStepRedrive = "redrive"

	StaleFinalizeClassDeadline      = "deadline"
	StaleFinalizeClassInvalidState  = "invalid_state"
	StaleFinalizeClassNotAuthorized = "not_authorized"
	StaleFinalizeClassUnavailable   = "unavailable"
)

// ErrStaleFinalizeInvalidState is what a redriver wraps when the redrive
// refused the run itself (a contract or policy rejection of the publish, a
// target day it cannot parse). It is deterministic for that run, so it is a
// class of its own.
var ErrStaleFinalizeInvalidState = errors.New("stale finalize redrive refused the run")

// StaleFinalizeRedriver redrives ONE run's finalize on the domain pool. It
// reports whether a fresh finalize was published. One run per call: a failure
// of one run never stops the next.
type StaleFinalizeRedriver interface {
	RedriveStaleFinalize(ctx context.Context, runID, nonce string, cutoff time.Time) (bool, error)
}

// StaleFinalizeFailure is one counted failure of a stage pass. It carries no
// run id and no error text: the class is what an operator acts on.
type StaleFinalizeFailure struct {
	Step  string
	Class string
}

// StaleFinalizeLevels are the standing refusals by class, as of the last pass
// that read them. Truncated means the read hit its cap and the counts are a
// floor.
type StaleFinalizeLevels struct {
	Live         int
	StrandShape  int
	Recent       int
	NoPartitions int
	Truncated    bool
}

type staleFinalizeCandidate struct {
	runID     string
	updatedAt time.Time
}

// staleFinalizeCursor is the keyset position of one traversal of the base.
// bound is the cutoff of the traversal's first pass: A reads no run written
// after it until the cursor wraps, so runs that keep arriving cannot push the
// wrap away and strand an older run behind the cursor.
type staleFinalizeCursor struct {
	updatedAt time.Time
	runID     string
	bound     time.Time
}

type staleFinalizeSurvey struct {
	candidates []staleFinalizeCandidate
	levels     *StaleFinalizeLevels
	failures   []StaleFinalizeFailure
}

type staleFinalizeSurveyFunc func(ctx context.Context, cutoff time.Time, limit int, cursor *staleFinalizeCursor) staleFinalizeSurvey

type staleFinalizePass struct {
	redriven []string
	failures []StaleFinalizeFailure
	deferred int
	levels   *StaleFinalizeLevels
}

// staleFinalizeStage holds the per-process cadence and keyset cursor. Each
// replica keeps its own; a restart surveys at once from the start.
type staleFinalizeStage struct {
	survey   staleFinalizeSurveyFunc
	redriver StaleFinalizeRedriver
	clock    func() time.Time
	newNonce func() string

	mu     sync.Mutex
	next   time.Time
	cursor *staleFinalizeCursor
}

// SetStaleFinalizeRedriver enables the stale-finalize stage on this repair.
// The survey runs on the repair's queue-control pool; the redriver must run on
// the domain pool.
func (repair *StrandRepair) SetStaleFinalizeRedriver(redriver StaleFinalizeRedriver) error {
	if repair == nil || repair.beginQueue == nil || redriver == nil || repair.riverJobTable == "" {
		return ErrInvalidConfiguration
	}
	baseSQL := staleFinalizeBaseExistsSQL
	candidatesSQL := fmt.Sprintf(staleFinalizeCandidatesSQLTemplate, repair.riverJobTable)
	levelsSQL := fmt.Sprintf(staleFinalizeLevelsSQLTemplate, repair.riverJobTable)
	beginQueue := repair.beginQueue
	repair.staleFinalize = &staleFinalizeStage{
		survey: func(ctx context.Context, cutoff time.Time, limit int, cursor *staleFinalizeCursor) staleFinalizeSurvey {
			return surveyStaleFinalize(ctx, beginQueue, baseSQL, candidatesSQL, levelsSQL, cutoff, limit, cursor)
		},
		redriver: redriver,
		clock:    time.Now,
		newNonce: uuid.NewString,
	}
	return nil
}

// step runs at most one pass per staleFinalizeInterval. It never returns an
// error: every failure is a counted StaleFinalizeFailure.
func (stage *staleFinalizeStage) step(ctx context.Context, now time.Time, limit int) staleFinalizePass {
	stage.mu.Lock()
	// Bounded like the idle backoff: a clock that steps backwards costs at
	// most one interval.
	if now.Before(stage.next) && stage.next.Sub(now) <= staleFinalizeInterval {
		stage.mu.Unlock()
		return staleFinalizePass{}
	}
	// The mark is set before the pass runs, so a failing pass is not retried
	// on the next tick.
	stage.next = now.Add(staleFinalizeInterval)
	cursor := stage.cursor
	stage.mu.Unlock()

	start := stage.clock()
	passCtx, cancel := context.WithTimeout(ctx, staleFinalizeBudget)
	defer cancel()
	remaining := func() time.Duration { return staleFinalizeBudget - stage.clock().Sub(start) }

	surveyCtx, cancelSurvey := context.WithTimeout(passCtx, staleFinalizeSurveyBudget)
	cutoff := now.UTC().Add(-StaleFinalizeHorizon)
	survey := stage.survey(surveyCtx, cutoff, limit, cursor)
	cancelSurvey()
	bound := cutoff
	if cursor != nil {
		bound = cursor.bound
	}
	pass := staleFinalizePass{levels: survey.levels, failures: survey.failures}

	nonce := stage.newNonce()
	deadlines := 0
	var lastTried *staleFinalizeCursor
	for index, candidate := range survey.candidates {
		if deadlines >= staleFinalizeMaxDeadlines || remaining() < staleFinalizeRunDeadline+staleFinalizeRunReserve {
			pass.deferred = len(survey.candidates) - index
			break
		}
		runCtx, cancelRun := context.WithTimeout(passCtx, staleFinalizeRunDeadline)
		redriven, err := stage.redriver.RedriveStaleFinalize(runCtx, candidate.runID, nonce, cutoff)
		deadline := errors.Is(runCtx.Err(), context.DeadlineExceeded)
		cancelRun()
		lastTried = &staleFinalizeCursor{updatedAt: candidate.updatedAt, runID: candidate.runID, bound: bound}
		if err != nil {
			class := classifyStaleFinalizeRedrive(err, deadline)
			if class == StaleFinalizeClassDeadline {
				deadlines++
			}
			pass.failures = append(pass.failures, StaleFinalizeFailure{Step: StaleFinalizeStepRedrive, Class: class})
			continue
		}
		if redriven {
			pass.redriven = append(pass.redriven, candidate.runID)
		}
	}

	// The cursor is the last run TRIED. It wraps to the start when the survey
	// returned fewer candidates than the limit and every one was tried, so a
	// run that fails in every pass cannot hide a newer one behind the limit.
	stage.mu.Lock()
	switch {
	case len(survey.failures) > 0 && survey.candidates == nil:
		// The candidate read failed: the cursor stays.
	case pass.deferred == 0 && len(survey.candidates) < limit:
		stage.cursor = nil
	case lastTried != nil:
		stage.cursor = lastTried
	}
	stage.mu.Unlock()
	return pass
}

// classifyStaleFinalizeRedrive names a redrive failure. not_authorized cannot
// come from the byte-equal redrive itself: redriveOneStrandedFinalize maps every
// database error of its row-lock read to daily.ErrUnavailable, so a domain grant
// drift shows as {redrive, unavailable} (the domain readiness posture still
// catches it). The case stays for a redriver that keeps the 42501.
func classifyStaleFinalizeRedrive(err error, deadline bool) string {
	switch {
	case deadline || errors.Is(err, context.DeadlineExceeded):
		return StaleFinalizeClassDeadline
	case errors.Is(err, ErrStaleFinalizeInvalidState):
		return StaleFinalizeClassInvalidState
	case errors.Is(err, ErrNotAuthorized):
		return StaleFinalizeClassNotAuthorized
	default:
		return StaleFinalizeClassUnavailable
	}
}

func classifyStaleFinalizeSurvey(ctx context.Context, err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57014", "55P03":
			return StaleFinalizeClassDeadline
		case insufficientPrivilege:
			return StaleFinalizeClassNotAuthorized
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return StaleFinalizeClassDeadline
	}
	return StaleFinalizeClassUnavailable
}

// surveyStaleFinalize reads, in one READ ONLY transaction on the queue pool,
// each statement under its own server-side timeout inside the survey's client
// deadline: first whether any run is in the base at all (statement 0, which
// reads only the runs and their partitions), then the candidates (A) and the
// standing refusals (B). When the base is empty the survey ends there: A and B
// would scan the outbox and river_job for nothing, and every level is an exact
// zero.
func surveyStaleFinalize(
	ctx context.Context,
	beginQueue func(context.Context) (pgx.Tx, error),
	baseSQL, candidatesSQL, levelsSQL string,
	cutoff time.Time,
	limit int,
	cursor *staleFinalizeCursor,
) staleFinalizeSurvey {
	fail := func(step string, err error) staleFinalizeSurvey {
		return staleFinalizeSurvey{failures: []StaleFinalizeFailure{{Step: step, Class: classifyStaleFinalizeSurvey(ctx, err)}}}
	}
	tx, err := beginQueue(ctx)
	if err != nil || tx == nil {
		if err == nil {
			err = ErrUnavailable
		}
		return fail(StaleFinalizeStepSurvey, err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), staleFinalizeSurveyRollback)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	for _, statement := range []string{"SET TRANSACTION READ ONLY", staleFinalizeLockTimeout, staleFinalizeJITOff, staleFinalizeBaseStatementTimeout} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fail(StaleFinalizeStepSurvey, err)
		}
	}
	var inBase bool
	if err := tx.QueryRow(ctx, baseSQL, cutoff).Scan(&inBase); err != nil {
		return fail(StaleFinalizeStepSurvey, err)
	}
	if !inBase {
		return staleFinalizeSurvey{candidates: []staleFinalizeCandidate{}, levels: &StaleFinalizeLevels{}}
	}
	if _, err := tx.Exec(ctx, staleFinalizeCandidatesStatementTimeout); err != nil {
		return fail(StaleFinalizeStepSurvey, err)
	}
	var cursorUpdated, cursorBound *time.Time
	var cursorID *string
	if cursor != nil {
		updated, id, bound := cursor.updatedAt, cursor.runID, cursor.bound
		cursorUpdated, cursorID, cursorBound = &updated, &id, &bound
	}
	rows, err := tx.Query(ctx, candidatesSQL, cutoff, limit, cursorUpdated, cursorID, cursorBound)
	if err != nil {
		return fail(StaleFinalizeStepSurvey, err)
	}
	candidates := make([]staleFinalizeCandidate, 0, limit)
	for rows.Next() {
		var found staleFinalizeCandidate
		if err := rows.Scan(&found.runID, &found.updatedAt); err != nil || !uuidPattern.MatchString(found.runID) {
			rows.Close()
			if err == nil {
				err = ErrUnavailable
			}
			return fail(StaleFinalizeStepSurvey, err)
		}
		candidates = append(candidates, found)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fail(StaleFinalizeStepSurvey, err)
	}
	result := staleFinalizeSurvey{candidates: candidates}

	// B is observability only: its failure keeps A's candidates.
	levels, err := readStaleFinalizeLevels(ctx, tx, levelsSQL, cutoff)
	if err != nil {
		result.failures = []StaleFinalizeFailure{{Step: StaleFinalizeStepLevels, Class: classifyStaleFinalizeSurvey(ctx, err)}}
		return result
	}
	result.levels = levels
	return result
}

func readStaleFinalizeLevels(ctx context.Context, tx pgx.Tx, levelsSQL string, cutoff time.Time) (*StaleFinalizeLevels, error) {
	if _, err := tx.Exec(ctx, staleFinalizeLevelsStatementTimeout); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, levelsSQL, cutoff, staleFinalizeLevelCap+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	levels := &StaleFinalizeLevels{}
	read := 0
	for rows.Next() {
		var disposition string
		var count, classRead int
		if err := rows.Scan(&disposition, &count, &classRead); err != nil {
			return nil, err
		}
		read += classRead
		switch disposition {
		case "live":
			levels.Live = count
		case "strand_shape":
			levels.StrandShape = count
		case "recent":
			levels.Recent = count
		case "no_partitions":
			levels.NoPartitions = count
		case "redrive":
		default:
			return nil, ErrUnavailable
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	levels.Truncated = read > staleFinalizeLevelCap
	return levels, nil
}

// All survey SQL is one grouped declaration so the queue-row census keys it
// once (internal/cigates/queuecreds). The fragments are shared by the
// candidate read (A) and the level read (B), so the two cannot disagree on
// what a refusal is. %[1]s is the sanitized river_job table; $1 is the
// horizon cutoff.
//
// A puts every refusal in its WHERE clause, so ORDER BY and LIMIT apply to
// runs that will be redriven only: standing refusals, which are the oldest
// rows, can never hide a candidate behind the limit. Its keyset cursor
// ($3, $4) is the last run the previous pass tried; $5 is the traversal's
// frozen upper bound on run.updated_at.
//
// A delivered finalize row older than the horizon whose River job is gone
// (River's cleaner removed it, retention has not pruned the row) is a
// candidate: no live job, not the strand shape (that shape JOINs the River
// row), not recent. Nothing else can deliver that finalize again.
const (
	staleFinalizeBaseSQL = `run.status = 'running'
	AND run.finalization_status = 'pending'
	AND run.updated_at <= $1
	AND NOT EXISTS (
		SELECT 1 FROM public.daily_metrics_partitions AS partition
		WHERE partition.run_id = run.id
			AND (partition.status <> 'succeeded' OR partition.updated_at > $1)
	)`
	// The River arm is an uncorrelated IN, so it reads river_job once per
	// statement (a hashed subplan), not once per base run: as a correlated
	// EXISTS the planner priced an early match and scanned river_job for every
	// run. The domain id is always a JSON string (jobcontract.DomainLink.ID),
	// so #>> equals the old @> containment; the IS NOT NULL keeps a job with no
	// domain id from putting a NULL into the set, which would turn
	// "run.id IN (...)" into NULL instead of false.
	staleFinalizeLiveJobSQL = `(run.id::text IN (
		SELECT job.args #>> '{domain,id}' FROM %[1]s AS job
		WHERE job.kind = 'metrics.daily_finalize'
			AND job.state::text NOT IN ('completed', 'discarded', 'cancelled')
			AND job.args #>> '{domain,id}' IS NOT NULL
	) OR EXISTS (
		SELECT 1 FROM public.worker_job_outbox AS outbox
		WHERE outbox.job_kind = 'metrics.daily_finalize'
			AND outbox.args #>> '{domain,id}' = run.id::text
			AND outbox.status IN ('pending', 'claimed')
	))`
	staleFinalizeStrandShapeSQL = `EXISTS (
		SELECT 1 FROM public.worker_job_outbox AS outbox
		JOIN %[1]s AS job ON job.id = outbox.river_job_id
		WHERE outbox.job_kind = 'metrics.daily_finalize'
			AND outbox.status = 'delivered'
			AND outbox.args #>> '{domain,id}' = run.id::text
	)`
	// The River arm has the live-job arm's uncorrelated form (one read of
	// river_job per statement, the same IS NOT NULL guard): correlated, the
	// planner probed river_job by kind for every base run.
	staleFinalizeRecentSQL = `(EXISTS (
		SELECT 1 FROM public.worker_job_outbox AS outbox
		WHERE outbox.job_kind = 'metrics.daily_finalize'
			AND outbox.args #>> '{domain,id}' = run.id::text
			AND outbox.created_at > $1
	) OR run.id::text IN (
		SELECT job.args #>> '{domain,id}' FROM %[1]s AS job
		WHERE job.created_at > $1
			AND job.args #>> '{domain,id}' IS NOT NULL
	))`
	staleFinalizeNoPartitionsSQL = `NOT EXISTS (
		SELECT 1 FROM public.daily_metrics_partitions AS partition
		WHERE partition.run_id = run.id
	)`
	// Statement 0: the base alone, the same fragment A and B use. It reads
	// daily_metrics_runs and daily_metrics_partitions only.
	staleFinalizeBaseExistsSQL = `
SELECT EXISTS (SELECT 1 FROM public.daily_metrics_runs AS run WHERE ` + staleFinalizeBaseSQL + `)`
	staleFinalizeCandidatesSQLTemplate = `
SELECT run.id::text, run.updated_at
FROM public.daily_metrics_runs AS run
WHERE ` + staleFinalizeBaseSQL + `
	AND NOT ` + staleFinalizeLiveJobSQL + `
	AND NOT ` + staleFinalizeStrandShapeSQL + `
	AND NOT ` + staleFinalizeRecentSQL + `
	AND NOT ` + staleFinalizeNoPartitionsSQL + `
	AND ($3::timestamptz IS NULL OR (run.updated_at, run.id) > ($3::timestamptz, $4::uuid))
	AND ($5::timestamptz IS NULL OR run.updated_at <= $5::timestamptz)
ORDER BY run.updated_at, run.id
LIMIT $2::int`
	staleFinalizeLevelsSQLTemplate = `
SELECT classified.disposition,
	count(*) FILTER (WHERE classified.ordinal < $2::int)::int,
	count(*)::int
FROM (
	SELECT row_number() OVER (ORDER BY run.updated_at, run.id) AS ordinal,
	CASE
		WHEN ` + staleFinalizeLiveJobSQL + ` THEN 'live'
		WHEN ` + staleFinalizeStrandShapeSQL + ` THEN 'strand_shape'
		WHEN ` + staleFinalizeRecentSQL + ` THEN 'recent'
		WHEN ` + staleFinalizeNoPartitionsSQL + ` THEN 'no_partitions'
		ELSE 'redrive'
	END AS disposition
	FROM public.daily_metrics_runs AS run
	WHERE ` + staleFinalizeBaseSQL + `
	ORDER BY run.updated_at, run.id
	LIMIT $2::int
) AS classified
GROUP BY classified.disposition`
)
