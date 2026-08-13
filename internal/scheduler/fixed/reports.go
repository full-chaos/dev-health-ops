package fixed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SkipNoDueScheduledReports marks a report-dispatch occurrence whose sweep
// found no report whose cron had come due. It is the overwhelmingly common
// outcome of a 300 second sweep and is recorded as a bounded skip so an
// operator can tell "nothing owed this tick" from "the sweep is broken".
const SkipNoDueScheduledReports = "no_due_scheduled_reports"

// SkipNoDueScheduledReportsClaimed marks a sweep that found due reports but
// materialized none because every occurrence identity was already durable.
// It is deliberately a different reason from SkipNoDueScheduledReports: the
// first means the cron grid owed nothing, the second means the work is already
// done, and conflating them would hide a schedule stuck re-reading the same
// due row forever.
const SkipNoDueScheduledReportsClaimed = "scheduled_reports_already_claimed"

// DegradedScheduledReportsUndeliverable marks a sweep that met at least one
// report run which is still pending while its durable handoff has exhausted its
// delivery budget. The occurrence still commits every other tenant's work; this
// names the condition so it is visible without being fatal.
//
// It is a PERMANENT condition until an operator acts. Outbox retention replaces
// the full dead row with a minimal delivery-abandonment fact, so the gauge stays
// raised after retention instead of starting a fresh attempt budget. Locate the
// affected runs with:
//
//	SELECT run.id, run.report_id
//	FROM report_runs AS run
//	JOIN worker_job_delivery_abandonments AS abandonment
//	  ON abandonment.dedupe_key = 'report.run:' || run.id::text
//	WHERE run.status = 'pending';
const DegradedScheduledReportsUndeliverable = "scheduled_reports_undeliverable"

// DegradedScheduledReportsDeferred marks a sweep that hit its per-occurrence
// materialization bound and left the remainder for the next tick. Unlike the
// condition above it is self-resolving, because the deferred reports stay due.
const DegradedScheduledReportsDeferred = "scheduled_reports_deferred"

const (
	// scheduledReportsScheduleID is the one schedule this producer serves.
	scheduledReportsScheduleID = "scheduled_reports_dispatch"

	// scheduledReportOccurrenceIdentityVersion mirrors
	// SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION in
	// src/dev_health_ops/reports/execution_trigger.py. The occurrence table is
	// shared with the Python dispatcher for the whole coexistence window, so
	// this string is a cross-runtime wire value, not a local constant.
	scheduledReportOccurrenceIdentityVersion = "report_scheduler_occurrence_v1"

	// activeScheduledJobStatus is JobStatus.ACTIVE from
	// src/dev_health_ops/models/settings.py. The column is an integer, so the
	// enum value is the wire value.
	activeScheduledJobStatus = 0

	// reportJobType is the ScheduledJob.job_type discriminator this sweep owns.
	reportJobType = "report"

	// scheduledReportTriggeredBy is the ReportRun.triggered_by value the Python
	// path writes for a scheduler-created run. The report handler reads it to
	// choose the scheduled payload shape, so it is behavioral, not cosmetic.
	scheduledReportTriggeredBy = "scheduler"

	// pendingReportRunStatus is ReportRunStatus.PENDING.
	pendingReportRunStatus = "pending"

	// maximumScheduledReportsPerOccurrence bounds the work ONE occurrence
	// materializes. It is not a bound on rows read, and exceeding it defers the
	// remainder to the next tick rather than failing.
	//
	// This deliberately differs from maximumFanoutOrganizations, which errors
	// rather than truncating. That producer must not dispatch a prefix, because a
	// skipped organization loses its nightly run for the whole day. Here a due
	// report that is not materialized STAYS due — its cron and last run are
	// unchanged — so the next 300 second tick continues from the same
	// deterministic order. Truncation is genuine forward progress, not a silent
	// partial success, and the remainder is reported as a degraded condition.
	maximumScheduledReportsPerOccurrence = 500

	// maximumScheduledReportCandidatesPerOccurrence is one larger than the work
	// budget so the producer can observe and signal a due remainder without ever
	// locking an installation-sized report set. The SQL page is ordered by the
	// durable next-run marker, so truncating here preserves the same oldest-due
	// delivery bound as the in-memory order below.
	maximumScheduledReportCandidatesPerOccurrence = maximumScheduledReportsPerOccurrence + 1
)

// ErrScheduledReportConfiguration identifies a persisted report schedule that
// cannot be evaluated: an unparseable cron expression, or one outside the
// deterministic croniter subset the Go evaluator implements.
//
// It fails the occurrence rather than skipping the offending row. That is a
// deliberate blast-radius choice and it needs stating. New GraphQL writes
// validate the cron through croniter; this backstop remains for legacy rows,
// direct database writes, and corruption. None of those may turn one report's
// missing runs into a healthy global sweep.
//
// Skipping the row instead would leave the schedule reporting success while one
// tenant's reports silently never ran, which is the exact false-pass class the
// cutover plan forbids. The engine gives per-schedule isolation but not
// per-row, so a partial sweep cannot be reported as a partial sweep: the only
// two honest outcomes are "everything owed was materialized" and "failed".
//
// The legacy Python dispatcher is no better off and is quieter about it: its
// try/except wraps the whole loop, so one row raising from croniter abandoned
// the entire sweep and still returned {"dispatched": [], "skipped": 0}. Failing
// loudly here is strictly more available than that, not less.
var ErrScheduledReportConfiguration = errors.New("scheduled report configuration cannot be evaluated")

// ErrScheduledReportConflict identifies a persisted occurrence whose identity
// fields disagree with the derived occurrence, or a schedule that owns more
// than one active report. Neither is retryable: the derivation or the data is
// wrong and replaying compounds it.
var ErrScheduledReportConflict = errors.New("scheduled report occurrence conflicts with persisted identity")

// ErrScheduledReportUndeliverable identifies a report run that is still pending
// while its durable handoff has exhausted its delivery budget. It needs operator
// repair — resetting the outbox row or cancelling the run — and is reported rather
// than re-armed, because re-publishing against a spent budget would loop forever
// while continuing to look like ordinary scheduler activity.
var ErrScheduledReportUndeliverable = errors.New("scheduled report run has no live delivery path")

// outboxDeadStatus is joboutbox's terminal failure state (internal/joboutbox's
// statusDead). Outbox retention deletes full rows in this state only after it
// records a durable abandonment fact.
const outboxDeadStatus = "dead"

// reportRunNamespace derives the durable ReportRun identity for one occurrence.
var reportRunNamespace = uuid.MustParse("5c1f8b02-1f4a-5d6e-9b3c-2a7e4f8d1c60")

// scheduledReportRunID derives the ReportRun identity for one occurrence.
//
// The Python path allocates uuid4 here and relies on reading the occurrence back
// to find it again. Deriving instead makes the whole materialization a function
// of durable inputs, so a transaction that claimed the occurrence and crashed
// before commit cannot allocate a second artifact identity on its next attempt.
// It is NOT what makes the producer idempotent — the occurrence primary key is —
// so it is deliberately covered by its own unit assertion rather than by any
// behavioral test, which could never observe it.
func scheduledReportRunID(occurrenceID string) string {
	return uuid.NewSHA1(reportRunNamespace, []byte(occurrenceID)).String()
}

// ScheduledReportCronEvaluator resolves the next cron occurrence strictly after
// base, evaluated as wall-clock time in timezoneName and returned in UTC. The
// second result reports that an unknown timezone fell back to UTC, which
// matches the Python helper's runtime defense and is not an error.
//
// It is a function type rather than a concrete call so this package does not
// have to re-implement croniter: production passes
// internal/scheduler/sync.NextOccurrence, which is the reviewed byte-exact port
// of dev_health_ops.workers.task_utils.cron_next_run, and tests pass a stub.
// Copying that parser into this package would create a second cron dialect that
// could drift from the one the sync scheduler is shadow-compared against.
type ScheduledReportCronEvaluator func(
	expression string,
	base time.Time,
	timezoneName string,
) (time.Time, bool, error)

// ScheduledReportsProducer materializes one ReportRun and one scheduled-report
// occurrence per due SavedReport, then hands the execution off through the
// engine's outbox.
//
// It replaces the 300 second dispatch-scheduled-reports Celery sweep
// (src/dev_health_ops/workers/report_scheduler.py). Due-ness stays where Python
// kept it — in the durable ScheduledJob cron and the report's own last run —
// and the fixed cadence only decides when the sweep runs.
type ScheduledReportsProducer struct {
	nextOccurrence ScheduledReportCronEvaluator
}

// NewScheduledReportsProducer constructs the report dispatch producer.
func NewScheduledReportsProducer(
	evaluator ScheduledReportCronEvaluator,
) (Producer, error) {
	if evaluator == nil {
		return nil, ErrProducerUnavailable
	}
	return &ScheduledReportsProducer{nextOccurrence: evaluator}, nil
}

func (*ScheduledReportsProducer) ID() string { return ProducerScheduledReports }

// dueReportCandidate is one locked (schedule, report) pair the sweep may owe.
type dueReportCandidate struct {
	JobID          string
	OrganizationID string
	Cron           string
	Timezone       string
	ReportID       string
	// NextDueAt is the SQL-sortable copy of the cron instant derived from Base.
	// Migration 0097 backfills it and the schedule write paths maintain it. The
	// producer still derives the value again, both to preserve the croniter oracle
	// as authority and to repair a marker after a completed run changes Base.
	NextDueAt *time.Time
	// AlreadyMaterialized is true when the stable occurrence identity already
	// exists. New work sorts before replays so an installation with many pending
	// handoff repairs cannot consume every bounded page forever.
	AlreadyMaterialized bool
	// Base is the instant cron due-ness is measured from: the report's own last
	// run, or its creation when it has never run. Python uses exactly this, and
	// deliberately does not advance it here — only a completed run moves it — so
	// a due report keeps resolving to the same occurrence identity until it
	// succeeds, and the occurrence table is what stops that from re-dispatching
	// every tick.
	Base time.Time
}

// dueEntry is one report the sweep has established is owed, with the cron instant
// it became owed.
type dueEntry struct {
	candidate    dueReportCandidate
	scheduledFor time.Time
}

// sortDueEntriesMostOverdueFirst orders the due set so the longest-owed report is
// served first. This is what makes deferral bounded PER REPORT rather than merely in
// aggregate.
//
// The two-pass claim/replay split alone guarantees only that SOME never-materialized
// work progresses each tick. It does not protect a specific report: under the sweep's
// storage order (job id, then report id) a deferred entry that happens to sort after
// a budget's worth of newly-due entries is passed over every tick, and if enough
// lower-sorting reports keep coming due it is deferred forever. A starved entry is
// never even classified as new or replayed, so a spent handoff behind it stays
// undetected while the deferred reason masks it.
//
// scheduledFor is the right key: it is the cron instant the report became owed,
// derived from durable inputs (the expression, timezone, and report's last run).
// ScheduledJob.next_run_at materializes that value so SQL can take a bounded page
// in the same order; this in-memory sort verifies the page against the croniter
// authority rather than replacing it. A carried-over report is therefore strictly
// more overdue than anything that came due afterwards. The bound this buys is a
// function of POPULATION, not arrival rate: a due report is materialized within
// ceil(N/maximumScheduledReportsPerOccurrence) ticks, where N counts active
// schedules whose due time is older or equal.
//
// DETERMINISM ACROSS REPLICAS. The report id tie-break makes the order TOTAL, so it
// does not depend on the order the rows arrived in. That is stronger than it needs to
// be — the sweep's SQL already applies ORDER BY, and a stable sort would preserve it
// — and deliberately so: relying on the input order would silently couple replica
// agreement to a clause in a different function, and two replicas do NOT read the
// same rows, because SKIP LOCKED gives each a different subset. A total order is
// agreement by construction rather than by coincidence. Report ids are unique within
// one sweep because saved_reports.schedule_id is a single foreign key, so a report
// appears under at most one job.
func sortDueEntriesMostOverdueFirst(entries []dueEntry) {
	sort.SliceStable(entries, func(first, second int) bool {
		if !entries[first].scheduledFor.Equal(entries[second].scheduledFor) {
			return entries[first].scheduledFor.Before(entries[second].scheduledFor)
		}
		return entries[first].candidate.ReportID < entries[second].candidate.ReportID
	})
}

// Produce sweeps the durable report schedule set and materializes every due
// occurrence.
//
// "Now" is the occurrence's canonical due time rather than the wall clock. The
// retention producer resolves its cutoff the same way and for the same reason:
// it makes the decision immutable across a retried occurrence, so a sweep that
// crashed after choosing a due set cannot resolve a different one on the next
// attempt. ScheduledFor is Cadence.Previous(observedAt), so it is never in the
// future; at worst this is up to one 300 second period more conservative than
// the Python wall clock, and the next tick covers whatever that deferred.
func (producer *ScheduledReportsProducer) Produce(
	ctx context.Context,
	tx pgx.Tx,
	schedule Schedule,
	occurrence Occurrence,
) (Outcome, error) {
	if producer == nil || producer.nextOccurrence == nil || ctx == nil || tx == nil {
		return Outcome{}, ErrProducerUnavailable
	}
	if schedule.ID != scheduledReportsScheduleID {
		return Outcome{}, fmt.Errorf(
			"%w: scheduled reports producer does not serve schedule %s",
			ErrProducerUnavailable, schedule.ID,
		)
	}
	// Ambiguity is checked BEFORE the locked sweep and without its locks: see
	// refuseAmbiguousReportSchedules for why the locked result set cannot answer it.
	if err := refuseAmbiguousReportSchedules(ctx, tx); err != nil {
		return Outcome{}, err
	}
	now := occurrence.ScheduledFor.UTC()
	candidates, err := producer.lockDueCandidates(ctx, tx, now)
	if err != nil {
		return Outcome{}, err
	}

	// Due-ness is resolved once, then the sweep runs in TWO passes: new claims
	// first, replays second.
	//
	// The ordering is load-bearing, not tidiness. With a single pass, replayed
	// occurrences competed for the same budget as new ones, so an installation with
	// more due reports than the bound could spend its entire budget re-arming
	// reports it had already materialized and never reach the remainder — on every
	// tick, forever. "The deferred report stays due" is true but insufficient:
	// staying due is not progress if the budget is refilled by replays each time.
	// Claiming first guarantees that every tick makes forward progress on work that
	// has never been materialized.
	dueEntries := make([]dueEntry, 0, len(candidates))
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return Outcome{}, err
		}
		scheduledFor, err := producer.nextRun(candidate, candidate.Base)
		if err != nil {
			return Outcome{}, err
		}
		if err := producer.recordNextDue(ctx, tx, occurrence, candidate, scheduledFor); err != nil {
			return Outcome{}, err
		}
		if scheduledFor.After(now) {
			continue
		}
		dueEntries = append(dueEntries, dueEntry{candidate: candidate, scheduledFor: scheduledFor})
	}

	sortDueEntriesMostOverdueFirst(dueEntries)

	requests := make([]JobRequest, 0, maximumScheduledReportsPerOccurrence)
	due := len(dueEntries)
	undeliverable := 0
	deferred := 0
	replays := make([]dueEntry, 0, len(dueEntries))

	// Pass one: occurrences this tick is the first to see.
	for _, entry := range dueEntries {
		if err := ctx.Err(); err != nil {
			return Outcome{}, err
		}
		if len(requests) >= maximumScheduledReportsPerOccurrence {
			deferred++
			continue
		}
		request, claimed, err := producer.claimNew(
			ctx, tx, occurrence, entry.candidate, entry.scheduledFor,
		)
		if err != nil {
			return Outcome{}, err
		}
		if !claimed {
			replays = append(replays, entry)
			continue
		}
		requests = append(requests, request)
	}

	// Pass two: already-durable occurrences, with whatever budget is left.
	for _, entry := range replays {
		if err := ctx.Err(); err != nil {
			return Outcome{}, err
		}
		if len(requests) >= maximumScheduledReportsPerOccurrence {
			deferred++
			continue
		}
		request, rearm, err := producer.replayExisting(ctx, tx, entry.candidate, entry.scheduledFor)
		if err != nil {
			// A report whose durable handoff is spent is a per-row fault: it needs
			// operator repair and it must NOT roll back the occurrence, because
			// doing so would discard every other tenant's freshly materialized run
			// and would do so on every subsequent tick, permanently. Nothing
			// clears the dead row on its own, so this condition persists until
			// someone acts on it — which is exactly why it must be visible rather
			// than fatal. Every other error still fails the occurrence.
			if errors.Is(err, ErrScheduledReportUndeliverable) {
				undeliverable++
				continue
			}
			return Outcome{}, err
		}
		if !rearm {
			continue
		}
		requests = append(requests, request)
	}

	// Degraded is ordered most-actionable first: a stranded run needs a human,
	// whereas a deferred remainder resolves itself on the next tick.
	degraded := ""
	switch {
	case undeliverable > 0:
		degraded = DegradedScheduledReportsUndeliverable
	case deferred > 0:
		degraded = DegradedScheduledReportsDeferred
	}
	switch {
	case len(requests) > 0:
		return Outcome{Requests: requests, Degraded: degraded}, nil
	case undeliverable > 0 || deferred > 0:
		// Nothing publishable, but the occurrence is not idle either: recording it
		// as "nothing was due" would erase the only signal these conditions have.
		return Outcome{SkipReason: SkipNoDueScheduledReportsClaimed, Degraded: degraded}, nil
	case due > 0:
		return Outcome{SkipReason: SkipNoDueScheduledReportsClaimed}, nil
	default:
		return Outcome{SkipReason: SkipNoDueScheduledReports}, nil
	}
}

// nextRun resolves the next cron occurrence strictly after base.
func (producer *ScheduledReportsProducer) nextRun(
	candidate dueReportCandidate,
	base time.Time,
) (time.Time, error) {
	next, _, err := producer.nextOccurrence(candidate.Cron, base, candidate.Timezone)
	if err != nil {
		// The offending job is named so an operator can repair the one row
		// rather than bisect the tenant set. The expression itself is not
		// echoed: it is tenant-authored text and this error reaches logs.
		return time.Time{}, fmt.Errorf(
			"%w: scheduled job %s", ErrScheduledReportConfiguration, candidate.JobID,
		)
	}
	if next.IsZero() {
		return time.Time{}, fmt.Errorf(
			"%w: scheduled job %s resolved no occurrence",
			ErrScheduledReportConfiguration, candidate.JobID,
		)
	}
	return next.UTC(), nil
}

// materialize performs the insert-or-verify for one due occurrence and returns
// the handoff the engine should publish. The boolean is false when the
// occurrence was already durable, in which case nothing was written.
//
// Replaying a durable occurrence does NOT unconditionally publish nothing. It
// verifies the linked run and both durable delivery stores so recovery and
// exhaustion cannot collapse into the same missing-row state:
//
//   - no outbox row and no abandonment fact means there is no durable evidence
//     that publication completed, so replay re-arms the linked run;
//   - a live outbox row stays with the relay;
//   - a dead outbox row, or the abandonment fact retention leaves in its place,
//     keeps the run degraded without minting a fresh attempt budget.
func (producer *ScheduledReportsProducer) claimNew(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	candidate dueReportCandidate,
	scheduledFor time.Time,
) (JobRequest, bool, error) {
	occurrenceID := ScheduledReportOccurrenceID(candidate.ReportID, scheduledFor)
	runID := scheduledReportRunID(occurrenceID)

	claimed, err := producer.claimOccurrence(
		ctx, tx, occurrence, candidate, scheduledFor, occurrenceID,
	)
	if err != nil || !claimed {
		return JobRequest{}, false, err
	}
	if err := producer.insertRun(ctx, tx, occurrence, candidate, occurrenceID, runID); err != nil {
		return JobRequest{}, false, err
	}
	if err := producer.advanceNextRun(ctx, tx, occurrence, candidate, scheduledFor); err != nil {
		return JobRequest{}, false, err
	}
	return producer.handoff(candidate, runID), true, nil
}

// replayExisting decides what an already-durable occurrence still owes.
//
// The authoritative run is whatever the persisted occurrence LINKS, which is not
// necessarily the identity this derivation would produce — see
// replayNeedsRearming. The derived identity is only ever used to CREATE a run,
// never to recognise one.
func (producer *ScheduledReportsProducer) replayExisting(
	ctx context.Context,
	tx pgx.Tx,
	candidate dueReportCandidate,
	scheduledFor time.Time,
) (JobRequest, bool, error) {
	occurrenceID := ScheduledReportOccurrenceID(candidate.ReportID, scheduledFor)
	linkedRunID, rearm, err := producer.replayNeedsRearming(ctx, tx, occurrenceID)
	if err != nil || !rearm {
		return JobRequest{}, false, err
	}
	return producer.handoff(candidate, linkedRunID), true, nil
}

// handoff builds the execution request for one durable run.
//
// OrganizationID is deliberately absent. report.execute_scheduled is declared
// OrganizationScope "global" in the compiled contract, which FORBIDS the field,
// and the Python path's enqueue_worker_job does not set it either: the run's own
// report_id carries the tenant. Setting it from candidate.OrganizationID — which
// is otherwise the natural thing to do, and which the fan-out producers all do —
// makes the envelope fail strict decode at publish time.
func (producer *ScheduledReportsProducer) handoff(
	candidate dueReportCandidate,
	runID string,
) JobRequest {
	return JobRequest{
		Kind: jobcontract.KindReportExecuteScheduled,
		Envelope: jobcontract.Envelope{
			ContractVersion: jobcontract.ContractVersionV1,
			// Both fields reproduce reports.execution_trigger._enqueue_run exactly.
			// The idempotency key is the outbox dedupe key, so a Python-authored and
			// a Go-authored handoff for the same run collapse to one row instead of
			// executing the report twice during coexistence.
			CorrelationID:  "report-run:" + runID,
			IdempotencyKey: "report.run:" + runID,
			Domain:         jobcontract.DomainLink{Type: "report_run", ID: runID},
			Payload:        jobcontract.ScheduledReportExecutionPayload{ReportID: candidate.ReportID},
		},
	}
}

// replayNeedsRearming decides what a durable occurrence's replay owes.
//
// It answers "does this already-claimed occurrence still have a live delivery
// path", and returns true only when the answer is "no, and re-publishing is the
// correct repair". See materialize's comment for why this exists at all.
//
// The authoritative run is the one the persisted occurrence LINKS, and it is
// deliberately NOT required to equal the identity this producer's derivation
// would produce. That distinction is the whole point of coexistence and an
// earlier version got it wrong: the Python dispatcher allocates ReportRun.id with
// uuid4, so a Python-authored occurrence links a run id that can never match the
// Go derivation. Requiring equality made every subsequent Go sweep fail that
// occurrence with a conflict — closing the schedule's readiness for as long as a
// Python-authored occurrence existed for a still-due report, which is exactly the
// interoperability the byte-exact occurrence identity was built to allow.
// Mutation M22a found this by surviving: removing the equality check broke no
// test, which is how a wrong check with no coverage looks.
//
// The derived identity is therefore used only to CREATE a run, never to recognise
// one, and the re-armed handoff carries the LINKED run's id so its outbox dedupe
// key matches whatever the other runtime already enqueued.
//
//   - The occurrence must link SOME run. A missing link means the run was deleted
//     under it (report_run_id is ON DELETE SET NULL), leaving an occurrence that
//     can never be executed; that is never retryable.
//
//     The two nil clauses are deliberately REDUNDANT and both are kept. Clause-by-
//     clause mutation showed each one alone covers every reachable state: a NULL
//     report_run_id makes the LEFT JOIN yield a NULL run status too, and the
//     foreign key makes "links a run that does not exist" unreachable. So each
//     survives removal — a mutation removing genuine redundancy, not a missing
//     test. They stay because they defend different future changes: the
//     linkedRunID check guards the *linkedRunID dereference below, and the
//     runStatus check would still hold if the join were ever narrowed to an inner
//     join or the foreign key relaxed. Do not "simplify" either away.
//
//   - A run that is no longer pending is not this producer's business. Running,
//     succeeded, failed and canceled are all states the report handler owns, and
//     re-arming any of them would either duplicate execution or resurrect work an
//     operator cancelled.
//
//   - A pending run with a live (non-dead) outbox row is delivery in flight. Stay
//     quiet: the relay owns it.
//
//   - A pending run with NO outbox row and NO abandonment fact has no durable
//     evidence of a handoff. Re-publishing is safe and is the only way the report
//     ever runs, so it is re-armed.
//
//   - A pending run with either a DEAD outbox row or a durable abandonment fact
//     has spent its attempt budget. Re-arming silently would cycle fresh budgets
//     forever, so it fails loudly instead and names the run for repair.
func (producer *ScheduledReportsProducer) replayNeedsRearming(
	ctx context.Context,
	tx pgx.Tx,
	occurrenceID string,
) (string, bool, error) {
	var linkedRunID *string
	var runStatus *string
	var handoffStatus *string
	var handoffAbandoned bool
	if err := tx.QueryRow(ctx, replayedReportRunSQL, occurrenceID).Scan(
		&linkedRunID, &runStatus, &handoffStatus, &handoffAbandoned,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// claimOccurrence proved the row exists, so losing it here is a
			// concurrent deletion inside this snapshot, which the claim's lock is
			// supposed to make impossible.
			return "", false, ErrScheduledReportConflict
		}
		return "", false, fmt.Errorf("read replayed scheduled report run: %w", err)
	}
	if linkedRunID == nil || runStatus == nil {
		return "", false, fmt.Errorf(
			"%w: occurrence %s links no executable run", ErrScheduledReportConflict, occurrenceID,
		)
	}
	if *runStatus != pendingReportRunStatus {
		return "", false, nil
	}
	switch {
	case handoffAbandoned || (handoffStatus != nil && *handoffStatus == outboxDeadStatus):
		return "", false, fmt.Errorf(
			"%w: report run %s is pending but its handoff exhausted its delivery budget",
			ErrScheduledReportUndeliverable, *linkedRunID,
		)
	case handoffStatus == nil:
		return *linkedRunID, true, nil
	default:
		return "", false, nil
	}
}

// claimOccurrence inserts the occurrence identity exactly once.
//
// This is the same mechanism PostgresLedger.Claim uses, for the same reason:
// the primary key serializes every writer racing one due time, and the
// verification read proves a rejected insert lost to the SAME occurrence rather
// than to a changed identity derivation. It is what makes the sweep safe
// against a second scheduler replica and against the Python dispatcher running
// concurrently through the whole coexistence window.
func (producer *ScheduledReportsProducer) claimOccurrence(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	candidate dueReportCandidate,
	scheduledFor time.Time,
	occurrenceID string,
) (bool, error) {
	var inserted string
	err := tx.QueryRow(
		ctx,
		insertScheduledReportOccurrenceSQL,
		occurrenceID,
		scheduledReportOccurrenceIdentityVersion,
		candidate.OrganizationID,
		candidate.ReportID,
		candidate.JobID,
		scheduledFor,
		occurrence.ObservedAt,
	).Scan(&inserted)
	if err == nil {
		if inserted != occurrenceID {
			return false, ErrScheduledReportConflict
		}
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("claim scheduled report occurrence: %w", err)
	}

	var identityVersion, organizationID, reportID, jobID string
	var persistedFor time.Time
	if err := tx.QueryRow(ctx, selectScheduledReportOccurrenceSQL, occurrenceID).Scan(
		&identityVersion, &organizationID, &reportID, &jobID, &persistedFor,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The insert was skipped but this identity does not exist, so the
			// unique (report_id, scheduled_for) constraint rejected a different
			// key for the same due time: the identity derivation changed.
			return false, ErrScheduledReportConflict
		}
		return false, fmt.Errorf("verify scheduled report occurrence: %w", err)
	}
	if identityVersion != scheduledReportOccurrenceIdentityVersion ||
		organizationID != candidate.OrganizationID || reportID != candidate.ReportID ||
		jobID != candidate.JobID || !persistedFor.Equal(scheduledFor) {
		return false, ErrScheduledReportConflict
	}
	return false, nil
}

// insertRun materializes the authoritative ReportRun and links the occurrence
// to it.
//
// The run identity is derived from the occurrence identity rather than
// allocated randomly the way the Python path does. That is deliberate: it makes
// the whole materialization a function of durable inputs, so a transaction that
// claimed the occurrence and crashed before commit cannot allocate a second
// artifact identity on its next attempt. The unique report_run_id constraint on
// the occurrence would catch a duplicate anyway; deriving means it never has to.
func (producer *ScheduledReportsProducer) insertRun(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	candidate dueReportCandidate,
	occurrenceID string,
	runID string,
) error {
	if _, err := tx.Exec(
		ctx,
		insertScheduledReportRunSQL,
		runID,
		candidate.ReportID,
		occurrenceID,
		pendingReportRunStatus,
		scheduledReportTriggeredBy,
		occurrence.ObservedAt,
	); err != nil {
		return fmt.Errorf("materialize scheduled report run: %w", err)
	}
	command, err := tx.Exec(ctx, linkScheduledReportOccurrenceRunSQL, occurrenceID, runID)
	if err != nil {
		return fmt.Errorf("link scheduled report occurrence to its run: %w", err)
	}
	if command.RowsAffected() != 1 {
		// The claim above proved this row exists and had no run. Losing it here
		// means something else mutated the occurrence inside this transaction's
		// snapshot, which the claim is supposed to make impossible.
		return ErrScheduledReportConflict
	}
	return nil
}

// recordNextDue keeps the SQL paging key equal to the croniter-derived authority.
//
// A NULL or stale marker can exist during a rolling upgrade or just after a report
// completion invalidates it. The bounded page repairs it from the authoritative
// cron inputs before making a due decision. A pending occurrence is different:
// advanceNextRun has already written the following projected fire, and the replay
// joins through its durable occurrence instead of moving that projection backward.
func (producer *ScheduledReportsProducer) recordNextDue(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	candidate dueReportCandidate,
	scheduledFor time.Time,
) error {
	if candidate.AlreadyMaterialized {
		return nil
	}
	if candidate.NextDueAt != nil && candidate.NextDueAt.Equal(scheduledFor) {
		return nil
	}
	command, err := tx.Exec(
		ctx, recordScheduledJobNextDueSQL, candidate.JobID, scheduledFor, occurrence.ObservedAt,
	)
	if err != nil {
		return fmt.Errorf("record scheduled report next due time: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: scheduled job %s disappeared", ErrScheduledReportConflict, candidate.JobID)
	}
	return nil
}

// advanceNextRun writes the projected fire after a newly materialized
// occurrence, matching the Python dispatcher and preserving the operator-facing
// meaning of ScheduledJob.next_run_at. A pending occurrence remains independently
// visible through scheduled_report_occurrences, so advancing this marker cannot
// hide its replay or handoff-repair state from the bounded page.
func (producer *ScheduledReportsProducer) advanceNextRun(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	candidate dueReportCandidate,
	scheduledFor time.Time,
) error {
	following, err := producer.nextRun(candidate, scheduledFor)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		ctx, recordScheduledJobNextDueSQL, candidate.JobID, following, occurrence.ObservedAt,
	); err != nil {
		return fmt.Errorf("advance scheduled report next run: %w", err)
	}
	return nil
}

// lockDueCandidates reads and locks one bounded, oldest-first report page.
//
// The lock set is `FOR UPDATE OF job, report SKIP LOCKED`, which is the Python
// dispatcher's lock set: it takes FOR UPDATE SKIP LOCKED on the ScheduledJob
// rows and FOR UPDATE on the SavedReport through _lock_report. Both are kept.
//
// Holding the report lock was reconsidered and kept rather than replaced by the
// occurrence-key claim alone. The claim does make double-materialization
// impossible, so the lock is not what protects idempotency. What it protects is
// narrower and real: without it, a report deactivated by a concurrent
// transaction between this sweep's read and its insert would still get a run,
// because nothing re-checks is_active after the read. The claim cannot see that
// race at all, so it is not a substitute for the lock.
//
// One divergence remains and is strictly conservative. SKIP LOCKED covers the
// report as well as the job, where Python's _lock_report blocks. A report locked
// by a concurrent on-demand trigger is therefore deferred to the next 300 second
// tick instead of waiting: this transaction also holds the fixed-schedule
// occurrence claim and a coordinator connection, and blocking either behind a
// tenant-triggered lock is a worse failure than a one-tick delay. It can never
// produce work Python would not have.
//
// The organization guard is computed in SQL with ordered boolean arms:
// evaluating organizations only for a UUID-shaped identifier is what keeps the
// query free of a text-to-uuid cast that would raise on the "default" sentinel
// and on legacy non-UUID identifiers. Comparing organizations.id::text against
// a lowercased identifier rather than casting the other direction is the same
// choice for the same reason.
func (producer *ScheduledReportsProducer) lockDueCandidates(
	ctx context.Context,
	tx pgx.Tx,
	dueThrough time.Time,
) ([]dueReportCandidate, error) {
	rows, err := tx.Query(
		ctx,
		dueScheduledReportsSQL,
		activeScheduledJobStatus,
		dueThrough.UTC(),
		maximumScheduledReportCandidatesPerOccurrence,
	)
	if err != nil {
		return nil, fmt.Errorf("read due scheduled reports: %w", err)
	}
	defer rows.Close()

	candidates := make([]dueReportCandidate, 0, maximumScheduledReportCandidatesPerOccurrence)
	for rows.Next() {
		var candidate dueReportCandidate
		if err := rows.Scan(
			&candidate.JobID,
			&candidate.OrganizationID,
			&candidate.Cron,
			&candidate.Timezone,
			&candidate.ReportID,
			&candidate.Base,
			&candidate.NextDueAt,
			&candidate.AlreadyMaterialized,
		); err != nil {
			return nil, fmt.Errorf("scan due scheduled report: %w", err)
		}
		candidate.Base = candidate.Base.UTC()
		if candidate.NextDueAt != nil {
			nextDueAt := candidate.NextDueAt.UTC()
			candidate.NextDueAt = &nextDueAt
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read due scheduled reports: %w", err)
	}
	if err := rejectAmbiguousReportSchedules(candidates); err != nil {
		return nil, err
	}
	return candidates, nil
}

// refuseAmbiguousReportSchedules refuses any report schedule owning more than one
// active report, measured WITHOUT the sweep's row locks.
//
// It exists because checking the locked result set is not sufficient, and that is
// a subtle interaction between two things this producer does. The sweep applies
// SKIP LOCKED to report rows, so when two active reports share one schedule and a
// concurrent on-demand trigger holds a lock on one of them, the sweep returns only
// the unlocked sibling. A duplicate check over that result sees one row, passes,
// and then materializes an ARBITRARY sibling while advancing the shared job's
// next_run_at — the ambiguity is concealed by the lock rather than detected.
// Python's unlocked one_or_none lookup sees both reports and fails closed, so this
// restores that behavior.
//
// The load-bearing property is that this query takes NO LOCKS, not that it runs
// before the sweep. Mutation testing established that: moving the call after
// lockDueCandidates changes nothing, because this reads the database
// independently rather than reading the sweep's result, so another transaction's
// lock cannot hide a sibling from it either way. Calling it first is a cheapness
// preference — it avoids taking row locks the occurrence is about to abandon —
// and must not be mistaken for the reason it is correct.
//
// It deliberately covers every active report schedule rather than only the due
// ones: a schedule that is ambiguous today will be due eventually, and reporting
// it now is more useful than discovering it at 03:00.
func refuseAmbiguousReportSchedules(ctx context.Context, tx pgx.Tx) error {
	var jobID, firstReport, secondReport string
	err := tx.QueryRow(ctx, ambiguousReportSchedulesSQL, activeScheduledJobStatus).
		Scan(&jobID, &firstReport, &secondReport)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check for ambiguous report schedules: %w", err)
	}
	return fmt.Errorf(
		"%w: scheduled job %s owns active reports %s and %s",
		ErrScheduledReportConflict, jobID, firstReport, secondReport,
	)
}

// rejectAmbiguousReportSchedules refuses a schedule that owns more than one
// active report within the locked candidate set.
//
// Alembic 0096 makes saved_reports.schedule_id unique. Keep this assertion for
// a partially migrated or manually drifted schema: two active reports pointing
// at one ScheduledJob would otherwise fan out silently and advance one
// schedule's next_run_at twice. Python's one_or_none() also refuses that state.
//
// This is the SECONDARY guard and cannot be the only one: it sees the sweep's
// post-SKIP-LOCKED result, where a locked sibling has already been removed.
// refuseAmbiguousReportSchedules is the authority and runs unlocked, before the
// sweep. This one is kept as a cheap invariant assertion on the set actually
// about to be materialized.
func rejectAmbiguousReportSchedules(candidates []dueReportCandidate) error {
	seen := make(map[string]string, len(candidates))
	for _, candidate := range candidates {
		if previous, duplicate := seen[candidate.JobID]; duplicate {
			return fmt.Errorf(
				"%w: scheduled job %s owns active reports %s and %s",
				ErrScheduledReportConflict, candidate.JobID, previous, candidate.ReportID,
			)
		}
		seen[candidate.JobID] = candidate.ReportID
	}
	return nil
}

// ScheduledReportOccurrenceID reproduces
// dev_health_ops.reports.execution_trigger.scheduled_report_occurrence_identity
// byte for byte.
//
// It must stay byte-identical, not merely deterministic: the Python dispatcher
// writes into the same scheduled_report_occurrences table for the whole
// coexistence window, and a differing digest would let both runtimes claim the
// same due time under two identities and execute one report twice.
//
// The framing is Python's, which is length-prefixed but NOT the same framing
// this package's own occurrence keys use (writeDigestField). They are two
// separate wire formats and neither may be substituted for the other.
func ScheduledReportOccurrenceID(reportID string, scheduledFor time.Time) string {
	hasher := sha256.New()
	writePythonDigestField(hasher, "identity_version", scheduledReportOccurrenceIdentityVersion)
	writePythonDigestField(hasher, "report_id", reportID)
	// Python builds this value as strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z":
	// six microsecond digits from %f, then three literal zeros and a literal Z.
	// The result is nine fractional digits and is not RFC3339 formatting.
	writePythonDigestField(
		hasher, "scheduled_for", scheduledFor.UTC().Format("2006-01-02T15:04:05.000000")+"000Z",
	)
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil))
}

// writePythonDigestField emits one length-prefixed field in the framing used by
// scheduled_report_occurrence_identity: len(name) ":" name len(value) ":" value
// "\n". Lengths are byte lengths, which is what Python measures after encode().
func writePythonDigestField(hasher io.Writer, name, value string) {
	_, _ = fmt.Fprintf(hasher, "%d:%s%d:%s\n", len(name), name, len(value), value)
}

// The sweep gives never-materialized work priority over replay, then orders each
// set by its durable cron instant and report id. That is the SQL form of Produce's
// two-pass fairness contract. A pending/running ReportRun remains visible through
// its occurrence even after next_run_at advances to the following projected fire.
// NULL next_run_at is included for bounded rolling-upgrade repair. The organization
// predicate is documented at lockDueCandidates.
const dueScheduledReportsSQL = `
SELECT job.id::text,
       job.org_id,
       job.schedule_cron,
       job.timezone,
       report.id::text,
       COALESCE(report.last_run_at, report.created_at),
       job.next_run_at,
       replay.occurrence_id IS NOT NULL
FROM public.scheduled_jobs AS job
JOIN public.saved_reports AS report
    ON report.schedule_id = job.id
   AND report.is_active
   -- Tenant ownership, not decoration. reports.execution_trigger._require_schedule
   -- rejects report.org_id != job.org_id explicitly, and the schema enforces only
   -- neither the schedule_id foreign key nor its unique constraint verifies
   -- organization ownership, so inconsistent data can point one tenant's job
   -- at another tenant's report. Without this the producer would
   -- file the occurrence under the JOB's organization while the global report
   -- worker executed the REPORT's tenant data.
   AND report.org_id = job.org_id
LEFT JOIN LATERAL (
    SELECT occurrence.occurrence_id, occurrence.scheduled_for
    FROM public.scheduled_report_occurrences AS occurrence
    WHERE occurrence.report_id = report.id
      -- The report's base advances only after a terminal transition. An
      -- occurrence newer than that base is therefore still the one this report
      -- owes, regardless of whether its run is pending, terminal, deleted, or
      -- no longer linked. Keeping terminal/corrupt rows in the replay page is
      -- load-bearing: replayExisting owns the quiet-vs-conflict decision.
      -- Conversely, the strict comparison excludes a completed/canceled
      -- occurrence when last_run_at equals its scheduled_for, so the next cron
      -- instant can be materialized.
      AND occurrence.scheduled_for > COALESCE(report.last_run_at, report.created_at)
    ORDER BY occurrence.scheduled_for
    LIMIT 1
) AS replay ON TRUE
WHERE job.job_type = '` + reportJobType + `'
  AND job.status = $1
  AND job.is_running = FALSE
  AND (
      replay.occurrence_id IS NOT NULL
      OR job.next_run_at IS NULL
      OR job.next_run_at <= $2
  )
  AND (
      job.org_id IN ('', 'default')
      OR job.org_id !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      OR EXISTS (
          SELECT 1
          FROM public.organizations AS organization
          WHERE organization.id::text = lower(job.org_id)
      )
  )
ORDER BY replay.occurrence_id IS NOT NULL,
         COALESCE(replay.scheduled_for, job.next_run_at, report.last_run_at, report.created_at),
         report.id
LIMIT $3
FOR UPDATE OF job, report SKIP LOCKED
`

const insertScheduledReportOccurrenceSQL = `
INSERT INTO public.scheduled_report_occurrences (
    occurrence_id,
    identity_version,
    org_id,
    report_id,
    scheduled_job_id,
    scheduled_for,
    created_at
) VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, $7)
ON CONFLICT DO NOTHING
RETURNING occurrence_id
`

const selectScheduledReportOccurrenceSQL = `
SELECT identity_version, org_id, report_id::text, scheduled_job_id::text, scheduled_for
FROM public.scheduled_report_occurrences
WHERE occurrence_id = $1
FOR UPDATE
`

// The remaining NOT NULL columns (attempt_count, notification_status) carry
// server defaults from alembic 0053, so omitting them takes the same values the
// Python ORM path writes.
const insertScheduledReportRunSQL = `
INSERT INTO public.report_runs (
    id, report_id, scheduled_occurrence_id, status, triggered_by, created_at
) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6)
`

const linkScheduledReportOccurrenceRunSQL = `
UPDATE public.scheduled_report_occurrences
SET report_run_id = $2::uuid
WHERE occurrence_id = $1
  AND report_run_id IS NULL
`

// Both delivery joins are LEFT so all lifecycle states stay distinct: never
// published, live, dead before retention, and abandoned after retention.
//
// The dedupe key is derived IN SQL from the linked run rather than passed in,
// because the authoritative run is whatever the occurrence links — which for a
// Python-authored occurrence is a uuid4 this producer could not have predicted.
// Passing a caller-computed key would look up the handoff of a run that may not
// be the one being replayed.
const replayedReportRunSQL = `
SELECT occurrence.report_run_id::text, run.status, handoff.status,
       abandonment.dedupe_key IS NOT NULL
FROM public.scheduled_report_occurrences AS occurrence
LEFT JOIN public.report_runs AS run
    ON run.id = occurrence.report_run_id
LEFT JOIN public.worker_job_outbox AS handoff
    ON handoff.dedupe_key = 'report.run:' || occurrence.report_run_id::text
LEFT JOIN public.worker_job_delivery_abandonments AS abandonment
    ON abandonment.dedupe_key = 'report.run:' || occurrence.report_run_id::text
WHERE occurrence.occurrence_id = $1
`

// The self-join finds any report schedule with two distinct active reports. It
// takes no locks, which is the entire point: SKIP LOCKED in the sweep would
// otherwise hide the sibling. LIMIT 1 because one instance is enough to refuse.
const ambiguousReportSchedulesSQL = `
SELECT job.id::text, first_report.id::text, second_report.id::text
FROM public.scheduled_jobs AS job
JOIN public.saved_reports AS first_report
    ON first_report.schedule_id = job.id
   AND first_report.is_active
   AND first_report.org_id = job.org_id
JOIN public.saved_reports AS second_report
    ON second_report.schedule_id = job.id
   AND second_report.is_active
   AND second_report.org_id = job.org_id
   AND second_report.id > first_report.id
WHERE job.job_type = '` + reportJobType + `'
  AND job.status = $1
ORDER BY job.id, first_report.id, second_report.id
LIMIT 1
`

const recordScheduledJobNextDueSQL = `
UPDATE public.scheduled_jobs
SET next_run_at = $2,
    updated_at = $3
WHERE id = $1::uuid
`
