package joboutbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Reason codes for an outbox delivery that never reached River. They are the
// complete, bounded vocabulary this sweep writes to worker_job_outbox's
// last_error_code (and reports for a canceled work-graph request), documented
// in docs/operate/run/job-recovery-lifecycle.md. A value outside this set is
// never written.
const (
	// UndeliveredReasonPrerequisiteFailed: the row waits on a completion fence
	// whose predecessor ended without success ('failed' or 'canceled'). The
	// fence is written only on success, so it can never arrive.
	UndeliveredReasonPrerequisiteFailed = "prerequisite_failed"
	// UndeliveredReasonPrerequisiteExpired: the fence did not arrive within the
	// undelivered ceiling, whatever state the predecessor is in (still
	// running, ambiguous, missing, or a domain this sweep does not read).
	UndeliveredReasonPrerequisiteExpired = "prerequisite_expired"
	// UndeliveredReasonRequestTerminal: the work-graph request the row would
	// deliver is already terminal -- most often 'canceled' by coalescing --
	// so a delivery has nothing left to run.
	UndeliveredReasonRequestTerminal = "request_terminal"
	// UndeliveredReasonDeliveryDead: the outbox row is already 'dead' (the
	// relay spent its attempts, or the contract/policy was rejected) while
	// its work-graph request is still 'pending'. Nothing can ever run that
	// request, so the request is canceled; the outbox row keeps its own code.
	UndeliveredReasonDeliveryDead = "delivery_dead"

	// DefaultUndeliveredCeiling bounds how long a fenced outbox row may wait
	// for its predecessor before it and its request go terminal. A post-sync
	// chain (daily -> workgraph.build -> investment.materialize) completes in
	// hours; three days leaves a slow chain untouched, while every later sync
	// writes its own chain that covers the same days.
	DefaultUndeliveredCeiling = 72 * time.Hour
)

var undeliveredReasonDetail = map[string]string{
	UndeliveredReasonPrerequisiteFailed:  "prerequisite ended without success; its completion fence can never be written",
	UndeliveredReasonPrerequisiteExpired: "prerequisite completion fence did not arrive within the undelivered ceiling",
	UndeliveredReasonRequestTerminal:     "work graph request is already terminal; the gated delivery has nothing to run",
}

// UndeliveredResolution is one outbox row (and, for work-graph kinds, its
// request) this sweep moved to a terminal state. It is reported rather than
// logged here for the same reason ProviderUnitRearm is: this package stays
// DB-only, and ReconcilerLoop is the layer that holds a logger.
type UndeliveredResolution struct {
	OutboxID        string
	JobKind         string
	RequestID       string
	Reason          string
	RequestCanceled bool
	OutboxDead      bool
}

type undeliveredCandidate struct {
	outboxID  string
	jobKind   string
	requestID string
	reason    string
}

// undeliveredResult is one pass of the undelivered sweep.
type undeliveredResult struct {
	resolutions []UndeliveredResolution
	blocked     int
	raceLost    int
}

// undeliveredClassifySQL classifies every outbox row that can never be
// claimed by the relay without outside help. It has two sources:
//
//  1. a PENDING row gated on a completion fence that is still absent. The
//     relay's claim SQL (repository.go) skips it silently on every tick and
//     it consumes no attempt, so without this sweep it waits forever. This is
//     the class that left post-sync workgraph.build and investment.materialize
//     requests pending for weeks: their predecessor failed, and a fence is
//     written only on success.
//  2. a DEAD row whose work-graph request is still 'pending'. The relay made
//     the delivery terminal, but nothing made the request -- the layer of
//     record -- terminal with it.
//
// 'blocked' rows are inside the ceiling and are counted, not acted on. The
// predecessor state is read for the two domains the queue role may read
// (work_graph_execution_requests and daily_metrics_runs); every other domain
// resolves only through the ceiling. The domain id is cast behind the uuid
// format guard, never compared as text, so both joins stay sargable against
// the primary key (CHAOS-4092's crash loop was a text cast of an id).
const undeliveredClassifySQL = `
	WITH gated AS (
		SELECT outbox.id, outbox.job_kind, outbox.args, outbox.prerequisite_completion_key,
			GREATEST(outbox.created_at, outbox.scheduled_at) AS waiting_since
		FROM public.worker_job_outbox AS outbox
		WHERE outbox.status = 'pending'
			AND outbox.river_job_id IS NULL
			AND outbox.prerequisite_completion_key IS NOT NULL
			AND NOT EXISTS (
				SELECT 1 FROM public.worker_job_completion_fences AS fence
				WHERE fence.completion_key = outbox.prerequisite_completion_key
			)
	), classified AS (
		SELECT gated.id, gated.job_kind, COALESCE(request.id::text, '') AS request_id,
			gated.waiting_since,
			CASE
				WHEN request.state IN ('succeeded', 'failed', 'canceled') THEN 'request_terminal'
				-- A running or ambiguous request is owned by its lease or by the
				-- ambiguous-repair contract, never by this sweep.
				WHEN request.state IN ('running', 'ambiguous') THEN 'blocked'
				WHEN graph_head.state IN ('failed', 'canceled')
					OR daily_head.status IN ('failed', 'canceled') THEN 'prerequisite_failed'
				WHEN gated.waiting_since <= $1 THEN 'prerequisite_expired'
				ELSE 'blocked'
			END AS reason
		FROM gated
		LEFT JOIN public.work_graph_execution_requests AS request
			ON gated.job_kind IN ('workgraph.build', 'investment.materialize')
			AND request.id = CASE
				WHEN (gated.args #>> '{domain,id}') ~
					'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
				THEN (gated.args #>> '{domain,id}')::uuid
				ELSE NULL
			END
			AND request.kind = gated.job_kind
			AND request.org_id::text = gated.args ->> 'organization_id'
		LEFT JOIN public.work_graph_execution_requests AS graph_head
			ON split_part(gated.prerequisite_completion_key, ':', 1) = 'work_graph_execution_request'
			AND graph_head.id = split_part(gated.prerequisite_completion_key, ':', 2)::uuid
		LEFT JOIN public.daily_metrics_runs AS daily_head
			ON split_part(gated.prerequisite_completion_key, ':', 1) = 'daily_metrics_run'
			AND daily_head.id = split_part(gated.prerequisite_completion_key, ':', 2)::uuid
		UNION ALL
		SELECT outbox.id, outbox.job_kind, request.id::text, outbox.updated_at, 'delivery_dead'
		FROM public.worker_job_outbox AS outbox
		JOIN public.work_graph_execution_requests AS request
			ON request.id = CASE
				WHEN (outbox.args #>> '{domain,id}') ~
					'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
				THEN (outbox.args #>> '{domain,id}')::uuid
				ELSE NULL
			END
			AND request.kind = outbox.job_kind
			AND request.org_id::text = outbox.args ->> 'organization_id'
		WHERE outbox.status = 'dead'
			AND outbox.job_kind IN ('workgraph.build', 'investment.materialize')
			AND request.state = 'pending'
			AND request.claim_token IS NULL
			AND request.lease_expires_at IS NULL
	)`

// undeliveredSurveySQL returns the actionable rows oldest first, then the
// blocked ones, with the blocked total counted before LIMIT applies. Sorting
// actionable rows first keeps a large blocked backlog from filling every pass.
const undeliveredSurveySQL = undeliveredClassifySQL + `
	SELECT classified.id::text, classified.job_kind, classified.request_id, classified.reason,
		count(*) FILTER (WHERE classified.reason = 'blocked') OVER () AS blocked
	FROM classified
	ORDER BY (classified.reason = 'blocked'), classified.waiting_since, classified.id
	LIMIT $2::int
`

// UndeliveredReportSQL is the operator view of the same classification: one
// row per (job kind, reason) with its count and oldest wait. It shares
// undeliveredClassifySQL so the CLI and the sweep cannot disagree about which
// rows are stuck. Parameter $1 is the ceiling cutoff (now - ceiling).
const UndeliveredReportSQL = undeliveredClassifySQL + `
	SELECT classified.job_kind, classified.reason, count(*)::bigint,
		min(classified.waiting_since)
	FROM classified
	GROUP BY classified.job_kind, classified.reason
	ORDER BY classified.job_kind, classified.reason
`

// undeliveredCancelRequestsSQL runs on the DOMAIN pool: the queue-control
// role holds SELECT only on work_graph_execution_requests. The request goes
// terminal FIRST, before its outbox row, so no re-arm path can resurrect it:
// strand repair re-arms a delivery only while its request is 'pending' or
// 'running' past its lease, and 'canceled' is immutable under alembic 0060's
// trigger. PostgresStore.Claim reports a canceled request as finished, so a
// delivery that races past this update retires without executing.
//
// The statement re-proves the eligibility it can observe under its own
// snapshot: the request is still unclaimed 'pending', its outbox row is
// still undeliverable (dead, or pending behind an absent fence), and no
// OTHER live outbox row references the same request.
const undeliveredCancelRequestsSQL = `
	UPDATE public.work_graph_execution_requests AS request
	SET state = 'canceled', claim_token = NULL, lease_expires_at = NULL,
		updated_at = statement_timestamp()
	FROM unnest($1::uuid[], $2::uuid[]) AS approved(request_id, outbox_id)
	JOIN public.worker_job_outbox AS outbox ON outbox.id = approved.outbox_id
	WHERE request.id = approved.request_id
		AND request.kind = outbox.job_kind
		AND request.state = 'pending'
		AND request.claim_token IS NULL
		AND request.lease_expires_at IS NULL
		AND outbox.river_job_id IS NULL
		AND (
			outbox.status = 'dead'
			OR (
				outbox.status = 'pending'
				AND outbox.prerequisite_completion_key IS NOT NULL
				AND NOT EXISTS (
					SELECT 1 FROM public.worker_job_completion_fences AS fence
					WHERE fence.completion_key = outbox.prerequisite_completion_key
				)
			)
		)
		AND NOT EXISTS (
			SELECT 1 FROM public.worker_job_outbox AS live
			WHERE live.id <> outbox.id
				AND live.job_kind = request.kind
				AND live.args #>> '{domain,id}' = request.id::text
				AND live.status IN ('pending', 'claimed', 'delivered')
		)
	RETURNING approved.outbox_id::text
`

// undeliveredMarkDeadSQL runs on the QUEUE pool and moves a fenced, pending
// row to 'dead' with its reason. It refuses a work-graph row whose request is
// still non-terminal, so the outbox row can never go terminal ahead of the
// request that is the layer of record. A dead row is kept by retention as a
// worker_job_delivery_abandonments fact, reason code included.
const undeliveredMarkDeadSQL = `
	UPDATE public.worker_job_outbox AS outbox
	SET status = 'dead', last_error_code = approved.reason,
		last_error_detail = approved.detail, last_error_at = $1, updated_at = $1
	FROM unnest($2::uuid[], $3::text[], $4::text[]) AS approved(outbox_id, reason, detail)
	WHERE outbox.id = approved.outbox_id
		AND outbox.status = 'pending'
		AND outbox.river_job_id IS NULL
		AND outbox.prerequisite_completion_key IS NOT NULL
		AND NOT EXISTS (
			SELECT 1 FROM public.worker_job_completion_fences AS fence
			WHERE fence.completion_key = outbox.prerequisite_completion_key
		)
		AND NOT EXISTS (
			SELECT 1 FROM public.work_graph_execution_requests AS request
			WHERE outbox.job_kind IN ('workgraph.build', 'investment.materialize')
				AND request.id = CASE
					WHEN (outbox.args #>> '{domain,id}') ~
						'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
					THEN (outbox.args #>> '{domain,id}')::uuid
					ELSE NULL
				END
				AND request.kind = outbox.job_kind
				AND request.state NOT IN ('succeeded', 'failed', 'canceled')
		)
	RETURNING outbox.id::text
`

// stepUndelivered runs one bounded pass: survey on the queue pool, cancel the
// affected work-graph requests on the domain pool, then mark the outbox rows
// dead on the queue pool. Each write is one autocommit statement that re-proves
// its own predicate, so a crash between the two writes leaves a canceled
// request with a pending row, which the next pass classifies as
// request_terminal and finishes.
func (repair *StrandRepair) stepUndelivered(
	ctx context.Context,
	now time.Time,
	limit int,
) (undeliveredResult, error) {
	result := undeliveredResult{}
	candidates, blocked, err := repair.surveyUndelivered(ctx, now, limit)
	if err != nil {
		return result, fmt.Errorf("undelivered: survey: %w", err)
	}
	result.blocked = blocked
	if len(candidates) == 0 {
		return result, nil
	}

	cancelRequestIDs := make([]string, 0, len(candidates))
	cancelOutboxIDs := make([]string, 0, len(candidates))
	deadIDs := make([]string, 0, len(candidates))
	deadReasons := make([]string, 0, len(candidates))
	deadDetails := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.requestID != "" && candidate.reason != UndeliveredReasonRequestTerminal {
			cancelRequestIDs = append(cancelRequestIDs, candidate.requestID)
			cancelOutboxIDs = append(cancelOutboxIDs, candidate.outboxID)
		}
		if candidate.reason != UndeliveredReasonDeliveryDead {
			deadIDs = append(deadIDs, candidate.outboxID)
			deadReasons = append(deadReasons, candidate.reason)
			deadDetails = append(deadDetails, undeliveredReasonDetail[candidate.reason])
		}
	}

	canceled := map[string]bool{}
	if len(cancelRequestIDs) > 0 {
		canceled, err = repair.returningIDs(ctx, repair.queryDomain,
			undeliveredCancelRequestsSQL, cancelRequestIDs, cancelOutboxIDs)
		if err != nil {
			// Nothing this pass wrote yet: the survey is read-only and this is
			// the first write. The blocked count stands.
			return result, fmt.Errorf("undelivered: cancel requests: %w", err)
		}
	}
	dead := map[string]bool{}
	if len(deadIDs) > 0 {
		dead, err = repair.returningIDs(ctx, repair.queryQueue,
			undeliveredMarkDeadSQL, now.UTC(), deadIDs, deadReasons, deadDetails)
	}
	// Record every committed cancel BEFORE checking the dead-mark error: the
	// cancel statement already committed on the domain pool, and discarding
	// that evidence is the discard-on-error class this file refuses.
	for _, candidate := range candidates {
		resolution := UndeliveredResolution{
			OutboxID:        candidate.outboxID,
			JobKind:         candidate.jobKind,
			RequestID:       candidate.requestID,
			Reason:          candidate.reason,
			RequestCanceled: canceled[candidate.outboxID],
			OutboxDead:      dead[candidate.outboxID],
		}
		if resolution.RequestCanceled || resolution.OutboxDead {
			result.resolutions = append(result.resolutions, resolution)
		} else if err == nil {
			result.raceLost++
		}
	}
	if err != nil {
		return result, fmt.Errorf("undelivered: mark dead: %w", err)
	}
	return result, nil
}

func (repair *StrandRepair) surveyUndelivered(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]undeliveredCandidate, int, error) {
	rows, err := repair.queryQueue(ctx, undeliveredSurveySQL,
		now.UTC().Add(-repair.undeliveredCeiling), limit)
	if err != nil || rows == nil {
		return nil, 0, classifyStrandError(err)
	}
	defer rows.Close()
	candidates := make([]undeliveredCandidate, 0, limit)
	blocked := 0
	scanned := 0
	for rows.Next() {
		var found undeliveredCandidate
		var blockedTotal int64
		if err := rows.Scan(&found.outboxID, &found.jobKind, &found.requestID,
			&found.reason, &blockedTotal); err != nil ||
			!uuidPattern.MatchString(found.outboxID) || found.jobKind == "" {
			return nil, 0, ErrUnavailable
		}
		scanned++
		blocked = int(blockedTotal)
		switch found.reason {
		case "blocked":
			continue
		case UndeliveredReasonPrerequisiteFailed, UndeliveredReasonPrerequisiteExpired,
			UndeliveredReasonRequestTerminal:
		case UndeliveredReasonDeliveryDead:
			if found.requestID == "" {
				return nil, blocked, fmt.Errorf("outbox %s (job_kind=%q): delivery_dead without a request: %w",
					found.outboxID, found.jobKind, ErrUnavailable)
			}
		default:
			return nil, blocked, fmt.Errorf("outbox %s (job_kind=%q): reason %q: %w",
				found.outboxID, found.jobKind, found.reason, ErrUnavailable)
		}
		if found.requestID != "" && !uuidPattern.MatchString(found.requestID) {
			return nil, blocked, ErrUnavailable
		}
		candidates = append(candidates, found)
	}
	rows.Close()
	if err := rows.Err(); err != nil || scanned > limit {
		return nil, blocked, classifyStrandError(rows.Err())
	}
	return candidates, blocked, nil
}

func (repair *StrandRepair) returningIDs(
	ctx context.Context,
	query func(context.Context, string, ...any) (pgx.Rows, error),
	statement string,
	args ...any,
) (map[string]bool, error) {
	rows, err := query(ctx, statement, args...)
	if err != nil || rows == nil {
		return nil, classifyStrandError(err)
	}
	defer rows.Close()
	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil || !uuidPattern.MatchString(id) {
			return nil, ErrUnavailable
		}
		ids[id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, classifyStrandError(err)
	}
	return ids, nil
}
