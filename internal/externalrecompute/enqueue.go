package externalrecompute

// enqueue.go is the production Enqueuer: it turns a bounded plan into the SAME
// native handoffs the scheduled fan-out and post-sync fanout publish
// (cmd/dev-health-worker/sync_dispatch.go), namely metrics.daily_dispatch via
// daily.PostgresStore.StartRunTx and investment.materialize via
// workgraph.RequestWriter.WriteRequestTx.
//
// Those two are the native equivalents of the two Celery task names the deleted
// Python planner dispatched by string (run_daily_metrics and
// dispatch_investment_materialize_partitioned), which is what keeps this port
// plumbing rather than a second compute path.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// enqueueNamespace derives this producer's investment request ids. It
// is distinct from postSyncFanoutNamespace and the workerctl trigger
// namespaces for the reason stated there: each producer must derive request ids
// from its own namespace so none can ever collide with another's.
var enqueueNamespace = uuid.MustParse("b7c41d92-3e58-4f6a-9d21-8c5e0a7f3b46")

type PostgresEnqueuer struct {
	dailyStore     *daily.PostgresStore
	dailyPublisher *daily.PostgresPublisher
	workGraph      *workgraph.RequestWriter
}

// NewPostgresEnqueuer builds the production enqueue seam. It lives in this
// package rather than in either binary because BOTH the worker's drain
// component and workerctl's one-shot replay must enqueue identically -- two
// copies of this logic in two `package main`s is exactly the kind of pair that
// drifts, and a drifted replay would write work shaped differently from the
// work the live drain writes.
func NewPostgresEnqueuer(
	dailyStore *daily.PostgresStore,
	dailyPublisher *daily.PostgresPublisher,
	workGraph *workgraph.RequestWriter,
) (PostgresEnqueuer, error) {
	if dailyStore == nil || dailyPublisher == nil || workGraph == nil {
		return PostgresEnqueuer{}, ErrInvalidConfig
	}
	return PostgresEnqueuer{
		dailyStore: dailyStore, dailyPublisher: dailyPublisher, workGraph: workGraph,
	}, nil
}

// Enqueue turns one bounded plan into native handoffs inside the drain's
// transaction. correlation is "ext-recompute:<bridge id>" -- it doubles as the
// daily generation, so it must stay deterministic per bridge row: a lease
// reclaim re-running this must land on StartRunTx's own ON CONFLICT DO NOTHING
// and the outbox's dedupe key rather than starting a second run for the day.
func (enqueuer PostgresEnqueuer) Enqueue(
	ctx context.Context,
	tx pgx.Tx,
	plan Plan,
	correlation string,
) (Enqueued, error) {
	if enqueuer.dailyStore == nil || enqueuer.dailyPublisher == nil || enqueuer.workGraph == nil {
		return Enqueued{}, ErrInvalidConfig
	}
	var enqueued Enqueued

	if plan.DispatchDaily && (len(plan.RepoIDs) > 0 || plan.FallbackOrgWideDaily) {
		// RepositoryIDs empty is not an omission here: it IS the D8 org-wide
		// fallback. An empty scope makes StartRunTx defer repository discovery
		// to the heavy worker's live ClickHouse read, which is exactly the
		// "day-bounded, all repositories" behaviour the Python planner got by
		// calling run_daily_metrics with repo_id=None.
		repositoryIDs := make([]daily.RepositoryID, 0, len(plan.RepoIDs))
		for _, repoID := range plan.RepoIDs {
			repositoryIDs = append(repositoryIDs, daily.RepositoryID(repoID))
		}
		for _, day := range plan.DailyTargetDays() {
			run, err := enqueuer.dailyStore.StartRunTx(ctx, tx, daily.StartRunRequest{
				OrganizationID: plan.OrgID,
				TargetDay:      day,
				Generation:     correlation,
				RepositoryIDs:  repositoryIDs,
			}, enqueuer.dailyPublisher)
			if err != nil {
				return Enqueued{}, classifyEnqueueError(
					fmt.Errorf("start daily metrics run for %s: %w", day.Format(time.DateOnly), err))
			}
			enqueued.DailyRunIDs = append(enqueued.DailyRunIDs, run.ID)
		}
	}

	if !plan.SkipInvestmentNoScope {
		requestID := uuid.NewSHA1(enqueueNamespace, []byte(correlation+":investment")).String()
		scope, err := materializeScope(plan)
		if err != nil {
			return Enqueued{}, err
		}
		outcome, err := enqueuer.workGraph.WriteRequestTx(ctx, tx, workgraph.Request{
			ID:             requestID,
			OrganizationID: plan.OrgID,
			Kind:           workgraph.KindMaterialize,
			Scope:          scope,
			LLMConcurrency: 1,
			// Coalesce is safe for THIS producer specifically: the request it
			// writes has no PrerequisiteCompletionKey and nothing downstream
			// fences on its completion, so superseding a queued one strands
			// no chain. It is also the producer the backlog came from -- one
			// request per flush, 2206 of them covering 65 distinct days,
			// every duplicate naming a day and scope another already named.
			Coalesce: true,
			// force stays false and the spend limit stays 0, matching the
			// Python planner's `force=False` and the post-sync producer's own
			// choices: an external-ingest recompute is a freshness pass, never
			// an operator-authorised full rematerialize.
			SpendLimitMicrounits: 0,
			CorrelationID:        correlation,
			IdempotencyKey:       correlation + ":" + investmentIdempotencySuffix,
		})
		if err != nil {
			return Enqueued{}, classifyEnqueueError(
				fmt.Errorf("write investment materialize request: %w", err))
		}
		enqueued.InvestmentRequestID = requestID
		enqueued.InvestmentSupersededRequestIDs = outcome.SupersededRequestIDs
		enqueued.InvestmentBoundedFromDate = outcome.BoundedFromDate
		enqueued.InvestmentBoundedWindowDays = outcome.BoundedWindowDays
	}
	return enqueued, nil
}

const investmentIdempotencySuffix = "investment.materialize"

// materializeScope builds the materialize scope from the plan.
//
// from_date/to_date are DATE-only strings, not the ISO datetimes the Python
// planner passed as Celery kwargs: the native executor parses both with
// time.DateOnly (internal/jobs/investment/nativeexecutor.go) and REFUSES
// anything else, so an ISO datetime here would fail every request at execution
// time rather than at enqueue time.
func materializeScope(plan Plan) ([]byte, error) {
	scope := map[string]any{"force": false}
	if !plan.FromDate.IsZero() {
		scope["from_date"] = plan.FromDate.UTC().Format(time.DateOnly)
	}
	if !plan.ToDate.IsZero() {
		scope["to_date"] = plan.ToDate.UTC().Format(time.DateOnly)
	}
	if len(plan.RepoIDs) > 0 {
		scope["repo_ids"] = plan.RepoIDs
	}
	if len(plan.TeamIDs) > 0 {
		scope["team_ids"] = plan.TeamIDs
	}
	encoded, err := json.Marshal(scope)
	if err != nil {
		return nil, fmt.Errorf("%w: encode materialize scope: %w", ErrPermanent, err)
	}
	return encoded, nil
}

// classifyEnqueueError separates "this row can never succeed" from
// "try again later". Without it, a batch carrying an organization id the job
// contract refuses would be reclaimed by the stale lease every five minutes
// forever, logging the same rejection each time and never becoming visible as a
// terminal failure (the CHAOS-3903 shape: a permanent rejection must degrade
// loudly, not retry silently).
func classifyEnqueueError(err error) error {
	switch {
	case errors.Is(err, daily.ErrInvalidState),
		errors.Is(err, workgraph.ErrInvalidState),
		errors.Is(err, joboutbox.ErrContractRejected),
		errors.Is(err, joboutbox.ErrPolicyRejected),
		errors.Is(err, ErrPermanent):
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	default:
		return err
	}
}
