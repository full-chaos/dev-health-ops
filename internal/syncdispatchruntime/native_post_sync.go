package syncdispatchruntime

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrPostSyncUnavailable = errors.New("native post-sync fanout is unavailable")

type PostSyncPlan struct {
	OrganizationID string
	SyncRunID      string
	TargetDay      time.Time
	BackfillDays   int
	RepositoryIDs  []string
	From           *time.Time
	To             *time.Time
	Daily          bool
	Complexity     bool
	DORA           bool
	WorkGraph      bool
	Investment     bool
	TeamAutoimport bool
}

type DailyPostSyncWriter interface {
	StartRunTx(context.Context, pgx.Tx, PostSyncPlan, string) (string, error)
}

type RemainingPostSyncWriter interface {
	StartRunTx(context.Context, pgx.Tx, string, PostSyncPlan, string) (string, error)
}

type WorkGraphInvestmentPostSyncWriter interface {
	StartRequestTx(context.Context, pgx.Tx, string, PostSyncPlan, string) (string, error)
}

type TeamAutoimportPostSyncWriter interface {
	PublishTx(context.Context, pgx.Tx, PostSyncPlan) error
}

type NativePostSyncService struct {
	pool       *pgxpool.Pool
	daily      DailyPostSyncWriter
	remaining  RemainingPostSyncWriter
	workGraph  WorkGraphInvestmentPostSyncWriter
	teamImport TeamAutoimportPostSyncWriter
	logger     *slog.Logger
	now        func() time.Time
}

// NewNativePostSyncService constructs the fanout. logger receives the
// best-effort team-autoimport failures Fanout deliberately does not propagate;
// a nil logger falls back to slog.Default() so the swallow is never silent.
func NewNativePostSyncService(
	pool *pgxpool.Pool,
	daily DailyPostSyncWriter,
	remaining RemainingPostSyncWriter,
	workGraph WorkGraphInvestmentPostSyncWriter,
	teamImport TeamAutoimportPostSyncWriter,
	logger *slog.Logger,
) (*NativePostSyncService, error) {
	if pool == nil || daily == nil || remaining == nil || workGraph == nil || teamImport == nil {
		return nil, ErrPostSyncUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &NativePostSyncService{
		pool: pool, daily: daily, remaining: remaining, workGraph: workGraph,
		teamImport: teamImport, logger: logger, now: time.Now,
	}, nil
}

// Fanout validates the exact River transport generation, reconstructs scope
// from authoritative SyncRun state, and stages every child in one transaction.
func (service *NativePostSyncService) Fanout(ctx context.Context, args PostSyncArgs) error {
	if service == nil || service.pool == nil || ctx == nil || args.valid() != nil {
		return ErrPostSyncUnavailable
	}
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return ErrPostSyncUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := currentPostSyncReference(ctx, tx, args)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}
	plan, err := loadPostSyncPlan(ctx, tx, args, service.now().UTC())
	if err != nil {
		return err
	}
	if plan == nil {
		return tx.Commit(ctx)
	}
	var orderedCompletion string
	if plan.Complexity {
		orderedCompletion, err = service.remaining.StartRunTx(
			ctx, tx, "complexity", *plan, orderedCompletion,
		)
		if err != nil {
			return err
		}
	}
	if plan.Daily {
		orderedCompletion, err = service.daily.StartRunTx(
			ctx, tx, *plan, orderedCompletion,
		)
		if err != nil {
			return err
		}
	}
	if plan.WorkGraph {
		orderedCompletion, err = service.workGraph.StartRequestTx(
			ctx, tx, "workgraph.build", *plan, orderedCompletion,
		)
		if err != nil {
			return err
		}
	}
	if plan.Investment {
		orderedCompletion, err = service.workGraph.StartRequestTx(
			ctx, tx, "investment.materialize", *plan, orderedCompletion,
		)
		if err != nil {
			return err
		}
		_, err = service.remaining.StartRunTx(
			ctx, tx, "membership_backfill", *plan, orderedCompletion,
		)
		if err != nil {
			return err
		}
	}
	if plan.DORA {
		if _, err := service.remaining.StartRunTx(ctx, tx, "dora", *plan, ""); err != nil {
			return err
		}
	}
	if err := service.publishTeamAutoimport(ctx, tx, *plan); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrPostSyncUnavailable
	}
	return nil
}

// publishTeamAutoimport stages the team-autoimport handoff without letting a
// verdict that can never change take the metric fanout down with it.
//
// src/dev_health_ops/workers/post_sync_dispatch.py:285-302 states the contract:
// "Best-effort: a dispatch failure must never break post-sync metric fan-out."
// Python dispatches this as a SEPARATE credential-resolving task, deliberately
// outside the metric fanout, because a work-RELATIONSHIP refresh (teams,
// projects, members) and work-UNIT metric work have different lifecycles. The
// Go port published it inside the metric transaction as the last statement
// before Commit and returned its error raw, so one permanently-rejected publish
// discarded the complexity run, the daily dispatch, the workgraph build, the
// investment materialize, the membership backfill and the DORA run with it, on
// every sync, for every organization with auto_import_teams enabled
// (CHAOS-3946).
//
// Only a DETERMINISTIC rejection is swallowed. An outbox policy or contract
// rejection is a verdict about the checked-in contract and the envelope shape:
// re-running the fanout reaches it again, so propagating it would only rebuild
// the same transaction to throw it away again. Everything else -- a savepoint
// that cannot be opened, a transient Postgres failure, a failed release -- is
// returned, because the fanout is duplicate-stable (proved by
// TestNativePostSyncFanoutIsDuplicateStableAndRollsBackWholeGeneration) and a
// retry re-stages all six handoffs AND gives this one another chance.
// Swallowing those would trade a recoverable blip for a permanently missing
// refresh whose only trace is a log line.
//
// The publish runs inside its own SAVEPOINT. Swallowing alone is not enough: a
// rejection that had already issued SQL leaves the enclosing transaction
// aborted, so the following Commit would fail and the metric fanout would be
// lost anyway -- silently, which is worse than the loud failure this ticket
// reported. Rolling back to the savepoint returns the enclosing transaction to
// a committable state.
//
// A failed RELEASE SAVEPOINT is deliberately NOT followed by a rollback of the
// nested transaction: pgx sets tx.closed before returning the error (tx.go:191)
// and closes the underlying connection when the transaction status is not idle,
// so that rollback would be a no-op returning ErrTxClosed while reading as
// recovery. The error is returned instead and the outer Commit is skipped.
//
// Team autoimport takes no prerequisite completion key and produces none, so
// nothing else in the fanout depends on it. The ordered chain
// (complexity -> daily -> workgraph -> investment -> membership) keeps its
// all-or-nothing transaction: those handoffs DO pass each other's durable
// completion fences.
func (service *NativePostSyncService) publishTeamAutoimport(
	ctx context.Context,
	tx pgx.Tx,
	plan PostSyncPlan,
) error {
	if !plan.TeamAutoimport {
		return nil
	}
	nested, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	if err := service.teamImport.PublishTx(ctx, nested, plan); err != nil {
		if rollbackErr := nested.Rollback(ctx); rollbackErr != nil {
			return err
		}
		if !deterministicHandoffRejection(err) {
			return err
		}
		service.observeTeamAutoimportFailure(ctx, plan, err)
		return nil
	}
	return nested.Commit(ctx)
}

// deterministicHandoffRejection reports whether the outbox refused the envelope
// on a rule that a later attempt of the same fanout would apply identically:
// the kind's registered route, its contract version, and the envelope's own
// shape. Availability and transport failures are excluded -- those a retry can
// clear.
func deterministicHandoffRejection(err error) bool {
	return errors.Is(err, joboutbox.ErrPolicyRejected) ||
		errors.Is(err, joboutbox.ErrContractRejected)
}

// PostSyncTeamAutoimportFailedMessage is the log message emitted when the
// best-effort team-autoimport handoff is dropped. It is a stable identifier so
// operators (and tests) can find the drops the fanout deliberately survives.
const PostSyncTeamAutoimportFailedMessage = "post_sync_team_autoimport_handoff_dropped"

func (service *NativePostSyncService) observeTeamAutoimportFailure(
	ctx context.Context,
	plan PostSyncPlan,
	err error,
) {
	logger := service.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.ErrorContext(ctx, PostSyncTeamAutoimportFailedMessage,
		slog.String("org_id", plan.OrganizationID),
		slog.String("sync_run_id", plan.SyncRunID),
		slog.String("error", err.Error()),
	)
}

func currentPostSyncReference(ctx context.Context, tx pgx.Tx, args PostSyncArgs) (bool, error) {
	var current bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM public.sync_dispatch_outbox AS outbox
    JOIN public.sync_dispatch_transport_routes AS route
      ON route.kind = outbox.kind
    WHERE outbox.id = $1::uuid
      AND outbox.sync_run_id = $2::uuid
      AND outbox.org_id = $3
      AND outbox.kind = 'post_sync'
      AND outbox.status = 'dispatched'
      AND outbox.dispatched_transport = 'river'
      AND outbox.dispatched_route_generation = $4
      AND route.transport = 'river'
      AND route.generation = $4
      AND route.paused = false
)`, args.OutboxID(), args.SyncRunID(), args.OrganizationID(), args.RouteGeneration()).Scan(&current)
	if err != nil {
		return false, ErrPostSyncUnavailable
	}
	return current, nil
}

func loadPostSyncPlan(
	ctx context.Context,
	tx pgx.Tx,
	args PostSyncArgs,
	now time.Time,
) (*PostSyncPlan, error) {
	var orgID, integrationID string
	err := tx.QueryRow(ctx, `
SELECT org_id, integration_id::text
FROM public.sync_runs
WHERE id = $1::uuid AND org_id = $2
FOR SHARE`, args.SyncRunID(), args.OrganizationID()).Scan(&orgID, &integrationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrPostSyncUnavailable
	}

	rows, err := tx.Query(ctx, `
SELECT provider, dataset_key, source_id::text, since_at, before_at
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND status = 'success'
ORDER BY id`, args.SyncRunID())
	if err != nil {
		return nil, ErrPostSyncUnavailable
	}
	defer rows.Close()
	var (
		targets        = map[string]struct{}{}
		repositories   = map[string]struct{}{}
		from           *time.Time
		to             *time.Time
		unboundedFrom  bool
		unboundedTo    bool
		successfulUnit bool
	)
	for rows.Next() {
		var provider, dataset, sourceID string
		var since, before *time.Time
		if err := rows.Scan(&provider, &dataset, &sourceID, &since, &before); err != nil {
			return nil, ErrPostSyncUnavailable
		}
		capability, ok := providersync.Capability(provider, dataset)
		if !ok {
			continue
		}
		successfulUnit = true
		unitGit := false
		for _, target := range capability.LegacyTargets {
			targets[target] = struct{}{}
			if target == "git" || target == "prs" {
				unitGit = true
			}
		}
		if unitGit {
			repositories[sourceID] = struct{}{}
		}
		if since == nil {
			unboundedFrom = true
		} else if from == nil || since.Before(*from) {
			value := since.UTC()
			from = &value
		}
		if before == nil {
			unboundedTo = true
		} else if to == nil || before.After(*to) {
			value := before.UTC()
			to = &value
		}
	}
	if rows.Err() != nil {
		return nil, ErrPostSyncUnavailable
	}
	if !successfulUnit || len(targets) == 0 {
		return nil, nil
	}
	if unboundedFrom {
		from = nil
	}
	if unboundedTo {
		to = nil
	}
	autoImport := false
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(sync_options->>'auto_import_teams' = 'true', false)
FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at, id
LIMIT 1`, orgID, integrationID).Scan(&autoImport); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPostSyncUnavailable
	}

	_, hasGit := targets["git"]
	_, hasPRs := targets["prs"]
	_, hasWorkItems := targets["work-items"]
	_, hasDeployments := targets["deployments"]
	_, hasCICD := targets["cicd"]
	_, hasIncidents := targets["incidents"]
	_, hasOperational := targets["operational"]
	git := hasGit || hasPRs
	dora := git || hasDeployments || hasCICD || hasIncidents || hasOperational
	dailyRelevant := dailyMetricsTrigger(git, hasWorkItems, hasCICD, hasDeployments, hasIncidents)
	targetDay := now
	backfillDays := 1
	if to != nil {
		targetDay = *to
	}
	if from != nil && to != nil {
		fromDay := utcDay(*from)
		toDay := utcDay(*to)
		backfillDays = int(toDay.Sub(fromDay)/(24*time.Hour)) + 1
		if backfillDays < 1 {
			backfillDays = 1
		}
	}
	currentSingleDay := (from == nil && to == nil) ||
		(from != nil && to != nil && sameUTCDate(*from, *to) && sameUTCDate(*to, now))
	repositoryIDs := make([]string, 0, len(repositories))
	for id := range repositories {
		repositoryIDs = append(repositoryIDs, id)
	}
	sort.Strings(repositoryIDs)
	return &PostSyncPlan{
		OrganizationID: orgID, SyncRunID: args.SyncRunID(), TargetDay: targetDay,
		BackfillDays: backfillDays, RepositoryIDs: repositoryIDs, From: from, To: to,
		Daily: dailyRelevant, Complexity: git && currentSingleDay, DORA: dora,
		WorkGraph: git || hasWorkItems, Investment: git || hasWorkItems,
		TeamAutoimport: autoImport,
	}, nil
}

// dailyMetricsTrigger decides whether a completed sync should re-trigger
// metrics.daily_partition for the organization (CHAOS-4246).
//
// Before this change the condition was git-only (git || hasWorkItems): a
// sync that synced ONLY cicd/deployments/incidents data never re-triggered
// the daily-metrics partition for the day(s) that data just landed in. A
// day's cicd/deploy/incident families could be computed once -- by an
// earlier git/work-item-triggered run, before that day's CI/deploy/incident
// sync had caught up -- and then never recomputed: the partition still
// reports succeeded, so nothing surfaces the staleness (measured in prod:
// cicd_metrics_daily/deploy_metrics_daily/testops_release_confidence/
// testops_pipeline_stability went to zero rows for every day after
// 2026-08-18 despite ci_pipeline_runs/deployments staying fresh).
//
// Registering cicd/deployments/incidents here closes that gap. This is safe
// to re-drive: cicd_metrics_daily, deploy_metrics_daily,
// incident_metrics_daily, testops_release_confidence, and
// testops_pipeline_stability are all registered in
// clickhouse_dedup._APPEND_ONLY_DAILY_KEYS (src/dev_health_ops/
// clickhouse_dedup.py) and api/queries/metrics.py's _DEDUP_BY_COMPUTED_AT,
// so every known reader collapses re-drive generations to the latest
// computed_at instead of double-counting.
func dailyMetricsTrigger(git, hasWorkItems, hasCICD, hasDeployments, hasIncidents bool) bool {
	return git || hasWorkItems || hasCICD || hasDeployments || hasIncidents
}

func utcDay(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func sameUTCDate(left, right time.Time) bool {
	left, right = left.UTC(), right.UTC()
	return left.Year() == right.Year() && left.YearDay() == right.YearDay()
}
