package syncdispatchruntime

import (
	"context"
	"errors"
	"sort"
	"time"

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
	StartRunTx(context.Context, pgx.Tx, PostSyncPlan) error
}

type RemainingPostSyncWriter interface {
	StartRunTx(context.Context, pgx.Tx, string, PostSyncPlan) error
}

type WorkGraphInvestmentPostSyncWriter interface {
	StartRequestTx(context.Context, pgx.Tx, string, PostSyncPlan) error
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
	now        func() time.Time
}

func NewNativePostSyncService(
	pool *pgxpool.Pool,
	daily DailyPostSyncWriter,
	remaining RemainingPostSyncWriter,
	workGraph WorkGraphInvestmentPostSyncWriter,
	teamImport TeamAutoimportPostSyncWriter,
) (*NativePostSyncService, error) {
	if pool == nil || daily == nil || remaining == nil || workGraph == nil || teamImport == nil {
		return nil, ErrPostSyncUnavailable
	}
	return &NativePostSyncService{
		pool: pool, daily: daily, remaining: remaining, workGraph: workGraph,
		teamImport: teamImport, now: time.Now,
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
	if plan.Complexity {
		if err := service.remaining.StartRunTx(ctx, tx, "complexity", *plan); err != nil {
			return err
		}
	}
	if plan.Daily {
		if err := service.daily.StartRunTx(ctx, tx, *plan); err != nil {
			return err
		}
	}
	if plan.WorkGraph {
		if err := service.workGraph.StartRequestTx(ctx, tx, "workgraph.build", *plan); err != nil {
			return err
		}
	}
	if plan.Investment {
		if err := service.workGraph.StartRequestTx(ctx, tx, "investment.dispatch", *plan); err != nil {
			return err
		}
	}
	if plan.DORA {
		if err := service.remaining.StartRunTx(ctx, tx, "dora", *plan); err != nil {
			return err
		}
	}
	if plan.TeamAutoimport {
		if err := service.teamImport.PublishTx(ctx, tx, *plan); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrPostSyncUnavailable
	}
	return nil
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
SELECT COALESCE(sync_options->'auto_import_teams' = 'true'::jsonb, false)
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
		Daily: git || hasWorkItems, Complexity: git && currentSingleDay, DORA: dora,
		WorkGraph: git || hasWorkItems, Investment: git || hasWorkItems,
		TeamAutoimport: autoImport,
	}, nil
}

func utcDay(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func sameUTCDate(left, right time.Time) bool {
	left, right = left.UTC(), right.UTC()
	return left.Year() == right.Year() && left.YearDay() == right.YearDay()
}
