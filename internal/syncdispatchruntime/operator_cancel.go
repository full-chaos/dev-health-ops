package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// operatorCancelErrorCategory is the error_category an operator cancel writes into every unit,
// run, ledger and observer result. It is one of the closed source-health stage codes, so a
// cancelled run reads as cancelled, not as "other".
const operatorCancelErrorCategory = string(jobruntime.CategoryCancelled)

// The outcomes CancelSyncRun reports.
const (
	CancelOutcomeCancelled       = "cancelled"
	CancelOutcomeAlreadyTerminal = "already_terminal"
	CancelOutcomeWouldCancel     = "would_cancel"
)

var (
	ErrCancelSyncRunInvalid     = errors.New("sync run cancel request is invalid")
	ErrSyncRunNotFound          = errors.New("sync run not found in the organization")
	ErrSyncRunHasRunningUnit    = errors.New("sync run has a running unit with a live lease")
	ErrSyncRunNotTerminal       = errors.New("sync run stayed open after the unit write; nothing was written")
	ErrSyncRunCancelUnavailable = errors.New("sync run cancel could not run")
)

// CancelSyncRunRequest names one unfinished sync run of one organization.
type CancelSyncRunRequest struct {
	OrgID     string
	SyncRunID string
	// ReasonCode is the operator's bounded reason code; it goes into the error text.
	ReasonCode string
	// AllowRunning lets the cancel fail a RUNNING unit whose lease is still live. Its worker then
	// loses the lease (renew and assert require a non-terminal run) and stops.
	AllowRunning bool
	DryRun       bool
	Now          time.Time
}

// CancelSyncRunUnits counts the run's units by status before the cancel.
type CancelSyncRunUnits struct {
	Planned        int `json:"planned"`
	Retrying       int `json:"retrying"`
	Dispatching    int `json:"dispatching"`
	RunningExpired int `json:"running_expired"`
	RunningLeased  int `json:"running_leased"`
	Success        int `json:"success"`
	Failed         int `json:"failed"`
	Other          int `json:"other"`
}

// CancelSyncRunDeliveries counts the provider-unit deliveries (worker_job_outbox rows) of the units
// the cancel failed. The cancel leaves them: a job of a failed unit of a terminal run finds the unit
// unclaimable and ends as a no-op.
type CancelSyncRunDeliveries struct {
	Delivered int `json:"delivered"`
	Pending   int `json:"pending"`
}

// CancelSyncRunOutcome is what CancelSyncRun did. UnitsFailed and ProviderUnitDeliveries count only
// the units the cancel's own writes failed.
type CancelSyncRunOutcome struct {
	SyncRunID              string                  `json:"sync_run_id"`
	Outcome                string                  `json:"outcome"`
	PriorStatus            string                  `json:"prior_status"`
	Units                  CancelSyncRunUnits      `json:"units"`
	UnitsFailed            int                     `json:"units_failed"`
	ProviderUnitDeliveries CancelSyncRunDeliveries `json:"provider_unit_deliveries"`
}

// CancelSyncRun closes one unfinished sync run the way the feature-disabled terminator does, in
// one transaction on the domain pool: bucket locks, every open unit failed, the run failed, the
// reference-discovery ledger, the dispatch outbox and finalize row, and the backfill_jobs and
// job_runs observers. A run that is still open after the unit write (a running unit kept or
// reclaimed in between) is rolled back whole. A run that is terminal at the first read, or under the
// run lock (a finalizer committed in between), is left alone: already_terminal.
func CancelSyncRun(ctx context.Context, pool *pgxpool.Pool, request CancelSyncRunRequest) (CancelSyncRunOutcome, error) {
	runID, err := uuid.Parse(request.SyncRunID)
	if pool == nil || ctx == nil || err != nil || request.OrgID == "" || request.Now.IsZero() ||
		(!request.DryRun && request.ReasonCode == "") {
		return CancelSyncRunOutcome{}, ErrCancelSyncRunInvalid
	}
	// Postgres keeps microseconds: the lease reads and writes compare against the same instant.
	now := request.Now.UTC().Truncate(time.Microsecond)
	options := pgx.TxOptions{}
	if request.DryRun {
		options.AccessMode = pgx.ReadOnly
	}
	tx, err := pool.BeginTx(ctx, options)
	if err != nil {
		return CancelSyncRunOutcome{}, ErrSyncRunCancelUnavailable
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return CancelSyncRunOutcome{}, ErrSyncRunCancelUnavailable
	}

	run, err := loadFinalizeRun(ctx, tx, request.OrgID, runID.String())
	if err != nil {
		return CancelSyncRunOutcome{}, ErrSyncRunCancelUnavailable
	}
	if run == nil {
		return CancelSyncRunOutcome{}, ErrSyncRunNotFound
	}
	outcome := CancelSyncRunOutcome{SyncRunID: run.id, PriorStatus: run.status}
	if outcome.Units, err = countCancelUnits(ctx, tx, run, now); err != nil {
		return CancelSyncRunOutcome{}, ErrSyncRunCancelUnavailable
	}
	if terminalSyncRunStatus(run.status) {
		outcome.Outcome = CancelOutcomeAlreadyTerminal
		return outcome, nil
	}
	if outcome.Units.RunningLeased > 0 && !request.AllowRunning {
		return outcome, ErrSyncRunHasRunningUnit
	}
	if request.DryRun {
		outcome.Outcome = CancelOutcomeWouldCancel
		return outcome, nil
	}

	message := fmt.Sprintf("%s: sync run cancelled by the operator (reason %s)", operatorCancelErrorCategory, request.ReasonCode)
	termination := runTermination{errorCategory: operatorCancelErrorCategory, operator: true, allowRunning: request.AllowRunning}
	transition, err := terminalizeRun(ctx, tx, run, termination, message, now)
	if errors.Is(err, errRunTerminalUnderLock) {
		outcome.PriorStatus = run.status
		outcome.Outcome = CancelOutcomeAlreadyTerminal
		return outcome, nil
	}
	if err != nil {
		return CancelSyncRunOutcome{}, fmt.Errorf("%w: %w", ErrSyncRunCancelUnavailable, err)
	}
	if !transition.RunTerminal {
		if !request.AllowRunning && transition.RunningUnits > 0 {
			return outcome, ErrSyncRunHasRunningUnit
		}
		return outcome, ErrSyncRunNotTerminal
	}
	if err := terminalizeRunGraph(ctx, tx, run, operatorCancelErrorCategory, message, *run.completedAt); err != nil {
		return CancelSyncRunOutcome{}, fmt.Errorf("%w: %w", ErrSyncRunCancelUnavailable, err)
	}
	outcome.UnitsFailed = len(transition.operatorUnitIDs)
	if outcome.ProviderUnitDeliveries, err = cancelledUnitDeliveries(ctx, tx, transition.operatorUnitIDs); err != nil {
		return CancelSyncRunOutcome{}, ErrSyncRunCancelUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return CancelSyncRunOutcome{}, ErrSyncRunCancelUnavailable
	}
	outcome.Outcome = CancelOutcomeCancelled
	return outcome, nil
}

func terminalSyncRunStatus(status string) bool {
	return status == syncRunStatusSuccess || status == syncRunStatusPartialFailed || status == syncRunStatusFailed
}

// countCancelUnits counts the run's units by status. A running unit's lease is live when it has
// no expiry or one after now: claimUnitSQL recovers only an expired one.
func countCancelUnits(ctx context.Context, tx pgx.Tx, run *finalizeSyncRun, now time.Time) (CancelSyncRunUnits, error) {
	rows, err := tx.Query(ctx, `
SELECT status, (status = 'running' AND (lease_expires_at IS NULL OR lease_expires_at > $3)) AS leased, count(*)
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND org_id = $2
GROUP BY 1, 2`, run.id, run.orgID, now)
	if err != nil {
		return CancelSyncRunUnits{}, err
	}
	defer rows.Close()
	var units CancelSyncRunUnits
	for rows.Next() {
		var status string
		var leased bool
		var count int
		if err := rows.Scan(&status, &leased, &count); err != nil {
			return CancelSyncRunUnits{}, err
		}
		switch {
		case status == "planned":
			units.Planned += count
		case status == "retrying":
			units.Retrying += count
		case status == "dispatching":
			units.Dispatching += count
		case status == "running" && leased:
			units.RunningLeased += count
		case status == "running":
			units.RunningExpired += count
		case status == syncRunUnitStatusSuccess:
			units.Success += count
		case status == syncRunUnitStatusFailed:
			units.Failed += count
		default:
			units.Other += count
		}
	}
	return units, rows.Err()
}

// ProviderUnitDeliveryKeys is every dedupe key a provider-unit delivery of one unit can carry: the
// dispatcher's original key and each reclaim generation the orphaned-unit repair mints
// (syncreconciler providerUnitCandidateKeys, orphaned_unit_repair.go:705; a cell pins the two equal).
func ProviderUnitDeliveryKeys(unitID string) []string {
	base := providerUnitDeliveryKind + ":" + unitID
	keys := []string{base}
	for generation := 1; generation <= providerUnitDeliveryReclaims; generation++ {
		keys = append(keys, fmt.Sprintf("%s/reclaim/%d", base, generation))
	}
	return keys
}

const (
	providerUnitDeliveryKind     = "sync.provider_unit"
	providerUnitDeliveryReclaims = 3
)

// cancelledUnitDeliveries counts the deliveries of the units the cancel's own writes failed, under
// every dedupe form: delivered, and not yet delivered (pending or claimed).
func cancelledUnitDeliveries(ctx context.Context, tx pgx.Tx, unitIDs []string) (CancelSyncRunDeliveries, error) {
	if len(unitIDs) == 0 {
		return CancelSyncRunDeliveries{}, nil
	}
	keys := make([]string, 0, len(unitIDs)*(providerUnitDeliveryReclaims+1))
	for _, id := range unitIDs {
		keys = append(keys, ProviderUnitDeliveryKeys(id)...)
	}
	var deliveries CancelSyncRunDeliveries
	if err := tx.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'delivered'), count(*) FILTER (WHERE status IN ('pending', 'claimed'))
FROM public.worker_job_outbox
WHERE job_kind = $1 AND dedupe_key = ANY($2::text[])`, providerUnitDeliveryKind, keys).Scan(&deliveries.Delivered, &deliveries.Pending); err != nil {
		return CancelSyncRunDeliveries{}, err
	}
	return deliveries, nil
}
