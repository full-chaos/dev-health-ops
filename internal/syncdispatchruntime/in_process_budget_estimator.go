package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/syncbudget"
)

// InProcessBudgetEstimator estimates dispatch budgets in this process
// (CHAOS-6243). It replaces the HTTP call to the Python api's
// /api/internal/worker-sync/dispatch-budget-estimate: the same reference
// checks, the same per-unit bootstrap (unit, integration, source, dataset
// options, run-stamped credential, decryption) and the six provider
// estimators, ported in internal/syncbudget.
type InProcessBudgetEstimator struct {
	loader syncbudget.Loader
	logger *slog.Logger
}

// BudgetEstimatorDependencies are what the in-process estimator reads.
type BudgetEstimatorDependencies struct {
	// Pool is the domain pool: the role that already reads
	// integration_credentials for provider sync.
	Pool *pgxpool.Pool
	// Decryptor decrypts integration_credentials rows.
	Decryptor providerfoundation.CredentialDecryptor
	// Getenv is the process environment: environment credentials,
	// SYNC_RUN_AUTH_STRICT, PAGER_DUTY_CLIENT_ID and the Jira estimator's
	// flags, which Python read from the api process with the same shared
	// ConfigMap and Secret.
	Getenv func(string) string
	Logger *slog.Logger
	// PagerDutyOAuth and PagerDutyDoer hydrate PagerDuty OAuth and
	// client-credentials descriptors, as resolve_run_auth did, so a unit
	// whose hydration fails still gets no estimate.
	PagerDutyOAuth providerfoundation.CredentialHydrator
	PagerDutyDoer  providerfoundation.HTTPDoer
}

// NewInProcessBudgetEstimator builds the estimator. Every dependency is
// required.
func NewInProcessBudgetEstimator(dependencies BudgetEstimatorDependencies) (*InProcessBudgetEstimator, error) {
	if dependencies.Pool == nil || dependencies.PagerDutyOAuth == nil || dependencies.PagerDutyDoer == nil {
		return nil, ErrInvalidBridge
	}
	return newInProcessBudgetEstimator(dependencies.Pool, dependencies)
}

func newInProcessBudgetEstimator(db syncbudget.Querier, dependencies BudgetEstimatorDependencies) (*InProcessBudgetEstimator, error) {
	if db == nil || dependencies.Decryptor == nil || dependencies.Getenv == nil {
		return nil, ErrInvalidBridge
	}
	logger := dependencies.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &InProcessBudgetEstimator{
		loader: syncbudget.Loader{
			DB: db, Decryptor: dependencies.Decryptor, Getenv: dependencies.Getenv, Logger: logger,
			PagerDutyOAuth: dependencies.PagerDutyOAuth, PagerDutyDoer: dependencies.PagerDutyDoer,
		},
		logger: logger,
	}, nil
}

// DispatchBudgetEstimate returns each unit's estimates. A unit whose
// bootstrap or estimator fails gets no estimates (no budget constraint),
// and the failure is logged, as batch_estimate_provider_budget_for_units
// did. A refused reference wraps ErrBridgeContractRejected; a failed batch
// read wraps ErrBridgeRequest.
func (estimator *InProcessBudgetEstimator) DispatchBudgetEstimate(
	ctx context.Context, orgID, runID string, unitIDs []string,
) (map[string][]budgetEstimate, error) {
	if estimator == nil {
		return nil, ErrInvalidBridge
	}
	return estimator.DispatchBudgetEstimateOn(ctx, estimator.loader.DB, orgID, runID, unitIDs)
}

// DispatchBudgetEstimateOn is DispatchBudgetEstimate reading through db instead
// of the estimator's own pool: a Dispatch pass hands it its own transaction, so
// the whole pass costs ONE domain connection. Reading through the pool needed a
// second connection for a pass that already held one, which several concurrent
// passes on a small pool turned into a deadlock (CHAOS-6889).
func (estimator *InProcessBudgetEstimator) DispatchBudgetEstimateOn(
	ctx context.Context, db syncbudget.Querier, orgID, runID string, unitIDs []string,
) (map[string][]budgetEstimate, error) {
	if estimator == nil || db == nil || len(unitIDs) == 0 {
		return nil, ErrInvalidBridge
	}
	loader := estimator.loader
	loader.DB = db
	results, err := loader.EstimateUnits(ctx, orgID, runID, unitIDs)
	if err != nil {
		switch {
		case errors.Is(err, syncbudget.ErrStaleRun),
			errors.Is(err, syncbudget.ErrUnitsOutsideRun),
			errors.Is(err, syncbudget.ErrInvalidReference):
			return nil, fmt.Errorf("%w: %v", ErrBridgeContractRejected, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrBridgeRequest, err)
	}
	estimates := make(map[string][]budgetEstimate, len(results))
	for unitID, result := range results {
		if result.Err != nil {
			estimator.logger.WarnContext(ctx, "dispatch_sync_run.budget_estimate_unit_failed",
				slog.String("sync_run_id", runID),
				slog.String("unit_id", unitID),
				slog.String("error", result.Err.Error()))
		}
		converted := make([]budgetEstimate, len(result.Estimates))
		for index, estimate := range result.Estimates {
			converted[index] = budgetEstimate{
				Bucket: budgetEstimateBucket{
					Provider:              estimate.Bucket.Provider,
					OrgID:                 estimate.Bucket.OrgID,
					Host:                  estimate.Bucket.Host,
					CredentialFingerprint: estimate.Bucket.CredentialFingerprint,
					Dimension:             estimate.Bucket.Dimension,
				},
				EstimatedUnits: estimate.EstimatedUnits,
				Confidence:     estimate.Confidence,
				RouteFamily:    estimate.RouteFamily,
				Notes:          estimate.Notes,
			}
		}
		estimates[unitID] = converted
	}
	return estimates, nil
}
