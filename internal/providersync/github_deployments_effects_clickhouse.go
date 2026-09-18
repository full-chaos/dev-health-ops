package providersync

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type GitHubDeploymentsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

// GitLabDeploymentsClickHouseEffects applies the same deployment-row
// persistence/readback contract without adding a provider column to the
// shared deployments table. Its fixed provider binding prevents a caller from
// using this sink for an unrelated provider just because the row shape matches.
type GitLabDeploymentsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type deploymentsClickHouseEffects struct {
	Conn     driver.Conn
	Lease    providerfoundation.LeaseGuard
	Provider string
}

func (sink GitHubDeploymentsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	return sink.shared().WriteEffect(ctx, claim, effect)
}

func (sink GitHubDeploymentsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	return sink.shared().InspectEffect(ctx, claim, effect)
}

func (sink GitHubDeploymentsClickHouseEffects) shared() deploymentsClickHouseEffects {
	return deploymentsClickHouseEffects{Conn: sink.Conn, Lease: sink.Lease, Provider: "github"}
}

func (sink GitLabDeploymentsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	return sink.shared().WriteEffect(ctx, claim, effect)
}

func (sink GitLabDeploymentsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	return sink.shared().InspectEffect(ctx, claim, effect)
}

func (sink GitLabDeploymentsClickHouseEffects) shared() deploymentsClickHouseEffects {
	return deploymentsClickHouseEffects{Conn: sink.Conn, Lease: sink.Lease, Provider: "gitlab"}
}

func (sink deploymentsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || (sink.Provider != "github" && sink.Provider != "gitlab") || claim.Validate() != nil || claim.Provider != sink.Provider || claim.Dataset != "deployments" || effect.Destination != "deployments" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[deploymentRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := row.validate(claim); err != nil {
			return err
		}
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	stored, storedErr := loadStoredDeploymentLifecycle(ctx, sink.Conn, claim.OrgID, rows)
	if storedErr != nil {
		slog.Warn("providersync.deployment.lifecycle_regression_guard_read_failed", "org_id", claim.OrgID, "provider", claim.Provider, "unit_id", claim.ID, "cause", storedErr.Error())
	} else {
		logDeploymentLifecycleRegressionGuarded(ctx, claim, guardDeploymentLifecycleRegressions(rows, stored))
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO deployments (repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.RepoID, row.DeploymentID, row.Status, row.Environment, row.StartedAt, row.FinishedAt, row.DeployedAt, row.MergedAt, nullableUInt32(row.PullRequestNumber), row.ReleaseRef, row.ReleaseRefConfidence, row.OrgID, row.LastSynced); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink deploymentsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || (sink.Provider != "github" && sink.Provider != "gitlab") || claim.Validate() != nil || claim.Provider != sink.Provider || claim.Dataset != "deployments" || effect.Destination != "deployments" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[deploymentRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	// A row this batch could not honestly derive a lifecycle for
	// (LifecycleLookupFailed, decoded straight from the original effect
	// bytes -- the field survives JSON round-tripping) had its
	// started_at/finished_at/status carried forward from the stored row at
	// WriteEffect time (guardDeploymentLifecycleRegressions above). Recovery
	// rebuilds `expected` from a fresh Collect() pass that knows nothing of
	// that carry-forward, so without applying the identical correction here
	// too, `expected` still shows the pre-guard nils while the actually
	// persisted row shows the carried-forward values -- a real row, freshly
	// re-verified, would read as EffectConflict (ErrEffectRecoveryAmbiguous)
	// purely from this mismatch, never from any genuine data divergence. A
	// guard-read failure here is non-fatal, the same as WriteEffect's own
	// choice: `expected` is compared uncorrected rather than failing the
	// whole inspection outright.
	if stored, storedErr := loadStoredDeploymentLifecycle(ctx, sink.Conn, claim.OrgID, expected); storedErr != nil {
		slog.Warn("providersync.deployment.lifecycle_regression_guard_read_failed", "org_id", claim.OrgID, "provider", claim.Provider, "unit_id", claim.ID, "cause", storedErr.Error())
	} else {
		guardDeploymentLifecycleRegressions(expected, stored)
	}
	exact, absent := 0, 0
	for _, row := range expected {
		inspection, err := sink.inspectDeployment(ctx, row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink deploymentsClickHouseEffects) inspectDeployment(ctx context.Context, expected deploymentRow) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `SELECT repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced FROM deployments FINAL WHERE org_id = ? AND repo_id = ? AND deployment_id = ?`, expected.OrgID, expected.RepoID, expected.DeploymentID)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var (
		actual            deploymentRow
		pullRequestNumber *uint32
	)
	found := false
	for rows.Next() {
		if err := rows.Scan(&actual.RepoID, &actual.DeploymentID, &actual.Status, &actual.Environment, &actual.StartedAt, &actual.FinishedAt, &actual.DeployedAt, &actual.MergedAt, &pullRequestNumber, &actual.ReleaseRef, &actual.ReleaseRefConfidence, &actual.OrgID, &actual.LastSynced); err != nil {
			return EffectConflict, err
		}
		actual.PullRequestNumber = uint32PointerAsInt(pullRequestNumber)
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareDeploymentVersion(expected, actual, found), nil
}

func compareDeploymentVersion(expected, actual deploymentRow, found bool) EffectInspection {
	if !found || actual.LastSynced.IsZero() {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) {
		return EffectConflict
	}
	if actual.RepoID != expected.RepoID || actual.DeploymentID != expected.DeploymentID || actual.OrgID != expected.OrgID || !stringPointersEqual(actual.Status, expected.Status) || !stringPointersEqual(actual.Environment, expected.Environment) || !timePointersEqual(actual.StartedAt, expected.StartedAt) || !timePointersEqual(actual.FinishedAt, expected.FinishedAt) || !timePointersEqual(actual.DeployedAt, expected.DeployedAt) || !timePointersEqual(actual.MergedAt, expected.MergedAt) || !intPointersEqual(actual.PullRequestNumber, expected.PullRequestNumber) || actual.ReleaseRef != expected.ReleaseRef || actual.ReleaseRefConfidence != expected.ReleaseRefConfidence {
		return EffectConflict
	}
	return EffectExact
}

func intPointersEqual(left, right *int) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func uint32PointerAsInt(value *uint32) *int {
	if value == nil {
		return nil
	}
	converted := int(*value)
	return &converted
}

func nullableUInt32(value *int) *uint32 {
	if value == nil {
		return nil
	}
	converted := uint32(*value)
	return &converted
}

// deploymentLifecycleGuardKey is the lifecycle regression guard's own
// lookup key. org_id is applied once as the query's WHERE clause, not per
// key, because row.validate already requires every row in a batch to
// share claim.OrgID.
type deploymentLifecycleGuardKey struct {
	RepoID       string
	DeploymentID string
}

type storedDeploymentLifecycle struct {
	StartedAt  *time.Time
	FinishedAt *time.Time
	Status     *string
}

// deploymentLifecycleCarriedForward is one row where a stored started_at/
// finished_at/status was carried forward over a nil this pass's statuses
// lookup produced by failing (never over an honest, successful-but-empty
// nil). It carries only the row's own key -- never environment or any
// other row content -- the same minimal shape
// pullRequestMergedAtRegressionGuarded already uses for its own write-once
// guard.
type deploymentLifecycleCarriedForward struct {
	RepoID       string
	DeploymentID string
}

// loadStoredDeploymentLifecycle is the guard's one extra read per
// WriteEffect call -- not per row, and not issued at all when no row in
// the batch has LifecycleLookupFailed set. It is a single FINAL point
// lookup scoped with IN(...) to exactly the deployment ids this pass
// could not honestly derive a lifecycle for.
func loadStoredDeploymentLifecycle(
	ctx context.Context, conn driver.Conn, orgID string, rows []deploymentRow,
) (map[deploymentLifecycleGuardKey]storedDeploymentLifecycle, error) {
	repoIDs := make([]string, 0, len(rows))
	deploymentIDs := make([]string, 0, len(rows))
	seenRepo := make(map[string]bool, len(rows))
	seenDeployment := make(map[string]bool, len(rows))
	for _, row := range rows {
		if !row.LifecycleLookupFailed {
			continue
		}
		if !seenRepo[row.RepoID] {
			seenRepo[row.RepoID] = true
			repoIDs = append(repoIDs, row.RepoID)
		}
		if !seenDeployment[row.DeploymentID] {
			seenDeployment[row.DeploymentID] = true
			deploymentIDs = append(deploymentIDs, row.DeploymentID)
		}
	}
	if len(deploymentIDs) == 0 {
		return nil, nil
	}
	dbRows, err := conn.Query(ctx, `
SELECT repo_id, deployment_id, started_at, finished_at, status
FROM deployments FINAL
WHERE org_id = ? AND repo_id IN (?) AND deployment_id IN (?)`,
		orgID, repoIDs, deploymentIDs,
	)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()
	stored := make(map[deploymentLifecycleGuardKey]storedDeploymentLifecycle, len(deploymentIDs))
	for dbRows.Next() {
		var repoID, deploymentID string
		var startedAt, finishedAt *time.Time
		var status *string
		if err := dbRows.Scan(&repoID, &deploymentID, &startedAt, &finishedAt, &status); err != nil {
			return nil, err
		}
		stored[deploymentLifecycleGuardKey{RepoID: repoID, DeploymentID: deploymentID}] = storedDeploymentLifecycle{StartedAt: startedAt, FinishedAt: finishedAt, Status: status}
	}
	return stored, dbRows.Err()
}

// guardDeploymentLifecycleRegressions is the pure decision at the center
// of the write-once lifecycle invariant, deliberately separated from the
// ClickHouse call above so it can be exercised without a live connection
// (the same split guardPullRequestMergedAtRegressions uses for
// git_pull_requests' own write-once merged_at guard). A row is only ever
// touched when LifecycleLookupFailed marks its nil as a FAILURE, never a
// successful-but-empty lookup -- that nil is the honest value the ticket's
// own class ruling requires, and carrying a stale value over it would
// contradict "never a copied value." A key absent from `stored` (a
// brand-new deployment, or the guard's own read failing upstream) is
// never carried forward because there is nothing yet to protect. Not a
// transactional compare-and-swap, the same limitation
// guardPullRequestMergedAtRegressions documents: the read above and the
// INSERT that follows are two separate statements with no lock between
// them.
func guardDeploymentLifecycleRegressions(
	rows []deploymentRow, stored map[deploymentLifecycleGuardKey]storedDeploymentLifecycle,
) []deploymentLifecycleCarriedForward {
	var carried []deploymentLifecycleCarriedForward
	for i, row := range rows {
		if !row.LifecycleLookupFailed {
			continue
		}
		prior, ok := stored[deploymentLifecycleGuardKey{RepoID: row.RepoID, DeploymentID: row.DeploymentID}]
		if !ok || (prior.StartedAt == nil && prior.FinishedAt == nil && prior.Status == nil) {
			continue
		}
		rows[i].StartedAt = prior.StartedAt
		rows[i].FinishedAt = prior.FinishedAt
		rows[i].Status = prior.Status
		carried = append(carried, deploymentLifecycleCarriedForward{RepoID: row.RepoID, DeploymentID: row.DeploymentID})
	}
	return carried
}

// deploymentLifecycleRegressionGuardedEvent is this guard's stable log
// event name, the same substitute-counter convention
// pullRequestMergedAtRegressionGuardedEvent already uses.
const deploymentLifecycleRegressionGuardedEvent = "providersync.deployment.lifecycle_regression_guarded"

// logDeploymentLifecycleRegressionGuarded emits one WARN line per carried
// row, never a batch aggregate -- this is the one place in the whole
// system where the stored value and the incoming nil are both in the same
// process at the same time.
func logDeploymentLifecycleRegressionGuarded(
	ctx context.Context, claim Claim, carried []deploymentLifecycleCarriedForward,
) {
	for _, event := range carried {
		slog.Default().LogAttrs(ctx, slog.LevelWarn, deploymentLifecycleRegressionGuardedEvent,
			slog.String("org_id", claim.OrgID),
			slog.String("provider", claim.Provider),
			slog.String("dataset", claim.Dataset),
			slog.String("unit_id", claim.ID),
			slog.String("repo_id", event.RepoID),
			slog.String("deployment_id", event.DeploymentID),
		)
	}
}

var _ EffectSink = GitHubDeploymentsClickHouseEffects{}
var _ EffectReadback = GitHubDeploymentsClickHouseEffects{}
var _ EffectSink = GitLabDeploymentsClickHouseEffects{}
var _ EffectReadback = GitLabDeploymentsClickHouseEffects{}
