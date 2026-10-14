package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Bounds of the status retry (CHAOS-9678). A deployment whose status is still
// empty is looked up again by every run, outside the claim window too, so a
// status that a rate-limited or failed lookup left out is stored by a later
// run. A deployment that has no status at all (GitHub keeps none for a
// deployment that nothing reported on) would be looked up for ever, so the
// retry covers the recent deployments only and at most this many per run.
const (
	deploymentStatusRetryHorizon = 30 * 24 * time.Hour
	deploymentStatusRetryMax     = 100
)

// GitHubDeploymentStatusHolds reads the deployments of one repository that
// have no stored status. It is the route's only read of the store: the route
// itself holds no connection.
type GitHubDeploymentStatusHolds interface {
	// DeploymentsWithoutStatus returns the ids of the repository's stored
	// deployments with no status that were deployed at or after since, newest
	// first, at most limit of them.
	DeploymentsWithoutStatus(ctx context.Context, orgID, repoID string, since time.Time, limit int) ([]string, error)
}

// GitHubDeploymentStatusHoldsClickHouse reads the held deployments from the
// deployments table, newest row per key (FINAL), behind the unit's lease.
type GitHubDeploymentStatusHoldsClickHouse struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (holds GitHubDeploymentStatusHoldsClickHouse) DeploymentsWithoutStatus(ctx context.Context, orgID, repoID string, since time.Time, limit int) ([]string, error) {
	if ctx == nil || holds.Conn == nil || holds.Lease == nil || orgID == "" || repoID == "" || limit < 1 {
		return nil, ErrInvalidConfiguration
	}
	if err := holds.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	rows, err := holds.Conn.Query(ctx, `SELECT deployment_id FROM deployments FINAL WHERE org_id = ? AND repo_id = ? AND (status IS NULL OR status = '') AND deployed_at >= ? ORDER BY deployed_at DESC LIMIT ?`, orgID, repoID, since.UTC(), uint64(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

var _ GitHubDeploymentStatusHolds = GitHubDeploymentStatusHoldsClickHouse{}
