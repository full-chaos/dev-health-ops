package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const maximumGitHubBlameCoveragePaths = 100_000

// GitHubBlameClickHouseCoverage reads the durable git_blame path set that the
// active Python producer uses as its incremental-progress marker. The full
// tenant key is mandatory because repository UUIDs are stable across tenants.
type GitHubBlameClickHouseCoverage struct {
	Conn     driver.Conn
	Lease    providerfoundation.LeaseGuard
	MaxPaths int
}

func (coverage GitHubBlameClickHouseCoverage) BlamedPaths(
	ctx context.Context,
	claim Claim,
	repoID string,
) ([]string, error) {
	if ctx == nil || coverage.Conn == nil || coverage.Lease == nil ||
		claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "blame" || repoID == "" {
		return nil, ErrInvalidConfiguration
	}
	if err := coverage.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	maxPaths := coverage.MaxPaths
	if maxPaths == 0 {
		maxPaths = maximumGitHubBlameCoveragePaths
	}
	if maxPaths < 1 || maxPaths > maximumGitHubBlameCoveragePaths {
		return nil, ErrInvalidConfiguration
	}
	rows, err := coverage.Conn.Query(ctx, `
SELECT DISTINCT path
FROM git_blame FINAL
WHERE org_id = ? AND repo_id = ?
LIMIT ?`, claim.OrgID, repoID, maxPaths+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	paths := make([]string, 0)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		if path == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		paths = append(paths, path)
		if len(paths) > maxPaths {
			return nil, ErrGitHubBlameProgressUnavailable
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return paths, nil
}

var _ GitHubBlameCoverage = GitHubBlameClickHouseCoverage{}
