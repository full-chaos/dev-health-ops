package daily

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"log/slog"
	"sort"
)

// RepositoryDiscoveryLogMessage is the one line the discoverer writes for each
// successful discovery. It says how many partition members the daily fan-out
// gets and whether one of them is the nil repository id (the work items that
// have no repository). Without it, "the daily job did not visit the items with
// no repository" and "the organization has no such item" look the same from
// outside. The attributes are an id, a flag and counts only.
const RepositoryDiscoveryLogMessage = "daily metrics repository discovery"

// repositoryRows is the narrow ClickHouse capability used by the scheduled
// daily fan-out. Keeping the adapter on this one method makes it impossible for
// the scheduler producer to gain a remote-read dependency by accident.
type repositoryRows interface {
	Query(context.Context, string, ...any) (driver.Rows, error)
}

// ClickHouseRepositoryDiscoverer reads the current repository identity set for
// one organization. It is owned by the heavy worker, after a durable scheduler
// run exists; it is never constructed by the scheduler process.
type ClickHouseRepositoryDiscoverer struct{ conn repositoryRows }

func NewClickHouseRepositoryDiscoverer(conn repositoryRows) (*ClickHouseRepositoryDiscoverer, error) {
	if conn == nil {
		return nil, ErrUnavailable
	}
	return &ClickHouseRepositoryDiscoverer{conn: conn}, nil
}

func (discoverer *ClickHouseRepositoryDiscoverer) RepositoryIDs(ctx context.Context, organizationID string) ([]RepositoryID, error) {
	if discoverer == nil || discoverer.conn == nil || !validUUID(organizationID) {
		return nil, ErrInvalidState
	}
	rows, err := discoverer.conn.Query(ctx, `
SELECT id
FROM (
  SELECT id, argMax(tuple(repo, settings, provider), last_synced) AS latest
  FROM repos
  WHERE org_id = ?
  GROUP BY org_id, id
)
ORDER BY id`, organizationID)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	identifiers := make([]RepositoryID, 0, 64)
	for rows.Next() {
		var repositoryID uuid.UUID
		if err := rows.Scan(&repositoryID); err != nil {
			return nil, ErrUnavailable
		}
		identifiers = append(identifiers, RepositoryID(repositoryID.String()))
	}
	if err := rows.Err(); err != nil {
		return nil, ErrUnavailable
	}
	// A work item with no repository (linear, jira, any external or internal
	// ingest provider that is not github or gitlab) is stored under the nil
	// repository id, which no repos row has. Without this partition member no
	// daily family ever reads those items (CHAOS-8821). The decision is a read
	// of the stored work items, never a list of provider names, and an
	// organization with no such item gets no extra id.
	hasNil, err := discoverer.hasWorkItemsWithoutRepository(ctx, organizationID)
	if err != nil {
		return nil, err
	}
	repositories := len(identifiers)
	if hasNil {
		identifiers = append(identifiers, RepositoryID(uuid.Nil.String()))
		sort.Slice(identifiers, func(left, right int) bool { return identifiers[left] < identifiers[right] })
	}
	slog.InfoContext(ctx, RepositoryDiscoveryLogMessage,
		"organization_id", organizationID,
		"no_repository_partition_added", hasNil,
		"repositories_discovered", repositories,
		"partitions_discovered", len(identifiers),
	)
	return identifiers, nil
}

func (discoverer *ClickHouseRepositoryDiscoverer) hasWorkItemsWithoutRepository(
	ctx context.Context, organizationID string,
) (bool, error) {
	rows, err := discoverer.conn.Query(ctx,
		`SELECT 1 FROM work_items WHERE org_id = ? AND repo_id = ? LIMIT 1`, organizationID, uuid.Nil)
	if err != nil {
		return false, ErrUnavailable
	}
	defer rows.Close()
	found := rows.Next()
	if err := rows.Err(); err != nil {
		return false, ErrUnavailable
	}
	return found, nil
}

var _ RepositoryDiscoverer = (*ClickHouseRepositoryDiscoverer)(nil)

// repositoryIDStrings converts to the plain []string clickhouse-go's
// Array(String) named-parameter binding is verified against.
func repositoryIDStrings(ids []RepositoryID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = string(id)
	}
	return result
}
