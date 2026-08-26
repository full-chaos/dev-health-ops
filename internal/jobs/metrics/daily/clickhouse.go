package daily

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

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
	return identifiers, nil
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
