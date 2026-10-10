package daily

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// LoadRepoProviders reads the provider of each of the given repositories: the
// input that says whether a repository has a changes-requested event at all
// (package prrework). repos keeps the newest row per repository; a repository
// with no row is absent from the map, and the caller treats it as a provider
// with no signal.
func LoadRepoProviders(ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	providers := make(map[uuid.UUID]string, len(repoIDs))
	if len(repoIDs) == 0 {
		return providers, nil
	}
	rows, err := conn.Query(ctx, `
SELECT id, argMax(provider, last_synced) AS provider
FROM repos
WHERE org_id = ? AND id IN ?
GROUP BY id`, organizationID, repositoryUUIDStrings(repoIDs))
	if err != nil {
		return nil, fmt.Errorf("load repo providers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var provider string
		if err := rows.Scan(&id, &provider); err != nil {
			return nil, fmt.Errorf("scan repo provider: %w", err)
		}
		providers[id] = provider
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repo providers: %w", err)
	}
	return providers, nil
}
