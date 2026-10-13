//go:build integration

package teamsidentity

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// joinedOwnersByRepo is teamownership.AuthoritativeOwnersByRepo with each repo's owners
// joined by ",": one team for a repo with one owner, "a,b" for the main
// owners of a full ownership tie (D5990).
func joinedOwnersByRepo(ctx context.Context, conn driver.Conn, orgID string, asOf time.Time) (map[string]string, error) {
	owners, err := teamownership.AuthoritativeOwnersByRepo(ctx, conn, orgID, asOf)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(owners))
	for repoID, teamIDs := range owners {
		out[repoID] = strings.Join(teamIDs, ",")
	}
	return out, nil
}
