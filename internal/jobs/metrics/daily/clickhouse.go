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

// RepositoryRowsNotDiscoveredLogMessage is the one line a whole-organization
// run writes, when its repositories are discovered, about the stored rows it
// cannot discover. The repos table is the only source of such a run's
// repositories, so rows stored under an id with no repos row of the
// organization (a writer that stored them before or without that row, a repos
// row that was removed) are computed by no such run. The run computes what it
// discovered; this line is the only sign of the rest. It is INFO with the
// three counts when all are 0, and WARN when one is above 0 or the count
// could not be read.
const RepositoryRowsNotDiscoveredLogMessage = "daily metrics repository discovery: stored rows under a repository with no repos row"

// The sources the count of not-discovered repositories reads, in the order of
// the log fields.
const (
	notDiscoveredPullRequests = "git_pull_requests"
	notDiscoveredCommits      = "git_commits"
	notDiscoveredWorkItems    = "work_items"
)

// notDiscoveredRepositoriesSQL counts, for each source table, the repository
// ids that hold rows of the organization and have no repos row of exactly
// that organization id: the ids the statement above cannot return. The nil
// repository id is not one of them (it is added by its own rule).
const notDiscoveredRepositoriesSQL = `
SELECT source, toUInt64(count()) AS repository_ids
FROM (
  SELECT 'git_pull_requests' AS source, repo_id FROM git_pull_requests WHERE org_id = ? GROUP BY repo_id
  UNION ALL
  SELECT 'git_commits' AS source, repo_id FROM git_commits WHERE org_id = ? GROUP BY repo_id
  UNION ALL
  SELECT 'work_items' AS source, repo_id FROM work_items WHERE org_id = ? AND repo_id != ? GROUP BY repo_id
)
WHERE repo_id NOT IN (SELECT id FROM repos WHERE org_id = ?)
GROUP BY source`

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

// ReportRepositoriesNotDiscovered says, in one line, how many repository ids
// hold stored rows that the discovery cannot reach. It is NOT part of
// RepositoryIDs: that read has other callers (the end of a run reads the
// present repositories once for each table it settles), and the count is a
// scan of three source tables that one run needs once. The dispatch of a run
// calls it after the one discovery it stores.
//
// It changes nothing the run computes and returns nothing, so it cannot fail
// the run: a count that could not be read is said in the same line
// (count_read false, WARN, no number), never taken as 0.
func (discoverer *ClickHouseRepositoryDiscoverer) ReportRepositoriesNotDiscovered(ctx context.Context, organizationID string) {
	if discoverer == nil || discoverer.conn == nil || !validUUID(organizationID) {
		return
	}
	counts, err := discoverer.repositoriesNotDiscovered(ctx, organizationID)
	if err != nil {
		slog.WarnContext(ctx, RepositoryRowsNotDiscoveredLogMessage,
			"organization_id", organizationID,
			"count_read", false,
		)
		return
	}
	level := slog.LevelInfo
	if counts[notDiscoveredPullRequests]+counts[notDiscoveredCommits]+counts[notDiscoveredWorkItems] > 0 {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, RepositoryRowsNotDiscoveredLogMessage,
		"organization_id", organizationID,
		"count_read", true,
		"repositories_with_pull_requests", counts[notDiscoveredPullRequests],
		"repositories_with_commits", counts[notDiscoveredCommits],
		"repositories_with_work_items", counts[notDiscoveredWorkItems],
	)
}

func (discoverer *ClickHouseRepositoryDiscoverer) repositoriesNotDiscovered(
	ctx context.Context, organizationID string,
) (map[string]uint64, error) {
	rows, err := discoverer.conn.Query(ctx, notDiscoveredRepositoriesSQL,
		organizationID, organizationID, organizationID, uuid.Nil, organizationID)
	if err != nil {
		return nil, ErrUnavailable
	}
	if rows == nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	counts := map[string]uint64{}
	for rows.Next() {
		var source string
		var repositories uint64
		if err := rows.Scan(&source, &repositories); err != nil {
			return nil, ErrUnavailable
		}
		switch source {
		case notDiscoveredPullRequests, notDiscoveredCommits, notDiscoveredWorkItems:
			counts[source] = repositories
		default:
			return nil, ErrUnavailable
		}
	}
	if err := rows.Err(); err != nil {
		return nil, ErrUnavailable
	}
	return counts, nil
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
var _ RepositoriesNotDiscoveredReporter = (*ClickHouseRepositoryDiscoverer)(nil)

// repositoryIDStrings converts to the plain []string clickhouse-go's
// Array(String) named-parameter binding is verified against.
func repositoryIDStrings(ids []RepositoryID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = string(id)
	}
	return result
}
