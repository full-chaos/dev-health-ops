package cicd

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// StoredKey is the newest stored row of one (repository, day) key of cicd_metrics_daily: its version and whether it
// is a measurement (retracted = 0).
type StoredKey struct {
	ComputedAt time.Time
	Live       bool
}

// storedKeysQuery reads the newest row of each key of the partition's repositories on one day.
const storedKeysQuery = `
SELECT repo_id, max(computed_at), argMax(retracted, computed_at)
FROM cicd_metrics_daily
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
GROUP BY repo_id`

// LoadStoredKeys returns the newest stored row of each key of repoIDs on day.
func (loader *ClickHouseLoader) LoadStoredKeys(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, day time.Time,
) (map[uuid.UUID]StoredKey, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("cicd: loader unavailable")
	}
	stored := map[uuid.UUID]StoredKey{}
	if len(repoIDs) == 0 {
		return stored, nil
	}
	rows, err := loader.conn.Query(ctx, storedKeysQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("day", day.UTC().Format(time.DateOnly)))
	if err != nil {
		return nil, fmt.Errorf("load stored cicd_metrics_daily keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var repoID uuid.UUID
		var computedAt time.Time
		var retracted uint8
		if err := rows.Scan(&repoID, &computedAt, &retracted); err != nil {
			return nil, fmt.Errorf("scan stored cicd_metrics_daily key: %w", err)
		}
		stored[repoID] = StoredKey{ComputedAt: computedAt.UTC(), Live: retracted == 0}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return stored, nil
}

// FenceDay returns the rows one compute of day writes, given the records it computed (stamped with snapshot, the
// instant of its source read) and the newest stored row of each key of its partition (CHAOS-14016):
//
//   - the write fence: a key whose stored row is not older than snapshot is left alone, so an older read written
//     last never replaces a newer count. computed_at is whole seconds, so a stored row of the same second is left
//     alone too; it stays until the next compute of the day in a later second.
//   - W1: a key whose newest stored row is a measurement and that this compute no longer produces (its runs moved to
//     another day, or were deleted) gets a retraction row: the key, snapshot, retracted = 1 and no measure. A key
//     with no stored row gets nothing.
func FenceDay(day time.Time, records []CICDMetric, stored map[uuid.UUID]StoredKey, snapshot time.Time) []CICDMetric {
	snapshot = snapshot.UTC()
	newer := func(repoID uuid.UUID) bool {
		key, ok := stored[repoID]
		return ok && !key.ComputedAt.Before(snapshot)
	}
	produced := make(map[uuid.UUID]struct{}, len(records))
	out := make([]CICDMetric, 0, len(records))
	for _, record := range records {
		produced[record.RepoID] = struct{}{}
		if !newer(record.RepoID) {
			out = append(out, record)
		}
	}
	retired := make([]uuid.UUID, 0)
	for repoID, key := range stored {
		if _, ok := produced[repoID]; ok || !key.Live || newer(repoID) {
			continue
		}
		retired = append(retired, repoID)
	}
	sort.Slice(retired, func(i, j int) bool { return retired[i].String() < retired[j].String() })
	for _, repoID := range retired {
		out = append(out, CICDMetric{RepoID: repoID, Day: day, ComputedAt: snapshot, Retracted: true})
	}
	return out
}
