// Postgres reader for the freshness panel's latest-successful-sync
// timestamp -- ports fetch_latest_successful_sync_at (api/queries/
// sync_freshness.py:12-39): the later of the most recent successful
// sync_runs row and the most recent successful job_runs row whose
// scheduled_jobs.job_type = 'sync', both scoped to org_id.
//
// Status values are the registry schema's own stored literals, not
// re-derived: sync_runs.status is a Text column storing SyncRunStatus's
// str value ('success' -- src/dev_health_ops/models/integrations.py);
// job_runs.status is an Integer column storing JobRunStatus's int value
// (2 -- src/dev_health_ops/models/settings.py).
package home

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const jobRunStatusSuccess = 2

const latestSuccessfulSyncSQL = `
SELECT max(completed_at) FROM (
    SELECT completed_at
    FROM sync_runs
    WHERE org_id = $1
      AND status = 'success'
      AND completed_at IS NOT NULL

    UNION ALL

    SELECT job_runs.completed_at
    FROM job_runs
    JOIN scheduled_jobs ON scheduled_jobs.id = job_runs.job_id
    WHERE scheduled_jobs.org_id = $1
      AND scheduled_jobs.job_type = 'sync'
      AND job_runs.status = $2
      AND job_runs.completed_at IS NOT NULL
) AS successful_runs
`

// FetchLatestSuccessfulSyncAt ports fetch_latest_successful_sync_at.
// nil, nil on no matching row, matching Python's scalar_one_or_none()
// returning None.
func FetchLatestSuccessfulSyncAt(ctx context.Context, pool PGQueryClient, orgID string) (*time.Time, error) {
	var latest *time.Time
	err := pool.QueryRow(ctx, latestSuccessfulSyncSQL, orgID, jobRunStatusSuccess).Scan(&latest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("home: fetch_latest_successful_sync_at: %w", err)
	}
	if latest != nil {
		utc := latest.UTC()
		latest = &utc
	}
	return latest, nil
}
