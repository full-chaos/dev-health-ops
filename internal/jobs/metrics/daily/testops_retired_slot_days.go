package daily

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// RetiredSlotHistoryDays is the history window of the daily recurrence: day X reads the days [X-29, X) (ComputeFamily of
// TestopsTestExecutor and of the testops_risk executor). A report slot retired on day D changes the stored recurrence of the
// days D+1 .. D+29 that were computed before the retirement; the touched-day record recomputes only D.
const RetiredSlotHistoryDays = 29

// RetiredSlotRepoDaysSQL selects the repository-days of every run that holds a retired report slot: a test_suite_results key
// whose newest version is a retraction row (retracted = 1), with the retraction's day in [from, to]. It names no provider: every
// writer of retraction rows is selected the same way. The day is the touched-day expression (recordTouchedTestsSQL), and the
// outer read takes every day of such a run, so a run whose suites span two days names both. The non-key filters sit outside
// the FINAL read, so they test the newest version of each key: a slot revived at the same key is not selected.
const RetiredSlotRepoDaysSQL = `SELECT toString(repo_id) AS repo, toDate(coalesce(started_at, finished_at), 'UTC') AS day
FROM test_suite_results
WHERE org_id = {org_id:String} AND coalesce(started_at, finished_at) IS NOT NULL
  AND (repo_id, run_id) IN (
      SELECT repo_id, run_id
      FROM (SELECT repo_id, run_id, retracted, started_at, finished_at FROM test_suite_results FINAL WHERE org_id = {org_id:String})
      WHERE retracted = 1
        AND toDate(coalesce(started_at, finished_at), 'UTC') BETWEEN {day_from:Date} AND {day_to:Date}
        AND (empty({repo_ids:Array(String)}) OR has({repo_ids:Array(String)}, toString(repo_id))))
GROUP BY repo, day
ORDER BY repo, day
LIMIT {row_cap:UInt32}`

// RetiredSlotRepoDay is one repository-day of a run with a retired report slot.
type RetiredSlotRepoDay struct {
	RepoID string
	Day    time.Time
}

// LoadRetiredSlotRepoDays runs RetiredSlotRepoDaysSQL for the organization, at most limit rows.
func LoadRetiredSlotRepoDays(
	ctx context.Context, conn driver.Conn, organizationID string, from, to time.Time, repoIDs []string, limit int,
) ([]RetiredSlotRepoDay, error) {
	if repoIDs == nil {
		repoIDs = []string{}
	}
	rows, err := conn.Query(ctx, RetiredSlotRepoDaysSQL,
		clickhouse.Named("org_id", organizationID), clickhouse.Named("day_from", from.UTC().Format(time.DateOnly)),
		clickhouse.Named("day_to", to.UTC().Format(time.DateOnly)), clickhouse.Named("repo_ids", repoIDs), clickhouse.Named("row_cap", uint32(limit)))
	if err != nil {
		return nil, fmt.Errorf("read the retired-slot repository days: %w", err)
	}
	defer rows.Close()
	var out []RetiredSlotRepoDay
	for rows.Next() {
		var row RetiredSlotRepoDay
		if err := rows.Scan(&row.RepoID, &row.Day); err != nil {
			return nil, fmt.Errorf("scan a retired-slot repository day: %w", err)
		}
		row.Day = time.Date(row.Day.Year(), row.Day.Month(), row.Day.Day(), 0, 0, 0, 0, time.UTC)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the retired-slot repository days: %w", err)
	}
	return out, nil
}

// RetiredSlotRecomputeDay is one day to compute again and the repositories to compute it for.
type RetiredSlotRecomputeDay struct {
	Day     time.Time
	RepoIDs []RepositoryID
}

// RetiredSlotRecomputeDays expands each repository-day D to the days D .. min(D+RetiredSlotHistoryDays, through) of its
// repository, and groups them by day: the days, ascending, each with its repositories, sorted. D itself is in the set, so a
// lost touched-day run of D is repaired too.
func RetiredSlotRecomputeDays(pairs []RetiredSlotRepoDay, through time.Time) []RetiredSlotRecomputeDay {
	through = time.Date(through.Year(), through.Month(), through.Day(), 0, 0, 0, 0, time.UTC)
	byDay := map[time.Time]map[RepositoryID]struct{}{}
	for _, pair := range pairs {
		for offset := 0; offset <= RetiredSlotHistoryDays; offset++ {
			day := pair.Day.AddDate(0, 0, offset)
			if day.After(through) {
				break
			}
			if byDay[day] == nil {
				byDay[day] = map[RepositoryID]struct{}{}
			}
			byDay[day][RepositoryID(pair.RepoID)] = struct{}{}
		}
	}
	out := make([]RetiredSlotRecomputeDay, 0, len(byDay))
	for day, repos := range byDay {
		item := RetiredSlotRecomputeDay{Day: day}
		for repo := range repos {
			item.RepoIDs = append(item.RepoIDs, repo)
		}
		sort.Slice(item.RepoIDs, func(i, j int) bool { return item.RepoIDs[i] < item.RepoIDs[j] })
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Day.Before(out[j].Day) })
	return out
}

// RetiredSlotRepoDayCount is the number of repository-days the recompute days hold.
func RetiredSlotRepoDayCount(days []RetiredSlotRecomputeDay) int {
	total := 0
	for _, day := range days {
		total += len(day.RepoIDs)
	}
	return total
}
