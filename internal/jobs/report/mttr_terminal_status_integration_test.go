//go:build integration

package report

import (
	"fmt"
	"testing"
	"time"
)

// insertBugWithStatus stores a bug work item with status, started at started and completed at completed.
func (e *pooledEnv) insertBugWithStatus(org, repo, id, status string, started, completed time.Time) {
	e.t.Helper()
	e.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, status, created_at, updated_at, started_at, completed_at, last_synced, org_id)
VALUES (?, ?, 'jira', 'bug', ?, ?, ?, ?, ?, '2026-02-01 00:00:00.000', ?)`,
		repo, id, status, started.Format(deliveryTimeFormat), completed.Format(deliveryTimeFormat), started.Format(deliveryTimeFormat), completed.Format(deliveryTimeFormat), org)
}

// A bug a provider reopened and that kept its completed_at is not repaired: MTTR counts a bug on the day of its
// completed_at only when its status is terminal (done or canceled). The stored mttr_hours (repo_user_commit writer), the
// pooled report chart and the operating review tile all read the same rule.
func TestMTTRCountsOnlyABugWithATerminalStatus(t *testing.T) {
	e := startPooledEnv(t)
	client := mttrTileClient(e)
	at := func(i int) time.Time { return deliveryDay1.Add(time.Duration(i+1) * 30 * time.Minute) }
	day := deliveryDay1.Format(time.DateOnly)
	open := []string{"in_progress", "todo", "backlog", "blocked", "in_review", "unknown", ""}

	// mixed: done 2 h and canceled 4 h are repairs; each open bug (100 h) is not. Want 3.
	orgMixed := e.newOrg()
	const repoMixed = "14154000-0000-4000-8000-0000000000a1"
	e.insertRepo(orgMixed, repoMixed)
	e.insertBugWithStatus(orgMixed, repoMixed, "done", "done", at(0), at(0).Add(2*time.Hour))
	e.insertBugWithStatus(orgMixed, repoMixed, "canceled", "canceled", at(1), at(1).Add(4*time.Hour))
	for i, status := range open {
		e.insertBugWithStatus(orgMixed, repoMixed, fmt.Sprintf("open-%d", i), status, at(2+i).Add(-100*time.Hour), at(2+i))
	}
	e.insertPullRequest(orgMixed, repoMixed, 1, at(0), 5, 5)
	e.computeDelivery(orgMixed, []string{repoMixed}, day)
	stored := e.storedDelivery(orgMixed, repoMixed, "repo_metrics_daily", "mttr_hours")
	pooled := e.deliveryChart(orgMixed, "mttr_hours", "", "scorecard", "2026-02-02", "2026-02-08")["total"]
	tile, hasTile := mttrTile(e, client, orgMixed)
	if !near(stored[day], 3) || !near(pooled, 3) || !hasTile || !near(tile, 3) {
		t.Errorf("mixed (done 2 h, canceled 4 h, %d open bugs of 100 h): stored %v, pooled %v, tile %v (hasData %v), want 3 on each",
			len(open), stored, pooled, tile, hasTile)
	}

	// only open: a single reopened bug that kept its completed_at. No repair: no stored MTTR, no pooled sample, no data.
	orgOpen := e.newOrg()
	const repoOpen = "14154000-0000-4000-8000-0000000000a2"
	e.insertRepo(orgOpen, repoOpen)
	e.insertBugWithStatus(orgOpen, repoOpen, "reopened", "in_progress", at(0), at(0).Add(12*time.Hour))
	e.insertPullRequest(orgOpen, repoOpen, 1, at(0), 5, 5)
	e.computeDelivery(orgOpen, []string{repoOpen}, day)
	storedOpen := e.storedDelivery(orgOpen, repoOpen, "repo_metrics_daily", "mttr_hours")
	tileOpen, hasOpen := mttrTile(e, client, orgOpen)
	if _, ok := storedOpen[day]; ok || hasOpen {
		t.Errorf("only open (in_progress, 12 h, completed_at kept): stored %v, tile %v (hasData %v), want no stored MTTR and no data",
			storedOpen, tileOpen, hasOpen)
	}
}
