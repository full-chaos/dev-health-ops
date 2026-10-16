//go:build integration

package home

import (
	"context"
	"testing"
	"time"
)

// A work-item metric under a repository filter is the repo-linked calculation,
// but the team daily table that names its drivers has no repository dimension:
// the caption would name teams outside the filtered repository. Such a signal
// carries no caption; the same request without the repository filter does.
func TestHomeSignalDriverIsNotServedForARepoFilteredWorkItemMetric(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)

	const org = "signal-driver-repo-filter"
	from, to := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	seedOracleWorld(ctx, t, admin, org, func(oracleItem) bool { return true })
	fillDailyTables(ctx, t, admin, org, from, to)
	synced := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	for id, name := range map[string]string{"t-alpha": "Team Alpha", "t-beta": "Team Beta"} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], ?, ?, 'github', 1)`, id, name, synced, org); err != nil {
			t.Fatal(err)
		}
	}
	end := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	now := end.Add(12 * time.Hour)
	build := func(f Filters) *Response {
		t.Helper()
		f.Time = TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: &end}
		f.Scope = ScopeFilter{Level: "org"}
		resp, err := BuildResponse(ctx, client, nil, org, f, now)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	workItem := map[string]bool{"metric:cycle_time": true, "metric:throughput": true, "metric:wip_saturation": true, "metric:blocked_work": true}
	unfilteredCaptions, filteredSignals := 0, 0
	for _, s := range build(Filters{}).Signals {
		if workItem[s.ID] && s.Driver != nil {
			unfilteredCaptions++
			t.Logf("unfiltered %s: %s", s.ID, *s.Driver)
		}
	}
	if unfilteredCaptions == 0 {
		t.Fatal("no work-item signal carries a caption without the repository filter: the fixture proves nothing")
	}
	for _, s := range build(Filters{What: WhatFilter{Repos: []string{oracleR1}}}).Signals {
		if !workItem[s.ID] {
			continue
		}
		filteredSignals++
		if s.Driver != nil {
			t.Errorf("%s under a repository filter carries the caption %q (it names teams of the whole organization)", s.ID, *s.Driver)
		}
	}
	if filteredSignals == 0 {
		t.Fatal("no work-item signal under the repository filter: the fixture proves nothing")
	}
}
