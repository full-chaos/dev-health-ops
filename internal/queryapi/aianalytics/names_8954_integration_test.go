//go:build integration

package aianalytics

import (
	"testing"
)

const (
	nameRepo  = "33333333-3333-3333-3333-333333333333"
	nameRepo2 = "44444444-4444-4444-4444-444444444444"
	nameRepo3 = "55555555-5555-5555-5555-555555555555"
)

func TestRealClickHouse_PRTitleBindsAndReads(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, created_at, org_id, last_synced)
        SELECT '%s', 7, 'Add retry', now64(3), '%s', toDateTime64('2026-08-01 00:00:00', 3, 'UTC')`, nameRepo, org1)
	exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, created_at, org_id, last_synced)
        SELECT '%s', 7, 'Other org title', now64(3), '%s', toDateTime64('2026-08-02 00:00:00', 3, 'UTC')`, nameRepo, org2)
	// Two stored versions of #9 and of #10 in both write orders: the latest wins.
	exec(t, ctx, conn, `SYSTEM STOP MERGES git_pull_requests`)
	for _, r := range []struct {
		n     int
		title string
		at    string
	}{{9, "New nine", "2026-08-02"}, {9, "Old nine", "2026-08-01"}, {10, "Old ten", "2026-08-01"}, {10, "New ten", "2026-08-02"}} {
		exec(t, ctx, conn, `INSERT INTO git_pull_requests (repo_id, number, title, created_at, org_id, last_synced)
            SELECT '%s', %d, '%s', now64(3), '%s', toDateTime64('%s 00:00:00', 3, 'UTC')`, nameRepo, r.n, r.title, org1, r.at)
	}
	keys := []prKey{{repoID: nameRepo, number: 7}, {repoID: nameRepo, number: 8}, {repoID: nameRepo, number: 9}, {repoID: nameRepo, number: 10}}
	got := loadPRTitles(ctx, client, org1, keys, "test")
	want := map[prKey]string{keys[0]: "Add retry", keys[2]: "New nine", keys[3]: "New ten"}
	if len(got) != len(want) {
		t.Fatalf("titles = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("titles = %v, want %v", got, want)
		}
	}
}

func TestRealClickHouse_NamesReadLatestRow(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `SYSTEM STOP MERGES repos`)
	exec(t, ctx, conn, `SYSTEM STOP MERGES teams`)
	// The newest version is written first and has no name; the older version
	// written after it still carries one. A raw read serves the older name.
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
        SELECT '%s', '', 'github', '%s', now64(3), toDateTime64('2026-08-02 00:00:00', 3, 'UTC')`, nameRepo2, org1)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
        SELECT '%s', 'acme/stale', 'github', '%s', now64(3), toDateTime64('2026-08-01 00:00:00', 3, 'UTC')`, nameRepo2, org1)
	// The same pair in the other write order, so the raw read cannot pass by
	// part order alone.
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
        SELECT '%s', 'acme/stale3', 'github', '%s', now64(3), toDateTime64('2026-08-01 00:00:00', 3, 'UTC')`, nameRepo3, org1)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
        SELECT '%s', '', 'github', '%s', now64(3), toDateTime64('2026-08-02 00:00:00', 3, 'UTC')`, nameRepo3, org1)
	exec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, org_id, updated_at)
        SELECT 'team-n', generateUUIDv4(), '', [], ['x/*'], '%s', toDateTime64('2026-08-02 00:00:00', 6, 'UTC')`, org1)
	exec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, org_id, updated_at)
        SELECT 'team-n', generateUUIDv4(), 'Stale Team', [], ['x/*'], '%s', toDateTime64('2026-08-01 00:00:00', 6, 'UTC')`, org1)
	c := loadRepoCatalogue(ctx, client, org1, []string{nameRepo2, nameRepo3}, "test")
	if n := c.repoName(nameRepo2); n != nil {
		t.Fatalf("repoName = %q, want nil (latest row has no name)", *n)
	}
	if n := c.repoName(nameRepo3); n != nil {
		t.Fatalf("repoName (other write order) = %q, want nil", *n)
	}
	id := "team-n"
	if n := c.teamName(&id); n != nil {
		t.Fatalf("teamName = %q, want nil (latest row has no name)", *n)
	}
	only := loadTeamNamesOnly(ctx, client, org1, "test")
	if n := only.teamName(&id); n != nil {
		t.Fatalf("teamName (names-only read) = %q, want nil", *n)
	}
}

func TestRealClickHouse_LoadRepoNamesReadsLatestRow(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `SYSTEM STOP MERGES repos`)
	for _, r := range []struct{ id, name, at string }{
		{nameRepo2, "", "2026-08-02"}, {nameRepo2, "acme/stale", "2026-08-01"},
		{nameRepo3, "acme/stale3", "2026-08-01"}, {nameRepo3, "", "2026-08-02"},
	} {
		exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
            SELECT '%s', '%s', 'github', '%s', now64(3), toDateTime64('%s 00:00:00', 3, 'UTC')`, r.id, r.name, org1, r.at)
	}
	got, err := loadRepoNames(ctx, client, org1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("repos = %v, want one row per id", got)
	}
	for _, r := range got {
		if r.fullName != "" {
			t.Fatalf("repo %s name %q, want the latest (empty) row", r.id, r.fullName)
		}
	}
}
