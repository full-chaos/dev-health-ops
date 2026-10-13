//go:build integration

package teamscope_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestOwnerSharesWeighByTheStoredLinkedShare pins the rule of record in team-attribution.md section 0.4d (chris
// 2026-10-12, D5988, D5990): one main owner for the whole history, every lesser owner keeps its OWN lesser linked
// share (team_repo_ownership.share), never an equal split; a full tie is stored as equal shares and reads equal; a
// single owner holds the whole repository. It runs once per provider ({jira, gitlab, github, linear}): the share is
// the same for every provider, the provider is never an input.
//
//   - repo-lead:   team-a main 0.7, team-b co-owner 0.3                  -> 0.7 / 0.3 (an equal split gives 0.5 / 0.5)
//   - repo-tie:    team-a and team-b both main, 0.5 each                 -> 0.5 / 0.5
//   - repo-three:  team-a main 0.5, team-b 0.3, team-c 0.2               -> 0.5 / 0.3 / 0.2
//   - repo-single: team-a only, share 1.0                                -> 1.0
//   - repo-gap:    team-a measured 0.9, team-b NOT measured (NULL)       -> 0.5 / 0.5 (no weight is invented)
//   - repo-none:   team-a only, share NULL                               -> 1.0
//
// A team scope narrows the rows and never re-weighs them: team-b alone holds repo-lead at 0.3 (CHAOS-9413).
func TestOwnerSharesWeighByTheStoredLinkedShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	type owner struct {
		team    string
		primary int
		share   any
	}
	const org = "owner-shares-org"
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)

	providers := []string{"jira", "gitlab", "github", "linear"}
	cases := []struct {
		name   string
		owners []owner
		want   map[string]float64
	}{
		{"lead", []owner{{"team-a", 1, 0.7}, {"team-b", 2, 0.3}}, map[string]float64{"team-a": 0.7, "team-b": 0.3}},
		{"tie", []owner{{"team-a", 1, 0.5}, {"team-b", 1, 0.5}}, map[string]float64{"team-a": 0.5, "team-b": 0.5}},
		{"three", []owner{{"team-a", 1, 0.5}, {"team-b", 2, 0.3}, {"team-c", 2, 0.2}}, map[string]float64{"team-a": 0.5, "team-b": 0.3, "team-c": 0.2}},
		{"single", []owner{{"team-a", 1, 1.0}}, map[string]float64{"team-a": 1}},
		{"gap", []owner{{"team-a", 1, 0.9}, {"team-b", 2, nil}}, map[string]float64{"team-a": 0.5, "team-b": 0.5}},
		{"none", []owner{{"team-a", 1, nil}}, map[string]float64{"team-a": 1}},
	}

	// repo ids: provider index in the third group, case index in the last.
	repoID := func(providerIndex, caseIndex int) string {
		return fmt.Sprintf("00000000-0000-4000-80%02d-%012d", providerIndex, caseIndex)
	}
	for pi, provider := range providers {
		for ci, tc := range cases {
			id := repoID(pi, ci)
			name := fmt.Sprintf("acme/%s-%s", provider, tc.name)
			crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
				id, name, provider, org, synced, synced)
			for _, o := range tc.owners {
				crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share) VALUES (?, ?, ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
					org, provider, o.team, id, name, o.primary, from, synced, o.share)
			}
		}
	}

	read := func(teams []string) map[string]float64 {
		t.Helper()
		source, bindings := teamscope.OwnerShares(org, teams, asOf)
		rows, err := client.Query(ctx, "SELECT owner_repo_id, owner_team_id, owner_share FROM "+source+" AS s", bindings)
		if err != nil {
			t.Fatalf("OwnerShares query: %v", err)
		}
		defer rows.Close()
		got := map[string]float64{}
		for rows.Next() {
			var repo, team string
			var share float64
			if err := rows.Scan(&repo, &team, &share); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got[repo+"|"+team] = share
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		return got
	}

	all := read(nil)
	for pi, provider := range providers {
		for ci, tc := range cases {
			t.Run(provider+"/"+tc.name, func(t *testing.T) {
				var sum float64
				for team, want := range tc.want {
					got, ok := all[repoID(pi, ci)+"|"+team]
					if !ok {
						t.Fatalf("no owner row for %s", team)
					}
					if math.Abs(got-want) > 1e-9 {
						t.Errorf("%s share = %v, want %v", team, got, want)
					}
					sum += got
				}
				if math.Abs(sum-1) > 1e-9 {
					t.Errorf("shares sum to %v, want 1", sum)
				}
			})
		}
	}

	// A team scope narrows the ROWS and keeps each owner's own share (CHAOS-9413): team-b alone holds repo-lead at its
	// own 0.3, not the whole repository, and repo-gap at the equal 0.5 of the whole owner set.
	scoped := read([]string{"team-b"})
	if len(scoped) == 0 {
		t.Fatal("team-b alone: no rows")
	}
	for pi := range providers {
		for ci, tc := range cases {
			want, owned := tc.want["team-b"]
			got, present := scoped[repoID(pi, ci)+"|team-b"]
			if owned != present {
				t.Errorf("%s: team-b row present = %v, want %v", tc.name, present, owned)
				continue
			}
			if owned && math.Abs(got-want) > 1e-9 {
				t.Errorf("team-b alone: %s share = %v, want its own %v", tc.name, got, want)
			}
		}
	}
	for key := range scoped {
		if len(key) < 7 || key[len(key)-7:] != "|team-b" {
			t.Errorf("team-b alone: unexpected owner row %s", key)
		}
	}

	// RepoShare is the sum of the scoped teams' own shares of a repository, 0 for a repository none of them owns.
	wantShare := map[string]float64{}
	for pi := range providers {
		for ci, tc := range cases {
			wantShare[repoID(pi, ci)] = tc.want["team-a"] + tc.want["team-b"]
		}
	}
	expr, shareBindings := teamscope.RepoShare(org, "id", []string{"team-a", "team-b"}, asOf)
	rows, err := client.Query(ctx, "SELECT toString(id) AS id, "+expr+" AS share FROM repos FINAL WHERE org_id = {share_org:String}",
		append(shareBindings, dhclickhouse.Binding{Name: "share_org", Value: org}))
	if err != nil {
		t.Fatalf("RepoShare query: %v", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var id string
		var share float64
		if err := rows.Scan(&id, &share); err != nil {
			t.Fatalf("scan: %v", err)
		}
		seen++
		if want := wantShare[id]; math.Abs(share-want) > 1e-9 {
			t.Errorf("team-a + team-b hold %s at %v, want %v", id, share, want)
		}
	}
	if seen != len(wantShare) {
		t.Errorf("RepoShare read %d repositories, want %d", seen, len(wantShare))
	}

	// A team that owns nothing holds 0 of every repository.
	none, noneBindings := teamscope.RepoShare(org, "id", []string{"team-none"}, asOf)
	noneRows, err := client.Query(ctx, "SELECT sum("+none+") AS total FROM repos FINAL WHERE org_id = {share_org:String}",
		append(noneBindings, dhclickhouse.Binding{Name: "share_org", Value: org}))
	if err != nil {
		t.Fatalf("RepoShare of a team that owns nothing: %v", err)
	}
	defer noneRows.Close()
	var total float64
	for noneRows.Next() {
		if err := noneRows.Scan(&total); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	if total != 0 {
		t.Errorf("team-none holds %v, want 0", total)
	}
}
