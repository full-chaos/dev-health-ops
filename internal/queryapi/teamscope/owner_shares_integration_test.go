//go:build integration

package teamscope_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

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
// A team scope renormalises over the owners in view: team-b alone holds repo-lead whole.
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

	// A team scope renormalises over the owners in view: one selected team holds the repository whole.
	scoped := read([]string{"team-b"})
	var keys []string
	for key := range scoped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if math.Abs(scoped[key]-1) > 1e-9 {
			t.Errorf("team-b alone: %s share = %v, want 1", key, scoped[key])
		}
	}
	if len(scoped) == 0 {
		t.Fatal("team-b alone: no rows")
	}
}
