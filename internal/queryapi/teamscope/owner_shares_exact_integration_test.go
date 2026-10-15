//go:build integration

package teamscope_test

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

const (
	exactShareParts = 16
	exactShareTeams = 640
	exactShareRuns  = 20
	exactShareRepo  = "00000000-0000-4000-8000-000000000001"
)

// exactStoredShare is team n's stored share: k * 2^e, 74 bits wide over the
// owners, so a Float64 sum of them depends on the order it is taken in.
func exactStoredShare(n int) (k int64, shift uint) {
	return int64((n*7919)%1000 + 1), uint(n % 55)
}

// A repository's owner shares divide each stored share by the total of the
// repository's stored shares, a window sum over its owners; RepoShare sums the
// shares of the teams in view. One repository with exactShareTeams owners,
// written in exactShareParts parts: both reads must give one answer every run,
// and each at the exact value.
func TestOwnerSharesAndRepoShareGiveOneExactAnswerOnEveryRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const org = "owner-shares-exact-org"
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/shared', 'github', ?, ?, ?)`,
		exactShareRepo, org, synced, synced)
	per := exactShareTeams / exactShareParts
	for part := 0; part < exactShareParts; part++ {
		crossorg.Exec(ctx, t, admin, fmt.Sprintf(`
INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary,
    specificity, priority, valid_from, valid_to, updated_at, share)
SELECT '%[1]s', 'github', concat('team-', toString(n)), toUUID('%[2]s'), 'acme/shared', 'exact', 'inferred', 0, 0, 0,
    toDateTime64('2026-01-01 00:00:00', 3), NULL, toDateTime64('2026-09-16 00:00:00', 3),
    ((n * 7919) %% 1000 + 1) * pow(2, toInt64(n %% 55) - 30)
FROM (SELECT number + %[3]d AS n FROM numbers(%[4]d))`, org, exactShareRepo, part*per, per))
	}

	readShares := func() map[string]float64 {
		source, bindings := teamscope.OwnerShares(org, nil, asOf)
		rows, err := client.Query(ctx, "SELECT owner_team_id, owner_share FROM "+source+" AS s", bindings)
		if err != nil {
			t.Fatalf("OwnerShares query: %v", err)
		}
		defer rows.Close()
		got := map[string]float64{}
		for rows.Next() {
			var team string
			var share float64
			if err := rows.Scan(&team, &share); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got[team] = share
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		return got
	}
	inView := make([]string, 0, exactShareTeams/2)
	for n := 0; n < exactShareTeams; n += 2 {
		inView = append(inView, fmt.Sprintf("team-%d", n))
	}
	readRepoShare := func() float64 {
		expr, bindings := teamscope.RepoShare(org, "rid", inView, asOf)
		rows, err := client.Query(ctx, "SELECT "+expr+" FROM (SELECT toUUID({rid:String}) AS rid)",
			append(bindings, dhclickhouse.Binding{Name: "rid", Value: exactShareRepo}))
		if err != nil {
			t.Fatalf("RepoShare query: %v", err)
		}
		defer rows.Close()
		var share float64
		if !rows.Next() {
			t.Fatal("RepoShare: no row")
		}
		if err := rows.Scan(&share); err != nil {
			t.Fatalf("scan: %v", err)
		}
		return share
	}

	shares := readShares()
	repoShare := readRepoShare()
	for run := 1; run < exactShareRuns; run++ {
		if got := readShares(); !reflect.DeepEqual(got, shares) {
			for team, share := range got {
				if share != shares[team] {
					t.Fatalf("owner shares run %d differ from run 0: %s %.17g vs %.17g", run, team, share, shares[team])
				}
			}
			t.Fatalf("owner shares run %d differ from run 0", run)
		}
		if got := readRepoShare(); got != repoShare {
			t.Fatalf("RepoShare run %d = %.17g, run 0 = %.17g", run, got, repoShare)
		}
	}

	total := new(big.Int)
	for n := 0; n < exactShareTeams; n++ {
		k, shift := exactStoredShare(n)
		total.Add(total, new(big.Int).Lsh(big.NewInt(k), shift))
	}
	if len(shares) != exactShareTeams {
		t.Fatalf("owner shares has %d teams, want %d", len(shares), exactShareTeams)
	}
	held := new(big.Int)
	for n := 0; n < exactShareTeams; n++ {
		k, shift := exactStoredShare(n)
		stored := new(big.Int).Lsh(big.NewInt(k), shift)
		want, _ := new(big.Rat).SetFrac(stored, total).Float64()
		got := shares[fmt.Sprintf("team-%d", n)]
		if math.Abs(got-want) > 1e-15*want {
			t.Errorf("team-%d share = %.17g, want %.17g", n, got, want)
		}
		if n%2 == 0 {
			held.Add(held, stored)
		}
	}
	wantHeld, _ := new(big.Rat).SetFrac(held, total).Float64()
	if math.Abs(repoShare-wantHeld) > 1e-14*wantHeld {
		t.Errorf("RepoShare of the even teams = %.17g, want %.17g", repoShare, wantHeld)
	}
}
