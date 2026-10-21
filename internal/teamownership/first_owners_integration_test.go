//go:build integration

package teamownership

import (
	"context"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// D5987 option C (CHAOS-9404): for an instant before the first recorded ownership of a repository, the repository's FIRST
// recorded owners apply. Every reader of this package answers the same, from the one rule of package ownershipwindow:
// an instant at or after the first record is read as before; an instant in a gap between records still has no owner; a
// team that is inactive owns nothing and does not set the first record.
func TestEveryReaderAppliesTheFirstRecordedOwnersBackward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const org = "org-first-owners"
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	synced := day(time.September, 1)
	repo := func(name string) uuid.UUID { return uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+name)) }
	pair, gap, grantOnly, onlyFuture, tie := repo("pair"), repo("gap"), repo("grant-only"), repo("only-future"), repo("tie")
	for _, id := range []uuid.UUID{pair, gap, grantOnly, onlyFuture, tie} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`,
			id, "acme/"+id.String(), org, synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
        VALUES ('team-gone', generateUUIDv4(), 'Gone', [], ?, ?, ?, 'github', 0)`, synced, synced, org); err != nil {
		t.Fatal(err)
	}
	type row struct {
		team    string
		repo    uuid.UUID
		primary uint8
		share   *float64
		from    time.Time
		to      *time.Time
	}
	f := func(v float64) *float64 { return &v }
	ended := day(time.April, 1)
	now := day(time.September, 21)
	rows := []row{
		// pair: team-a main 0.6 and team-b co-owner 0.4 from March 1.
		{"team-a", pair, 1, f(0.6), day(time.March, 1), nil},
		{"team-b", pair, 2, f(0.4), day(time.March, 1), nil},
		// gap: team-a March 1 to April 1, team-c from May 1.
		{"team-a", gap, 1, f(1), day(time.March, 1), &ended},
		{"team-c", gap, 1, f(1), day(time.May, 1), nil},
		// grant-only: the earlier row is an inactive team's; the first record is team-a's, March 1.
		{"team-gone", grantOnly, 1, f(1), day(time.February, 1), nil},
		{"team-a", grantOnly, 1, f(1), day(time.March, 1), nil},
		// only-future: its one row starts after the instant of the read.
		{"team-a", onlyFuture, 1, f(1), now.AddDate(0, 0, 30), nil},
		// tie: two main owners at the same first record (D5990).
		{"team-a", tie, 1, f(0.5), day(time.March, 1), nil},
		{"team-b", tie, 1, f(0.5), day(time.March, 1), nil},
	}
	for _, r := range rows {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 100, 0, ?, ?, ?, ?)
        `, org, r.team, r.repo, "acme/"+r.repo.String(), r.primary, r.share, r.from, r.to, synced); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}

	beforeAll := day(time.February, 15)
	atFirst := day(time.March, 1)
	inGap := day(time.April, 15)
	later := now

	ids := func(m map[string][]string) map[string][]string {
		for _, v := range m {
			sort.Strings(v)
		}
		return m
	}
	for _, tc := range []struct {
		name string
		at   time.Time
		want map[string][]string
	}{
		{"before every record", beforeAll, map[string][]string{
			pair.String(): {"team-a", "team-b"}, gap.String(): {"team-a"}, grantOnly.String(): {"team-a"},
			onlyFuture.String(): {"team-a"}, tie.String(): {"team-a", "team-b"},
		}},
		{"at the first record", atFirst, map[string][]string{
			pair.String(): {"team-a", "team-b"}, gap.String(): {"team-a"}, grantOnly.String(): {"team-a"},
			onlyFuture.String(): {"team-a"}, tie.String(): {"team-a", "team-b"},
		}},
		{"in a gap between records", inGap, map[string][]string{
			pair.String(): {"team-a", "team-b"}, grantOnly.String(): {"team-a"},
			onlyFuture.String(): {"team-a"}, tie.String(): {"team-a", "team-b"},
		}},
		{"after the records", later, map[string][]string{
			pair.String(): {"team-a", "team-b"}, gap.String(): {"team-c"}, grantOnly.String(): {"team-a"},
			onlyFuture.String(): {"team-a"}, tie.String(): {"team-a", "team-b"},
		}},
	} {
		got, err := OwnersByRepo(ctx, conn, org, tc.at)
		if err != nil {
			t.Fatalf("%s: OwnersByRepo: %v", tc.name, err)
		}
		if !reflect.DeepEqual(ids(got), tc.want) {
			t.Errorf("%s: OwnersByRepo = %v, want %v", tc.name, got, tc.want)
		}
	}

	// The shares and the main owners are the same rule: before the first record the owners and shares of that record.
	shares, err := OwnerSharesByRepo(ctx, conn, org, beforeAll)
	if err != nil {
		t.Fatalf("OwnerSharesByRepo: %v", err)
	}
	wantPair := []OwnerShare{{"team-a", true, 0.6}, {"team-b", false, 0.4}}
	gotPair := shares[pair.String()]
	if len(gotPair) != len(wantPair) {
		t.Fatalf("pair shares before the first record = %+v, want %+v", gotPair, wantPair)
	}
	for i := range wantPair {
		if gotPair[i].TeamID != wantPair[i].TeamID || gotPair[i].Main != wantPair[i].Main || math.Abs(gotPair[i].Share-wantPair[i].Share) > 1e-9 {
			t.Errorf("pair owner %d before the first record = %+v, want %+v", i, gotPair[i], wantPair[i])
		}
	}
	if _, owned := shares[gap.String()]; !owned {
		t.Errorf("a repository with a record at a later instant has its first owners before it: %v", shares)
	}
	if got := shares[grantOnly.String()]; len(got) != 1 || got[0].TeamID != "team-a" {
		t.Errorf("an inactive team's earlier row set the first record: %+v", got)
	}
	inGapShares, err := OwnerSharesByRepo(ctx, conn, org, inGap)
	if err != nil {
		t.Fatalf("OwnerSharesByRepo in the gap: %v", err)
	}
	if _, owned := inGapShares[gap.String()]; owned {
		t.Errorf("an instant between two records of a repository has no owner: %v", inGapShares[gap.String()])
	}
	mains, err := AuthoritativeOwnersByRepo(ctx, conn, org, beforeAll)
	if err != nil {
		t.Fatalf("AuthoritativeOwnersByRepo: %v", err)
	}
	if got := mains[pair.String()]; !reflect.DeepEqual(got, []string{"team-a"}) {
		t.Errorf("main owners of pair before the first record = %v, want [team-a]", got)
	}
	if got := ids(mains)[tie.String()]; !reflect.DeepEqual(got, []string{"team-a", "team-b"}) {
		t.Errorf("a full tie before the first record = %v, want both main owners (D5990)", got)
	}

	// OwnedRepoIDs is the same rule seen from the team.
	owned := func(team string, at time.Time) []string {
		t.Helper()
		got, err := OwnedRepoIDs(ctx, conn, org, team, at)
		if err != nil {
			t.Fatalf("OwnedRepoIDs(%s): %v", team, err)
		}
		var text []string
		for _, id := range got {
			text = append(text, id.String())
		}
		sort.Strings(text)
		return text
	}
	wantA := []string{pair.String(), gap.String(), grantOnly.String(), onlyFuture.String(), tie.String()}
	sort.Strings(wantA)
	if got := owned("team-a", beforeAll); !reflect.DeepEqual(got, wantA) {
		t.Errorf("OwnedRepoIDs(team-a) before every record = %v, want %v", got, wantA)
	}
	if got := owned("team-c", beforeAll); len(got) != 0 {
		t.Errorf("OwnedRepoIDs(team-c) before every record = %v: team-c is no first owner of any repository", got)
	}
}
