//go:build integration

package teamownership

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9548: a sync that repeats an open ownership row no longer writes it
// again, so the row keeps the updated_at of its last real change. Two orgs
// hold the same ownership. The first has every row written again at three
// later syncs (what a writer did before), the second has each row once. The
// owner readers rank by updated_at after is_primary and specificity; they must
// give the same owners for both.
func TestOwnerReadersAnswerTheSameWhenARepeatedRowIsNotWrittenAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	asOf := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	start := asOf.AddDate(0, 0, -30)
	handover := asOf.AddDate(0, 0, -5)
	repoMain, repoTie, repoGrant, repoHandover := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	type fact struct {
		team, source string
		repo         uuid.UUID
		isPrimary    uint8
		specificity  uint16
		validFrom    time.Time
		validTo      *time.Time
	}
	facts := []fact{
		// A main owner with a co-owner of lesser share (D5432).
		{"team-a", "inferred", repoMain, 1, 100, start, nil},
		{"team-b", "inferred", repoMain, 2, 100, start, nil},
		// A full tie: both teams are main owners (D5990).
		{"team-a", "inferred", repoTie, 1, 100, start, nil},
		{"team-b", "inferred", repoTie, 1, 100, start, nil},
		// A provider grant under a main owner.
		{"team-a", "inferred", repoGrant, 1, 100, start, nil},
		{"team-c", "provider_access", repoGrant, 0, 10, start, nil},
		// A winner change: team-a closed at the handover, team-b main from then on.
		{"team-a", "inferred", repoHandover, 1, 100, start, &handover},
		{"team-b", "inferred", repoHandover, 1, 100, handover, nil},
	}
	insert := func(org string, f fact, updatedAt time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', ?, ?, ?, 0, ?, ?, ?)
        `, org, f.team, f.repo, "acme/"+f.repo.String(), f.source, f.isPrimary, f.specificity, f.validFrom, f.validTo, updatedAt); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}
	const repeated, once = "org-rewritten-at-every-sync", "org-written-once"
	for _, f := range facts {
		changedAt := f.validFrom
		if f.validTo != nil {
			changedAt = *f.validTo
		}
		insert(once, f, changedAt)
		for sync := 0; sync < 4; sync++ {
			insert(repeated, f, changedAt.Add(time.Duration(sync)*time.Hour))
		}
	}

	owners := func(org string) map[string][]string {
		t.Helper()
		got, err := AuthoritativeOwnersByRepo(ctx, conn, org, asOf)
		if err != nil {
			t.Fatalf("AuthoritativeOwnersByRepo(%s): %v", org, err)
		}
		for _, list := range got {
			sort.Strings(list)
		}
		return got
	}
	gotOnce, gotRepeated := owners(once), owners(repeated)
	if !reflect.DeepEqual(gotOnce, gotRepeated) {
		t.Fatalf("owners differ: written once %v, rewritten at every sync %v", gotOnce, gotRepeated)
	}
	want := map[string][]string{
		repoMain.String(): {"team-a"}, repoTie.String(): {"team-a", "team-b"},
		repoGrant.String(): {"team-a"}, repoHandover.String(): {"team-b"},
	}
	if !reflect.DeepEqual(gotOnce, want) {
		t.Fatalf("owners = %v, want %v", gotOnce, want)
	}
	for _, team := range []string{"team-a", "team-b", "team-c"} {
		onceRepos, err := OwnedRepoIDs(ctx, conn, once, team, asOf)
		if err != nil {
			t.Fatal(err)
		}
		repeatedRepos, err := OwnedRepoIDs(ctx, conn, repeated, team, asOf)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(onceRepos, func(i, j int) bool { return onceRepos[i].String() < onceRepos[j].String() })
		sort.Slice(repeatedRepos, func(i, j int) bool { return repeatedRepos[i].String() < repeatedRepos[j].String() })
		if !reflect.DeepEqual(onceRepos, repeatedRepos) {
			t.Fatalf("%s owns %v when written once, %v when rewritten at every sync", team, onceRepos, repeatedRepos)
		}
	}
}
