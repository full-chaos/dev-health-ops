//go:build integration

package teamownership

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestOwnedRepoIDs runs against the real migrated team_repo_ownership schema
// and pins every boundary CHAOS-4897's follow-up depends on: the bitemporal
// window, the NULL-repo_id (pattern-unresolved) exclusion, org isolation, and
// that shared ownership is membership rather than a resolved single winner.
func TestOwnedRepoIDs(t *testing.T) {
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

	const orgID = "org-teamownership"
	const otherOrgID = "org-teamownership-other"
	teamA, teamB := "team-a", "team-b"
	repoOwnedByA := uuid.New()
	repoOwnedByBoth := uuid.New()
	repoExpired := uuid.New()
	repoNotYetActive := uuid.New()
	repoOtherOrg := uuid.New()

	asOf := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)

	insert := func(orgID, teamID string, repoID uuid.UUID, validFrom time.Time, validTo *time.Time) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, ?, ?)
        `, orgID, teamID, repoID, repoID.String(), validFrom, validTo, validFrom); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}
	insertUnresolved := func(orgID, teamID string, validFrom time.Time) {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, NULL, 'acme/*', 'pattern', 'inferred', 0, 0, 0, ?, NULL, ?)
        `, orgID, teamID, validFrom, validFrom); err != nil {
			t.Fatalf("seed unresolved team_repo_ownership: %v", err)
		}
	}

	before := asOf.AddDate(0, 0, -30)
	past := asOf.AddDate(0, 0, -1)
	future := asOf.AddDate(0, 0, 30)

	// teamA: one plain active row.
	insert(orgID, teamA, repoOwnedByA, before, nil)
	// A repo BOTH teams claim -- membership, not a resolved single owner:
	// both must see it.
	insert(orgID, teamA, repoOwnedByBoth, before, nil)
	insert(orgID, teamB, repoOwnedByBoth, before, nil)
	// A row that EXPIRED before asOf -- must not count.
	insert(orgID, teamA, repoExpired, before, &past)
	// A row that has not ACTIVATED yet as of asOf -- must not count.
	insert(orgID, teamA, repoNotYetActive, future, nil)
	// A pattern-unresolved row (repo_id NULL) -- must not count, and must not
	// error the scan.
	insertUnresolved(orgID, teamA, before)
	// Same team_id, different org -- must not leak across tenants.
	insert(otherOrgID, teamA, repoOtherOrg, before, nil)

	assertOwned := func(t *testing.T, orgID, teamID string, want ...uuid.UUID) {
		t.Helper()
		got, err := OwnedRepoIDs(ctx, conn, orgID, teamID, asOf)
		if err != nil {
			t.Fatalf("OwnedRepoIDs(%s, %s): %v", orgID, teamID, err)
		}
		sort.Slice(got, func(i, j int) bool { return got[i].String() < got[j].String() })
		sort.Slice(want, func(i, j int) bool { return want[i].String() < want[j].String() })
		if len(got) != len(want) {
			t.Fatalf("OwnedRepoIDs(%s, %s) = %v, want %v", orgID, teamID, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("OwnedRepoIDs(%s, %s) = %v, want %v", orgID, teamID, got, want)
			}
		}
	}

	assertOwned(t, orgID, teamA, repoOwnedByA, repoOwnedByBoth)
	assertOwned(t, orgID, teamB, repoOwnedByBoth)

	// A team with no rows at all owns nothing -- and that must come back as
	// an EMPTY, non-error result, never an error masquerading as "no data".
	empty, err := OwnedRepoIDs(ctx, conn, orgID, "team-with-no-rows", asOf)
	if err != nil {
		t.Fatalf("OwnedRepoIDs for a team with no rows: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("team-with-no-rows owns %v, want none", empty)
	}
}
