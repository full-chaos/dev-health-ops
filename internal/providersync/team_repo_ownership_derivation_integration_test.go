//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TestTeamRepoOwnershipDerivationAgainstMigratedSchema is the standing
// obligation's red-first proof, run against the REAL migration chain (via
// newWorkItemEffectsConn, shared with the GitHub direct-effects integration
// tests): a fixture-shaped org with team_project_ownership + work_items +
// work_item_dependencies + work_graph_issue_pr rows -- everything a real
// sync could have already written -- produces ZERO team_repo_ownership rows
// before this producer runs (nothing in this repo derives it), and the
// expected rows after Derive() is called. Exercises all three signal paths:
// own project_id (design check a), the dependency-donor walk gated to
// inheritance-safe relationship types, and PR inheritance via
// work_graph_issue_pr (design check b) using that table's OWN repo_id for a
// genuine cross-repo link.
func TestTeamRepoOwnershipDerivationAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4365-item1b-org"
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	repoA := uuid.New() // owned via design check (a): work item's own project_id
	repoB := uuid.New() // owned via the dependency-donor walk (design check a2)
	repoC := uuid.New() // owned via PR inheritance (design check b), cross-repo
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{
		repoA: "acme/repo-a",
		repoB: "acme/repo-b",
		repoC: "acme/repo-c",
	})
	seedTeamProjectOwnership(t, ctx, conn, orgID, "proj-1", "team-platform", true, now)
	seedWorkItem(t, ctx, conn, orgID, "linear:PLAT-1", "linear", uuid.Nil, "proj-1", now)
	seedWorkItem(t, ctx, conn, orgID, "gh:acme/repo-a#1", "github", repoA, "proj-1", now)
	seedWorkItem(t, ctx, conn, orgID, "ghpr:acme/repo-b#7", "github", repoB, "", now)
	seedWorkItemDependency(t, ctx, conn, "ghpr:acme/repo-b#7", "linear:PLAT-1", "relates_to", now)
	seedWorkGraphIssuePR(t, ctx, conn, repoC, "linear:PLAT-1", 42, now)

	// Red: before Derive() runs, nothing in this repo has ever written
	// team_repo_ownership for this org -- confirmed against the real
	// migrated schema, not a mock.
	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgID, 0)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if written != 3 {
		t.Fatalf("expected 3 rows written (repo-a, repo-b, repo-c), got %d", written)
	}

	got := readTeamRepoOwnership(t, ctx, conn, orgID)
	want := map[string]string{
		"acme/repo-a": "team-platform",
		"acme/repo-b": "team-platform",
		"acme/repo-c": "team-platform",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d rows, got %d: %+v", len(want), len(got), got)
	}
	for repoFullName, wantTeam := range want {
		row, ok := got[repoFullName]
		if !ok {
			t.Fatalf("missing row for %s: %+v", repoFullName, got)
		}
		if row.teamID != wantTeam {
			t.Fatalf("%s: expected team %s, got %s", repoFullName, wantTeam, row.teamID)
		}
		if row.source != "inferred" {
			t.Fatalf("%s: expected source=inferred, got %s", repoFullName, row.source)
		}
		if row.provider != "github" {
			t.Fatalf("%s: expected provider=github (from repos.provider, not the work item's tracker provider), got %s", repoFullName, row.provider)
		}
	}

	// Idempotent per (org, sync run): calling Derive() again for the same
	// already-derived state does not duplicate rows -- ReplacingMergeTree
	// dedup on (org_id, provider, repo_full_name, team_id, source,
	// valid_from) collapses re-derivation to the same logical row set once
	// merged, and this read path uses FINAL, so it must already read back
	// exactly the same 3 rows.
	written2, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("second Derive: %v", err)
	}
	if written2 != 3 {
		t.Fatalf("expected the second Derive to also write 3 rows (re-derivation is a no-op in effect, not in row count -- FINAL read confirms it below), got %d", written2)
	}
	gotAfterSecondRun := readTeamRepoOwnership(t, ctx, conn, orgID)
	if len(gotAfterSecondRun) != len(want) {
		t.Fatalf("re-derivation duplicated rows under FINAL dedup: expected %d, got %d: %+v", len(want), len(gotAfterSecondRun), gotAfterSecondRun)
	}
}

// TestTeamRepoOwnershipDerivationNoProjectOwnershipIsNotAnError covers the
// designed-empty case (§0.2): an org with zero team_project_ownership rows
// (a GitHub-only org, or team auto-import never configured) derives zero
// rows and returns no error -- never guessed, never a failure.
func TestTeamRepoOwnershipDerivationNoProjectOwnershipIsNotAnError(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "chaos-4365-item1b-empty-org"

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	written, err := service.Derive(ctx, orgID)
	if err != nil {
		t.Fatalf("Derive on an org with no project ownership: %v", err)
	}
	if written != 0 {
		t.Fatalf("expected 0 rows written, got %d", written)
	}
	assertTeamRepoOwnershipRowCount(t, ctx, conn, orgID, 0)
}

func seedTeamRepoOwnershipRepos(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repos map[uuid.UUID]string) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced)`)
	if err != nil {
		t.Fatalf("prepare repos batch: %v", err)
	}
	for id, fullName := range repos {
		if err := batch.Append(id, fullName, orgID, "github", time.Now().UTC()); err != nil {
			t.Fatalf("append repos row: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send repos batch: %v", err)
	}
}

func seedTeamProjectOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, projectID, teamID string, isPrimary bool, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`)
	if err != nil {
		t.Fatalf("prepare team_project_ownership batch: %v", err)
	}
	primary := uint8(0)
	if isPrimary {
		primary = 1
	}
	if err := batch.Append(orgID, "linear", teamID, projectID, "native", primary, uint16(100), int32(0), now, nil, now); err != nil {
		t.Fatalf("append team_project_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_project_ownership batch: %v", err)
	}
}

func seedWorkItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID, provider string, repoID uuid.UUID, projectID string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, project_id, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, provider, projectID, orgID, now); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

func seedWorkItemDependency(
	t *testing.T, ctx context.Context, conn driver.Conn,
	sourceWorkItemID, targetWorkItemID, relationshipType string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_item_dependencies batch: %v", err)
	}
	if err := batch.Append(sourceWorkItemID, targetWorkItemID, relationshipType, relationshipType, now); err != nil {
		t.Fatalf("append work_item_dependencies row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_item_dependencies batch: %v", err)
	}
}

func seedWorkGraphIssuePR(
	t *testing.T, ctx context.Context, conn driver.Conn,
	repoID uuid.UUID, workItemID string, prNumber uint32, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_graph_issue_pr batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, prNumber, float32(1.0), "native", "test-seed", now); err != nil {
		t.Fatalf("append work_graph_issue_pr row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_issue_pr batch: %v", err)
	}
}

func assertTeamRepoOwnershipRowCount(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, want int) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("count team_repo_ownership: %v", err)
	}
	defer rows.Close()
	var got uint64
	if rows.Next() {
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("scan count: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("count rows.Err: %v", err)
	}
	if int(got) != want {
		t.Fatalf("expected %d team_repo_ownership rows for org %s, got %d", want, orgID, got)
	}
}

type teamRepoOwnershipReadRow struct {
	teamID   string
	provider string
	source   string
}

func readTeamRepoOwnership(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) map[string]teamRepoOwnershipReadRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT repo_full_name, team_id, provider, source
FROM team_repo_ownership FINAL
WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("read team_repo_ownership: %v", err)
	}
	defer rows.Close()
	out := map[string]teamRepoOwnershipReadRow{}
	for rows.Next() {
		var repoFullName, teamID, provider, source string
		if err := rows.Scan(&repoFullName, &teamID, &provider, &source); err != nil {
			t.Fatalf("scan team_repo_ownership row: %v", err)
		}
		out[repoFullName] = teamRepoOwnershipReadRow{teamID: teamID, provider: provider, source: source}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("team_repo_ownership rows.Err: %v", err)
	}
	return out
}
