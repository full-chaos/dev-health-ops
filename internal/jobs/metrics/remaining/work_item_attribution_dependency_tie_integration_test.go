//go:build integration

package remaining

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// TestANativeDonorSurvivesASameSyncBlocksEdge: a pull request has a native
// relates_to link to issue #11 (github_closing_reference) and its body says
// "Blocks #11"; one sync writes both rows with the same last_synced. On a real
// store the first run gives the pull request #11's team, and when #11 moves to
// another team's project the donor-change trigger rederives it. With a second
// donor #22 the pull request still copies #11, never #22.
func TestANativeDonorSurvivesASameSyncBlocksEdge(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	now := time.Now().UTC()
	setup := now.Add(-72 * time.Hour)
	const prID, donorID, otherID = "github-pr:acme/web#7", "gh:acme/api#11", "gh:acme/api#22"

	for _, shape := range []string{"only_donor", "second_donor"} {
		t.Run(shape, func(t *testing.T) {
			orgID := "org-dependency-tie-" + shape + "-" + uuid.NewString()
			for _, team := range []string{"team-a", "team-b"} {
				if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], [], 1, ?, ?, ?, 'github', '')`,
					team, uuid.New(), "Team "+team, setup, setup, orgID); err != nil {
					t.Fatalf("insert team %s: %v", team, err)
				}
			}
			seedWorkItemAttributionProjectOwnership(t, ctx, conn, orgID, "github", "proj-a", nil, "team-a", setup)
			seedWorkItemAttributionProjectOwnership(t, ctx, conn, orgID, "github", "proj-b", nil, "team-b", setup)
			seedItem := func(id, itemType, project string, repo uuid.UUID, syncedAt time.Time) {
				t.Helper()
				if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, status, created_at, org_id, last_synced)
				VALUES (?, ?, 'github', ?, ?, 'in_progress', ?, ?, ?)`, repo, id, itemType, project, setup, orgID, syncedAt); err != nil {
					t.Fatalf("seed %s: %v", id, err)
				}
			}
			seedEdge := func(target, relationship, raw string) {
				t.Helper()
				if err := conn.Exec(ctx, `INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version)
				VALUES (?, ?, ?, ?, ?, ?, 'canonical-blocks.v2')`, prID, target, relationship, raw, setup, orgID); err != nil {
					t.Fatalf("seed edge %s %s: %v", target, relationship, err)
				}
			}
			seedItem(donorID, "issue", "proj-a", uuid.Nil, setup)
			seedItem(prID, "pr", "", uuid.New(), setup)
			seedEdge(donorID, "relates_to", "github_closing_reference")
			seedEdge(donorID, "blocks", "blocks #11")
			if shape == "second_donor" {
				seedItem(otherID, "issue", "proj-b", uuid.Nil, setup)
				seedEdge(otherID, "relates_to", "github_closing_reference")
			}

			primary := func() string {
				t.Helper()
				var got []string
				for _, row := range latestAttributions(t, ctx, conn, orgID, prID) {
					if row.isPrimary == 1 {
						got = append(got, fmt.Sprintf("%s/%s/%s", row.teamID, row.source, linkedIssueEvidenceDonor(row.evidence)))
					}
				}
				return fmt.Sprint(got)
			}

			yesterday := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return now.Add(-24 * time.Hour) }}
			if _, err := yesterday.ComputeOrg(ctx, orgID); err != nil {
				t.Fatalf("first run: %v", err)
			}
			if got, want := primary(), "[team-a/linked_issue/"+donorID+"]"; got != want {
				t.Errorf("cascade: pull request primary %s, want %s", got, want)
			}

			seedItem(donorID, "issue", "proj-b", uuid.Nil, time.Now().UTC())
			subjects, err := LoadWorkItemDonorSubjects(ctx, conn, orgID, []string{donorID}, nil)
			if err != nil || len(subjects) != 1 {
				t.Fatalf("load donor subject: %v %v", subjects, err)
			}
			at := now.Add(-6 * time.Hour)
			facts, err := LoadWorkItemDerivationFacts(ctx, conn, orgID, at)
			if err != nil {
				t.Fatalf("load facts: %v", err)
			}
			rows := BuildWorkItemAttributionRows(orgID, at, map[string]struct{}{donorID: {}}, subjects, teamattribution.NewGitHubWorkItemDerivationContext(facts))
			if _, err := writer.WriteAttributions(ctx, WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: uuid.NewString()}, rows); err != nil {
				t.Fatalf("write donor: %v", err)
			}

			owed, err := yesterday.loadDonorChangedDependents(ctx, orgID)
			if err != nil {
				t.Fatalf("load donor-changed dependents: %v", err)
			}
			if fmt.Sprint(owed) != "["+prID+"]" {
				t.Errorf("donor change: owed %v, want [%s]", owed, prID)
			}
			backstop, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
			if err != nil {
				t.Fatalf("new executor: %v", err)
			}
			if _, err := backstop.ComputeOrg(ctx, orgID); err != nil {
				t.Fatalf("backstop: %v", err)
			}
			if got, want := primary(), "[team-b/linked_issue/"+donorID+"]"; got != want {
				t.Errorf("after the donor moved: pull request primary %s, want %s", got, want)
			}
		})
	}
}
