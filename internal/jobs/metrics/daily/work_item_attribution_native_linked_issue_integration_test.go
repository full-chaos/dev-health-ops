//go:build integration

package daily

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWorkItemAttributionExecutorPreservesLinkedIssueFallback is the red-first
// regression test for codex r1's F1 (P1): WorkItemAttributionExecutor never
// called BuildLinkedIssueIndex, so derived.LinkedIssue stayed permanently
// empty (NewGitHubWorkItemDerivationContext initializes it to an empty map
// and nothing else in the daily executor populated it) -- a work item whose
// ONLY attribution path is a linked donor issue resolved to unassigned
// instead of inheriting the donor's team.
//
// Fixture: item "A" has no native/project/repo/member attribution of its own
// (its own repo is unowned). It has a `relates_to` dependency edge to item
// "D", which lives in a DIFFERENT repo owned by "team-linked" -- D is
// deliberately OUTSIDE the partition's own repo scope, so the only way A can
// see D at all is through the donor-loading path this fix adds (proving the
// fix actually reaches cross-repo donors, not just same-repo ones).
//
// Before the fix: A resolves zero candidates and is either dropped (if
// BuildWorkItemAttributionRows writes nothing for a subject with no
// candidates) or written unassigned. After the fix: A resolves exactly one
// candidate, source=linked_issue, team_id=team-linked, is_primary=1.
func TestWorkItemAttributionExecutorPreservesLinkedIssueFallback(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-linked-" + uuid.NewString()
	repoA := uuid.New()
	repoD := uuid.New()
	targetDay := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seededAt := targetDay.Add(time.Hour)

	seedWorkItemAttributionLinkedItem(t, ctx, conn, orgID, "A", repoA, seededAt)
	seedWorkItemAttributionLinkedItem(t, ctx, conn, orgID, "D", repoD, seededAt)
	// valid_from MUST be <= targetDay (the executor's asOf), not seededAt
	// (targetDay+1h) -- LoadRepos' bitemporal predicate is
	// `valid_from <= asOf AND (valid_to IS NULL OR valid_to > asOf)`, so an
	// ownership row valid only from AFTER the target day would be invisible
	// to this run and D would never resolve, making the test vacuous no
	// matter what the fix does.
	seedWorkItemAttributionLinkedRepoOwnership(t, ctx, conn, orgID, repoD, "team-linked", targetDay)
	seedWorkItemAttributionLinkedDependency(t, ctx, conn, orgID, "A", "D", "relates_to", seededAt)

	executor, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	run := Run{OrganizationID: orgID, TargetDay: targetDay}
	// Deliberately repoA ONLY -- D's repo is out of partition scope, so D
	// must arrive as a DONOR, not as a partition subject.
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoA.String())}}

	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("ComputeFamily: %v", err)
	}

	rows := queryWorkItemAttributionLinkedRows(t, ctx, conn, orgID, "A")
	if len(rows) != 1 {
		t.Fatalf("work_item A has %d attribution rows, want exactly 1 (linked_issue). Rows: %+v", len(rows), rows)
	}
	row := rows[0]
	if row.source != "linked_issue" {
		t.Fatalf("row.source = %q, want \"linked_issue\" -- LinkedIssue was never populated, or a "+
			"different source won the tie. Row: %+v", row.source, row)
	}
	if row.teamID == nil || *row.teamID != "team-linked" {
		t.Fatalf("row.team_id = %v, want \"team-linked\" (D's owning team, inherited through the "+
			"relates_to edge). Row: %+v", row.teamID, row)
	}
	if row.isPrimary != 1 {
		t.Fatalf("row.is_primary = %d, want 1 (the only candidate). Row: %+v", row.isPrimary, row)
	}
}

func workItemAttributionLinkedIssueMigratedClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := instance.Close(ctx); closeErr != nil {
			t.Logf("close clickhouse container: %v", closeErr)
		}
	})
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	password, _ := parsed.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func seedWorkItemAttributionLinkedItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID string, repoID uuid.UUID, createdAt time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		`INSERT INTO work_items (repo_id, work_item_id, provider, created_at, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, "github", createdAt, orgID, createdAt); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

func seedWorkItemAttributionLinkedRepoOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, teamID string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_repo_ownership (
		org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", teamID, repoID, "acme/"+teamID, "exact", "native",
		uint8(1), uint16(100), int32(0), now, nil, now,
	); err != nil {
		t.Fatalf("append team_repo_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}
}

func seedWorkItemAttributionLinkedDependency(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, sourceID, targetID, relationshipType string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_dependencies (
		org_id, source_work_item_id, target_work_item_id, relationship_type, last_synced
	)`)
	if err != nil {
		t.Fatalf("prepare work_item_dependencies batch: %v", err)
	}
	if err := batch.Append(orgID, sourceID, targetID, relationshipType, now); err != nil {
		t.Fatalf("append work_item_dependencies row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_item_dependencies batch: %v", err)
	}
}

type workItemAttributionLinkedRow struct {
	source    string
	teamID    *string
	isPrimary uint8
}

func queryWorkItemAttributionLinkedRows(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID string,
) []workItemAttributionLinkedRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT source, team_id, is_primary
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND work_item_id = ?
ORDER BY source`, orgID, workItemID)
	if err != nil {
		t.Fatalf("query work_item_team_attributions: %v", err)
	}
	defer rows.Close()
	var result []workItemAttributionLinkedRow
	for rows.Next() {
		var row workItemAttributionLinkedRow
		if err := rows.Scan(&row.source, &row.teamID, &row.isPrimary); err != nil {
			t.Fatalf("scan work_item_team_attributions row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_team_attributions: %v", err)
	}
	return result
}
