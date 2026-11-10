//go:build integration

package edges

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestDeleteStalePRTargetCommitIssueEdgesAgainstLiveClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)

	const org = "70d529e0-3c06-4597-8480-794fd0230000"
	const otherOrg = "70d529e0-3c06-4597-8480-794fd0239999"
	seededAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	insert := func(id, sourceType, sourceID, targetID, orgID string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_graph_edges
(edge_id, source_type, source_id, target_type, target_id, edge_type, provenance,
 confidence, evidence, discovered_at, last_synced, event_ts, day, org_id)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, sourceType, sourceID, "issue", targetID, "references", "explicit_text",
			float32(0.85), "#7", seededAt, seededAt, seededAt, seededAt, orgID,
		); err != nil {
			t.Fatal(err)
		}
	}
	insert("ghpr-target", "commit", "c1", "ghpr:acme/app#7", org)
	insert("gitlab-mr-target", "commit", "c2", "gitlab:g/p!8", org)
	insert("gh-target", "commit", "c3", "gh:acme/app#7", org)
	insert("linear-target", "commit", "c4", "linear:ACME-1", org)
	insert("jira-target", "commit", "c5", "jira:ACME-2", org)
	insert("pr-source-ghpr-target", "pr", "r#pr1", "ghpr:acme/app#9", org)
	insert("other-org-ghpr-target", "commit", "c6", "ghpr:acme/app#7", otherOrg)

	for run := 1; run <= 2; run++ {
		if err := DeleteStalePRTargetCommitIssueEdges(ctx, conn, org, time.Now()); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
	}

	live := func(id string) uint64 {
		t.Helper()
		var got uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM (
SELECT argMax(is_deleted, last_synced) AS d FROM work_graph_edges WHERE edge_id = ? GROUP BY org_id, edge_id) WHERE d = 0`, id,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	for _, id := range []string{"ghpr-target", "gitlab-mr-target"} {
		if got := live(id); got != 0 {
			t.Errorf("%s still live after cleanup", id)
		}
	}
	for _, id := range []string{"gh-target", "linear-target", "jira-target", "pr-source-ghpr-target", "other-org-ghpr-target"} {
		if got := live(id); got != 1 {
			t.Errorf("%s not live (count=%d), must survive", id, got)
		}
	}
	var versions uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_graph_edges WHERE edge_id = 'ghpr-target' AND org_id = ?`, org).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 2 {
		t.Errorf("ghpr-target has %d stored versions, want 2 (seed + one tombstone; second run idempotent)", versions)
	}
	if err := DeleteStalePRTargetCommitIssueEdges(ctx, conn, "", time.Now()); err == nil {
		t.Fatal("empty org must be refused")
	}
}
