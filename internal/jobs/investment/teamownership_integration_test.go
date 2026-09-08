//go:build integration

package investment

import (
	"context"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTeamOwnershipFallbackPersistedUnion proves the production materializer
// writes the distinct union of both issues' ownership, for every provider.
// Ownership uses the real producer's name-only GitHub provider_access shape:
// repo_id is NULL, is_primary is zero, and repo identity comes from repos.
func TestTeamOwnershipFallbackPersistedUnion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	repos := []string{testRepoID, otherRepoID, "33333333-3333-4333-8333-333333333333"}
	for i, repo := range repos {
		mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id) VALUES (?, ?, 'github', ?)`, repo, "acme/repo-"+string(rune('a'+i)), hierarchyCascadeTestOrg)
	}
	sources := map[string]string{"linear": "native_team", "jira": "issue_project", "gitlab": "project_ownership", "github": "repo_ownership"}
	for _, provider := range []string{"jira", "gitlab", "github", "linear"} {
		for _, suffix := range []string{"a", "b"} {
			issue, team := provider+":"+suffix, provider+"-team-"+suffix
			seedWorkItem(t, ctx, conn, issue, "", at)
			// Update to each provider using a newer complete row from the same
			// persisted producer shape. No native_team_key shortcut is seeded.
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO work_items SELECT * REPLACE (? AS provider, ? AS last_synced) FROM work_items FINAL WHERE org_id = ? AND work_item_id = ?`, provider, at.Add(time.Hour), hierarchyCascadeTestOrg, issue)
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, provider, is_active, updated_at, org_id) VALUES (?, generateUUIDv4(), ?, ?, 1, ?, ?)`, team, team, provider, at, hierarchyCascadeTestOrg)
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, source, is_primary, confidence, evidence, computed_at) VALUES (?, toUUID('00000000-0000-0000-0000-000000000000'), ?, ?, ?, ?, 1, 'high', 'provider ownership', ?)`, hierarchyCascadeTestOrg, issue, provider, team, sources[provider], at)
			names := []string{"acme/repo-a", "acme/repo-b"}
			if suffix == "b" {
				names = []string{"acme/repo-b", "acme/repo-c"}
			}
			for _, name := range names {
				for _, version := range []time.Time{at, at.Add(time.Minute)} {
					mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'github', ?, ?, 'exact', 'provider_access', 0, 50, 0, ?, ?)`, hierarchyCascadeTestOrg, team, name, version, version)
				}
			}
		}
		seedIssueEdge(t, ctx, conn, provider+":a", provider+":b", "", at)
	}
	stats, err := materializer.Run(ctx, Config{OrgID: hierarchyCascadeTestOrg, FromTS: at.Add(-time.Hour), ToTS: at.Add(2 * time.Hour), RunID: "team-ownership-union", ComputedAt: at.Add(2 * time.Hour), ProviderName: "mock"})
	if err != nil {
		t.Fatal(err)
	}
	if stats.RepoOwnershipFallback != 4 {
		t.Fatalf("fallback count=%d want4", stats.RepoOwnershipFallback)
	}
	if stats.Components != 4 {
		t.Fatalf("components=%d, want 4 nonempty provider cases", stats.Components)
	}
	for _, provider := range []string{"jira", "gitlab", "github", "linear"} {
		unit := units.WorkUnitID([]units.NodeKey{{Type: "issue", ID: provider + ":a"}, {Type: "issue", ID: provider + ":b"}})
		rows, err := conn.Query(ctx, `SELECT toString(repo_id), allocation_weight, allocation_source, ifNull(repo_source, '') FROM work_unit_repo_effort FINAL WHERE org_id = ? AND work_unit_id = ? ORDER BY repo_id`, hierarchyCascadeTestOrg, unit)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		weightSum := 0.0
		for rows.Next() {
			var repo *string
			var weight float64
			var source, evidence string
			if err := rows.Scan(&repo, &weight, &source, &evidence); err != nil {
				t.Fatal(err)
			}
			if repo == nil {
				t.Errorf("%s: persisted NULL repository; team fallback did not execute", provider)
				continue
			}
			got = append(got, *repo)
			weightSum += weight
			if math.Abs(weight-1.0/3.0) > 1e-12 || source != "team_ownership" || evidence == "" {
				t.Errorf("%s: repo=%s weight=%g source=%s evidence=%s", provider, *repo, weight, source, evidence)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if len(got) != 3 || math.Abs(weightSum-1) > 1e-12 {
			t.Errorf("%s: distinct repos=%v total weight=%g, want three shares with total 1", provider, got, weightSum)
		}
	}
	// Query failure must stop the materializer before it writes a new output
	// generation. Dropping this test instance's donor table is a real failure,
	// not a fake configured to return the expected answer.
	mustTeamOwnershipExec(t, ctx, conn, `DROP TABLE work_item_team_attributions`)
	_, err = materializer.Run(ctx, Config{OrgID: hierarchyCascadeTestOrg, FromTS: at.Add(-time.Hour), ToTS: at.Add(2 * time.Hour), RunID: "failed-donor-read", ComputedAt: at.Add(3 * time.Hour), ProviderName: "mock"})
	if err == nil {
		t.Fatal("missing donor table was treated as missing ownership")
	}
	var writes uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_unit_repo_effort WHERE org_id = ? AND categorization_run_id = 'failed-donor-read'`, hierarchyCascadeTestOrg).Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("failed donor read wrote %d allocation rows", writes)
	}

}

func mustTeamOwnershipExec(t *testing.T, ctx context.Context, conn driver.Conn, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("seed team ownership fixture: %v", err)
	}
}
