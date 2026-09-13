//go:build integration

package edges

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The delete path this package ran before tombstones, kept verbatim so the
// parity test replays the old behaviour rather than a paraphrase of it.
const (
	legacyDeleteEdgesByIDSQL = `
        ALTER TABLE work_graph_edges DELETE WHERE
        org_id = {org_id:String} AND edge_id IN {edge_ids:Array(String)}
        SETTINGS mutations_sync=2
`
	legacyStaleDependencyIssueEdgesDeleteSQL = `
        ALTER TABLE work_graph_edges DELETE WHERE
        source_type = 'issue' AND target_type = 'issue' AND evidence = 'linear_attachment'
        AND startsWith(target_id, 'linear:')
        AND (startsWith(source_id, 'ghpr:') OR startsWith(source_id, 'gitlab:'))
        AND org_id = {org_id:String}
        SETTINGS mutations_sync=2
`
)

// liveEdgeReaderClause is the query-api / Python GraphQL raw-reader predicate.
const liveEdgeReaderClause = `is_deleted = 0 AND (org_id, source_type, source_id, edge_type, target_type, target_id) NOT IN (
                SELECT org_id, source_type, source_id, edge_type, target_type, target_id
                FROM work_graph_edges
                WHERE org_id = {org_id:String}
                GROUP BY org_id, source_type, source_id, edge_type, target_type, target_id
                HAVING argMax(is_deleted, last_synced) = 1
            )`

type parityPlane struct {
	name string
	conn driver.Conn
}

func startParityPlane(ctx context.Context, t *testing.T, name string) parityPlane {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	chschema.Apply(ctx, t, instance)
	return parityPlane{name: name, conn: conn}
}

// legacyBuild is issueIssueEdgesPreStep.Run's edge sequence as it was, preceded
// by the stale PR-dependency pre-step.
func legacyBuild(ctx context.Context, t *testing.T, conn driver.Conn, org string, dependencies []DependencyRow, clock time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, legacyStaleDependencyIssueEdgesDeleteSQL, clickhouse.Named("org_id", org)); err != nil {
		t.Fatalf("legacy stale delete: %v", err)
	}
	derived, err := DeriveIssueIssueEdges(dependencies, clock)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := ReadExistingBlockerEdgeIDs(ctx, conn, org)
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range BuildCleanupPlan(dependencies, existing).Pages {
		if err := conn.Exec(ctx, legacyDeleteEdgesByIDSQL,
			clickhouse.Named("org_id", org), clickhouse.Named("edge_ids", page)); err != nil {
			t.Fatalf("legacy edge delete: %v", err)
		}
	}
	if _, err := WriteEdges(ctx, conn, org, derived.Edges); err != nil {
		t.Fatal(err)
	}
}

// tombstoneBuild is the same sequence as the pre-steps now run it. It returns
// how many tombstones it wrote and how many re-writes it had to stamp past a
// stored version, so the scenario can prove each branch was exercised.
func tombstoneBuild(ctx context.Context, t *testing.T, conn driver.Conn, org string, dependencies []DependencyRow, clock time.Time) (tombstones, stamped int) {
	t.Helper()
	staleVersions, err := readEdgeVersions(ctx, conn, org, stalePRDependencyIdentitySQL)
	if err != nil {
		t.Fatal(err)
	}
	tombstones += len(PlanStalePRDependencyTombstones(staleVersions, clock))
	if err := DeleteStalePRDependencyIssueEdges(ctx, conn, org, clock); err != nil {
		t.Fatalf("DeleteStalePRDependencyIssueEdges: %v", err)
	}
	derived, err := DeriveIssueIssueEdges(dependencies, clock)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := ReadExistingBlockerEdgeIDs(ctx, conn, org)
	if err != nil {
		t.Fatal(err)
	}
	plan := BuildCleanupPlan(dependencies, existing)
	versions, err := ReadIssueIssueEdgeVersions(ctx, conn, org)
	if err != nil {
		t.Fatal(err)
	}
	tombstones += len(PlanCleanupTombstones(plan, versions, derived.Edges, clock))
	if err := DeleteEdgesByID(ctx, conn, org, plan, versions, derived.Edges, clock); err != nil {
		t.Fatalf("DeleteEdgesByID: %v", err)
	}
	rewrites := StampRewrites(derived.Edges, plan, versions)
	for index := range rewrites {
		if !rewrites[index].LastSynced.Equal(derived.Edges[index].LastSynced) {
			stamped++
		}
	}
	if _, err := WriteEdges(ctx, conn, org, rewrites); err != nil {
		t.Fatal(err)
	}
	return tombstones, stamped
}

type parityRead struct {
	Deduped    []string
	Final      []string
	RawCounts  []string
	Investment []string
}

func readParity(ctx context.Context, t *testing.T, conn driver.Conn, org string) parityRead {
	t.Helper()
	collect := func(query string) []string {
		t.Helper()
		rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", org))
		if err != nil {
			t.Fatalf("parity read: %v\n%s", err, query)
		}
		defer rows.Close()
		columns := rows.ColumnTypes()
		var out []string
		for rows.Next() {
			values := make([]any, len(columns))
			for index, column := range columns {
				values[index] = reflect.New(column.ScanType()).Interface()
			}
			if err := rows.Scan(values...); err != nil {
				t.Fatal(err)
			}
			line := ""
			for _, value := range values {
				line += fmt.Sprintf("%v|", reflect.ValueOf(value).Elem().Interface())
			}
			out = append(out, line)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	read := parityRead{
		Deduped: collect(`
            SELECT source_type, source_id, edge_type, target_type, target_id,
                   winner.1, winner.2, winner.3, winner.4, ifNull(toString(winner.5), '')
            FROM (
                SELECT source_type, source_id, edge_type, target_type, target_id,
                       argMax(tuple(edge_id, provenance, confidence, evidence, repo_id, is_deleted), last_synced) AS winner
                FROM work_graph_edges
                WHERE org_id = {org_id:String}
                GROUP BY source_type, source_id, edge_type, target_type, target_id
            )
            WHERE winner.6 = 0
            ORDER BY source_type, source_id, edge_type, target_type, target_id`),
		Final: collect(`
            SELECT edge_id, source_type, source_id, edge_type, target_type, target_id, confidence, evidence
            FROM work_graph_edges FINAL
            WHERE org_id = {org_id:String} AND is_deleted = 0
            ORDER BY source_type, source_id, edge_type, target_type, target_id`),
		RawCounts: collect(`
            SELECT source_type, target_type, uniqExact(edge_id)
            FROM work_graph_edges
            WHERE org_id = {org_id:String} AND ` + liveEdgeReaderClause + `
            GROUP BY source_type, target_type
            ORDER BY source_type, target_type`),
	}
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	edges, err := reader.FetchWorkGraphEdges(ctx, chquery.EdgeQueryOptions{OrganizationID: org, IncludeHeuristic: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range edges {
		read.Investment = append(read.Investment, fmt.Sprintf("%+v", edge))
	}
	return read
}

func countMutations(ctx context.Context, t *testing.T, conn driver.Conn) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'work_graph_edges'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// TestTombstoneCleanupMatchesDeleteCleanup is the parity proof for retiring
// work_graph_edges rows with tombstones. One write sequence is replayed through
// the delete path and the tombstone path on two migrated ClickHouse databases,
// and after every build each dedup reader form must return the same edges.
// The tombstone database must end with zero mutations on the table; the delete
// database must not, or the comparison proved nothing.
func TestTombstoneCleanupMatchesDeleteCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	legacy := startParityPlane(ctx, t, "delete")
	tombstone := startParityPlane(ctx, t, "tombstone")
	planes := []parityPlane{legacy, tombstone}

	const org = "70d529e0-3c06-4597-8480-794fd0235707"
	const otherOrg = "70d529e0-3c06-4597-8480-794fd0239999"
	seededAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	blocks := func(source, target string) DependencyRow {
		return DependencyRow{
			SourceWorkItemID: source, TargetWorkItemID: target,
			RelationshipType: "blocks", RelationshipRaw: "blocks",
			LastSynced: time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC),
		}
	}
	relates := func(source, target string) DependencyRow {
		row := blocks(source, target)
		row.RelationshipType, row.RelationshipRaw = "relates", "relates"
		return row
	}
	const (
		a, b = "gh:acme/app#101", "gh:acme/app#102"
		c, d = "gh:acme/app#201", "gh:acme/app#202"
		e, f = "gh:acme/app#301", "gh:acme/app#302"
		g, h = "gh:acme/app#401", "gh:acme/app#402"
	)

	seed := func(org, id, sourceType, sourceID, edgeType, targetType, targetID, evidence string, confidence float32, lastSynced time.Time) {
		t.Helper()
		for _, plane := range planes {
			if err := plane.conn.Exec(ctx, `INSERT INTO work_graph_edges
(edge_id, source_type, source_id, target_type, target_id, edge_type, provenance,
 confidence, evidence, discovered_at, last_synced, event_ts, day, org_id)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				id, sourceType, sourceID, targetType, targetID, edgeType, "native",
				confidence, evidence, seededAt, lastSynced, seededAt, seededAt, org,
			); err != nil {
				t.Fatalf("seed %s on %s: %v", id, plane.name, err)
			}
		}
	}

	// A legacy `relates` orientation of the a->b blocker, an orphan blocker
	// with no dependency row, the stale PR-dependency shape with a control that
	// must survive, an issue->pr edge no cleanup touches, and the same stale
	// shape in another org.
	seed(org, EdgeID(NodeTypeIssue, a, EdgeTypeRelates, NodeTypeIssue, b), NodeTypeIssue, a, EdgeTypeRelates, NodeTypeIssue, b, "legacy-orientation", 0.9, seededAt)
	seed(org, EdgeID(NodeTypeIssue, c, EdgeTypeBlocks, NodeTypeIssue, d), NodeTypeIssue, c, EdgeTypeBlocks, NodeTypeIssue, d, "orphan", 0.9, seededAt)
	seed(org, "stale-ghpr", NodeTypeIssue, "ghpr:acme/app#1", EdgeTypeRelates, NodeTypeIssue, "linear:ACME-1", "linear_attachment", 0.9, seededAt)
	seed(org, "stale-gitlab", NodeTypeIssue, "gitlab:acme/app#2", EdgeTypeRelates, NodeTypeIssue, "linear:ACME-2", "linear_attachment", 0.9, seededAt)
	seed(org, "stale-control", NodeTypeIssue, "ghpr:acme/app#3", EdgeTypeRelates, NodeTypeIssue, "linear:ACME-3", "text_reference", 0.9, seededAt)
	seed(org, "issue-pr", NodeTypeIssue, a, "implements", "pr", "acme/app#pr9", "untouched", 0.8, seededAt)
	seed(otherOrg, "other-org-stale", NodeTypeIssue, "ghpr:acme/app#5", EdgeTypeRelates, NodeTypeIssue, "linear:ACME-5", "linear_attachment", 0.9, seededAt)

	// The e->f blocker is stored AHEAD of every build clock with a different
	// confidence: the delete path clears it before re-writing, so the re-write
	// must win on the tombstone path too.
	futureEF, err := DeriveIssueIssueEdges([]DependencyRow{blocks(e, f)}, seededAt)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range futureEF.Edges {
		seed(org, row.EdgeID, row.SourceType, row.SourceID, row.EdgeType, row.TargetType, row.TargetID,
			"stored-ahead-of-clock", 0.5, time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	}
	ghEdges, err := DeriveIssueIssueEdges([]DependencyRow{blocks(g, h)}, seededAt)
	if err != nil {
		t.Fatal(err)
	}

	liveIDs := func(plane parityPlane) map[string]bool {
		t.Helper()
		ids := map[string]bool{}
		rows, err := plane.conn.Query(ctx,
			`SELECT edge_id FROM work_graph_edges FINAL WHERE org_id = ? AND is_deleted = 0`, org)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids[id] = true
		}
		return ids
	}

	builds := []struct {
		name         string
		dependencies []DependencyRow
		clock        time.Time
		wantTombs    bool
		wantStamped  bool
		ghLive       bool
	}{
		{"initial build clears legacy, orphan and stale rows", []DependencyRow{blocks(a, b), blocks(e, f), blocks(g, h)},
			time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), true, true, true},
		{"removed blockers are retired", []DependencyRow{blocks(a, b)},
			time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), true, false, false},
		{"clock regression re-creates a retired blocker", []DependencyRow{blocks(a, b), blocks(g, h)},
			time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), false, true, true},
		{"clock regression re-creates a retired orientation outside the cleanup", []DependencyRow{relates(a, b), blocks(g, h)},
			time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), true, true, true},
	}
	for _, build := range builds {
		legacyBuild(ctx, t, legacy.conn, org, build.dependencies, build.clock)
		tombs, stamped := tombstoneBuild(ctx, t, tombstone.conn, org, build.dependencies, build.clock)
		if build.wantTombs != (tombs > 0) {
			t.Fatalf("%s: tombstone path wrote %d tombstones, want some=%v", build.name, tombs, build.wantTombs)
		}
		if build.wantStamped != (stamped > 0) {
			t.Fatalf("%s: tombstone path stamped %d re-writes past a stored version, want some=%v", build.name, stamped, build.wantStamped)
		}
		for _, check := range []string{org, otherOrg} {
			want := readParity(ctx, t, legacy.conn, check)
			got := readParity(ctx, t, tombstone.conn, check)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("%s: org %s reads diverge\ndelete path:    %+v\ntombstone path: %+v", build.name, check, want, got)
			}
		}
		for _, plane := range planes {
			ids := liveIDs(plane)
			for _, row := range ghEdges.Edges {
				if ids[row.EdgeID] != build.ghLive {
					t.Fatalf("%s: %s path g->h edge %s live=%v, want %v -- the scenario did not move the edge set",
						build.name, plane.name, row.EdgeID, ids[row.EdgeID], build.ghLive)
				}
			}
		}
	}

	if got := countMutations(ctx, t, tombstone.conn); got != 0 {
		t.Fatalf("tombstone path issued %d mutations on work_graph_edges, want 0", got)
	}
	if got := countMutations(ctx, t, legacy.conn); got == 0 {
		t.Fatal("delete path issued no mutations; the parity comparison did not exercise a delete")
	}
}
