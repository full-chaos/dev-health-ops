//go:build integration

package operationaledges

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// operationalSchemaDDL creates exactly the tables BuildOperationalIncidentEdges
// and BuildFlagGuardsEdges read/write, matching the LIVE production column
// set (066_operational_canonical.sql / 014_work_graph.sql /
// 034_feature_flag_user_impact_tables.sql) -- not the full migration chain,
// the same "inline the DDL this test actually needs" shape
// ai_impact_native_integration_test.go uses for its own tables.
var operationalSchemaDDL = []string{
	`CREATE TABLE operational_services (
    org_id String, provider LowCardinality(String), provider_instance_id String,
    source_entity_type LowCardinality(String), external_id String,
    source_version_at DateTime64(6, 'UTC'), id String,
    source_url Nullable(String), observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    name String, owning_team_id Nullable(String), escalation_policy_id Nullable(String), is_deleted UInt8
) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)`,
	`CREATE TABLE operational_incidents (
    org_id String, provider LowCardinality(String), provider_instance_id String,
    source_entity_type LowCardinality(String), external_id String,
    source_version_at DateTime64(6, 'UTC'), id String, source_url Nullable(String),
    observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    service_id Nullable(String), escalation_policy_id Nullable(String), title String,
    started_at Nullable(DateTime64(6, 'UTC')), is_deleted UInt8
) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)`,
	`CREATE TABLE operational_service_repository_mappings (
    org_id String, provider LowCardinality(String), provider_instance_id String,
    source_entity_type LowCardinality(String), external_id String,
    source_version_at DateTime64(6, 'UTC'), id String, source_url Nullable(String),
    observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    service_id String, repo_id Nullable(UUID), mapping_kind Nullable(String), rule_id Nullable(String),
    valid_from Nullable(DateTime64(6, 'UTC')), valid_to Nullable(DateTime64(6, 'UTC')), is_active UInt8
) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)`,
	`CREATE TABLE operational_alerts (
    org_id String, provider LowCardinality(String), provider_instance_id String,
    source_entity_type LowCardinality(String), external_id String,
    source_version_at DateTime64(6, 'UTC'), id String, source_url Nullable(String),
    observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    incident_id Nullable(String), title String, triggered_at Nullable(DateTime64(6, 'UTC')),
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)`,
	`CREATE TABLE operational_incident_timeline_events (
    org_id String, provider LowCardinality(String), provider_instance_id String,
    source_entity_type LowCardinality(String), external_id String,
    source_version_at DateTime64(6, 'UTC'), id String, source_url Nullable(String),
    observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    incident_id String, event_type String, body Nullable(String), actor_id Nullable(String),
    occurred_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)`,
	`CREATE TABLE operational_incident_notes (
    org_id String, provider LowCardinality(String), provider_instance_id String,
    source_entity_type LowCardinality(String), external_id String,
    source_version_at DateTime64(6, 'UTC'), id String, source_url Nullable(String),
    observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    incident_id String, body String, author_user_id Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)`,
	`CREATE TABLE operational_incident_responders (
    org_id String, provider LowCardinality(String), provider_instance_id String,
    source_entity_type LowCardinality(String), external_id String,
    source_version_at DateTime64(6, 'UTC'), id String, source_url Nullable(String),
    observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    incident_id String, user_id Nullable(String), assigned_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)`,
	`CREATE TABLE work_items (
    org_id String, work_item_id String, title Nullable(String), description Nullable(String)
) ENGINE = ReplacingMergeTree() ORDER BY (org_id, work_item_id)`,
	`CREATE TABLE repos (
    org_id String, id UUID, repo String
) ENGINE = ReplacingMergeTree() ORDER BY (org_id, id)`,
	`CREATE TABLE deployments (
    org_id String, repo_id UUID, deployment_id String, environment String,
    deployed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree() ORDER BY (org_id, repo_id, deployment_id)`,
	`CREATE TABLE feature_flag (
    org_id String DEFAULT 'default', provider String, flag_key String, project_key String,
    repo_id String, environment String, flag_type String,
    created_at DateTime64(3, 'UTC'), archived_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, provider, project_key, flag_key)`,
	`CREATE TABLE feature_flag_link (
    org_id String DEFAULT 'default', flag_key String, target_type String, target_id String,
    provider String, link_source String, link_type String, evidence_type String,
    confidence Float32, valid_from Nullable(DateTime64(3, 'UTC')),
    valid_to Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, flag_key, target_type, target_id)`,
	`CREATE TABLE work_graph_edges (
    edge_id String, source_type String, source_id String, target_type String, target_id String,
    edge_type String, repo_id Nullable(UUID), provider Nullable(String), provenance String,
    confidence Float32, evidence String, discovered_at DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (source_type, source_id, edge_type, target_type, target_id)`,
}

// goldenEdge mirrors one entry of *_python_golden.json's operational_incident_edges
// / flag_guards_edges arrays.
type goldenEdge struct {
	EdgeID     string  `json:"edge_id"`
	SourceType string  `json:"source_type"`
	SourceID   string  `json:"source_id"`
	TargetType string  `json:"target_type"`
	TargetID   string  `json:"target_id"`
	EdgeType   string  `json:"edge_type"`
	Provenance string  `json:"provenance"`
	Confidence float32 `json:"confidence"`
	Evidence   string  `json:"evidence"`
	RepoID     *string `json:"repo_id"`
	Provider   *string `json:"provider"`
}

type goldenFile struct {
	OrgID                    string       `json:"org_id"`
	OperationalIncidentEdges []goldenEdge `json:"operational_incident_edges"`
	FlagGuardsEdges          []goldenEdge `json:"flag_guards_edges"`
}

func loadGolden(t *testing.T, path string) goldenFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var g goldenFile
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatalf("parse golden %s: %v", path, err)
	}
	return g
}

// comparableEdge strips the two fields known to be batch-clock-stamped
// (DiscoveredAt/LastSynced -- see BuildOperationalIncidentEdges' own doc
// comment) so a Go edges.Row and a goldenEdge can be compared on everything
// else.
type comparableEdge struct {
	EdgeID, SourceType, SourceID, TargetType, TargetID, EdgeType, Provenance, Evidence string
	Confidence                                                                         float32
	RepoID, Provider                                                                   string
}

func fromRow(r edges.Row) comparableEdge {
	c := comparableEdge{
		EdgeID: r.EdgeID, SourceType: r.SourceType, SourceID: r.SourceID,
		TargetType: r.TargetType, TargetID: r.TargetID, EdgeType: r.EdgeType,
		Provenance: r.Provenance, Evidence: r.Evidence, Confidence: r.Confidence,
	}
	if r.RepoID != nil {
		c.RepoID = r.RepoID.String()
	}
	if r.Provider != nil {
		c.Provider = *r.Provider
	}
	return c
}

func fromGolden(g goldenEdge) comparableEdge {
	c := comparableEdge{
		EdgeID: g.EdgeID, SourceType: g.SourceType, SourceID: g.SourceID,
		TargetType: g.TargetType, TargetID: g.TargetID, EdgeType: g.EdgeType,
		Provenance: g.Provenance, Evidence: g.Evidence, Confidence: g.Confidence,
	}
	if g.RepoID != nil {
		c.RepoID = *g.RepoID
	}
	if g.Provider != nil {
		c.Provider = *g.Provider
	}
	return c
}

// assertEdgesMatchGolden asserts got (the Go port's output) and want (the
// golden's edges) are identical modulo DiscoveredAt/LastSynced, and asserts
// those two fields separately equal the run's own clock -- the documented
// batch-clock divergence, checked explicitly rather than silently ignored.
func assertEdgesMatchGolden(t *testing.T, got []edges.Row, want []goldenEdge, now time.Time) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("edge count mismatch: got %d, golden has %d", len(got), len(want))
	}
	gotComparable := make([]comparableEdge, len(got))
	for i, r := range got {
		gotComparable[i] = fromRow(r)
		if !r.DiscoveredAt.Equal(now) || !r.LastSynced.Equal(now) {
			t.Errorf("edge %s: DiscoveredAt/LastSynced must equal the run clock %v, got %v/%v",
				r.EdgeID, now, r.DiscoveredAt, r.LastSynced)
		}
	}
	wantComparable := make([]comparableEdge, len(want))
	for i, g := range want {
		wantComparable[i] = fromGolden(g)
	}
	sort.Slice(gotComparable, func(i, j int) bool { return gotComparable[i].EdgeID < gotComparable[j].EdgeID })
	sort.Slice(wantComparable, func(i, j int) bool { return wantComparable[i].EdgeID < wantComparable[j].EdgeID })
	for i := range gotComparable {
		if gotComparable[i] != wantComparable[i] {
			t.Errorf("edge %d mismatch:\n  got:  %+v\n  want: %+v", i, gotComparable[i], wantComparable[i])
		}
	}
}

func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "tests", "fixtures", name))
	if err != nil {
		t.Fatalf("resolve fixture path %s: %v", name, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fixture %s not found at %s: %v", name, path, err)
	}
	return path
}

// splitSQLStatements strips `--` comment LINES first, then splits what
// remains on semicolons. Stripping comments before splitting matters: a
// header comment's PROSE can legitimately contain a semicolon (this
// package's own committed seed files do -- "not any shared org; e.g. ..."),
// and a naive split-then-skip-comment-chunks approach breaks on exactly that,
// severing a comment mid-sentence into a chunk that no longer starts with
// `--` and gets executed as SQL. Committed seed files use `--` line comments
// only (no `/* */` blocks, no semicolon inside a string literal), so this
// stays a fixture loader rather than a general SQL parser.
func splitSQLStatements(sql string) []string {
	var codeLines []string
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		codeLines = append(codeLines, line)
	}
	raw := strings.Split(strings.Join(codeLines, "\n"), ";")
	out := make([]string, 0, len(raw))
	for _, stmt := range raw {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func execSQLFile(t *testing.T, ctx context.Context, conn driver.Conn, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read seed %s: %v", path, err)
	}
	for _, statement := range splitSQLStatements(string(data)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("exec seed statement from %s: %v\nstatement: %s", path, err, statement)
		}
	}
}

func newSchemaConn(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	for _, statement := range operationalSchemaDDL {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("apply schema statement: %v\nstatement: %s", err, statement)
		}
	}
	return conn
}

// TestBuildOperationalIncidentEdgesMatchesOrg70d529e0Golden replays the
// minimal real-data seed (seed_workgraph_operational_edges_org_70d529e0.sql)
// that reproduces org 70d529e0's own golden byte-for-byte (proven by hand
// against the live local stack before this seed was committed -- see the
// seed file's own header) and asserts the Go port matches it.
func TestBuildOperationalIncidentEdgesMatchesOrg70d529e0Golden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := newSchemaConn(t, ctx)
	execSQLFile(t, ctx, conn, fixturePath(t, "seed_workgraph_operational_edges_org_70d529e0.sql"))

	golden := loadGolden(t, fixturePath(t, "workgraph_operational_edges_python_golden.json"))
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	got, err := BuildOperationalIncidentEdges(ctx, conn, golden.OrgID, now, 7, 0.3, nil, nil, nil)
	if err != nil {
		t.Fatalf("BuildOperationalIncidentEdges: %v", err)
	}
	assertEdgesMatchGolden(t, got, golden.OperationalIncidentEdges, now)
}

// TestBuildOperationalEdgesMatchSyntheticGolden replays the synthetic seed
// (seed_workgraph_operational_edges_synthetic.sql) that closes the coverage
// gap the real-org golden documented -- maps_to_repository/repo_id-non-null,
// linked_incident, has_alert, has_responder, remediated_by,
// references(pr)/references(issue), and a real GUARDS edge -- and the
// CHAOS-4269 case (one mapping chain with valid_from NULL, absent from
// Python's own golden for that reason; the OTHER chain, valid_from set, IS
// in both).
func TestBuildOperationalEdgesMatchSyntheticGolden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := newSchemaConn(t, ctx)
	execSQLFile(t, ctx, conn, fixturePath(t, "seed_workgraph_operational_edges_synthetic.sql"))

	golden := loadGolden(t, fixturePath(t, "workgraph_operational_edges_synthetic_python_golden.json"))
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	incidentEdges, err := BuildOperationalIncidentEdges(ctx, conn, golden.OrgID, now, 7, 0.3, nil, nil, nil)
	if err != nil {
		t.Fatalf("BuildOperationalIncidentEdges: %v", err)
	}

	// CHAOS-4269: svc-1/map-1's valid_from is NULL. Python's own unguarded
	// predicate drops that mapping, so its maps_to_repository/linked_incident
	// edges are ABSENT from golden.OperationalIncidentEdges by construction
	// (see the seed file's header) -- the Go port's NULL-OK-guarded read must
	// still produce them. Assert presence directly rather than folding this
	// into the byte-identity comparison, which would otherwise fail on a
	// count mismatch for the wrong reason.
	// Python's golden does NOT include these two -- its unguarded predicate
	// drops the NULL-valid_from mapping before either edge can be derived
	// (see the synthetic seed file's header). Split them out of got before
	// the byte-identity comparison below: the golden's count is Python's
	// count, and the Go port is EXPECTED to have exactly these two more.
	const repoAA = "c4924000-0000-0000-0000-0000000000aa"
	var chaos4269Edges, remaining []edges.Row
	for _, e := range incidentEdges {
		isChaos4269Edge := (e.EdgeType == edges.EdgeTypeMapsToRepository && e.TargetID == repoAA) ||
			(e.EdgeType == edges.EdgeTypeLinkedIncident && e.RepoID != nil && e.RepoID.String() == repoAA)
		if isChaos4269Edge {
			chaos4269Edges = append(chaos4269Edges, e)
		} else {
			remaining = append(remaining, e)
		}
	}
	if len(chaos4269Edges) != 2 {
		t.Fatalf("CHAOS-4269 fix regression: expected exactly 2 edges (maps_to_repository + "+
			"linked_incident) for repo %s (valid_from IS NULL, absent from Python's own golden "+
			"by design) -- the Go port's NULL-OK guard should see this mapping. Got %d: %+v",
			repoAA, len(chaos4269Edges), chaos4269Edges)
	}

	// Everything else must match Python's golden byte-for-byte -- this is
	// where the svc-2/map-2 chain (valid_from set, both planes agree) proves
	// the divergence above is scoped to the NULL case, not a general drift.
	assertEdgesMatchGolden(t, remaining, golden.OperationalIncidentEdges, now)

	flagEdges, flagLinks, err := BuildFlagGuardsEdges(ctx, conn, golden.OrgID, now)
	if err != nil {
		t.Fatalf("BuildFlagGuardsEdges: %v", err)
	}
	assertEdgesMatchGolden(t, flagEdges, golden.FlagGuardsEdges, now)
	if len(flagLinks) != 1 {
		t.Errorf("expected 1 feature_flag_link row, got %d", len(flagLinks))
	}
}
