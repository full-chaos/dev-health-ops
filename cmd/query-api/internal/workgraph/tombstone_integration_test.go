//go:build integration

package workgraph

import (
	"context"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWorkGraphReadersHideTombstonedEdges seeds three edge identities as
// separate inserts so their versions stay in separate parts: one live, one
// whose latest version is a tombstone, and one re-created after a tombstone.
// The edge list, flow and artifacts readers must all see exactly the live and
// the re-created edge.
func TestWorkGraphReadersHideTombstonedEdges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const orgID = "org-tombstone-readers"
	t1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)

	insert := func(edgeID, sourceID, targetID string, lastSynced time.Time, isDeleted uint8) {
		t.Helper()
		if err := admin.Exec(ctx, `
            INSERT INTO work_graph_edges (
                edge_id, source_type, source_id, target_type, target_id, edge_type,
                provenance, confidence, evidence,
                discovered_at, last_synced, event_ts, org_id, is_deleted
            ) VALUES (?, 'issue', ?, 'pr', ?, 'implements', 'native', 0.9, 'seed', ?, ?, ?, ?, ?)
        `, edgeID, sourceID, targetID, t1, lastSynced, t1, orgID, isDeleted); err != nil {
			t.Fatalf("insert %s @%s deleted=%d: %v", edgeID, lastSynced, isDeleted, err)
		}
	}
	insert("edge-live", "issue:LIVE-1", "pr:owner/repo#1", t1, 0)
	insert("edge-retired", "issue:RETIRED-1", "pr:owner/repo#2", t1, 0)
	insert("edge-retired", "issue:RETIRED-1", "pr:owner/repo#2", t2, 1)
	insert("edge-recreated", "issue:RECREATED-1", "pr:owner/repo#3", t1, 0)
	insert("edge-recreated", "issue:RECREATED-1", "pr:owner/repo#3", t2, 1)
	insert("edge-recreated", "issue:RECREATED-1", "pr:owner/repo#3", t3, 0)

	// The retired identity is still stored: a reader without the tombstone
	// filter would return it, so the assertions below observe the filter.
	var storedRetired uint64
	if err := admin.QueryRow(ctx,
		`SELECT count() FROM work_graph_edges FINAL WHERE org_id = ? AND edge_id = 'edge-retired'`, orgID,
	).Scan(&storedRetired); err != nil {
		t.Fatal(err)
	}
	if storedRetired != 1 {
		t.Fatalf("retired identity has %d FINAL rows, want its tombstone stored", storedRetired)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	filters := &model.WorkGraphEdgeFilterInput{Limit: 1000}

	edgeRows, err := fetchDedupedEdgeRows(ctx, client, orgID, newFilterScope(filters, nil), 1000)
	if err != nil {
		t.Fatalf("fetchDedupedEdgeRows: %v", err)
	}
	var edgeIDs []string
	for _, row := range edgeRows {
		edgeIDs = append(edgeIDs, row.edgeID)
	}
	sort.Strings(edgeIDs)
	if len(edgeIDs) != 2 || edgeIDs[0] != "edge-live" || edgeIDs[1] != "edge-recreated" {
		t.Fatalf("edge list = %v, want [edge-live edge-recreated]", edgeIDs)
	}

	flow, err := ResolveFlow(ctx, client, orgID, filters)
	if err != nil {
		t.Fatalf("ResolveFlow: %v", err)
	}
	inflow, outflow := 0, 0
	for _, row := range flow.Rows {
		inflow += row.Inflow
		outflow += row.Outflow
	}
	if inflow != 2 || outflow != 2 {
		t.Fatalf("flow inflow=%d outflow=%d (%+v), want 2 and 2", inflow, outflow, flow.Rows)
	}

	artifacts, err := ResolveArtifacts(ctx, client, orgID, filters)
	if err != nil {
		t.Fatalf("ResolveArtifacts: %v", err)
	}
	var nodes []string
	for _, row := range artifacts.Rows {
		if row.Degree != 1 {
			t.Fatalf("artifact %s degree = %d, want 1", row.NodeID, row.Degree)
		}
		nodes = append(nodes, row.NodeID)
	}
	sort.Strings(nodes)
	want := []string{"issue:LIVE-1", "issue:RECREATED-1", "pr:owner/repo#1", "pr:owner/repo#3"}
	if len(nodes) != len(want) {
		t.Fatalf("artifact nodes = %v, want %v", nodes, want)
	}
	for index := range want {
		if nodes[index] != want[index] {
			t.Fatalf("artifact nodes = %v, want %v", nodes, want)
		}
	}
}
