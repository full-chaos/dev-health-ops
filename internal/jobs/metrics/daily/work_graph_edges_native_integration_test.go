//go:build integration

package daily

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	wgeOrg  = "00000000-0000-4000-8000-0000000000c0"
	wgeRepo = "00000000-0000-4000-8000-0000000000c1"
)

func wgeClickHouse(ctx context.Context, t *testing.T) driver.Conn {
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
	return conn
}

// TestWorkGraphEdgesDecodesInvalidUTF8LikePython proves the ClickHouse String
// decode (#2240 round 1, P2) against a real driver rather than by inspection.
//
// clickhouse-go scans a String column into a Go string WITHOUT validating it,
// so a column holding invalid UTF-8 arrives as raw bytes. Python's driver has
// already hex-encoded the same value by the time the extractor sees it. That
// value feeds edge_id's sha256, so without the decode the two planes derive
// DIFFERENT ids for the same row -- and nothing downstream can notice, because
// both ids are well-formed hex of the right length.
//
// Fixture is `unhex('61ff62')` = a\xffb: real bytes, invalid UTF-8, short
// enough that the expected value is readable in a failure message.
func TestWorkGraphEdgesDecodesInvalidUTF8LikePython(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := wgeClickHouse(ctx, t)

	if err := conn.Exec(ctx, `CREATE TABLE deployments (
    repo_id UUID, deployment_id String, pull_request_number Nullable(UInt32),
    started_at Nullable(DateTime64(3,'UTC')), finished_at Nullable(DateTime64(3,'UTC')),
    deployed_at Nullable(DateTime64(3,'UTC')), last_synced DateTime64(3,'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, deployment_id)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO deployments
  (repo_id, deployment_id, pull_request_number, started_at, finished_at, deployed_at, last_synced, org_id)
SELECT toUUID('`+wgeRepo+`'), unhex('61ff62'), toNullable(toUInt32(101)),
       NULL, NULL, toDateTime64('2026-09-03 14:00:00',3,'UTC'),
       toDateTime64('2026-09-03 23:00:00',3,'UTC'), '`+wgeOrg+`'`); err != nil {
		t.Fatal(err)
	}

	// POSITIVE CONTROL. A fixture that turned out to be valid UTF-8 would make
	// this test pass while exercising nothing -- the failure mode that has bitten
	// this lane repeatedly.
	var validUTF8 uint8
	if err := conn.QueryRow(ctx,
		`SELECT isValidUTF8(deployment_id) FROM deployments WHERE org_id = ?`, wgeOrg,
	).Scan(&validUTF8); err != nil {
		t.Fatal(err)
	}
	if validUTF8 != 0 {
		t.Fatal("fixture deployment_id is VALID UTF-8, so this test cannot exercise the decode")
	}

	start := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	rows, err := LoadWorkGraphEdgeDeployments(
		ctx, conn, wgeOrg, []uuid.UUID{uuid.MustParse(wgeRepo)}, start, start.Add(24*time.Hour),
	)
	if err != nil {
		t.Fatalf("load deployments: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 deployment row, got %d", len(rows))
	}
	if got, want := rows[0].DeploymentID, "61ff62"; got != want {
		t.Errorf("deployment_id: got %q, want %q.\nPython's driver yields the hex form, so "+
			"without DecodeClickHouseStringValue the raw bytes reach _hash and edge_id diverges "+
			"from Python for this row -- silently, since both ids look well-formed.", got, want)
	}
}

// TestWorkGraphEdgesPartialWriteAgainstRealClickHouse exercises the
// fail-after-the-first-table case end to end (#2240 round 1, P2).
//
// The failure is induced STRUCTURALLY: the review-outcome table exists and the
// deployment table does not, so the first Send succeeds against a real server
// and the second fails inside the real driver. That is closer to production
// than a double returning an error on command, and it removes any argument
// about what a fake "should" do.
//
// It then asserts the rows really are in ClickHouse -- which is the fact that
// makes a compatibility-bridge rewrite a duplication rather than a repair, and
// therefore the reason the family must be skipped rather than failed open.
func TestWorkGraphEdgesPartialWriteAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := wgeClickHouse(ctx, t)

	// ONLY the review table. work_graph_pr_deployment_edges is deliberately
	// absent so the SECOND write fails after the first has landed.
	if err := conn.Exec(ctx, `CREATE TABLE work_graph_pr_review_outcome_edges (
    edge_id String, org_id UUID, pr_id String, review_outcome_id String,
    outcome Nullable(String), provider LowCardinality(String), repo_id Nullable(UUID),
    confidence Float32, source LowCardinality(String), evidence String,
    observed_at DateTime64(3,'UTC'), computed_at DateTime64(3,'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(computed_at) ORDER BY (org_id, pr_id, review_outcome_id, source)`); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	orgUUID := uuid.MustParse(wgeOrg)
	repoUUID := uuid.MustParse(wgeRepo)

	reviewWritten, err := WriteWorkGraphPRReviewOutcomeEdges(ctx, conn,
		[]workgraphedges.PRReviewOutcomeEdge{{
			EdgeID: "edge-review-1", OrgID: orgUUID, PRID: wgeRepo + ":101",
			ReviewOutcomeID: "review-1", Provider: "github", RepoID: &repoUUID,
			Confidence: 1.0, Source: "native", Evidence: `{"review_id":"review-1"}`,
			ObservedAt: now,
		}}, now)
	if err != nil {
		t.Fatalf("the FIRST write must succeed for this test to mean anything: %v", err)
	}
	if reviewWritten == 0 {
		t.Fatal("first write reported 0 rows, so the partial case cannot arise")
	}

	_, deploymentErr := WriteWorkGraphPRDeploymentEdges(ctx, conn,
		[]workgraphedges.PRDeploymentEdge{{
			EdgeID: "edge-deploy-1", OrgID: orgUUID, PRID: wgeRepo + ":101",
			DeploymentID: "dep-1", Provider: "github", RepoID: &repoUUID,
			Confidence: 1.0, Source: "native", Evidence: `{"deployment_id":"dep-1"}`,
			ObservedAt: now,
		}}, now)
	if deploymentErr == nil {
		t.Fatal("the deployment write must FAIL (its table does not exist); fixture is wrong")
	}

	rows, wrapped := wrapWorkGraphEdgesPartialWrite(reviewWritten, 2, deploymentErr)
	if !errors.Is(wrapped, ErrPartialWrite) {
		t.Errorf("a real mid-sequence failure must wrap ErrPartialWrite so the bridge is "+
			"skipped rather than rewriting these tables; got %v", wrapped)
	}
	if !errors.Is(wrapped, deploymentErr) {
		t.Errorf("the driver's own error must survive wrapping; got %v", wrapped)
	}
	if rows != reviewWritten {
		t.Errorf("rows-written: got %d, want %d (the count that actually landed)", rows, reviewWritten)
	}

	var persisted uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM work_graph_pr_review_outcome_edges WHERE org_id = ?`, wgeOrg,
	).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != uint64(reviewWritten) {
		t.Errorf("ClickHouse holds %d review edges but the executor reported %d written; "+
			"the skip decision depends on that count being true", persisted, reviewWritten)
	}
}

// TestWorkGraphEdgesPartialWriteNamesEachTableByPosition pins the operator-facing
// message at EVERY write position, not just the one the ClickHouse test happens
// to exercise.
//
// It exists because the index is positional: a fourth write inserted in the
// middle renumbers the ones after it, and the ONLY thing that still needs a
// human is adding the new table's name to workGraphEdgesWriteOrder. If someone
// inserts a write and forgets the name, this test fails here rather than
// shipping a message that names the wrong table -- which would be worse than no
// name at all, since a wrong table sends an operator to the wrong data.
func TestWorkGraphEdgesPartialWriteNamesEachTableByPosition(t *testing.T) {
	cause := errors.New("clickhouse: connection reset")
	total := len(workGraphEdgesWriteOrder)

	for step, want := range []string{
		"work_graph_pr_review_outcome_edges",
		"work_graph_pr_deployment_edges",
		"work_graph_deployment_incident_edges",
	} {
		position := step + 1
		_, err := wrapWorkGraphEdgesPartialWrite(11, position, cause)
		msg := err.Error()
		if !strings.Contains(msg, want) {
			t.Errorf("write %d: message does not name %q: %s", position, want, msg)
		}
		if fragment := fmt.Sprintf("write %d of %d", position, total); !strings.Contains(msg, fragment) {
			t.Errorf("write %d: message does not carry %q: %s", position, fragment, msg)
		}
		if !strings.Contains(msg, "11 row(s) landed") {
			t.Errorf("write %d: message drops the true row count: %s", position, msg)
		}
	}

	// NEGATIVE CONTROL. A step past the end means a write was added without its
	// name. The message must SAY so rather than silently naming the last table
	// or panicking, so this asserts the loud marker and asserts the absence of
	// any real table name.
	_, err := wrapWorkGraphEdgesPartialWrite(11, total+1, cause)
	if !strings.Contains(err.Error(), "UNREGISTERED TABLE") {
		t.Errorf("an out-of-range write position must be called out loudly, got: %v", err)
	}
	for _, name := range workGraphEdgesWriteOrder {
		if strings.Contains(err.Error(), name) {
			t.Errorf("out-of-range position named a real table (%q), which would mislead an operator: %v", name, err)
		}
	}
}
