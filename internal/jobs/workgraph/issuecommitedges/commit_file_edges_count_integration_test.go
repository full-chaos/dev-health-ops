//go:build integration

package issuecommitedges_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuecommitedges"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// countGolden is the frozen readback from the last Python producer of this
// count, `_count_commit_file_edges` (builder.py:1569-1583, deleted at the
// commit_file_edges native cutover): the same `SELECT count(*) AS total FROM
// git_commit_stats` text, with the same optional `WHERE ... org_id = ...`
// clause, run once against a real ClickHouse instance seeded with
// seed_rows -- captured to prove Go's identical-shape query returns the
// identical answer, not merely that it compiles.
type countGolden struct {
	OrgA string `json:"org_a"`
	OrgB string `json:"org_b"`
	Seed []struct {
		OrgID      string `json:"org_id"`
		RepoID     string `json:"repo_id"`
		CommitHash string `json:"commit_hash"`
		FilePath   string `json:"file_path"`
	} `json:"seed_rows"`
	Counts struct {
		OrgA     int `json:"org_a"`
		OrgB     int `json:"org_b"`
		Unscoped int `json:"unscoped"`
	} `json:"counts"`
}

func loadCountGolden(t *testing.T) countGolden {
	t.Helper()
	path := filepath.Join(repositoryRoot(t), "tests", "fixtures", "commit_file_edges_count_python_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document countGolden
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

// TestCountCommitFileEdgesMatchesFrozenPythonGolden seeds git_commit_stats
// with the golden's own rows -- four under one org, two under a second -- and
// asserts CountCommitFileEdges reproduces the frozen Python readback for the
// org-scoped, foreign-org, and unscoped calls alike.
func TestCountCommitFileEdgesMatchesFrozenPythonGolden(t *testing.T) {
	ctx := context.Background()
	golden := loadCountGolden(t)
	conn := countCommitFileEdgesConnect(ctx, t)

	for _, row := range golden.Seed {
		if err := conn.Exec(ctx,
			`INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, old_file_mode, new_file_mode, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			row.OrgID, row.RepoID, row.CommitHash, row.FilePath, 1, 0, "unknown", "unknown", golden.mustSyncedAt(),
		); err != nil {
			t.Fatalf("seed git_commit_stats: %v", err)
		}
	}

	if got, err := issuecommitedges.CountCommitFileEdges(ctx, conn, golden.OrgA); err != nil || got != golden.Counts.OrgA {
		t.Errorf("CountCommitFileEdges(org_a) = %d, %v, want %d, nil", got, err, golden.Counts.OrgA)
	}
	if got, err := issuecommitedges.CountCommitFileEdges(ctx, conn, golden.OrgB); err != nil || got != golden.Counts.OrgB {
		t.Errorf("CountCommitFileEdges(org_b) = %d, %v, want %d, nil", got, err, golden.Counts.OrgB)
	}
	if got, err := issuecommitedges.CountCommitFileEdges(ctx, conn, ""); err != nil || got != golden.Counts.Unscoped {
		t.Errorf("CountCommitFileEdges(\"\") = %d, %v, want %d, nil", got, err, golden.Counts.Unscoped)
	}
}

// mustSyncedAt is a fixed, arbitrary last_synced -- the count query never
// filters or orders on it, so any value seeds a valid row.
func (countGolden) mustSyncedAt() string { return "2020-01-01 00:00:00" }

func countCommitFileEdgesConnect(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	opts, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
