//go:build integration

package pgmigrate_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// archivedBuild is what the build was before the squash: the 0138 baseline and the chain 0139..0162, read from
// archive/ as released.
func archivedBuild(t *testing.T) (pgmigrate.Baseline, []pgmigrate.ChainFile) {
	t.Helper()
	data, err := os.ReadFile("archive/baseline-0138/head.json")
	if err != nil {
		t.Fatal(err)
	}
	var baseline pgmigrate.Baseline
	if err := json.Unmarshal(data, &baseline); err != nil {
		t.Fatalf("decode the archived baseline: %v", err)
	}
	manifest, err := os.ReadFile("archive/chain_manifest.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var chain []pgmigrate.ChainFile
	for _, line := range strings.Split(strings.TrimSuffix(string(manifest), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		sql, err := os.ReadFile("archive/sql/" + fields[2])
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, pgmigrate.ChainFile{Revision: fields[0], Name: fields[2], SQL: string(sql)})
	}
	return baseline, chain
}

// archivedUpgradeTo brings the database behind conn to revision through the chain as it was released.
func archivedUpgradeTo(t *testing.T, ctx context.Context, conn *pgx.Conn, revision string) {
	t.Helper()
	baseline, chain := archivedBuild(t)
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, pgmigrate.ChainThrough(chain, revision)); err != nil {
		t.Fatalf("upgrade to %s: %v", revision, err)
	}
}
