package server

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/workgraph"
)

// The shared bound is /query's own derivation (CHAOS-9126, D5879).
func TestSharedResultRowBoundIsTheQueryRoutesDerivation(t *testing.T) {
	if want := uint(4*workgraph.MaxEdgesLimit + 100_000); chclient.MaxResultRows != want {
		t.Fatalf("chclient.MaxResultRows = %d, want %d (4 x MaxEdgesLimit + 100,000)", chclient.MaxResultRows, want)
	}
	if queryRouteMaxResultRows != chclient.MaxResultRows {
		t.Fatalf("/query bound %d != shared bound %d", queryRouteMaxResultRows, chclient.MaxResultRows)
	}
	opts := newUnrestrictedReadClickHouseOptions("clickhouse://x")
	if opts.MaxResultRows == nil || *opts.MaxResultRows != chclient.MaxResultRows {
		t.Fatalf("REST client options carry MaxResultRows %v, want the shared bound", opts.MaxResultRows)
	}
	if opts.MaxBytesToRead == nil || *opts.MaxBytesToRead != 0 {
		t.Fatalf("MaxBytesToRead = %v, want a pointer to 0", opts.MaxBytesToRead)
	}
}
