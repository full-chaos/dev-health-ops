package issuecommitedges_test

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuecommitedges"
)

// TestCountCommitFileEdgesReturnsErrUnavailableForNilConnection mirrors
// Loader's own nil-dependency guard: a caller that failed to wire ClickHouse
// gets ErrUnavailable, never a nil-pointer panic.
func TestCountCommitFileEdgesReturnsErrUnavailableForNilConnection(t *testing.T) {
	if _, err := issuecommitedges.CountCommitFileEdges(context.Background(), nil, "any-org"); err != issuecommitedges.ErrUnavailable {
		t.Fatalf("CountCommitFileEdges(nil conn) error = %v, want %v", err, issuecommitedges.ErrUnavailable)
	}
}
