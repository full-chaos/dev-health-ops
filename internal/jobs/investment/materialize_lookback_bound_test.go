package investment

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// TestDefaultWindowDaysMatchesTheEnqueueTimeLookbackBound pins the two halves
// of one number to each other.
//
// defaultWindowDays here is what the executor applies to a materialize scope
// that names no start. workgraph.MaxMaterializeLookbackDays is what the
// request writer WRITES INTO such a scope so it stops being start-less. They
// are the same rule expressed at two moments, and they are two constants
// rather than one only because the dependency runs one way: this package
// imports workgraph, never the reverse.
//
// Divergence would be silent and would reopen the exact gap the enqueue-time
// bound closes. If the writer pinned 30 days while the executor defaulted to
// 90, every start-less request would be narrowed by the bound without anyone
// asking for it; if the writer pinned 90 while the executor defaulted to 30,
// the bound would be decorative and the scope it wrote would name a window the
// executor never runs. Either way the request would stop describing its own
// work, which is the property coalescing depends on.
func TestDefaultWindowDaysMatchesTheEnqueueTimeLookbackBound(t *testing.T) {
	if defaultWindowDays != workgraph.MaxMaterializeLookbackDays {
		t.Fatalf("defaultWindowDays = %d, workgraph.MaxMaterializeLookbackDays = %d; "+
			"the execution-time default and the enqueue-time bound must be the same window",
			defaultWindowDays, workgraph.MaxMaterializeLookbackDays)
	}
}
