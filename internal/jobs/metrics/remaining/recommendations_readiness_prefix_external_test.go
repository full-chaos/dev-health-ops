package remaining_test

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// TestReadinessPrefixMatchesTheProducer pins the readiness gate's fan-out
// generation prefix to the one the PRODUCER writes.
//
// The gate cannot import internal/jobs/metrics/daily -- that is an import cycle
// (remaining -> daily -> daily/cicd -> remaining) -- so it re-declares the
// literal. This EXTERNAL test package can import both, which is what makes the
// duplication safe rather than a drift hazard.
//
// Drift here would be silent AND would defeat the gate completely: a mismatched
// prefix matches no fan-out run, the gate reads that as "no evidence of partial
// data" and proceeds -- on exactly the partial tables it exists to avoid. It
// would look like a working gate that simply never fires.
func TestReadinessPrefixMatchesTheProducer(t *testing.T) {
	if remaining.ScheduledFanoutGenerationPrefixForTest != daily.ScheduledFanoutGenerationPrefix {
		t.Fatalf("readiness gate prefix %q != producer prefix %q -- the gate would "+
			"match no fan-out run and silently proceed on partial data",
			remaining.ScheduledFanoutGenerationPrefixForTest,
			daily.ScheduledFanoutGenerationPrefix)
	}
}
