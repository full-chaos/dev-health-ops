package providersync

import (
	"strings"
	"testing"
)

// The chunk count of ONE 13,875-case report under the TestOps policy: without a big row and with one 2 MB failure message in the
// middle. The count is logged for the record (main's 100-row rule gives 139 for both, by the rule, head d8f96d6e2 gives 28 and 448).
func TestSplitChunkCountsOfA13875CaseReport(t *testing.T) {
	t.Parallel()
	for _, shape := range []splitShape{
		{name: "13,875 cases", cases: 13875, filled: true, basePad: 300, policy: TestOpsRouteChunkPolicy()},
		{name: "13,875 cases, one 2 MB failure message in the middle", cases: 13875, filled: true, basePad: 300, pads: map[int]int{13875 / 2: 2_000_000}, policy: TestOpsRouteChunkPolicy()},
	} {
		batch, _ := shape.batch(t)
		chunks, err := SplitCompleteRouteBatch(batch, shape.policy)
		if err != nil {
			t.Fatalf("%s: %v", shape.name, err)
		}
		want := map[bool]int{false: 28, true: 29}[len(shape.pads) > 0]
		t.Logf("%s: %d chunks", shape.name, len(chunks))
		if len(chunks) != want {
			t.Fatalf("%s: %d chunks, want %d", shape.name, len(chunks), want)
		}
		_ = strings.TrimSpace
	}
}
