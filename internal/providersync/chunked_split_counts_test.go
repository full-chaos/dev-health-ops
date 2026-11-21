package providersync

import "testing"

// The chunk count of ONE 13,875-case report (cases of about 450 bytes, the 7 destinations) under the TestOps policy and under main's 100-row
// split, without a big row and with one 2 MB failure message in the middle. Logged for the record.
func TestSplitChunkCountsOfA13875CaseReport(t *testing.T) {
	t.Parallel()
	for _, shape := range []gateShape{
		{name: "13,875 cases", cases: 13875, filled: true, basePad: 300, policy: TestOpsRouteChunkPolicy()},
		{name: "13,875 cases, one 2 MB failure message in the middle", cases: 13875, filled: true, basePad: 300, pads: map[int]int{13875 / 2: 2_000_000}, policy: TestOpsRouteChunkPolicy()},
	} {
		batch, _ := shape.batch(t)
		chunks, err := splitUnderTest(shape.claim(), batch, shape.policy)
		if err != nil {
			t.Fatalf("%s: %v", shape.name, err)
		}
		mainChunks, mainErr := mainSplitCompleteRouteBatch(batch, ShadowComparison{}, DefaultChunkPolicy())
		t.Logf("%s: %d chunks (main's 100-row split: %d, err=%v)", shape.name, len(chunks), len(mainChunks), mainErr)
		if len(chunks) > len(mainChunks) {
			t.Fatalf("%s: %d chunks, main's split makes %d", shape.name, len(chunks), len(mainChunks))
		}
	}
}
