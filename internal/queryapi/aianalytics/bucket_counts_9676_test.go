package aianalytics

import (
	"math"
	"testing"
)

// CHAOS-9676: the stored ai_assisted_prs, agent_created_prs, human_prs and
// unknown_prs repeat the whole group's counts on every bucket row, so the
// summary takes each count from its own bucket's rows (bucketPrs). The planted
// case adds the repeated column over the rows, the way the reader did before the
// fix, and requires the overcount to reproduce: the gate must keep finding it.
func TestBucketPrsCountsEachBucketOnceNotTheRepeatedGroupCounts(t *testing.T) {
	// One group: 2 AI-assisted, 1 agent-created, 3 human and 0 unknown pull
	// requests, as the writer stores it: the same group counts on every row.
	group := func(bucket string, prs float64) dailyRow {
		return dailyRow{Bucket: bucket, PrsTotal: prs, AIAssistedPrs: 2, AgentCreatedPrs: 1, HumanPrs: 3, UnknownPrs: 0}
	}
	rows := []dailyRow{group("ai_assisted", 2), group("agent_created", 1), group("human", 3), group("unknown", 0)}

	want := map[string]float64{"ai_assisted": 2, "agent_created": 1, "human": 3, "unknown": 0}
	for bucket, count := range want {
		if got := bucketPrs(rows, bucket); math.Abs(got-count) > 1e-12 {
			t.Errorf("%s pull requests = %v, want %v", bucket, got, count)
		}
	}

	legacy := sumInt(rows, func(r dailyRow) float64 { return r.AIAssistedPrs })
	if legacy != 8 {
		t.Fatalf("the pre-fix sum over the repeated ai_assisted_prs reads %v, want 8 (2 x 4 rows): the planted overcount no longer reproduces", legacy)
	}
	if got := bucketPrs(rows, "ai_assisted"); got == legacy {
		t.Errorf("the fixed count equals the planted overcount %v", legacy)
	}
}
