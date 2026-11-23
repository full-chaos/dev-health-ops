package daily

import "strings"

// A retraction of a report slot on day D changes the history that the recurrence of D+1..D+29 reads ([X-29, X)). The post-sync
// record marks those days in a lane of its own (the testops window lane), and the runs of that lane compute only the families
// that read the trailing window of test suites. Every other family of such a run is skipped: its measures did not change.

// TestopsWindowSyncGenerationPrefix and TestopsWindowDrainGenerationPrefix identify the runs of the testops window lane: the
// first is started by a post-sync fan-out (the sync run id follows), the second by a drain pass (the pass id follows). Both are
// shorter than the prefixes of the touched-day lane, so a generation stays within the 64 bytes of a new run.
const (
	TestopsWindowSyncGenerationPrefix  = "tw-sync:"
	TestopsWindowDrainGenerationPrefix = "tw-drain:"
	testopsWindowGenerationPrefix      = "tw-"
)

// IsTestopsWindowGeneration reports whether a run belongs to the testops window lane.
func IsTestopsWindowGeneration(generation string) bool {
	return strings.HasPrefix(generation, testopsWindowGenerationPrefix)
}

// testopsWindowFamilies are the families a run of the window lane computes: the ones that read the trailing window of test
// suites (testops_test's failure recurrence, and testops_risk, which scores the same record and the suite history).
var testopsWindowFamilies = map[string]struct{}{"testops_test": {}, "testops_risk": {}}

// familyInRunScope reports whether a run computes the family: every family for a run of the touched-day lane or the schedule,
// the window families only for a run of the window lane.
func familyInRunScope(run Run, family string) bool {
	if !IsTestopsWindowGeneration(run.Generation) {
		return true
	}
	_, ok := testopsWindowFamilies[family]
	return ok
}

// familiesInRunScope keeps the families the run computes.
func familiesInRunScope(run Run, families []string) []string {
	if !IsTestopsWindowGeneration(run.Generation) {
		return families
	}
	kept := make([]string, 0, len(families))
	for _, family := range families {
		if familyInRunScope(run, family) {
			kept = append(kept, family)
		}
	}
	return kept
}

// TouchedLane is the part of the daily-run state one lane of the touched-day record reads: the generations of the runs whose
// start marks its keys (a post-sync fan-out and a drain pass), and which runs its skip of failed days counts.
type TouchedLane struct {
	postSyncPrefix string
	drainPrefix    string
	window         bool
}

// TouchedLaneDays is the lane of the days that raw rows touched: every family of the day is computed again.
var TouchedLaneDays = TouchedLane{postSyncPrefix: postSyncGenerationPrefix, drainPrefix: TouchedDrainGenerationPrefix}

// TouchedLaneTestopsWindow is the lane of the days after a retraction whose trailing window holds it.
var TouchedLaneTestopsWindow = TouchedLane{
	postSyncPrefix: TestopsWindowSyncGenerationPrefix, drainPrefix: TestopsWindowDrainGenerationPrefix, window: true,
}

// DrainGeneration is the generation of the runs a drain pass of the lane starts.
func (lane TouchedLane) DrainGeneration(passID string) string { return lane.drainPrefix + passID }

// PostSyncGeneration is the generation of the runs a post-sync fan-out of the lane starts.
func (lane TouchedLane) PostSyncGeneration(syncRunID string) string {
	return lane.postSyncPrefix + syncRunID
}

func (lane TouchedLane) valid() bool { return lane.postSyncPrefix != "" && lane.drainPrefix != "" }

// runOfLanePattern is the LIKE pattern the skip of failed days compares with, and whether a run of the lane matches it (true)
// or does not (false): the window lane counts its own runs only, the touched-day lane every run but those.
func (lane TouchedLane) runOfLanePattern() (string, bool) {
	return escapeLikePrefix(testopsWindowGenerationPrefix) + "%", lane.window
}
