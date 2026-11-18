package providersync

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestRunReportVerdictVersionRanksCompleteOverPartialThenNewerWalk(t *testing.T) {
	early := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	late := early.Add(time.Hour)
	if !(testOpsRunReportVerdictVersion(true, early) > testOpsRunReportVerdictVersion(false, late)) {
		t.Fatal("an earlier complete verdict must outrank a later partial one (no downgrade on a later walk of the same attempt)")
	}
	if !(testOpsRunReportVerdictVersion(true, late) > testOpsRunReportVerdictVersion(true, early)) {
		t.Fatal("between complete verdicts the newer walk must win")
	}
	if !(testOpsRunReportVerdictVersion(false, late) > testOpsRunReportVerdictVersion(false, early)) {
		t.Fatal("between partial verdicts the newer walk must win")
	}
	if far := time.Date(2099, 12, 31, 23, 59, 59, 999e6, time.UTC); uint64(far.UnixMilli()) >= 1<<testOpsRunReportVerdictShift {
		t.Fatalf("a timestamp in 2099 reaches the verdict bit")
	}
}

func TestRunReportCompletenessCommitsAfterTheRunsReportRows(t *testing.T) {
	effects, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sortEffectBatches(effects)
	order := make([]string, 0, len(effects))
	for _, effect := range effects {
		order = append(order, effect.Destination)
	}
	at := slices.Index(order, testOpsRunReportCompletenessDestination)
	for _, rows := range []string{"test_suite_results", "test_case_results", "coverage_snapshots"} {
		if index := slices.Index(order, rows); index < 0 || at < index {
			t.Fatalf("commit order %v: the completeness row must commit after %s", order, rows)
		}
	}
}

// Every cause a run's walk can record is classified as a loss or not, and nothing else is: a new cause (a covgap ticket, a new
// skip site) fails here until it is classified.
func TestRunReportLossCausesMatchTheRunVocabularies(t *testing.T) {
	recordable := map[string]struct{}{}
	for component := range githubTestsRunReportComponents {
		causes, ok := githubTestsIncompleteVocabulary[component]
		if !ok {
			t.Fatalf("component %s is not in githubTestsIncompleteVocabulary", component)
		}
		for cause := range causes {
			recordable[cause] = struct{}{}
		}
	}
	for cause := range recordable {
		_, loss := githubTestsRunReportLossCauses[cause]
		_, notALoss := githubTestsRunReportNotALossCauses[cause]
		if loss == notALoss {
			t.Errorf("cause %q: loss=%v not-a-loss=%v, want exactly one", cause, loss, notALoss)
		}
	}
	for _, set := range []map[string]struct{}{githubTestsRunReportLossCauses, githubTestsRunReportNotALossCauses} {
		for cause := range set {
			if _, ok := recordable[cause]; !ok {
				t.Errorf("classified cause %q is not recordable by a run's walk", cause)
			}
		}
	}
	if _, loss := githubTestsRunReportLossCauses[githubTestsAggregateOfPartsCause]; !loss {
		t.Error("aggregate_of_parts must stay a loss until the producer drop is lossless (acp C7)")
	}
}

// The reasons come from the exact per-cause counter, which counts a marker before the bounded sample drops it.
func TestRunLossReasonsSeeASkipTheMarkerSampleDropped(t *testing.T) {
	var cursor githubTestsChunkCursor
	for index := 0; index < githubTestsMaxSkippedArtifactRecords; index++ {
		cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
			cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
			GitHubTestsSkippedArtifact{RunID: "1", ArtifactID: "a", Cause: githubTestsArtifactUnavailableCause},
		)
	}
	before := githubTestsTakeRunLossCounts(cursor)
	cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
		cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
		GitHubTestsSkippedArtifact{RunID: "2", ArtifactID: "b", Cause: githubTestsUnreadableArchiveCause},
	)
	if cursor.SkippedArtifactsOverflow == 0 {
		t.Fatal("the sample did not overflow; the cell does not measure")
	}
	got := githubTestsRunLossReasons(before, githubTestsTakeRunLossCounts(cursor), 0)
	if want := []string{githubTestsUnreadableArchiveCause}; !slices.Equal(got, want) {
		t.Fatalf("reasons %v, want %v", got, want)
	}
}

func TestRunLossReasonsCountOnlyThisRunsReportComponents(t *testing.T) {
	cursor := githubTestsChunkCursor{Incomplete: []GitHubTestsIncomplete{
		{Component: githubTestsReportMemberComponent, Cause: githubTestsMalformedCause, Count: 2},
		{Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunCapCause, Count: 1},
	}}
	before := githubTestsTakeRunLossCounts(cursor)
	cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, GitHubTestsIncomplete{Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunCapCause, Count: 1})
	if got := githubTestsRunLossReasons(before, githubTestsTakeRunLossCounts(cursor), 0); len(got) != 0 {
		t.Fatalf("a jobs cap is not a report loss; reasons %v", got)
	}
	cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, GitHubTestsIncomplete{Component: githubTestsReportMemberComponent, Cause: githubTestsMalformedCause, Count: 1})
	if got, want := githubTestsRunLossReasons(before, githubTestsTakeRunLossCounts(cursor), 0), []string{githubTestsMalformedCause}; !slices.Equal(got, want) {
		t.Fatalf("reasons %v, want %v (in-place count growth of an existing cause)", got, want)
	}
	if got, want := githubTestsRunLossReasons(before, before, 1), []string{githubTestsArtifactExpiredCause}; !slices.Equal(got, want) {
		t.Fatalf("reasons %v, want %v (expired counted from the walk)", got, want)
	}
}

func TestRunReportCompletenessReadbackFollowsTheVersion(t *testing.T) {
	at := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	row := func(complete bool, when time.Time) testOpsRunReportCompletenessRow {
		reasons := []string{githubTestsArtifactExpiredCause}
		if complete {
			reasons = []string{}
		}
		return testOpsRunReportCompletenessRow{OrgID: "org", RepoID: "repo", RunID: "1", RunAttempt: 1, Provider: "github_actions",
			Complete: complete, Reasons: reasons, LastSynced: when, VerdictVersion: testOpsRunReportVerdictVersion(complete, when)}
	}
	cells := []struct {
		name     string
		expected testOpsRunReportCompletenessRow
		stored   *testOpsRunReportCompletenessRow
		want     EffectInspection
	}{
		{"nothing stored", row(false, at), nil, EffectAbsent},
		{"the same row", row(false, at), ptr(row(false, at)), EffectExact},
		{"a complete row outranks a later partial write", row(false, at.Add(time.Hour)), ptr(row(true, at)), EffectExact},
		{"an older partial row is not the written complete row", row(true, at.Add(time.Hour)), ptr(row(false, at)), EffectAbsent},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			got, err := inspectTestOpsRunReportCompleteness([]testOpsRunReportCompletenessRow{cell.expected},
				func(testOpsRunReportCompletenessRow) (testOpsRunReportCompletenessRow, bool, error) {
					if cell.stored == nil {
						return testOpsRunReportCompletenessRow{}, false, nil
					}
					return *cell.stored, true, nil
				})
			if err != nil || got != cell.want {
				t.Fatalf("inspection %v err %v, want %v", got, err, cell.want)
			}
		})
	}
	same := row(false, at)
	other := same
	other.ReportsExpected = 9
	if got, _ := inspectTestOpsRunReportCompleteness([]testOpsRunReportCompletenessRow{same},
		func(testOpsRunReportCompletenessRow) (testOpsRunReportCompletenessRow, bool, error) {
			return other, true, nil
		}); got != EffectConflict {
		t.Fatalf("same version, other content: %v, want conflict", got)
	}
	if _, err := inspectTestOpsRunReportCompleteness([]testOpsRunReportCompletenessRow{same},
		func(testOpsRunReportCompletenessRow) (testOpsRunReportCompletenessRow, bool, error) {
			return testOpsRunReportCompletenessRow{}, false, errors.New("read failed")
		}); err == nil {
		t.Fatal("a failed read must surface")
	}
}

func ptr[T any](value T) *T { return &value }
