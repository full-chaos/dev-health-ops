package providersync

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	last := time.UnixMilli(1<<testOpsRunReportVerdictShift - 1).UTC()
	if got, want := testOpsRunReportVerdictVersion(true, last), uint64(1<<testOpsRunReportVerdictShift)+uint64(last.UnixMilli()); got != want {
		t.Fatalf("complete version at the span end = %d, want the formula %d", got, want)
	}
	for _, outside := range []time.Time{time.UnixMilli(-1).UTC(), time.UnixMilli(1 << testOpsRunReportVerdictShift).UTC()} {
		if testOpsRunReportTimeInSpan(outside) {
			t.Fatalf("%v is accepted as inside the table's span", outside)
		}
	}
}

func TestRunReportCompletenessRowShapeIsTheOneTheSinkAccepts(t *testing.T) {
	at := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	good := testOpsRunReportCompletenessRow{OrgID: "org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", RunAttempt: 1,
		Provider: "github_actions", Reasons: []string{githubTestsArtifactExpiredCause, githubTestsUnreadableArchiveCause},
		ReportsExpected: 2, LastSynced: at, VerdictVersion: testOpsRunReportVerdictVersion(false, at)}
	if !validTestOpsRunReportCompletenessRow(good) {
		t.Fatal("the canonical partial row is refused")
	}
	complete := good
	complete.Complete, complete.Reasons, complete.VerdictVersion = true, []string{}, testOpsRunReportVerdictVersion(true, at)
	if !validTestOpsRunReportCompletenessRow(complete) {
		t.Fatal("the canonical complete row is refused")
	}
	cells := map[string]func(*testOpsRunReportCompletenessRow){
		"attempt 0":                  func(row *testOpsRunReportCompletenessRow) { row.RunAttempt = 0 },
		"upper-case repository UUID": func(row *testOpsRunReportCompletenessRow) { row.RepoID = "C7198FBC-1945-3717-05D8-EB78866B4E79" },
		"repository not a UUID":      func(row *testOpsRunReportCompletenessRow) { row.RepoID = "repo" },
		"null reasons":               func(row *testOpsRunReportCompletenessRow) { row.Reasons = nil },
		"reasons out of order": func(row *testOpsRunReportCompletenessRow) {
			row.Reasons = []string{githubTestsUnreadableArchiveCause, githubTestsArtifactExpiredCause}
		},
		"a repeated reason": func(row *testOpsRunReportCompletenessRow) {
			row.Reasons = []string{githubTestsArtifactExpiredCause, githubTestsArtifactExpiredCause}
		},
		"complete with reasons": func(row *testOpsRunReportCompletenessRow) {
			row.Complete, row.VerdictVersion = true, testOpsRunReportVerdictVersion(true, row.LastSynced)
		},
		"version of the other verdict": func(row *testOpsRunReportCompletenessRow) {
			row.VerdictVersion = testOpsRunReportVerdictVersion(true, row.LastSynced)
		},
		"sub-millisecond time": func(row *testOpsRunReportCompletenessRow) {
			row.LastSynced = row.LastSynced.Add(time.Microsecond)
			row.VerdictVersion = testOpsRunReportVerdictVersion(false, row.LastSynced)
		},
		"non-UTC time": func(row *testOpsRunReportCompletenessRow) {
			row.LastSynced = row.LastSynced.In(time.FixedZone("plus2", 2*3600))
		},
		"time past the span": func(row *testOpsRunReportCompletenessRow) {
			row.LastSynced = time.UnixMilli(1 << testOpsRunReportVerdictShift).UTC()
			row.VerdictVersion = testOpsRunReportVerdictVersion(false, row.LastSynced)
		},
		"negative report count": func(row *testOpsRunReportCompletenessRow) { row.ReportsAdmitted = -1 },
	}
	for name, mutate := range cells {
		row := good
		row.Reasons = slices.Clone(good.Reasons)
		mutate(&row)
		if validTestOpsRunReportCompletenessRow(row) {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A row the write refuses is never settled by the readback, whatever the stored version.
func TestRunReportCompletenessReadbackRefusesARowTheWriteRefuses(t *testing.T) {
	at := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "tests")
	bad := testOpsRunReportCompletenessRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", RunAttempt: 1,
		Provider: "github_actions", Reasons: []string{}, LastSynced: at, VerdictVersion: testOpsRunReportVerdictVersion(false, at)}
	if err := validateTestOpsRunReportCompletenessRow(claim, bad); err == nil {
		t.Fatal("a partial row with no reason passed the row check")
	}
	stored := bad
	stored.Complete, stored.VerdictVersion = true, testOpsRunReportVerdictVersion(true, at.Add(time.Hour))
	got, err := inspectTestOpsRunReportCompleteness([]testOpsRunReportCompletenessRow{bad},
		func(expected testOpsRunReportCompletenessRow) (testOpsRunReportCompletenessRow, bool, error) {
			if err := validateTestOpsRunReportCompletenessRow(claim, expected); err != nil {
				return testOpsRunReportCompletenessRow{}, false, err
			}
			return stored, true, nil
		})
	if err == nil || got == EffectExact {
		t.Fatalf("an invalid row read back as %v err %v, want refused", got, err)
	}
}

// The run's verdict row commits in the last sub-chunk of its emission, after every report row, however the emission splits.
func TestRunReportCompletenessRowIsSplitIntoTheLastSubChunk(t *testing.T) {
	rows := func(destination string, count int) []json.RawMessage {
		out := make([]json.RawMessage, 0, count)
		for index := 0; index < count; index++ {
			out = append(out, json.RawMessage(fmt.Sprintf(`{"destination":%q,"id":%d,"pad":"%s"}`, destination, index, strings.Repeat("x", 200))))
		}
		return out
	}
	for _, cell := range []struct {
		name   string
		count  int
		policy ChunkPolicy
	}{
		{"row bound", 101, DefaultChunkPolicy()},
		{"byte bound", 80, ChunkPolicy{MaxSourceItems: 1000, MaxEffectRows: 1000, MaxPreparedBytes: 24 << 10, MaxChunksPerAttempt: 8, MaxWallTime: time.Minute}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var effects []EffectBatch
			for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results", "test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination} {
				var destinationRows []json.RawMessage
				switch destination {
				case "test_suite_results", "test_case_results", "coverage_snapshots":
					destinationRows = rows(destination, cell.count)
				case testOpsRunReportCompletenessDestination:
					destinationRows = rows(destination, 1)
				}
				effect, err := BuildEffectBatch(destination, EffectReadbackRequired, destinationRows)
				if err != nil {
					t.Fatal(err)
				}
				effects = append(effects, effect)
			}
			chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, cell.policy)
			if err != nil {
				t.Fatal(err)
			}
			if len(chunks) < 2 {
				t.Fatalf("%d chunk(s): the cell does not split", len(chunks))
			}
			lastReport, verdictChunk, total := -1, -1, 0
			for ordinal, chunk := range chunks {
				for _, effect := range chunk.Effects {
					if len(effect.Rows) == 0 {
						continue
					}
					switch effect.Destination {
					case testOpsRunReportCompletenessDestination:
						verdictChunk = ordinal
					case "test_suite_results", "test_case_results", "coverage_snapshots":
						lastReport, total = ordinal, total+len(effect.Rows)
					}
				}
			}
			if total != 3*cell.count || verdictChunk != len(chunks)-1 || verdictChunk < lastReport {
				t.Fatalf("report rows %d (want %d) last in chunk %d, verdict in chunk %d of %d", total, 3*cell.count, lastReport, verdictChunk, len(chunks))
			}
		})
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
	if _, notALoss := githubTestsRunReportNotALossCauses[githubTestsAggregateOfPartsCause]; !notALoss {
		t.Error("aggregate_of_parts is not a loss now that the producer drops only lossless aggregates (acp C7)")
	}
	if _, loss := githubTestsRunReportLossCauses[githubTestsAggregateMismatchCause]; !loss {
		t.Error("aggregate_mismatch must stay a loss (acp C7)")
	}
	for reason := range githubTestsRunReportVerdictOnlyReasons {
		if _, ok := recordable[reason]; ok {
			t.Errorf("verdict-only reason %q is also a recorded cause", reason)
		}
	}
}

// The reasons come from the exact per-cause counter, which counts a marker before the bounded sample drops it.
func TestRunLossReasonsSeeASkipTheMarkerSampleDropped(t *testing.T) {
	var cursor githubTestsChunkCursor
	for index := 0; index < githubTestsMaxSkippedArtifactRecords; index++ {
		cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
			cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
			GitHubTestsSkippedArtifact{RunID: "1", ArtifactID: fmt.Sprint("a", index), Cause: githubTestsArtifactUnavailableCause},
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
		{"the same row", row(false, at), ptrOf(row(false, at)), EffectExact},
		{"a complete row outranks a later partial write", row(false, at.Add(time.Hour)), ptrOf(row(true, at)), EffectExact},
		{"an older partial row is not the written complete row", row(true, at.Add(time.Hour)), ptrOf(row(false, at)), EffectAbsent},
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

func ptrOf[T any](value T) *T { return &value }
