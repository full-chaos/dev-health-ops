package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type githubTestsReplayPinVerdict struct {
	Complete bool     `json:"complete"`
	Reasons  []string `json:"reasons"`
}

type githubTestsReplayPinOutcome struct {
	count         int
	causeCount    int
	watermarkNil  bool
	reportsOK     any
	reasons       []githubTestsReplayPinVerdict
	partialReason string
}

// Pins the accepted behavior: once the bounded marker sample overflowed, a replay may count a guard skip again (over-count),
// never fewer than the distinct skips; the watermark, reports_complete, the per-run verdict and the partial-success reason
// must equal a single walk.
func TestGitHubTestsGuardSkipCountAfterSampleOverflowNeverUnderCounts(t *testing.T) {
	const artifacts = githubTestsMaxSkippedArtifactRecords + 1
	verdicts := func(t *testing.T, batch CompleteRouteBatch) []githubTestsReplayPinVerdict {
		t.Helper()
		var rows []githubTestsReplayPinVerdict
		for _, effect := range batch.Effects {
			if effect.Destination != testOpsRunReportCompletenessDestination {
				continue
			}
			for _, raw := range effect.Rows {
				var row githubTestsReplayPinVerdict
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, row)
			}
		}
		return rows
	}
	outcome := func(t *testing.T, cause string, final githubTestsChunkCursor, batch CompleteRouteBatch, rows []githubTestsReplayPinVerdict) githubTestsReplayPinOutcome {
		t.Helper()
		observation := githubTestsSkipObservation(t, final.Incomplete, githubTestsReportMemberComponent)
		if observation.Cause != cause {
			t.Fatalf("observation=%+v, want cause %s", observation, cause)
		}
		return githubTestsReplayPinOutcome{
			count: observation.Count, causeCount: final.SkippedArtifactCauseCount[cause],
			watermarkNil: batch.Watermark == nil, reportsOK: batch.Result[GitHubTestsResultKeyReportsComplete],
			reasons: rows, partialReason: GitHubTestsCicdPartialSuccessReason(final.Incomplete),
		}
	}
	for cause, archive := range silentSkipArchives(t) {
		t.Run(cause, func(t *testing.T) {
			newDoer := func() *githubTestsCorruptArtifactDoer {
				return &githubTestsCorruptArtifactDoer{t: t, artifacts: artifacts, corrupt: map[int]bool{}, archive: archive}
			}
			single := newDoer()
			singleClient := githubTestsClient(t, fakehttp.Client(single))
			claim := nativeTestClaim("github", "cicd")
			now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
			var singleRows []githubTestsReplayPinVerdict
			var singleFinal githubTestsChunkCursor
			var singleBatch CompleteRouteBatch
			err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, singleClient, now, "",
				func(emission ChunkRouteEmission) error {
					singleRows = append(singleRows, verdicts(t, emission.Batch)...)
					if cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter); decodeErr == nil {
						singleFinal = cursor
					}
					if emission.Final {
						singleBatch = emission.Batch
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			want := outcome(t, cause, singleFinal, singleBatch, nil)
			if want.count != artifacts || want.watermarkNil {
				t.Fatalf("single walk %+v, want count %d and an advancing watermark", want, artifacts)
			}

			doer := newDoer()
			client := githubTestsClient(t, fakehttp.Client(doer))
			stop := errors.New("stop after every artifact's skip is counted")
			var checkpoint githubTestsChunkCursor
			var firstRows []githubTestsReplayPinVerdict
			err = GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, "",
				func(emission ChunkRouteEmission) error {
					firstRows = append(firstRows, verdicts(t, emission.Batch)...)
					cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
					if decodeErr == nil && cursor.SkippedArtifactCauseCount[cause] == artifacts {
						checkpoint = cursor
						return stop
					}
					return nil
				})
			if !errors.Is(err, stop) {
				t.Fatalf("err=%v, want the walk stopped at the checkpoint holding %d skips", err, artifacts)
			}
			if checkpoint.SkippedArtifactsOverflow < 1 || len(checkpoint.SkippedArtifacts) != githubTestsMaxSkippedArtifactRecords {
				t.Fatalf("overflow=%d sample=%d, want the sample full and a marker dropped", checkpoint.SkippedArtifactsOverflow, len(checkpoint.SkippedArtifacts))
			}
			checkpoint.Phase, checkpoint.Index, checkpoint.NextURL, checkpoint.Relist = "artifacts", 0, "", 0
			encoded, encodeErr := encodeGitHubTestsChunkCursor(checkpoint)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			downloads := doer.archiveRequests
			var final githubTestsChunkCursor
			var finalBatch CompleteRouteBatch
			var replayRows []githubTestsReplayPinVerdict
			resumeErr := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, encoded,
				func(emission ChunkRouteEmission) error {
					replayRows = append(replayRows, verdicts(t, emission.Batch)...)
					if cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter); decodeErr == nil {
						final = cursor
					}
					if emission.Final {
						finalBatch = emission.Batch
					}
					return nil
				})
			if resumeErr != nil {
				t.Fatal(resumeErr)
			}
			if doer.archiveRequests-downloads < artifacts {
				t.Fatalf("replay downloads=%d, want %d: the artifacts were not read again", doer.archiveRequests-downloads, artifacts)
			}
			if len(firstRows) == 0 || len(replayRows) == 0 {
				t.Fatalf("verdict rows first=%d replay=%d, want the run's verdict in both walks", len(firstRows), len(replayRows))
			}
			got := outcome(t, cause, final, finalBatch, replayRows[len(replayRows)-1:])
			if got.count < artifacts || got.causeCount < artifacts {
				t.Fatalf("Count=%d cause_count=%d, want at least the %d distinct skips (an under-count hides a lost member)", got.count, got.causeCount, artifacts)
			}
			if got.count != got.causeCount {
				t.Fatalf("Count=%d cause_count=%d, want the two to move together", got.count, got.causeCount)
			}
			if got.watermarkNil != want.watermarkNil || !reflect.DeepEqual(got.reportsOK, want.reportsOK) || got.partialReason != want.partialReason {
				t.Fatalf("replay watermark_nil=%v reports_complete=%v partial=%q, want single walk's %v %v %q",
					got.watermarkNil, got.reportsOK, got.partialReason, want.watermarkNil, want.reportsOK, want.partialReason)
			}
			if len(singleRows) == 0 || !reflect.DeepEqual(got.reasons[0], singleRows[len(singleRows)-1]) {
				t.Fatalf("replay verdict=%+v, want the single walk's %+v", got.reasons, singleRows)
			}
		})
	}
}
