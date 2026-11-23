package providersync

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-14487: the walk record bound (githubTestsWalkRecords) must charge every row the walk writes. The cells below count
// the rows of every emission and want the bound to see each one: the run-report completeness row of a report run (artifacts
// phase) and the deleted pipeline row of an open run GitHub no longer has (open phase).

// githubTestsRowCountWalk is one walk driven to its final emission, with every row it emitted counted.
type githubTestsRowCountWalk struct {
	cursor githubTestsChunkCursor
	final  CompleteRouteBatch
	// written is the rows of every emission; byDestination splits them by destination.
	written       int
	byDestination map[string]int
	// reportRuns is, in emission order, the report-bearing rows (suites, cases, coverage, completeness) of each emission that
	// carried any.
	reportRuns []int
}

// walkGitHubTestsCountingRows drives CollectChunks to its final emission. yieldEvery > 0 stops each pass after that many
// emissions and resumes from the CursorAfter of the last one, as the executor does; 0 walks in one pass. The rows an open-runs
// store sees follow the written pipeline rows, as in walkListing.
func walkGitHubTestsCountingRows(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer, yieldEvery int,
) githubTestsRowCountWalk {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := githubTestsRowCountWalk{byDestination: map[string]int{}}
	resume := ""
	for pass := 1; ; pass++ {
		if pass > 1000 {
			t.Fatal("the walk never reached a final emission")
		}
		emitted, last, finalSeen := 0, resume, false
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				if emission.Barrier {
					return nil
				}
				last = emission.CursorAfter
				reportRows := 0
				for _, effect := range emission.Batch.Effects {
					walk.written += len(effect.Rows)
					walk.byDestination[effect.Destination] += len(effect.Rows)
					switch effect.Destination {
					case "test_suite_results", "test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination:
						reportRows += len(effect.Rows)
					case "ci_pipeline_runs":
						if store, ok := handler.OpenRuns.(*fakeOpenRuns); ok {
							rows, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
							if decodeErr != nil {
								t.Fatal(decodeErr)
							}
							store.finalize(rows)
						}
					}
				}
				if reportRows > 0 {
					walk.reportRuns = append(walk.reportRuns, reportRows)
				}
				if emission.Final {
					walk.final, finalSeen = emission.Batch, true
					return nil
				}
				emitted++
				if yieldEvery > 0 && emitted >= yieldEvery {
					return errGitHubTestsWalkContinuation
				}
				return nil
			})
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			cursor, decodeErr := decodeGitHubTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			walk.cursor = cursor
			return walk
		}
		if !errors.Is(err, errGitHubTestsWalkContinuation) {
			t.Fatalf("pass %d err=%v, want a continuation yield", pass, err)
		}
		resume = last
	}
}

// githubTestsRowCountPasses walks each cell in one pass and with a resume after every emission (the counters ride the cursor).
func githubTestsRowCountPasses() []struct {
	name       string
	yieldEvery int
} {
	return []struct {
		name       string
		yieldEvery int
	}{{"one pass", 0}, {"a resume after every emission", 1}}
}

// The artifacts phase writes one run-report completeness row per report run next to its report rows. The bound charges it:
// a bound of the runs phase plus the rows of the first two report runs stops the walk at the end of the first artifacts page.
func TestGitHubTestsWalkBoundChargesRunReportCompletenessRows(t *testing.T) {
	archive := githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})
	claim := nativeTestClaim("github", "tests")
	for _, pass := range githubTestsRowCountPasses() {
		t.Run(pass.name, func(t *testing.T) {
			full := walkGitHubTestsCountingRows(t, GitHubTestsRouteHandler{}, claim,
				githubTestsPagedReportDoer{githubTestsPagedDoer: &githubTestsPagedDoer{t: t, pages: 3, perPage: 2}, archive: archive}, pass.yieldEvery)
			completeness := full.byDestination[testOpsRunReportCompletenessDestination]
			if full.cursor.Pipelines != 6 || len(full.reportRuns) != 6 || completeness != 6 || full.cursor.LateRecords != 0 {
				t.Fatalf("premise broken: pipelines=%d report runs=%v completeness rows=%d late records=%d, want 6 report runs with one verdict each",
					full.cursor.Pipelines, full.reportRuns, completeness, full.cursor.LateRecords)
			}
			if charged := githubTestsWalkRecords(full.cursor); charged != full.written {
				t.Errorf("walk records %d, rows written %d (%d completeness rows): every written row must count against the bound",
					charged, full.written, completeness)
			}

			runsPhase := full.byDestination["ci_pipeline_runs"] + full.byDestination["ci_job_runs"] + full.byDestination["ci_acceptance_checks"]
			bound := runsPhase + full.reportRuns[0] + full.reportRuns[1]
			listing := &githubTestsPagedDoer{t: t, pages: 3, perPage: 2}
			bounded := walkGitHubTestsCountingRows(t, GitHubTestsRouteHandler{MaxWalkRecords: bound}, claim,
				githubTestsPagedReportDoer{githubTestsPagedDoer: listing, archive: archive}, pass.yieldEvery)
			if bounded.written > bound {
				t.Errorf("rows written %d over the bound %d (completeness rows %d): the bound must stop the walk after the second report run",
					bounded.written, bound, bounded.byDestination[testOpsRunReportCompletenessDestination])
			}
			if charged := githubTestsWalkRecords(bounded.cursor); charged != bounded.written {
				t.Errorf("bounded walk records %d, rows written %d", charged, bounded.written)
			}
			if pass.yieldEvery == 0 && listing.artifactListRequests != 1 {
				t.Errorf("artifacts listing requests=%d, want only the first page", listing.artifactListRequests)
			}
			if !githubTestsHasPageBudgetObservation(bounded.cursor, githubTestsArtifactInventoryComponent) || bounded.final.Watermark != nil {
				t.Errorf("incomplete=%+v watermark=%v, want the artifact inventory truncated and no watermark",
					bounded.cursor.Incomplete, bounded.final.Watermark)
			}
		})
	}
}

// The open phase writes a deleted pipeline row for a stored open run GitHub answers 404 for. The bound charges it: a bound
// of the inventory plus one row stops the open phase after the first deleted row.
func TestGitHubTestsWalkBoundChargesOpenRunDeletedRows(t *testing.T) {
	repoID := listingRepoID(t)
	stale := listingSince.Add(-30 * 24 * time.Hour)
	setup := func() (*listingFake, *fakeOpenRuns) {
		fake := newListingFake(t, makeListingRuns(1, 2, listingSince, time.Minute))
		reader := &fakeOpenRuns{}
		for index, id := range []int{7001, 7002, 7003} {
			fake.gone[id] = true
			reader.rows = append(reader.rows, openRow(repoID, strconv.Itoa(id), stale.Add(time.Duration(index)*time.Minute), "running"))
		}
		return fake, reader
	}
	for _, pass := range githubTestsRowCountPasses() {
		t.Run(pass.name, func(t *testing.T) {
			fake, _ := setup()
			inventory := walkGitHubTestsCountingRows(t, GitHubTestsRouteHandler{}, openWindowClaim(), fake, pass.yieldEvery)
			if inventory.cursor.LateRecords != 0 || githubTestsWalkRecords(inventory.cursor) != inventory.written || inventory.written == 0 {
				t.Fatalf("premise broken: late records=%d walk records=%d rows written=%d, want an inventory charged row for row",
					inventory.cursor.LateRecords, githubTestsWalkRecords(inventory.cursor), inventory.written)
			}

			fake, reader := setup()
			full := walkGitHubTestsCountingRows(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), fake, pass.yieldEvery)
			if full.written != inventory.written+3 {
				t.Fatalf("premise broken: rows written %d, want the inventory %d and 3 deleted rows", full.written, inventory.written)
			}
			if charged := githubTestsWalkRecords(full.cursor); charged != full.written {
				t.Errorf("walk records %d, rows written %d: every deleted pipeline row must count against the bound", charged, full.written)
			}

			bound := inventory.written + 1
			fake, reader = setup()
			bounded := walkGitHubTestsCountingRows(t, GitHubTestsRouteHandler{OpenRuns: reader, MaxWalkRecords: bound}, openWindowClaim(), fake, pass.yieldEvery)
			if bounded.written > bound {
				t.Errorf("rows written %d over the bound %d: the bound must stop the open phase after the first deleted row", bounded.written, bound)
			}
			if charged := githubTestsWalkRecords(bounded.cursor); charged != bounded.written {
				t.Errorf("bounded walk records %d, rows written %d", charged, bounded.written)
			}
			if pass.yieldEvery == 0 && len(fake.runGets) != 1 {
				t.Errorf("by-id reads=%v, want only 7001 before the bound", fake.runGets)
			}
			if capReached, _ := bounded.final.Result[GitHubTestsResultKeyOpenCapReached].(bool); !capReached {
				t.Errorf("open_runs_cap_reached=%v, want true: the bound left open runs unread", bounded.final.Result[GitHubTestsResultKeyOpenCapReached])
			}
		})
	}
}
