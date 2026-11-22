package providersync

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabShrinkFake is gitLabResumeFake with GitLab's clock (a Date header from now) that calls move just before the
// moveOnPageTwo-th listing request for page 2: the listing changes between two page requests of one invocation.
type gitLabShrinkFake struct {
	*gitLabResumeFake
	now           time.Time
	moveOnPageTwo int
	pageTwos      int
	move          func()
}

func (fake *gitLabShrinkFake) Do(req *http.Request) (*http.Response, error) {
	if req.URL.Path == "/api/v4/projects/123/pipelines" && req.URL.Query().Get("page") == "2" {
		fake.pageTwos++
		if fake.pageTwos == fake.moveOnPageTwo {
			fake.move()
		}
	}
	response, err := fake.gitLabResumeFake.Do(req)
	if response != nil {
		response.Header.Set("Date", fake.now.UTC().Format(http.TimeFormat))
	}
	return response, err
}

func gitLabShrinkWalk(t *testing.T, claim Claim, fake *gitLabShrinkFake, phase string, resume bool) (CompleteRouteBatch, int) {
	t.Helper()
	client := gitLabRepositoryClient(t, fake, "https://gitlab.example")
	stop := errors.New("yield")
	cursor, phaseEmissions, pipelineReads := "", 0, 0
	fake.reportGets = map[int]int{}
	var final CompleteRouteBatch
	for {
		err := (GitLabTestsRouteHandler{}).CollectChunks(t.Context(), claim, providerfoundation.Credential{}, client, fake.now, cursor,
			func(emission ChunkRouteEmission) error {
				cursor = emission.CursorAfter
				if emission.Final {
					final = emission.Batch
					return nil
				}
				for _, effect := range emission.Batch.Effects {
					if effect.Destination != "ci_pipeline_runs" {
						continue
					}
					for _, row := range effect.Rows {
						if gitLabShrinkIsVictim(row) {
							pipelineReads++
						}
					}
				}
				before, err := decodeGitLabTestsChunkCursor(emission.CursorBefore)
				if err != nil {
					t.Fatal(err)
				}
				if before.Phase == phase {
					phaseEmissions++
					if resume && phaseEmissions == 50 {
						return stop
					}
				}
				return nil
			})
		if errors.Is(err, stop) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if phase == "reports" {
		return final, fake.reportGets[1803]
	}
	return final, pipelineReads
}

func gitLabShrinkIsVictim(raw []byte) bool {
	var row struct {
		RunID string `json:"run_id"`
	}
	return json.Unmarshal(raw, &row) == nil && row.RunID == "1803"
}

// A pipeline (1803) enters the window above the page boundary while the first walk reads page 1 (the pipeline is
// updated into the window and a newer one, 2001, arrives with it). The next window, starting at the stored watermark,
// then sees the listing shrink between its own two page requests (2001 deleted, or updated past that window's BeforeAt),
// or stay stable. In every case, both phases, the first walk fresh or resumed, pipeline 1803 is read by one of the two
// walks: a walk reads what it lists, it does not defer an entering pipeline to a next window that can skip it.
func TestGitLabTestsAnEnteringPipelineIsNotLostToAShrinkInTheNextWindow(t *testing.T) {
	start := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	nextStart := start.Add(5 * time.Minute)
	for _, phase := range []string{"pipelines", "reports"} {
		for _, resume := range []bool{false, true} {
			for _, shrink := range []string{"none", "delete", "update_past_upper_bound"} {
				t.Run(fmt.Sprintf("%s/resume=%t/%s", phase, resume, shrink), func(t *testing.T) {
					claim := gitLabEntryClaim(start.Add(24 * time.Hour))
					claim.Mode = "backfill"
					since := start.Add(-time.Hour)
					claim.SinceAt = &since
					fake := &gitLabShrinkFake{
						gitLabResumeFake: &gitLabResumeFake{t: t},
						now:              start, moveOnPageTwo: 1,
					}
					if phase == "reports" {
						fake.moveOnPageTwo = 2
					}
					for id := 2000; id >= 1796; id -= 2 {
						fake.pipelines = append(fake.pipelines, gitLabResumePipeline{
							id: id, started: start.Add(-5 * time.Minute), updated: start.Add(-time.Duration(10+id%50) * time.Second),
						})
					}
					victim := gitLabResumePipeline{id: 1803, started: since.Add(-10 * time.Minute), updated: since.Add(-time.Second)}
					fake.pipelines = append(fake.pipelines, victim)
					fake.move = func() {
						fake.now = start.Add(3 * time.Second)
						victim.updated = start.Add(2 * time.Second)
						fake.pipelines[len(fake.pipelines)-1] = victim
						fake.pipelines = append(fake.pipelines, gitLabResumePipeline{
							id: 2001, started: start.Add(time.Second), updated: start.Add(2 * time.Second),
						})
					}
					first, firstReads := gitLabShrinkWalk(t, claim, fake, phase, resume)
					if fake.pageTwos < fake.moveOnPageTwo || first.Watermark == nil {
						t.Fatalf("first walk did not reach the page change with a watermark: page2s=%d watermark=%v", fake.pageTwos, first.Watermark)
					}
					watermark := gitLabEntryStored(claim, first, nextStart)
					next := claim
					next.SinceAt, next.BeforeAt = &watermark, &nextStart
					next.Mode = "incremental"
					fake.now, fake.pageTwos = nextStart, 0
					fake.move = func() {
						switch shrink {
						case "delete":
							fake.pipelines = slices.DeleteFunc(fake.pipelines, func(p gitLabResumePipeline) bool { return p.id == 2001 })
						case "update_past_upper_bound":
							fake.now = nextStart.Add(2 * time.Second)
							fake.pipelines[len(fake.pipelines)-1].updated = nextStart.Add(time.Second)
						}
					}
					second, secondReads := gitLabShrinkWalk(t, next, fake, phase, false)
					if fake.pageTwos < fake.moveOnPageTwo || second.Watermark == nil {
						t.Fatalf("next walk did not reach the page change with a watermark: page2s=%d watermark=%v", fake.pageTwos, second.Watermark)
					}
					t.Logf("id=%d updated_at=%s first_reads=%d next_since=%s next_reads=%d next_watermark=%s next_coverage_complete=%v",
						victim.id, victim.updated.Format(time.RFC3339), firstReads, watermark.Format(time.RFC3339), secondReads, second.Watermark.Format(time.RFC3339), second.Result["coverage_complete"])
					if firstReads+secondReads == 0 {
						t.Error("pipeline was read by neither walk")
					}
				})
			}
		}
	}
}
