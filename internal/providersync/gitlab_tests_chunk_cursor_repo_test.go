package providersync

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func gitLabLongRepo(length int) string { return "g/" + strings.Repeat("a", length-2) }

// gitLabCollectFrom runs CollectChunks once from raw (no yield) and returns the last cursor and the final batch.
func gitLabCollectFrom(t *testing.T, fake *gitLabResumeFake, raw string) (last string, final CompleteRouteBatch, err error) {
	t.Helper()
	client := gitLabRepositoryClient(t, fake, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	err = GitLabTestsRouteHandler{}.CollectChunks(context.Background(), nativeTestClaim("gitlab", "tests"),
		providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), raw,
		func(emission ChunkRouteEmission) error {
			last = emission.CursorAfter
			if emission.Final {
				final = emission.Batch
			}
			return nil
		})
	return last, final, err
}

func gitLabAssertEachPipelineReadOnce(t *testing.T, fake *gitLabResumeFake, phase string, reads gitLabResumeReads) {
	t.Helper()
	listed := gitLabResumeListed(fake)
	for _, id := range listed {
		if count := reads.of(phase)[id]; count != 1 {
			t.Fatalf("%s: pipeline %d read %d times, want 1 (%d listed)", phase, id, count, len(listed))
		}
	}
	if len(reads.of(phase)) != len(listed) {
		t.Fatalf("%s: %d pipelines read, %d listed", phase, len(reads.of(phase)), len(listed))
	}
}

// A repository path of 4000 bytes (GitLab allows deep groups) must not stop the unit saving a resume point: the walk
// resumes across a chunk boundary, reads each pipeline once, and no cursor carries the path while the project is read
// again on resume.
func TestGitLabTestsResumeWithA4000BytePathReadsEachPipelineOnce(t *testing.T) {
	for _, phase := range []string{"pipelines", "reports"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			repo := gitLabLongRepo(4000)
			fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture(), repoPath: repo}
			longest := 0
			result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
				phase: phase, yields: []int{50, 150},
				observe: func(cursor string) {
					longest = max(longest, len(cursor))
					if strings.Contains(cursor, `"repo"`) {
						t.Fatalf("a %s cursor carries the repository path: %.80s", phase, cursor)
					}
				},
			})
			if err != nil {
				t.Fatalf("a 4000-byte repository path failed the unit: %v", err)
			}
			gitLabAssertEachPipelineReadOnce(t, fake, phase, result.reads)
			if result.final.Result["repo"] != repo {
				t.Fatalf("result repo = %.40v, want the 4000-byte path", result.final.Result["repo"])
			}
			t.Logf("longest in-flight cursor %d bytes", longest)
		})
	}
}

func gitLabDoneCursor(repo string) string {
	raw, err := encodeGitLabTestsChunkCursor(gitLabTestsChunkCursor{Phase: "done", Pipelines: 5, ProjectID: 123, Repo: repo, Requests: 7})
	if err != nil {
		panic(err)
	}
	return raw
}

// The done cursor keeps a path up to gitLabTestsCursorRepoMaxBytes and its resume reads no project; one byte more drops
// the path and the done resume reads the project exactly once. Both publish the right repo and project id.
func TestGitLabTestsDoneResumeReadsTheProjectOnlyForAPathOverTheLimit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		repo  string
		gets  int
		inRaw bool
	}{
		{"short path", "acme/api", 0, true},
		{"path at the limit", gitLabLongRepo(gitLabTestsCursorRepoMaxBytes), 0, true},
		{"path one byte over the limit", gitLabLongRepo(gitLabTestsCursorRepoMaxBytes + 1), 1, false},
		{"path of 4000 bytes", gitLabLongRepo(4000), 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture(), repoPath: tc.repo}
			raw := gitLabDoneCursor(tc.repo)
			if strings.Contains(raw, `"repo"`) != tc.inRaw {
				t.Fatalf("done cursor carries repo = %v, want %v", !tc.inRaw, tc.inRaw)
			}
			_, final, err := gitLabCollectFrom(t, fake, raw)
			if err != nil {
				t.Fatal(err)
			}
			if fake.projectGets != tc.gets {
				t.Fatalf("project reads = %d, want %d", fake.projectGets, tc.gets)
			}
			if final.Result["repo"] != tc.repo || final.Result["project_id"] != int64(123) {
				t.Fatalf("result repo/project_id = %.40v/%v", final.Result["repo"], final.Result["project_id"])
			}
		})
	}
}

// A cursor an older binary wrote carries the whole path. It decodes in every phase and the resume goes on: the path is
// replaced from the project, so the cursors written after it carry none.
func TestGitLabTestsOldCursorWithTheFullPathStillDecodesAndResumes(t *testing.T) {
	old := `{"phase":"pipelines","page":2,"index":3,"pipelines":4,"jobs":5,"acceptance":0,"suites":0,"cases":0,"coverage":0,` +
		`"requests":9,"pages":2,"pipeline_pages":2,"report_pages":0,"repo":"` + gitLabLongRepo(3000) + `","project_id":123,"li":1100}`
	decoded, err := decodeGitLabTestsChunkCursor(old)
	if err != nil || decoded.Repo != gitLabLongRepo(3000) || decoded.ProjectID != 123 || decoded.LastID != 1100 || decoded.Page != 2 {
		t.Fatalf("old cursor decoded to %+v err=%v", decoded, err)
	}
	t.Run("mid phase", func(t *testing.T) {
		fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
		last, final, err := gitLabCollectFrom(t, fake, old)
		if err != nil {
			t.Fatalf("an old cursor failed the unit: %v", err)
		}
		if strings.Contains(last, `"repo":"`+gitLabLongRepo(3000)) || final.Result["repo"] != "acme/api" {
			t.Fatalf("old path kept: last=%.60s repo=%v", last, final.Result["repo"])
		}
	})
	t.Run("done", func(t *testing.T) {
		fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
		_, final, err := gitLabCollectFrom(t, fake,
			`{"phase":"done","pipelines":1,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":2,"pages":1,`+
				`"pipeline_pages":1,"report_pages":1,"repo":"acme/api","project_id":123}`)
		if err != nil || final.Result["repo"] != "acme/api" || fake.projectGets != 0 {
			t.Fatalf("old done cursor: err=%v repo=%v project reads=%d", err, final.Result["repo"], fake.projectGets)
		}
	})
}

// A project renamed or moved between two chunks resumes where it stopped (the resume key is the pipeline id, not the
// path) and publishes the new path.
func TestGitLabTestsResumeAfterTheProjectWasRenamed(t *testing.T) {
	fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture(), repoPath: "acme/api"}
	result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
		phase: "pipelines", yields: []int{50},
		mutate: func(int) { fake.repoPath = "other-group/sub/renamed" },
	})
	if err != nil {
		t.Fatal(err)
	}
	gitLabAssertEachPipelineReadOnce(t, fake, "pipelines", result.reads)
	if result.final.Result["repo"] != "other-group/sub/renamed" {
		t.Fatalf("result repo = %v", result.final.Result["repo"])
	}
}

// Control: a cursor naming another repository and project id is not compared with the project (nothing refuses or
// restarts on it); the unit reads the project it was claimed for.
func TestGitLabTestsResumeIgnoresARepoAndProjectIDInTheCursor(t *testing.T) {
	fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}
	result, err := gitLabResumeRun(t, fake, gitLabResumeWalkOptions{
		phase: "pipelines", yields: []int{50},
		rewrite: func(raw string) string {
			var fields map[string]any
			if err := json.Unmarshal([]byte(raw), &fields); err != nil {
				t.Fatal(err)
			}
			fields["repo"], fields["project_id"] = "other/project", 999
			out, _ := json.Marshal(fields)
			return string(out)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gitLabAssertEachPipelineReadOnce(t, fake, "pipelines", result.reads)
	if result.final.Result["repo"] != "acme/api" || result.final.Result["project_id"] != int64(123) {
		t.Fatalf("result repo/project_id = %v/%v", result.final.Result["repo"], result.final.Result["project_id"])
	}
}

// A done cursor an older binary wrote carries the whole path, whatever its length. The done resume applies the same
// length policy to it as to a new one: a path up to the limit is published as stored with no project read; a longer one is
// replaced from the project, read exactly once, so a project renamed after the cursor was written publishes its new path.
func TestGitLabTestsLegacyDoneCursorFollowsTheSameLengthPolicy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		length   int
		gets     int
		wantLive bool
	}{
		{"at the limit", gitLabTestsCursorRepoMaxBytes, 0, false},
		{"one byte over the limit", gitLabTestsCursorRepoMaxBytes + 1, 1, true},
		{"3800 bytes", 3800, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stored := gitLabLongRepo(tc.length)
			fake := &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture(), repoPath: "acme/renamed"}
			_, final, err := gitLabCollectFrom(t, fake,
				`{"phase":"done","pipelines":1,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":2,"pages":1,`+
					`"pipeline_pages":1,"report_pages":1,"repo":"`+stored+`","project_id":123}`)
			if err != nil {
				t.Fatal(err)
			}
			want := stored
			if tc.wantLive {
				want = "acme/renamed"
			}
			if fake.projectGets != tc.gets || final.Result["repo"] != want {
				t.Fatalf("project reads = %d (want %d), result repo = %.40v (want %.40s)", fake.projectGets, tc.gets, final.Result["repo"], want)
			}
		})
	}
}
