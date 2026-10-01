package synccli

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/batch_loop_oracle.py
var batchLoopOracleProgram string

type loopRepo struct {
	Name string `json:"name"`
	Fail string `json:"fail"` // "", "error" or "ratelimit"
}

type loopCase struct {
	Provider      string     `json:"provider"`
	Repos         []loopRepo `json:"repos"`
	BatchSize     string     `json:"batch_size"`
	MaxConcurrent string     `json:"max_concurrent"`
}

// loopShape is what a run's events reduce to: the repositories that ran, the most that
// ran at once, and whether the groups of batch_size ran one after the other.
type loopShape struct {
	Processed []string
	Peak      int
	Barrier   bool
}

// shapeOf reduces the events ("start"/"end" of a repository, in the order they
// happened). Group g is the repositories at positions [g*size, (g+1)*size): the barrier
// holds when every repository of a group has ended before any of the next has started.
func shapeOf(events [][2]string, order []string, size int) loopShape {
	position := map[string]int{}
	for i, name := range order {
		position[name] = i
	}
	shape := loopShape{Barrier: true}
	seen := map[string]bool{}
	running, lastEnd, firstStart := 0, map[int]int{}, map[int]int{}
	for at, event := range events {
		group := position[event[1]] / size
		switch event[0] {
		case "start":
			running++
			shape.Peak = max(shape.Peak, running)
			if !seen[event[1]] {
				seen[event[1]] = true
				shape.Processed = append(shape.Processed, event[1])
			}
			if _, ok := firstStart[group]; !ok {
				firstStart[group] = at
			}
		case "end":
			running--
			lastEnd[group] = at
		}
	}
	for group, end := range lastEnd {
		if next, ok := firstStart[group+1]; ok && next < end {
			shape.Barrier = false
		}
	}
	sort.Strings(shape.Processed)
	return shape
}

func loopCorpus() []loopCase {
	var out []loopCase
	names := func(n int) []loopRepo {
		repos := make([]loopRepo, n)
		for i := range repos {
			repos[i] = loopRepo{Name: fmt.Sprintf("acme/r%d", i)}
		}
		return repos
	}
	huge := "99999999999999999999"
	for _, provider := range []string{"github", "gitlab"} {
		for _, n := range []int{1, 3, 5, 7} {
			for _, batch := range []string{"1", "2", "3", "100", "0", "-3", huge} {
				for _, concurrent := range []string{"1", "2", "4", "0", "-1", huge} {
					out = append(out, loopCase{provider, names(n), batch, concurrent})
				}
			}
		}
		// Failures: one repository's own error is logged and the rest go on (Python exits 0,
		// the port exits 1: a named divergence), a rate limit stops Python.
		for _, batch := range []string{"2", "100"} {
			for _, concurrent := range []string{"2", "4"} {
				for name, fails := range map[string][]int{"first": {0}, "middle and last": {2, 4}} {
					repos := names(5)
					for _, i := range fails {
						repos[i].Fail = "error"
					}
					_ = name
					out = append(out, loopCase{provider, repos, batch, concurrent})
				}
				for _, at := range []int{1, 4} {
					repos := names(5)
					repos[at].Fail = "ratelimit"
					out = append(out, loopCase{provider, repos, batch, concurrent})
				}
			}
		}
	}
	return out
}

type loopAnswer struct {
	Events [][2]string `json:"events"`
	Stage  typed       `json:"stage"`
	Type   typed       `json:"type"`
}

// loopRecorded is what the golden stores per case: the reduction of the producer's events (the raw events
// interleave differently from run to run; the shape is what the comparison reads), its stage and error type.
type loopRecorded struct {
	Stage typed     `json:"stage"`
	Type  typed     `json:"type"`
	Shape loopShape `json:"shape"`
}

// batchLoopPythonBuild is the build whose Python batch loops answered the corpus: a build that still carried
// the Python CLI.
const batchLoopPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

var batchLoopPythonSettings = map[string]string{"PYTHONHASHSEED": "0", "OTEL_ENABLED": "false"}

func batchLoopPythonEnv(root string) []string {
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "PYTHONPATH=" + filepath.Join(root, "src"), "PYTHONDONTWRITEBYTECODE=1"}
	for _, name := range []string{"OTEL_ENABLED", "PYTHONHASHSEED"} {
		env = append(env, name+"="+batchLoopPythonSettings[name])
	}
	return env
}

// TestBatchLoopMatchesFrozenPython runs the batch LOOP of `dho sync <target> --search` and compares each run's
// shape with what the REAL process_github_repos_batch / process_gitlab_projects_batch did with the network and
// the store replaced (testdata/batch_loop_oracle.py): the same scripted repositories, batch size and
// concurrency go to both, and the shape of each run (which repositories ran, how many at once, whether the
// batches ran in turn) must match. The answers were executed once on batchLoopPythonBuild and are frozen in
// testdata/golden/batch_loop.json (the recipe regenerates them by execution); the corpus is the golden's key.
//
// Named divergences, asserted where they occur rather than compared:
//   - a repository that fails on its own: dev-hops logs it and exits 0, dho reports it
//     on stderr and exits 1; every other repository still runs in both;
//   - a rate-limit error: dev-hops raises out of the batch (the run ends, exit 1) while
//     dho, whose routes back off on their own, records the failure and runs the rest (exit 1).
func TestBatchLoopMatchesFrozenPython(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/batch_loop.json",
		PythonBuild: batchLoopPythonBuild,
		SHA256:      "PIN:batch_loop",
		Recipe: "git worktree add --detach $DIR " + batchLoopPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/synccli/ -test '^TestBatchLoopMatchesFrozenPython$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	corpus := loopCorpus()
	var input strings.Builder
	for _, c := range corpus {
		line, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(line)
		input.WriteByte('\n')
	}
	request := venueoracle.ProgramRequest("batch loop corpus", batchLoopOracleProgram, []byte(input.String()), batchLoopPythonSettings)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(root string, _ []venueoracle.Request) []venueoracle.Response {
		python := pyoracle.Resolve(t, root)
		command := exec.Command(python, "-c", batchLoopOracleProgram)
		command.Stdin = strings.NewReader(input.String())
		command.Env = batchLoopPythonEnv(root)
		output, err := command.Output()
		if err != nil {
			var stderr []byte
			if exitErr, ok := err.(*exec.ExitError); ok {
				stderr = exitErr.Stderr
			}
			t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr))
		}
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		if len(lines) != len(corpus) {
			t.Fatalf("python answered %d for %d cases", len(lines), len(corpus))
		}
		recorded := make([]loopRecorded, len(corpus))
		for index, c := range corpus {
			var answer loopAnswer
			if err := json.Unmarshal([]byte(lines[index]), &answer); err != nil {
				t.Fatalf("decode python answer %d: %v", index, err)
			}
			order := make([]string, len(c.Repos))
			for i, r := range c.Repos {
				order[i] = r.Name
			}
			batch, _ := new(big.Int).SetString(c.BatchSize, 10)
			recorded[index] = loopRecorded{Stage: answer.Stage, Type: answer.Type, Shape: shapeOf(answer.Events, order, max(1, clampInt(batch)))}
		}
		body, err := json.Marshal(recorded)
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(body)}}
	})
	golden.Consumed(t, answers...)
	var frozen []loopRecorded
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(corpus) {
		t.Fatalf("the golden answered %d for %d cases", len(frozen), len(corpus))
	}

	mismatches, divergences := 0, map[string]int{}
	for index, c := range corpus {
		want := frozen[index]
		order := make([]string, len(c.Repos))
		for i, r := range c.Repos {
			order[i] = r.Name
		}
		batch, _ := new(big.Int).SetString(c.BatchSize, 10)
		size := max(1, clampInt(batch))
		label := fmt.Sprintf("%s repos=%d fail=%v batch=%s concurrent=%s", c.Provider, len(c.Repos), failuresOf(c), c.BatchSize, c.MaxConcurrent)

		h := &batchHarness{}
		byID := map[string]string{}
		args := []string{"--provider", "github", "-s", "acme/*", "--auth", "tok"}
		for i, r := range c.Repos {
			listed := providersync.ListedRepository{Name: strings.TrimPrefix(r.Name, "acme/"), FullName: r.Name}
			if c.Provider == "gitlab" {
				listed.ProjectID = int64(i + 1)
				byID[fmt.Sprint(i+1)] = r.Name
			} else {
				byID[r.Name] = r.Name
			}
			h.repos = append(h.repos, listed)
		}
		if c.Provider == "gitlab" {
			args = []string{"--provider", "gitlab", "-s", "acme/*", "--auth", "tok", "--group", "acme"}
		}
		args = append(args, "--batch-size", c.BatchSize, "--max-concurrent", c.MaxConcurrent)
		failing := map[string]string{}
		for _, r := range c.Repos {
			if r.Fail != "" {
				failing[r.Name] = r.Fail
			}
		}
		h.hold = func(providersync.InProcessRun) { time.Sleep(50 * time.Millisecond) }
		h.fail = func(run providersync.InProcessRun) error {
			if failing[byID[run.SourceExternalID]] != "" {
				return fmt.Errorf("scripted failure of %s", byID[run.SourceExternalID])
			}
			return nil
		}
		code, _, stderr := runVerb(t, "deployments", h.executor(), args, inlineEnv)

		var got [][2]string
		for _, event := range h.events {
			kind, id, _ := strings.Cut(event, ":")
			got = append(got, [2]string{kind, byID[id]})
		}
		gotShape, wantShape := shapeOf(got, order, size), want.Shape
		if c.Provider == "gitlab" {
			// GitLab (every target but git) has no groups to wait on: batch_size is not read, so
			// "the groups ran in turn" is a coincidence of timing there, not behaviour; the
			// repositories that ran and how many at once are compared.
			gotShape.Barrier, wantShape.Barrier = true, true
		}
		anyFail := len(failing) > 0
		rateLimited := false
		for _, kind := range failing {
			rateLimited = rateLimited || kind == "ratelimit"
		}
		wantStage := fmt.Sprint(want.Stage.V)

		switch {
		case rateLimited:
			// Python ends the run with the rate-limit error; the port runs every repository
			// and reports the failed one (exit 1).
			divergences["rate limit"]++
			if wantStage != "error" || want.Type.V != "RateLimitException" || code != cli.ExitFailure || len(gotShape.Processed) != len(order) ||
				!strings.Contains(stderr, "scripted failure") {
				mismatches++
				t.Errorf("%s: a named divergence must be python raising RateLimitException and dho running everything and exiting 1: python %v/%v, dho exit %d processed %d/%d",
					label, wantStage, want.Type.V, code, len(gotShape.Processed), len(order))
			}
		case anyFail:
			// Python logs the failure and exits 0; the port exits 1. Every repository runs in both.
			divergences["repository failure"]++
			if wantStage != "ok" || code != cli.ExitFailure || !strings.Contains(stderr, "scripted failure") ||
				fmt.Sprint(gotShape.Processed) != fmt.Sprint(wantShape.Processed) {
				mismatches++
				t.Errorf("%s: a named divergence must be python ok and dho exit 1 with the same repositories run: python %v processed %v, dho exit %d processed %v",
					label, wantStage, wantShape.Processed, code, gotShape.Processed)
			}
		default:
			if wantStage != "ok" || code != cli.ExitOK {
				mismatches++
				t.Errorf("%s: python ended %v, dho exit %d (%s)", label, wantStage, code, stderr)
			}
			if fmt.Sprintf("%+v", gotShape) != fmt.Sprintf("%+v", wantShape) {
				mismatches++
				t.Errorf("%s: loop shape differs\npython: %+v\ndho:    %+v", label, wantShape, gotShape)
			}
		}
	}
	t.Logf("%d batch runs compared with the frozen Python loop, %d mismatches; named divergences asserted: %v", len(corpus), mismatches, divergences)
	if mismatches > 0 {
		t.Fatalf("%d of %d batch runs differ", mismatches, len(corpus))
	}
	if divergences["rate limit"] == 0 || divergences["repository failure"] == 0 {
		t.Fatalf("the corpus never reached a named divergence: %v", divergences)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

func failuresOf(c loopCase) []string {
	var out []string
	for _, r := range c.Repos {
		if r.Fail != "" {
			out = append(out, r.Name+"="+r.Fail)
		}
	}
	return out
}
