package rivermigrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// The composite runs the migrate Job's three verbs, in the Job's order, and
// each is the stand-alone verb's own Run.
func TestUpgradeStepsAreTheJobsVerbsInOrder(t *testing.T) {
	var names []string
	for _, step := range upgradeSteps() {
		if step.run == nil {
			t.Fatalf("step %s has no Run", step.name)
		}
		names = append(names, step.name)
	}
	want := []string{"migrate postgres upgrade", "admin features seed", "migrate clickhouse upgrade"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("steps = %v, want %v", names, want)
	}
	var found bool
	for _, child := range Command().Children {
		if child.Name == "upgrade" && child.Kind == cli.Verb && child.Run != nil {
			found = true
		}
	}
	if !found {
		t.Fatal("dho migrate has no upgrade verb")
	}
}

// fakeSteps records which steps ran; the step named in fail exits with code.
func fakeSteps(ran *[]string, fail string, code int) []upgradeStep {
	var steps []upgradeStep
	for _, name := range []string{"one", "two", "three"} {
		steps = append(steps, upgradeStep{name: name, run: func(_ context.Context, env cli.Env) int {
			*ran = append(*ran, name)
			if len(env.Args) != 0 {
				return 99
			}
			fmt.Fprintf(env.Stdout, "{%q:true}\n", name)
			if name == fail {
				return code
			}
			return cli.ExitOK
		}})
	}
	return steps
}

func TestRunStepsRunsEveryStepInOrder(t *testing.T) {
	var ran []string
	var stdout, stderr bytes.Buffer
	code := runSteps(context.Background(), cli.Env{Stdout: &stdout, Stderr: &stderr}, fakeSteps(&ran, "", 0))
	if code != cli.ExitOK || !reflect.DeepEqual(ran, []string{"one", "two", "three"}) {
		t.Fatalf("exit %d, ran %v; want 0 and every step in order", code, ran)
	}
	if stdout.String() != "{\"one\":true}\n{\"two\":true}\n{\"three\":true}\n" {
		t.Fatalf("stdout = %q, want each step's result in order", stdout.String())
	}
	for _, name := range []string{"one", "two", "three"} {
		if !strings.Contains(stderr.String(), `"msg":"migrate step done","step":"`+name+`","duration_ms":`) {
			t.Fatalf("no Info line with a duration for %s: %s", name, stderr.String())
		}
	}
	if !strings.Contains(stderr.String(), `"msg":"migrate upgrade done"`) {
		t.Fatalf("no completion line: %s", stderr.String())
	}
}

// The first failing step stops the run; its exit code is the composite's,
// and the error names it and the steps that did not run.
func TestRunStepsStopsAtTheFirstFailure(t *testing.T) {
	for _, code := range []int{cli.ExitFailure, cli.ExitUsage} {
		var ran []string
		var stdout, stderr bytes.Buffer
		got := runSteps(context.Background(), cli.Env{Stdout: &stdout, Stderr: &stderr}, fakeSteps(&ran, "two", code))
		if got != code || !reflect.DeepEqual(ran, []string{"one", "two"}) {
			t.Fatalf("exit %d, ran %v; want %d and a stop after two", got, ran, code)
		}
		var last struct {
			Error struct {
				Code    string   `json:"code"`
				Step    string   `json:"step"`
				Skipped []string `json:"skipped"`
			} `json:"error"`
		}
		lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
		if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
			t.Fatalf("last stderr line is not JSON: %v: %s", err, stderr.String())
		}
		if last.Error.Code != "step_failed" || last.Error.Step != "two" || !reflect.DeepEqual(last.Error.Skipped, []string{"three"}) {
			t.Fatalf("error = %+v, want step_failed naming two with three skipped", last.Error)
		}
		if !strings.Contains(stderr.String(), `"level":"ERROR","msg":"migrate step failed","step":"two"`) {
			t.Fatalf("no failure log line: %s", stderr.String())
		}
	}
}

func TestRunStepsArguments(t *testing.T) {
	var ran []string
	var stdout, stderr bytes.Buffer
	if code := runSteps(context.Background(), cli.Env{Args: []string{"-h"}, Stdout: &stdout, Stderr: &stderr}, fakeSteps(&ran, "", 0)); code != cli.ExitOK || len(ran) != 0 ||
		!strings.Contains(stderr.String(), "Usage: dho migrate upgrade") {
		t.Fatalf("-h: exit %d, ran %v, stderr %q", code, ran, stderr.String())
	}
	if code := runSteps(context.Background(), cli.Env{Args: []string{"extra"}, Stdout: &stdout, Stderr: &stderr}, fakeSteps(&ran, "", 0)); code != cli.ExitUsage || len(ran) != 0 {
		t.Fatalf("a positional argument: exit %d, ran %v; want 2 and nothing run", code, ran)
	}
}
