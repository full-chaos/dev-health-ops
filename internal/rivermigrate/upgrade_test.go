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
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The composite runs the migrate Job's three verbs, in the Job's order, and
// each is the stand-alone verb's own Run.
func TestUpgradeStepsAreTheJobsVerbsInOrder(t *testing.T) {
	for river, want := range map[bool][]string{
		false: {"migrate postgres upgrade", "admin features seed", "migrate clickhouse upgrade"},
		true:  {"migrate postgres upgrade", "migrate river --apply-and-check", "admin features seed", "migrate clickhouse upgrade"},
	} {
		var names []string
		for _, step := range upgradeSteps(river) {
			if step.run == nil {
				t.Fatalf("step %s has no Run", step.name)
			}
			names = append(names, step.name)
		}
		if !reflect.DeepEqual(names, want) {
			t.Fatalf("river=%v: steps = %v, want %v", river, names, want)
		}
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
// With river, a conditional step "river" runs between one and two when
// RIVER_ON is set.
func fakeSteps(ran *[]string, fail string, code int) func(bool) []upgradeStep {
	return func(river bool) []upgradeStep {
		names := []string{"one", "two", "three"}
		if river {
			names = []string{"one", "river", "two", "three"}
		}
		return fakeNamedSteps(ran, names, fail, code)
	}
}

func fakeNamedSteps(ran *[]string, names []string, fail string, code int) []upgradeStep {
	var steps []upgradeStep
	for _, name := range names {
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
		if name == "river" {
			steps[len(steps)-1].when = func(lookup platformsecrets.LookupEnv) (bool, string) {
				if _, ok := lookup("RIVER_ON"); ok {
					return true, ""
				}
				return false, "RIVER_ON is not set"
			}
		}
	}
	return steps
}

// --river adds its step, which runs only when its condition holds and is
// otherwise logged as skipped.
func TestRunStepsRiverStep(t *testing.T) {
	for _, testCase := range []struct {
		args []string
		env  map[string]string
		want []string
	}{
		{nil, map[string]string{"RIVER_ON": "1"}, []string{"one", "two", "three"}},
		{[]string{"--river"}, map[string]string{"RIVER_ON": "1"}, []string{"one", "river", "two", "three"}},
		{[]string{"--river"}, nil, []string{"one", "two", "three"}},
	} {
		var ran []string
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := testCase.env[key]; return value, ok }
		code := runSteps(context.Background(), cli.Env{Args: testCase.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}, fakeSteps(&ran, "", 0))
		if code != cli.ExitOK || !reflect.DeepEqual(ran, testCase.want) {
			t.Fatalf("args %v env %v: exit %d, ran %v; want %v", testCase.args, testCase.env, code, ran, testCase.want)
		}
		// A skipped River step is a Warn: an explicit --river that does not
		// run is something an operator must see at the default log level.
		skipped := strings.Contains(stderr.String(), `"level":"WARN","msg":"migrate step skipped","step":"river","reason":"RIVER_ON is not set"`)
		if wantSkip := len(testCase.args) == 1 && testCase.env == nil; skipped != wantSkip {
			t.Fatalf("args %v env %v: skip logged %v, want %v: %s", testCase.args, testCase.env, skipped, wantSkip, stderr.String())
		}
	}
}

// The River step's condition is MIGRATION_DATABASE_URI in any configured form.
func TestMigrationDatabaseConfigured(t *testing.T) {
	for _, testCase := range []struct {
		env  map[string]string
		want bool
	}{
		{nil, false},
		{map[string]string{"POSTGRES_URI": "postgresql://u:p@h/db"}, false},
		{map[string]string{"MIGRATION_DATABASE_URI": "postgresql://u:p@h/db"}, true},
		{map[string]string{"DEV_HEALTH_MIGRATION_PG_HOST": "h", "DEV_HEALTH_MIGRATION_PG_USER": "u", "DEV_HEALTH_MIGRATION_PG_PASSWORD": "p"}, true},
	} {
		lookup := func(key string) (string, bool) { value, ok := testCase.env[key]; return value, ok }
		if got, _ := migrationDatabaseConfigured(lookup); got != testCase.want {
			t.Fatalf("env %v: configured = %v, want %v", testCase.env, got, testCase.want)
		}
	}
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

// With MIGRATION_DATABASE_URI configured the caller must say whether this run
// applies River; without --river or --river=false the run is refused before
// any step, so River is never skipped without a word.
func TestRunStepsRequiresARiverChoiceWhenTheMigrationURIIsSet(t *testing.T) {
	uri := map[string]string{"MIGRATION_DATABASE_URI": "postgresql://u:p@h/db"}
	for _, testCase := range []struct {
		args []string
		env  map[string]string
		code int
		ran  []string
	}{
		{nil, uri, cli.ExitUsage, nil},
		{[]string{"--river=false"}, uri, cli.ExitOK, []string{"one", "two", "three"}},
		{[]string{"--river"}, uri, cli.ExitOK, []string{"one", "two", "three"}},
		{nil, nil, cli.ExitOK, []string{"one", "two", "three"}},
	} {
		var ran []string
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := testCase.env[key]; return value, ok }
		code := runSteps(context.Background(), cli.Env{Args: testCase.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}, fakeSteps(&ran, "", 0))
		if code != testCase.code || !reflect.DeepEqual(ran, testCase.ran) {
			t.Fatalf("args %v env %v: exit %d, ran %v; want %d, %v", testCase.args, testCase.env, code, ran, testCase.code, testCase.ran)
		}
		if refused := strings.Contains(stderr.String(), `"code":"river_step_unspecified"`); refused != (testCase.code == cli.ExitUsage) {
			t.Fatalf("args %v env %v: refusal logged %v: %s", testCase.args, testCase.env, refused, stderr.String())
		}
	}
}
