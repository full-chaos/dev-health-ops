package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// call records one Run invocation.
type call struct {
	path string
	args []string
}

// testTree is a small tree with every kind: a group holding a verb and a
// nested group, a service with a utility child, and a top-level verb.
func testTree(calls *[]call) []Command {
	run := func(path string, code int) func(context.Context, Env) int {
		return func(_ context.Context, env Env) int {
			*calls = append(*calls, call{path: path, args: append([]string(nil), env.Args...)})
			return code
		}
	}
	return []Command{
		{Name: "g", Summary: "group", Kind: Group, Children: []Command{
			{Name: "v", Summary: "verb in group", Kind: Verb, Run: run("g v", ExitOK)},
			{Name: "n", Summary: "nested group", Kind: Group, Children: []Command{
				{Name: "w", Summary: "nested verb", Kind: Verb, Run: run("g n w", ExitRefused)},
			}},
		}},
		{Name: "s", Summary: "service", Kind: Service, Run: run("s", ExitOK), Children: []Command{
			{Name: "h", Summary: "utility", Kind: Verb, Run: run("s h", ExitFailure)},
		}},
		{Name: "x", Summary: "verb", Kind: Verb, Run: run("x", ExitOK)},
	}
}

// model is the dispatch contract written independently of Execute: it says,
// for an argument list, which command runs with which arguments, or which
// exit code a non-running outcome returns.
func model(args []string) (ran *call, code int) {
	if len(args) == 0 {
		return nil, ExitUsage
	}
	switch args[0] {
	case "-h", "--help", "help":
		// help never runs a command; unknown help paths are usage errors.
		return nil, helpModel(args[1:])
	case "--version", "version":
		if len(args) != 1 {
			return nil, ExitUsage
		}
		return nil, ExitOK
	}
	type node struct {
		kind     Kind
		code     int
		children map[string]node
	}
	tree := map[string]node{
		"g": {kind: Group, children: map[string]node{
			"v": {kind: Verb, code: ExitOK},
			"n": {kind: Group, children: map[string]node{"w": {kind: Verb, code: ExitRefused}}},
		}},
		"s": {kind: Service, code: ExitOK, children: map[string]node{"h": {kind: Verb, code: ExitFailure}}},
		"x": {kind: Verb, code: ExitOK},
	}
	level := tree
	var path []string
	for {
		current, found := level[args[0]]
		if !found {
			return nil, ExitUsage
		}
		path = append(path, args[0])
		args = args[1:]
		if len(args) > 0 {
			if _, isChild := current.children[args[0]]; isChild {
				level = current.children
				continue
			}
		}
		if current.kind == Group {
			if len(args) > 0 && (args[0] == "-h" || args[0] == "--help") {
				return nil, ExitOK
			}
			return nil, ExitUsage
		}
		rest := args
		if rest == nil {
			rest = []string{}
		}
		return &call{path: strings.Join(path, " "), args: rest}, current.code
	}
}

func helpModel(path []string) int {
	known := map[string]bool{"": true, "g": true, "g v": true, "g n": true, "g n w": true, "s": true, "s h": true, "x": true}
	walked := ""
	for _, word := range path {
		next := strings.TrimSpace(walked + " " + word)
		if !known[next] {
			return ExitUsage
		}
		// A leaf answers help and stops walking, whatever follows.
		if next == "g v" || next == "g n w" || next == "s h" || next == "x" {
			return ExitOK
		}
		// The service's help at a leaf-less level: "s" has a child, so walk on.
		walked = next
	}
	return ExitOK
}

// TestDispatchMatchesTheModelForEveryArgumentList enumerates every argument
// list of length 0..4 over an alphabet that holds every command name, every
// built-in, a flag, a help flag and an unknown word, and requires Execute and
// the model to agree on what ran, with which args, and the exit code.
func TestDispatchMatchesTheModelForEveryArgumentList(t *testing.T) {
	alphabet := []string{"g", "v", "n", "w", "s", "h", "x", "help", "version", "--help", "-h", "--flag", "zz"}
	var lists [][]string
	var build func(prefix []string, depth int)
	build = func(prefix []string, depth int) {
		lists = append(lists, append([]string(nil), prefix...))
		if depth == 0 {
			return
		}
		for _, word := range alphabet {
			build(append(prefix, word), depth-1)
		}
	}
	build(nil, 4)
	// 1 + 13 + 13^2 + 13^3 + 13^4
	if len(lists) != 1+13+169+2197+28561 {
		t.Fatalf("enumerated %d lists", len(lists))
	}

	for _, args := range lists {
		var calls []call
		var stdout, stderr bytes.Buffer
		code := Execute(context.Background(), "dho", testTree(&calls), Env{Args: args, Stdout: &stdout, Stderr: &stderr})
		wantCall, wantCode := model(args)
		if code != wantCode {
			t.Fatalf("args %q: exit %d, model %d (stderr %q)", args, code, wantCode, stderr.String())
		}
		switch {
		case wantCall == nil && len(calls) != 0:
			t.Fatalf("args %q: ran %v, model runs nothing", args, calls)
		case wantCall != nil && len(calls) != 1:
			t.Fatalf("args %q: ran %d commands, model runs %s", args, len(calls), wantCall.path)
		case wantCall != nil:
			got := calls[0]
			if got.args == nil {
				got.args = []string{}
			}
			if got.path != wantCall.path || fmt.Sprint(got.args) != fmt.Sprint(wantCall.args) {
				t.Fatalf("args %q: ran %s %q, model %s %q", args, got.path, got.args, wantCall.path, wantCall.args)
			}
		}
		if code == ExitUsage && stderr.Len() == 0 {
			t.Fatalf("args %q: a usage error must say something on stderr", args)
		}
	}
}

func TestVersionPrintsBuildJSON(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}} {
		var stdout bytes.Buffer
		if code := Execute(context.Background(), "dho", testTree(new([]call)), Env{Args: args, Stdout: &stdout}); code != ExitOK {
			t.Fatalf("%q: exit %d", args, code)
		}
		var info map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &info); err != nil {
			t.Fatalf("%q: not JSON: %v (%q)", args, err, stdout.String())
		}
		if info["service"] != "dho" {
			t.Fatalf("%q: service %v", args, info["service"])
		}
	}
}

func TestValidateRejectsEveryMalformedShape(t *testing.T) {
	ok := func(context.Context, Env) int { return ExitOK }
	verb := Command{Name: "v", Summary: "s", Kind: Verb, Run: ok}
	cases := map[string][]Command{
		"empty name":              {{Name: "", Summary: "s", Kind: Verb, Run: ok}},
		"leading dash":            {{Name: "-v", Summary: "s", Kind: Verb, Run: ok}},
		"upper case":              {{Name: "V", Summary: "s", Kind: Verb, Run: ok}},
		"space":                   {{Name: "a b", Summary: "s", Kind: Verb, Run: ok}},
		"duplicate":               {verb, verb},
		"builtin help":            {{Name: "help", Summary: "s", Kind: Verb, Run: ok}},
		"builtin version":         {{Name: "version", Summary: "s", Kind: Verb, Run: ok}},
		"no summary":              {{Name: "v", Summary: " ", Kind: Verb, Run: ok}},
		"group with run":          {{Name: "g", Summary: "s", Kind: Group, Run: ok, Children: []Command{verb}}},
		"empty group":             {{Name: "g", Summary: "s", Kind: Group}},
		"verb without run":        {{Name: "v", Summary: "s", Kind: Verb}},
		"verb with children":      {{Name: "v", Summary: "s", Kind: Verb, Run: ok, Children: []Command{verb}}},
		"service without run":     {{Name: "s", Summary: "s", Kind: Service}},
		"service holding a group": {{Name: "s", Summary: "s", Kind: Service, Run: ok, Children: []Command{{Name: "g", Summary: "s", Kind: Group, Children: []Command{verb}}}}},
		"unknown kind":            {{Name: "v", Summary: "s", Kind: Kind(9), Run: ok}},
		"bad nested child":        {{Name: "g", Summary: "s", Kind: Group, Children: []Command{{Name: "", Summary: "s", Kind: Verb, Run: ok}}}},
		"duplicate nested child":  {{Name: "g", Summary: "s", Kind: Group, Children: []Command{verb, verb}}},
	}
	for name, tree := range cases {
		if err := Validate(tree); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
		var stderr bytes.Buffer
		if code := Execute(context.Background(), "dho", tree, Env{Args: []string{"v"}, Stderr: &stderr}); code != ExitFailure {
			t.Errorf("%s: Execute returned %d on a malformed tree, want %d", name, code, ExitFailure)
		}
	}
	// A nested "help" is not a built-in clash: only the top level is reserved.
	nested := []Command{{Name: "g", Summary: "s", Kind: Group, Children: []Command{{Name: "help", Summary: "s", Kind: Verb, Run: ok}}}}
	if err := Validate(nested); err != nil {
		t.Fatalf("nested help rejected: %v", err)
	}
	if err := Validate(testTree(new([]call))); err != nil {
		t.Fatalf("the test tree is valid: %v", err)
	}
}

func TestKindString(t *testing.T) {
	for kind, want := range map[Kind]string{Group: "group", Verb: "verb", Service: "service", Kind(7): "kind(7)"} {
		if got := kind.String(); got != want {
			t.Errorf("%d: %q, want %q", int(kind), got, want)
		}
	}
}
