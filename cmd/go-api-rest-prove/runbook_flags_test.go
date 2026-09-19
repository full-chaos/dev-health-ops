package main

import (
	"flag"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The operator runbook's own invocation is the command an operator
// copies; every flag it types must be one this binary defines.
func TestRunbookInvocationUsesRealFlags(t *testing.T) {
	documented := documentedInvocations(t, "go-api-rest-prove")
	if len(documented) == 0 {
		t.Fatal("docs carry no go-api-rest-prove invocation")
	}

	original, originalArgs := flag.CommandLine, os.Args
	t.Cleanup(func() { flag.CommandLine, os.Args = original, originalArgs })
	flag.CommandLine = flag.NewFlagSet("go-api-rest-prove", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = []string{"go-api-rest-prove"}
	_, _ = parseFlags()

	for _, invocation := range documented {
		// The documented argv itself, parsed by the binary's own flag set:
		// an unknown flag or a value its type rejects fails here.
		if err := flag.CommandLine.Parse(invocation.args); err != nil {
			t.Errorf("%s: the documented %s command does not parse: %v", invocation.doc, "go-api-rest-prove", err)
		}
		for _, name := range invocation.flags {
			if flag.CommandLine.Lookup(name) == nil {
				t.Errorf("%s documents -%s, which go-api-rest-prove does not define", invocation.doc, name)
			}
		}
	}
}

// documentedInvocation is one fenced command in the docs tree.
type documentedInvocation struct {
	doc   string
	flags []string
	args  []string
}

// documentedInvocations collects every command in docs/ that invokes
// binary, following backslash continuations.
func documentedInvocations(t *testing.T, binary string) []documentedInvocation {
	t.Helper()
	var out []documentedInvocation
	err := filepath.WalkDir("../../docs", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".md") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, command := range runbookCommands(string(raw), binary) {
			var flags []string
			for _, match := range runbookFlagPattern.FindAllStringSubmatch(command, -1) {
				flags = append(flags, match[1])
			}
			out = append(out, documentedInvocation{doc: path, flags: flags, args: runbookArgv(command)[1:]})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk docs: %v", err)
	}
	return out
}

var runbookFlagPattern = regexp.MustCompile(`(?m)(?:^|\s)-([a-z][a-z-]+)`)

// runbookCommands returns every fenced command line in doc that invokes
// binary, backslash continuations joined.
func runbookCommands(doc, binary string) []string {
	var out []string
	lines := strings.Split(doc, "\n")
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(strings.TrimSpace(lines[i]), binary+" ") && strings.TrimSpace(lines[i]) != binary+" \\" {
			continue
		}
		command := strings.TrimSpace(lines[i])
		for strings.HasSuffix(command, "\\") && i+1 < len(lines) {
			i++
			command = strings.TrimSuffix(command, "\\") + " " + strings.TrimSpace(lines[i])
		}
		out = append(out, command)
	}
	return out
}

// runbookArgv splits a documented shell command into its argv the way a
// POSIX shell does for the quoting runbook commands use (single and
// double quotes, no expansion).
func runbookArgv(command string) []string {
	var args []string
	var current strings.Builder
	inArg := false
	var quote rune
	for _, r := range command {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote, inArg = r, true
		case r == ' ' || r == '\t':
			if inArg {
				args = append(args, current.String())
				current.Reset()
				inArg = false
			}
		default:
			current.WriteRune(r)
			inArg = true
		}
	}
	if inArg {
		args = append(args, current.String())
	}
	return args
}
