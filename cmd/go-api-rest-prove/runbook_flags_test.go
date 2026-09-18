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

	original, originalArgs := flag.CommandLine, os.Args
	t.Cleanup(func() { flag.CommandLine, os.Args = original, originalArgs })
	flag.CommandLine = flag.NewFlagSet("go-api-rest-prove", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = []string{"go-api-rest-prove"}
	_, _ = parseFlags()

	for _, invocation := range documented {
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
		for _, flags := range runbookInvocations(string(raw), binary) {
			out = append(out, documentedInvocation{doc: path, flags: flags})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk docs: %v", err)
	}
	return out
}

var runbookFlagPattern = regexp.MustCompile(`(?m)(?:^|\s)-([a-z][a-z-]+)`)

// runbookInvocations returns the flag names of every fenced command in
// doc that invokes binary, following backslash continuations.
func runbookInvocations(doc, binary string) [][]string {
	var out [][]string
	lines := strings.Split(doc, "\n")
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(strings.TrimSpace(lines[i]), binary) {
			continue
		}
		command := lines[i]
		for strings.HasSuffix(strings.TrimSpace(command), "\\") && i+1 < len(lines) {
			i++
			command = strings.TrimSuffix(strings.TrimSpace(command), "\\") + " " + lines[i]
		}
		var names []string
		for _, match := range runbookFlagPattern.FindAllStringSubmatch(command, -1) {
			names = append(names, match[1])
		}
		out = append(out, names)
	}
	return out
}
