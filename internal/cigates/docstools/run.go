// Package docstools holds the documentation, docs-delivery and commit-message gates that were Python scripts under
// scripts/ (CHAOS-10374). `cigate docs <verb> [args]` dispatches here; every verb runs from the repository root.
package docstools

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// verb is one gate: it returns the process exit code.
type verb func(args []string, stdout, stderr io.Writer) int

var verbs = map[string]verb{}

// register adds a verb; each verb file calls it from init.
func register(name string, fn verb) {
	if _, dup := verbs[name]; dup {
		panic("docstools: duplicate verb " + name)
	}
	verbs[name] = fn
}

// Run dispatches args[0] as the verb name.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, "usage: cigate docs <%s> [args]\n", strings.Join(names(), "|"))
		return 2
	}
	fn, ok := verbs[args[0]]
	if !ok {
		fmt.Fprintf(stderr, "cigate docs: unknown verb %q (have %s)\n", args[0], strings.Join(names(), ", "))
		return 2
	}
	return fn(args[1:], stdout, stderr)
}

func names() []string {
	out := make([]string, 0, len(verbs))
	for name := range verbs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
