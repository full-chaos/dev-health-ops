package analytics

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoBoundTimeoutParameterInSQL is CHAOS-4730's class-closing static
// gate. Every SETTINGS max_execution_time clause this package's query
// compilers emit must render its timeout as a LITERAL integer via
// settingsMaxExecutionTime (cost.go), never as a bound native ClickHouse
// query parameter -- a parameter substituted inside a SETTINGS clause
// fails to PARSE (ClickHouse Code: 62, "Expected substitution type
// (identifier)") on 26.6.1.1193, the exact digest-pinned image
// internal/testsupport/containers.StartClickHouse uses for every Go
// Testcontainers integration test in this repo, even though it parses
// fine on 26.7.5.10 (dev-stack/prod) -- which is exactly why 12 (then 13)
// sites carried this defect for a long time with every unit test green:
// none of them ever executed real SQL against a real engine.
//
// This test greps every non-test .go file in this package's own
// directory (not subpackages, not vendored code) for the old
// placeholder's literal opening text and fails if a future query
// compiler reintroduces it -- by copy-paste from an older example, a
// stale snippet, or a hand-written new site that never learned about
// settingsMaxExecutionTime. It is deliberately a plain grep, not a
// go/ast walk: the bug this closes is exactly "the literal text
// reappeared somewhere," and a text search proves that directly without
// having to keep an AST visitor in sync with the language.
func TestNoBoundTimeoutParameterInSQL(t *testing.T) {
	// Built from two literal halves so this test file itself doesn't
	// trip its own gate.
	forbidden := "{timeout" + ":"

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, forbidden) {
				offenders = append(offenders, fmt.Sprintf("%s:%d: %s", name, i+1, strings.TrimSpace(line)))
			}
		}
	}

	if len(offenders) > 0 {
		t.Fatalf("found %d bound-timeout-parameter occurrence(s) in this package's SQL/comment text -- "+
			"render the timeout as a literal via settingsMaxExecutionTime (cost.go) instead, CHAOS-4730:\n%s",
			len(offenders), strings.Join(offenders, "\n"))
	}
}
