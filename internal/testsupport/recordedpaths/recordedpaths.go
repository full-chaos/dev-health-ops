// Package recordedpaths closes the world of a recorded JSON file a test compares.
//
// A frozen golden is read by a test that compares some of its fields and leaves others unread;
// nothing shows which. A test that calls Check declares every scalar path of the file as either
// a CLAIM (the test asserts it) or a NOT-A-CLAIM (with the reason), and Check fails on a path
// that is in neither list, on a path in both, on a listed path the file no longer has, and on a
// reason that is empty. A new field in a re-recorded file, or a field the test stopped reading,
// can then not slip by unseen.
//
// The CLAIMS list is itself a claim: that changing any scalar at the path makes the test fail.
// Check cannot prove it (it walks paths, it never runs the test): the mutation run that backs a
// pull request (every scalar at a claimed path changed, the package tests run, red expected;
// controls: an unchanged rewrite stays green, every scalar changed is red) is the proof, and the
// PR body carries its table. No mutation engine lives in the tree.
//
// A path is written as the leaf of the JSON document with object keys as ".key" and every array
// index folded to "[]" (".cases[].records[].org_id"; a top-level array is "[]" first). Only
// scalar leaves are paths: an empty array or object holds no value to change.
package recordedpaths

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Paths returns the sorted scalar paths of the JSON document raw, with array indices folded.
func Paths(raw []byte) ([]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	seen := map[string]bool{}
	var walk func(node any, path string)
	walk = func(node any, path string) {
		switch value := node.(type) {
		case map[string]any:
			for key, child := range value {
				walk(child, path+"."+key)
			}
		case []any:
			for _, child := range value {
				walk(child, path+"[]")
			}
		default:
			seen[path] = true
		}
	}
	walk(document, "")
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

// Problems compares the scalar paths of raw with the two lists and returns what is wrong, one
// line each; nil means the world is closed.
func Problems(raw []byte, claims []string, notClaims map[string]string) []string {
	paths, err := Paths(raw)
	if err != nil {
		return []string{err.Error()}
	}
	present := map[string]bool{}
	for _, path := range paths {
		present[path] = true
	}
	claimed := map[string]bool{}
	var problems []string
	for _, path := range claims {
		if claimed[path] {
			problems = append(problems, "claimed twice: "+path)
		}
		claimed[path] = true
		if !present[path] {
			problems = append(problems, "stale claim (the file has no such path): "+path)
		}
		if _, both := notClaims[path]; both {
			problems = append(problems, "in both lists: "+path)
		}
	}
	for path, reason := range notClaims {
		if !present[path] {
			problems = append(problems, "stale not-a-claim (the file has no such path): "+path)
		}
		if strings.TrimSpace(reason) == "" {
			problems = append(problems, "not-a-claim without a reason: "+path)
		}
	}
	for _, path := range paths {
		_, declaredNot := notClaims[path]
		if !claimed[path] && !declaredNot {
			problems = append(problems, "undeclared path (claim it or declare it a not-a-claim with a reason): "+path)
		}
	}
	sort.Strings(problems)
	return problems
}

// Check fails the test with every problem Problems finds.
func Check(t testing.TB, raw []byte, claims []string, notClaims map[string]string) {
	t.Helper()
	if len(claims) == 0 {
		t.Fatal("recordedpaths: no claims declared: the check would close nothing")
	}
	if problems := Problems(raw, claims, notClaims); len(problems) > 0 {
		t.Fatalf("recordedpaths: the recorded file's paths are not all declared (%d problems):\n%s", len(problems), strings.Join(problems, "\n"))
	}
}
