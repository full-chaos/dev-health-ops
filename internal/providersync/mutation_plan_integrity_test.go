package providersync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A mutation plan is a CLAIM about coverage, and two ways of breaking that
// claim are both silent and both happened in this package's history:
//
//  1. ANCHOR DRIFT. An entry's `find` text stops matching the source it
//     targets, because the line was edited or removed. The harness reports
//     STALE_DECLARATION -- but only for entries in a plan someone remembered to
//     re-run. An entry in an untouched plan whose target file was edited by
//     another change just rots.
//  2. A PROOF THAT NAMES NOTHING. `go test -run ^TestGone$` exits 0 when the
//     pattern matches no test, so an entry mutating live code can lose its
//     guard entirely while the plan still lists it as proven.
//
// Both were real in the Projects v2 work: a `find` broke when a redundant
// clause was removed (twice), and deleting a route test left a live-code
// mutation pointing at a `-run` pattern matching nothing.
//
// The harness now catches (2) at run time as PROOF_VACUOUS. This is the
// standing half: it checks EVERY plan in the directory on every `go test` run,
// including plans this change never touched and plans nobody thought to
// re-run. It is a test, not a script, so it cannot be skipped by forgetting to
// invoke it.
//
// It deliberately does NOT run any mutation or any proof command -- it is a
// static integrity check and stays fast enough to live in the normal suite.
func TestMutationPlansHaveLiveAnchorsAndExistingProofTests(t *testing.T) {
	planDir := filepath.Join("testdata", "mutation-plans")
	entries, err := os.ReadDir(planDir)
	if err != nil {
		t.Fatal(err)
	}

	declaredTests := goTestNamesInPackage(t)
	sourceCache := map[string]string{}
	checkedPlans, checkedEntries := 0, 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		planPath := filepath.Join(planDir, entry.Name())
		raw, readErr := os.ReadFile(planPath)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var plan struct {
			Mutations []struct {
				ID               string     `json:"id"`
				File             string     `json:"file"`
				Find             string     `json:"find"`
				ExpectOccurrence *int       `json:"expect_occurrences"`
				Proof            [][]string `json:"proof"`
			} `json:"mutations"`
		}
		if err := json.Unmarshal(raw, &plan); err != nil {
			t.Fatalf("%s: %v", planPath, err)
		}
		if len(plan.Mutations) == 0 {
			t.Errorf("%s declares no mutations; an empty plan reads as a "+
				"verified one", planPath)
			continue
		}
		checkedPlans++

		for _, mutation := range plan.Mutations {
			checkedEntries++
			want := 1
			if mutation.ExpectOccurrence != nil {
				want = *mutation.ExpectOccurrence
			}

			source, cached := sourceCache[mutation.File]
			if !cached {
				bytes, sourceErr := os.ReadFile(filepath.Join("..", "..", mutation.File))
				if sourceErr != nil {
					t.Errorf("%s: %s targets %s, which cannot be read: %v",
						planPath, mutation.ID, mutation.File, sourceErr)
					continue
				}
				source = string(bytes)
				sourceCache[mutation.File] = source
			}
			if got := strings.Count(source, mutation.Find); got != want {
				t.Errorf("%s: %s anchor matches %d time(s) in %s, want %d -- a "+
					"drifted anchor measures nothing while the plan still lists "+
					"the entry as coverage",
					planPath, mutation.ID, got, mutation.File, want)
			}

			for _, command := range mutation.Proof {
				// Only this package's own tests can be checked from here. A
				// proof targeting another package is left to that package's
				// sweep rather than reported as missing, which would be a false
				// alarm dressed as a finding.
				if !commandTargetsThisPackage(command) {
					continue
				}
				for _, named := range goTestPatternNames(command) {
					if !declaredTests[named] {
						t.Errorf("%s: %s names proof test %q, which does not "+
							"exist in this package -- `go test -run` exits 0 when "+
							"its pattern matches nothing, so this entry would "+
							"report as measured while running no test at all",
							planPath, mutation.ID, named)
					}
				}
			}
		}
	}

	// R4: a sweep that silently examined nothing is the failure mode this test
	// exists to prevent, so it must not be able to pass by finding no work.
	if checkedPlans == 0 || checkedEntries == 0 {
		t.Fatalf("swept %d plan(s) and %d entr(ies) -- the sweep found nothing "+
			"to check, which is a broken sweep, not a clean result",
			checkedPlans, checkedEntries)
	}
	t.Logf("swept %d plans, %d entries, %d distinct target files",
		checkedPlans, checkedEntries, len(sourceCache))
}

var goTestFunctionPattern = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]*)\(`)

// goTestNamesInPackage collects every Test function declared in this package's
// _test.go files, read from source rather than from the running binary: the
// point is to catch a plan naming a test that no longer exists, and a deleted
// test is by definition absent from the binary too, so reflection over the
// binary could not tell "deleted" from "not selected".
func goTestNamesInPackage(t *testing.T) map[string]bool {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, file := range files {
		source, readErr := os.ReadFile(file)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, match := range goTestFunctionPattern.FindAllStringSubmatch(string(source), -1) {
			names[match[1]] = true
		}
	}
	if len(names) == 0 {
		t.Fatal("found no Test functions in this package, so every proof-name " +
			"check below would vacuously pass")
	}
	return names
}

// commandTargetsThisPackage reports whether a proof command runs the
// providersync package. Plans in this directory may legitimately prove an entry
// by running a test in another package (a scheduler planner oracle, a worker
// construction test), and those names cannot be resolved from here.
func commandTargetsThisPackage(command []string) bool {
	for _, argument := range command {
		if strings.HasPrefix(argument, "./") || strings.HasPrefix(argument, "github.com/") {
			return strings.Contains(argument, "internal/providersync")
		}
	}
	// No package argument means `go test` runs the current directory, which for
	// these plans is the repository root -- not this package.
	return false
}

// goTestPatternNames extracts the concrete TOP-LEVEL test names a `-run`
// pattern pins, and deliberately returns nothing when it cannot be certain.
//
// Go's -run grammar is a regexp, and being wrong here is expensive in both
// directions: a missed name lets a dead proof through, while a name invented
// from a pattern this function misreads produces a finding about a test that
// was never named. Anything with regexp metacharacters beyond the alternation
// and anchoring handled below is left alone.
//
// Two shapes matter in practice and both are handled:
//
//   - per-alternative anchors, `^TestA$|^TestB$`, not just `^(TestA|TestB)$`;
//   - subtest paths, `^TestParent/subtest$`, where only TestParent is a
//     function and the segment after the slash is a t.Run name.
func goTestPatternNames(command []string) []string {
	for index, argument := range command {
		if argument != "-run" || index+1 >= len(command) {
			continue
		}
		pattern := command[index+1]
		// A nested group means the pattern builds names by combination
		// (`^TestPrefix(One|Two)$`); splitting it textually would fabricate
		// names like "TestPrefix(One". Leave it alone.
		if strings.Count(pattern, "(") != strings.Count(pattern, ")") ||
			strings.Count(pattern, "(") > 1 {
			return nil
		}
		if strings.ContainsAny(pattern, ".*+?[]{}\\") {
			return nil
		}
		var names []string
		for _, alternative := range strings.Split(pattern, "|") {
			alternative = strings.TrimSpace(alternative)
			// Anchoring is checked BEFORE trimming: an unanchored alternative
			// matches by substring and may legitimately select several tests,
			// so it is not a name this function may claim was pinned.
			leftAnchored := strings.HasPrefix(alternative, "^") ||
				strings.HasPrefix(alternative, "(")
			// Right-anchoring is judged on the FIRST path segment: a subtest
			// path anchors each segment separately, so `^TestParent$/sub` pins
			// the parent exactly even though the whole string does not end in
			// `$`.
			head, _, _ := strings.Cut(alternative, "/")
			rightAnchored := strings.HasSuffix(head, "$") || strings.HasSuffix(head, ")")
			if !leftAnchored || !rightAnchored {
				return nil
			}
			// The subtest path is split FIRST. Go anchors each path segment
			// independently -- `^TestParent$/^sub$` and `^TestParent$/sub$` are
			// both ordinary -- so stripping anchors before splitting leaves the
			// parent carrying a trailing `$` and invents a name nobody wrote.
			alternative, _, _ = strings.Cut(alternative, "/")
			alternative = strings.TrimPrefix(alternative, "^(")
			alternative = strings.TrimSuffix(alternative, ")$")
			alternative = strings.TrimPrefix(alternative, "^")
			alternative = strings.TrimSuffix(alternative, "$")
			alternative = strings.Trim(alternative, "()")
			if alternative == "" {
				continue
			}
			if !strings.HasPrefix(alternative, "Test") {
				return nil
			}
			names = append(names, alternative)
		}
		return names
	}
	return nil
}
